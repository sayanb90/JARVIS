#!/usr/bin/env bash
# Historical data run: everything the live feeds could not collect, saved into the lake.  Resumable: run it again and it continues.
#
#   tmux new -s history                              # so a dropped SSH connection does not stop it
#   cd ~/JARVIS && bash deploy/historical-run.sh                    # DRY RUN: prints each stage and its commands, does nothing
#   cd ~/JARVIS && bash deploy/historical-run.sh --apply            # runs every stage that is not done yet
#   bash deploy/historical-run.sh status                            # which stages are done, partial or failed
#   bash deploy/historical-run.sh --apply --only spot,expired       # chosen stages only (comma separated)
#   bash deploy/historical-run.sh --apply --redo daily              # run a finished stage again
#
# Stages, in order:
#   universe       the global stock universe (S&P 500, FTSE 100, STOXX 600, Nikkei 225, KOSPI 200, Taiwan 50, STI, CSI 300, Hang Seng),
#                  only if data/universe_global.csv does not exist yet
#   daily          Yahoo daily candles, all markets, DAILY_YEARS years (default 20), then loaded into the lake; re-adjusts every
#                  downloaded symbol on one basis
#   1m             Yahoo 1-minute bars for every market but India, the last 29 days (all Yahoo serves), then loaded into the lake
#   fx             daily FX rates
#   fundamentals   Yahoo annual statements, profile, dividends and splits for every symbol that is due, then the pillar inputs and the
#                  completeness check.  Saves as it goes; stops cleanly at the time limit and continues next run
#   trial-spot     3 stocks and the indices, last 40 days, from Upstox: proves the candle endpoint and the storage before the long run
#   trial-expired  one expired NIFTY expiry from Upstox (needs the token, and Upstox Plus): proves the shapes of the expired-contract API
#   spot           1-minute candles from 2022-01-01 for the 500 stocks, Nifty 50, Nifty Bank, India VIX and the other index keys (Upstox)
#   expired        1-minute candles of expired NIFTY and BANKNIFTY options (5 strikes either side of the money) and futures (Upstox Plus)
#
# Timing: start after 10:30 UTC (the feeds compact at 10:05) and the script stops long stages at STOP_BY (default 21:30 UTC), before the
# 22:00 UTC cron jobs.  A long stage that is cut off is marked "partial": run the script again the next day.  It refuses to start inside
# 21:30-10:30 UTC unless FORCE=1.  Upstox REST calls use BUDGET calls per 30 minutes (default 1500 of the 2000 allowed), leaving room for the
# 12:50 UTC fundamentals job that shares the quota.
#
# Environment: PY (default .venv/bin/python)  DAILY_YEARS (20)  STOP_BY (21:30)  BUDGET (1500)  FROM_UPSTOX (2022-01-01)  TO_UPSTOX (2026-11-02)  MIN_FREE_GB (40)  FORCE
# Exit code: 0 everything done, 75 something partial (run again), 1 something failed.  Nothing is deleted.  Logs: logs/historical-run-<day>.log.
set -u
cd "$(dirname "$0")/.." || exit 1
PY="${PY:-.venv/bin/python}"
DAILY_YEARS="${DAILY_YEARS:-20}"
STOP_BY="${STOP_BY:-21:30}"
BUDGET="${BUDGET:-1500}"
FROM_UPSTOX="${FROM_UPSTOX:-2022-01-01}"
TO_UPSTOX="${TO_UPSTOX:-2026-11-02}"           # the live feed has recorded India since 2026-11-03: REST candles must not replace those bars
MIN_FREE_GB="${MIN_FREE_GB:-40}"
STATE="logs/historical-run.state"
ALL_STAGES="universe daily 1m fx fundamentals trial-spot trial-expired spot expired"
APPLY=0; ONLY=""; REDO=""; CMD="run"
while [ $# -gt 0 ]; do
    case "$1" in
        --apply) APPLY=1;;
        --only) ONLY="${2:-}"; shift;;
        --redo) REDO="${2:-}"; shift;;
        status) CMD="status";;
        *) echo "unknown argument: $1"; exit 1;;
    esac
    shift
done
mkdir -p logs
touch "$STATE"

state_of() { awk -F'\t' -v s="$1" '$1==s {v=$2} END {print v}' "$STATE"; }
set_state() { printf '%s\t%s\t%s\n' "$1" "$2" "$(date -u +%F)" >> "$STATE"; }

if [ "$CMD" = "status" ]; then
    printf '%-14s %-8s %s\n' stage status last-update
    for s in $ALL_STAGES; do
        last=$(awk -F'\t' -v s="$s" '$1==s {d=$3} END {print d}' "$STATE")
        printf '%-14s %-8s %s\n' "$s" "$(state_of "$s" | sed 's/^$/-/')" "${last:--}"
    done
    [ -x "$PY" ] && "$PY" -m data_layer.upstox.backfill status 2>/dev/null
    exit 0
fi

if [ "$APPLY" = 1 ]; then
    LOG="logs/historical-run-$(date -u +%Y%m%d).log"
    exec > >(tee -a "$LOG") 2>&1
    echo "historical run, started $(date -u '+%F %H:%M:%S') UTC (log: $LOG)"
else
    echo "DRY RUN: nothing is run. Add --apply to run the stages that are not done yet."
fi

minutes_until() {                                  # whole minutes from now until the next HH:MM UTC
    local now t
    now=$(date -u +%s); t=$(date -u -d "today $1" +%s)
    [ "$t" -le "$now" ] && t=$(date -u -d "tomorrow $1" +%s)
    echo $(( (t - now) / 60 ))
}

if [ "$APPLY" = 1 ]; then
    [ -x "$PY" ] || { echo "STOPPED: $PY not found: run this from the repository with its virtual environment (or set PY)"; exit 1; }
    hm=$((10#$(date -u +%H%M)))
    if [ "${FORCE:-0}" != "1" ] && { [ "$hm" -ge 2130 ] || [ "$hm" -lt 1030 ]; }; then
        echo "STOPPED: it is $(date -u +%H:%M) UTC. Start after 10:30 UTC (the feeds compact at 10:05) so the run ends before the 22:00 UTC jobs; FORCE=1 overrides."
        exit 1
    fi
    free=$(df -BG --output=avail . | tail -1 | tr -dc 0-9)
    [ "${free:-0}" -ge "$MIN_FREE_GB" ] || { echo "STOPPED: ${free}G free on this disk, less than MIN_FREE_GB=${MIN_FREE_GB}"; exit 1; }
    echo "free disk: ${free}G; long stages stop at $STOP_BY UTC ($(minutes_until "$STOP_BY") minutes from now)"
fi

run() {                                            # run a command (or print it on a dry run); returns its exit code
    printf '+ %s\n' "$*"
    if [ "$APPLY" = 1 ]; then "$@"; else return 0; fi
}
LIMIT_MIN=$(minutes_until "$STOP_BY")

selected() {                                       # is this stage chosen by --only (or is no --only given)?
    [ -z "$ONLY" ] && return 0
    case ",$ONLY," in *",$1,"*) return 0;; *) return 1;; esac
}

# stage functions: each returns 0 (done), 75 (partial: run again), anything else (failed)
st_universe() {
    if [ -s data/universe_global.csv ]; then echo "data/universe_global.csv exists: kept"; return 0; fi
    run "$PY" -m data_layer.index_universe fetch
    run "$PY" -m data_layer.index_universe build --validate || return 1
    [ "$APPLY" = 0 ] || [ -s data/universe_global.csv ] || { echo "the universe file was not built"; return 1; }
}
st_daily() {
    run "$PY" -m data_layer.yf_client --mode full --market ALL --years "$DAILY_YEARS" || return 1
    run "$PY" -m data_layer.lake --load ohlc
}
st_1m() {
    run "$PY" scheduler.py --job bars --market ALL --interval 1m --backfill
}
st_fx() {
    run "$PY" -m data_layer.fx refresh
}
st_fundamentals() {
    run timeout -s INT "${LIMIT_MIN}m" "$PY" -m data_layer.yf_fundamentals run
    local rc=$?
    if [ "$rc" = 124 ] || [ "$rc" = 130 ] || [ "$rc" = 75 ]; then echo "stopped at the time limit; the symbols fetched so far are saved"; return 75; fi
    [ "$rc" = 0 ] || return "$rc"
    run "$PY" -m data_layer.yf_pillars build || return 1
    run "$PY" -m data_layer.yf_fundamentals check || echo "(the completeness check reports markets below its threshold: see the lines above)"
    return 0
}
st_trial_spot() {
    run "$PY" -m data_layer.upstox.backfill spot --limit 3 --from "$(date -u -d '40 days ago' +%F)" --to "$TO_UPSTOX" --budget-30min "$BUDGET" --dump-raw 2
}
st_trial_expired() {
    run "$PY" -m data_layer.upstox.backfill expired --underlying NIFTY --limit 1 --budget-30min "$BUDGET" --dump-raw 5
}
st_spot() {
    [ "$APPLY" = 0 ] || [ "$(state_of trial-spot)" = ok ] || { echo "trial-spot is not done: run it first"; return 1; }
    run "$PY" -m data_layer.upstox.backfill spot --from "$FROM_UPSTOX" --to "$TO_UPSTOX" --budget-30min "$BUDGET" --max-minutes "$LIMIT_MIN"
}
st_expired() {
    [ "$APPLY" = 0 ] || [ "$(state_of trial-expired)" = ok ] || { echo "trial-expired is not done (it needs the Upstox token and Upstox Plus): skipping"; return 1; }
    run "$PY" -m data_layer.upstox.backfill expired --from "$FROM_UPSTOX" --budget-30min "$BUDGET" --max-minutes "$LIMIT_MIN"
}

PARTIAL=0; FAILED=0
for s in $ALL_STAGES; do
    selected "$s" || continue
    if [ "$REDO" = "$s" ]; then echo "(redo $s)"; elif [ "$(state_of "$s")" = ok ]; then echo "--- $s: done on an earlier run, skipped"; continue; fi
    echo; echo "=== stage $s ==="
    fn="st_${s//-/_}"
    "$fn"; rc=$?
    if [ "$APPLY" = 0 ]; then continue; fi
    case "$rc" in
        0)  set_state "$s" ok;      echo "--- $s: done";;
        75) set_state "$s" partial; echo "--- $s: PARTIAL, run the script again to continue"; PARTIAL=1;;
        *)  set_state "$s" failed;  echo "--- $s: FAILED (exit $rc), see the lines above"; FAILED=1;;
    esac
done

echo
if [ "$APPLY" = 1 ]; then
    bash "$0" status
    if [ "$FAILED" = 1 ]; then echo "SOME STAGES FAILED"; exit 1; fi
    if [ "$PARTIAL" = 1 ]; then echo "NOT FINISHED: run this command again tomorrow after 10:30 UTC; it continues where it stopped"; exit 75; fi
    echo "ALL SELECTED STAGES DONE"
fi
exit 0
