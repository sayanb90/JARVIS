#!/usr/bin/env bash
# One-shot history for every daily data collector added for the signals (SCRUM-175, 183-187), then the nightly cron lines in
# deploy/crontab keep each one current.  Every step is resumable and skips what is stored, so the script can be stopped and rerun.
# Run in tmux on the VPS; NSE archives only (no Upstox calls), about 1 request a second per step.
#
#   bash deploy/data-backfill.sh                 # dry run: prints the commands
#   bash deploy/data-backfill.sh --apply         # runs them, one after another
#   bash deploy/data-backfill.sh --apply --only participant,eod_fo
#
# Steps and rough size (request count at 1 a second):
#   participant   NSE participant-wise OI and volume, 2012 onwards           ~7,500 requests (~2 h), ~3 MB
#   eod_fo        NSE F&O bhavcopy (every contract, end of day), 2015 onward  ~3,000 requests (~1 h), ~4-5 GB
#   fii           NSE FII derivative statistics, Dec 2014 onwards            ~3,000 requests (~1 h), small
#   nse_files     delivery, ban list, combined OI / MWPL, short selling, bulk and block deals, 2015 onwards (each as far back as the archive goes)
#   weights       index members and weights: today's snapshot (history builds forward from now)
#   rates         RBI rates: today's values; MIBOR history needs a file saved by hand (python -m data_layer.rates import FILE)
#   signals       the signal store over the history (only after `python -m signals validate-iv` passes; see PR notes)
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PY:-.venv/bin/python}"
APPLY=0
ONLY=""
while [ $# -gt 0 ]; do
    case "$1" in
        --apply) APPLY=1 ;;
        --only) ONLY="$2"; shift ;;
        *) echo "unknown argument $1"; exit 2 ;;
    esac
    shift
done
mkdir -p logs
want() { [ -z "$ONLY" ] || [[ ",$ONLY," == *",$1,"* ]]; }
run() {
    echo "+ $*"
    if [ "$APPLY" = 1 ]; then "$@" 2>&1 | tee -a logs/data-backfill.log; fi
}
want participant && run "$PY" -m data_layer.participant_oi backfill --from 2012-01-01
want eod_fo      && run "$PY" -m data_layer.eod_fo backfill --from 2015-01-01
want fii         && run "$PY" -m data_layer.fii_derivatives backfill --from 2014-12-01
want nse_files   && run "$PY" -m data_layer.nse_daily_files backfill --from 2015-01-01
want weights     && run "$PY" -m data_layer.index_weights refresh
want rates       && run "$PY" -m data_layer.rates daily
if want signals && [ -n "$ONLY" ]; then
    for U in NIFTY BANKNIFTY; do
        run "$PY" -m signals run --lake lake --root "${JARVIS_SIGNAL_STORE:-/srv/jarvis-research/store}" --underlying "$U" \
            --from 2022-11-01 --to "$(date -u +%F)"
    done
fi
if [ "$APPLY" = 1 ]; then
    for m in participant_oi eod_fo fii_derivatives nse_daily_files index_weights rates; do
        echo "== $m"; "$PY" -m "data_layer.$m" status || true
    done
else
    echo "dry run: add --apply to run (signals only with --only signals, after the IV gate passes)"
fi
