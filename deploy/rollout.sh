#!/usr/bin/env bash
# Roll out the feed fixes of 2026-11-03 in one go, without a Claude session (SCRUM-108, SCRUM-114, SCRUM-115).
#
#   tmux new -s rollout            # so a dropped SSH connection does not stop it
#   cd ~/JARVIS && bash deploy/rollout.sh
#
# What it does, in order (everything is also written to logs/rollout-<day>.log):
#   0. git pull (fast-forward only)               stops if it fails
#   1. the tests that cover these fixes           stops if any fails
#   2. restarts the jarvis-feed and jarvis-viewer user services (the viewer is a long-running process: without a restart it keeps the
#      Python it started with, SCRUM-125)
#   3. re-runs the Greeks calibration on probe_data/stream_fo
#        PASS -> adds JARVIS_GREEKS_RATE, JARVIS_GREEKS_CARRY, JARVIS_GREEKS_IV_SCALE, JARVIS_GREEKS_THETA_BASIS and JARVIS_GREEKS=core to .env
#                (a backup copy is made first, values are never printed) and restarts jarvis-feed again
#        FAIL -> changes nothing
#   4. the scheduler check and the health check
#   5. the read-only diagnostics (logs/diag-<day>.md)
#
# It refuses to run while the market session is on (03:35 to 10:10 UTC, Monday to Friday): restarting the feed then would lose the
# ticks it holds in memory.  Set FORCE=1 to override.  Nothing is deleted, and no data is changed.
set -u
cd "$(dirname "$0")/.." || exit 1
DAY="${1:-$(date -u +%F)}"
TAG="${DAY//-/}"
PY=".venv/bin/python"
mkdir -p logs
LOG="logs/rollout-${TAG}.log"
exec > >(tee -a "$LOG") 2>&1

step() { printf '\n=== %s ===\n' "$*"; }
stop() { printf '\nSTOPPED: %s\n' "$*"; exit 1; }

dow=$(date -u +%u)
hhmm=$((10#$(date -u +%H%M)))
if [ "${FORCE:-0}" != "1" ] && [ "$dow" -le 5 ] && [ "$hhmm" -ge 335 ] && [ "$hhmm" -lt 1010 ]; then
    stop "the market session is on ($(date -u +%H:%M) UTC); restarting the feed now would lose the ticks it holds. Run it after 10:10 UTC, or FORCE=1 to override."
fi
[ -x "$PY" ] || stop "$PY not found: run this from the repository with its virtual environment"
echo "rollout for $DAY, started $(date -u '+%F %H:%M:%S') UTC"

step "0. update the code"
git pull --ff-only || stop "git pull failed"
git log --oneline -1

step "1. tests"
$PY -m pytest -q -p no:cacheprovider tests/unit/test_greeks_calc.py tests/unit/test_envfile.py tests/unit/test_day_diag.py \
    tests/unit/test_feed_flush.py tests/unit/test_tick_compaction.py tests/unit/test_cron_check.py tests/unit/test_ops.py \
    || stop "tests failed: nothing was changed on the server"

step "2. restart jarvis-feed and jarvis-viewer"
systemctl --user restart jarvis-feed
systemctl --user restart jarvis-viewer
sleep 5
echo "jarvis-feed: $(systemctl --user is-active jarvis-feed)"

step "3. Greeks calibration"
CAL="logs/calibration-${TAG}b.txt"
$PY -m data_layer.greeks_calc calibrate --stream probe_data/stream_fo | tee "$CAL"
rc=${PIPESTATUS[0]}
if [ "$rc" -eq 0 ]; then
    pairs=$(grep -o 'JARVIS_GREEKS_[A-Z_]*=[A-Za-z0-9.eE+-]*' "$CAL" | sort -u | tr '\n' ' ')
    if [ "$(echo $pairs | wc -w)" -eq 4 ]; then
        # shellcheck disable=SC2086
        $PY -m data_layer.envfile set $pairs JARVIS_GREEKS=core
        systemctl --user restart jarvis-feed
        systemctl --user restart jarvis-viewer             # it reads the settings and the code at start
        sleep 5
        echo "calibration PASSED: .env now has the four calibration settings and JARVIS_GREEKS=core; jarvis-feed: $(systemctl --user is-active jarvis-feed)"
    else
        echo "calibration passed but the settings line was not found in $CAL: .env left as it is"
    fi
else
    echo "calibration FAILED (exit $rc): .env left as it is (JARVIS_GREEKS stays as configured)"
fi

step "4. scheduler and health checks"
$PY -m data_layer.cron_check | tee "logs/cron-check-${TAG}b.txt"
$PY -m data_layer.health --after-close | tee "logs/health-${TAG}.txt"

step "5. read-only diagnostics"
$PY -m data_layer.day_diag --day "$DAY"

step "done"
echo "finished $(date -u '+%F %H:%M:%S') UTC. Files to send back:"
ls -1 "$LOG" "$CAL" "logs/cron-check-${TAG}b.txt" "logs/health-${TAG}.txt" "logs/diag-${TAG}.md" 2>/dev/null
