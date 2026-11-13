#!/usr/bin/env bash
# Release the options-signal work (PR #42: SCRUM-146, 148, 173-195) on the VPS in one go, without a Claude session.
#
#   tmux new -s release
#   cd ~/JARVIS && bash deploy/release.sh            # after the PR is merged to master
#
# Steps, in order (everything is also written to logs/release-<day>.log):
#   0. git pull (fast-forward only)                          stops if it fails
#   1. pip install -r requirements.txt (adds xlrd, pypdf)    stops if it fails
#   2. the full test suite                                   stops if any test fails: nothing on the server has changed yet
#   3. .env: JARVIS_DEPTH5 (default fo: the 5-level book on the F&O feed) and JARVIS_EXPIRED_STRIKES (default 10).  A backup is made
#      first.  DEPTH5=off leaves the depth switch out; STRIKES=15 widens further.
#   4. crontab: the current one is saved to logs/crontab-backup-<time>.txt, then deploy/crontab is installed.  It adds the second F&O
#      feed connection, the daily NSE/RBI collectors, and the signals health check (11:40 UTC) and nightly run (12:15 UTC).
#   5. restarts jarvis-feed and jarvis-viewer (new feed and book code; the F&O feed itself starts from cron each morning)
#   6. the scheduler check, the signal input health check on the last session, and the signal list
#
# It refuses to run while the market is open (03:35 to 10:10 UTC, Monday to Friday).  FORCE=1 overrides.  Nothing is deleted.
# The one-off history comes after this:  bash deploy/data-backfill.sh --apply   (see that script).
set -u
cd "$(dirname "$0")/.." || exit 1
DAY="$(date -u +%F)"
TAG="${DAY//-/}"
PY="${PY:-.venv/bin/python}"
DEPTH5="${DEPTH5:-fo}"
STRIKES="${STRIKES:-10}"
mkdir -p logs
LOG="logs/release-${TAG}.log"
exec > >(tee -a "$LOG") 2>&1

step() { printf '\n=== %s ===\n' "$*"; }
stop() { printf '\nSTOPPED: %s\n' "$*"; exit 1; }

dow=$(date -u +%u)
hhmm=$((10#$(date -u +%H%M)))
if [ "${FORCE:-0}" != "1" ] && [ "$dow" -le 5 ] && [ "$hhmm" -ge 335 ] && [ "$hhmm" -lt 1010 ]; then
    stop "the market is open ($(date -u +%H:%M) UTC); restarting the feed now would lose ticks. Run after 10:10 UTC, or FORCE=1."
fi
[ -x "$PY" ] || stop "$PY not found: run this from the repository with its virtual environment"
echo "release started $(date -u '+%F %H:%M:%S') UTC"

step "0. update the code"
git pull --ff-only || stop "git pull failed"
git log --oneline -1

step "1. dependencies"
$PY -m pip install -q -r requirements.txt || stop "pip install failed"

step "2. tests"
$PY -m pytest -q -p no:cacheprovider || stop "tests failed: nothing was changed on the server"

step "3. settings in .env"
pairs="JARVIS_EXPIRED_STRIKES=${STRIKES}"
[ "$DEPTH5" != "off" ] && pairs="$pairs JARVIS_DEPTH5=${DEPTH5}"
# shellcheck disable=SC2086
$PY -m data_layer.envfile set $pairs || stop "could not update .env"

step "4. crontab"
crontab -l > "logs/crontab-backup-$(date -u +%Y%m%d-%H%M%S).txt" 2>/dev/null || true
crontab deploy/crontab || stop "crontab install failed (the previous one is saved in logs/)"
crontab -l | grep -c -E '^[0-9*]' | xargs -I{} echo "{} cron lines installed"

step "5. restart jarvis-feed and jarvis-viewer"
systemctl --user restart jarvis-feed
systemctl --user restart jarvis-viewer
sleep 5
echo "jarvis-feed: $(systemctl --user is-active jarvis-feed)   jarvis-viewer: $(systemctl --user is-active jarvis-viewer)"

step "6. checks"
$PY -m data_layer.cron_check | tee "logs/cron-check-${TAG}-release.txt"
$PY -m signals health | tee "logs/signals-health-${TAG}-release.txt"
$PY -m signals list | tail -n +1 | wc -l | xargs -I{} echo "{} signal outputs registered"

step "done"
echo "finished $(date -u '+%F %H:%M:%S') UTC.  Next, in tmux:  bash deploy/data-backfill.sh --apply"
echo "The F&O feed starts with two connections and the 5-level book at the next session (cron, 03:40 UTC)."
echo "After that session:  $PY -m data_layer.tick_store size \$(date -u +%F) --segment fo"
