#!/usr/bin/env bash
# Set up safe, read-only research access to the production lake on the VPS (workstream 1 of the research build).
#
#   sudo bash deploy/research/setup.sh                 # DRY RUN: checks the server and prints every command it would run; changes nothing
#   sudo bash deploy/research/setup.sh --apply         # does it
#
# Options (environment): LAKE (default /home/jarvis/JARVIS/lake)   SVC_USER (jarvis)   RES_USER (jarvis-research)
#                        ROOT (/srv/jarvis-research)   CPU_QUOTA (200%)   MEM_HIGH (3G)   MEM_MAX (4G)   TASKS_MAX (512)
#
# What it sets up, and why each part is safe for the collector:
#   1. a separate Unix user (no password, no sudo, no shared home) for research: it cannot write to the lake or read the secrets file;
#   2. /srv/jarvis-research/snapshots (written by the service user, read by the research group) and /work (the research user's own space):
#      research reads only frozen snapshots (hard links to closed files, see research/snapshot.py), never the files the collector writes;
#   3. operating-system limits on the research user's processes (systemd user-slice drop-in): CPU, memory, task count, low disk priority,
#      so a heavy backtest is slowed or stopped before it can starve the collector;
#   4. a Python environment for the research user, from the repository copy WITHOUT .env, the lake, logs or git credentials;
#   5. checks, run as the research user, that it can read a snapshot and can NOT write the lake or read .env.
# It never changes the mode or owner of anything in the lake, the service user's home or .env (a hard link shares its file with the lake,
# so that would change production).  Where a check shows production would need a change, it prints the command and leaves it to the owner.
# It creates no SSH key and opens no network port: add the research user's public key yourself (printed at the end).
set -u
LAKE="${LAKE:-/home/jarvis/JARVIS/lake}"
SVC_USER="${SVC_USER:-jarvis}"
RES_USER="${RES_USER:-jarvis-research}"
ROOT="${ROOT:-/srv/jarvis-research}"
CPU_QUOTA="${CPU_QUOTA:-200%}"
MEM_HIGH="${MEM_HIGH:-3G}"
MEM_MAX="${MEM_MAX:-4G}"
TASKS_MAX="${TASKS_MAX:-512}"
REPO="${REPO:-$(cd "$(dirname "$0")/../.." && pwd)}"
APPLY=0
[ "${1:-}" = "--apply" ] && APPLY=1

say()  { printf '%s\n' "$*"; }
section() { printf '\n=== %s ===\n' "$*"; }
warn() { printf 'WARNING: %s\n' "$*"; WARNINGS=$((WARNINGS + 1)); }
WARNINGS=0
run()  { if [ "$APPLY" = 1 ]; then printf '+ %s\n' "$*"; eval "$@" || { say "FAILED: $*"; exit 1; }; else printf 'would run: %s\n' "$*"; fi; }

if [ "$APPLY" = 1 ] && [ "$(id -u)" != "0" ]; then
    say "STOPPED: --apply needs root (sudo bash deploy/research/setup.sh --apply)"; exit 1
fi
[ "$APPLY" = 1 ] && say "APPLYING changes" || say "DRY RUN: nothing is changed. Run again with --apply to do it."

section "checks on this server (read-only)"
[ -d "$LAKE" ] || { say "STOPPED: no lake at $LAKE (set LAKE=...)"; exit 1; }
id "$SVC_USER" >/dev/null 2>&1 || { say "STOPPED: the service user $SVC_USER does not exist"; exit 1; }
say "lake: $LAKE  (service user $SVC_USER, research user $RES_USER, research root $ROOT)"
LAKE_DEV=$(stat -c %d "$LAKE")
PARENT="$ROOT"; while [ ! -e "$PARENT" ]; do PARENT=$(dirname "$PARENT"); done
ROOT_DEV=$(stat -c %d "$PARENT")
if [ "$LAKE_DEV" != "$ROOT_DEV" ]; then
    warn "$ROOT is on a different filesystem from the lake: hard links are impossible there.  Choose a ROOT on the lake's filesystem (for example a folder next to the lake), or use 'snapshot create --copy', which spends disk space and needs your yes."
else
    say "ok: the research root and the lake are on the same filesystem, so snapshots cost no disk space"
fi
say "free disk at $PARENT: $(df -h --output=avail "$PARENT" | tail -1 | tr -d ' ')"
if [ "$(cat /proc/sys/fs/protected_hardlinks 2>/dev/null)" = "1" ]; then say "ok: protected_hardlinks is on (the service user links only files it owns; it does)"; fi
SAMPLE=$(find "$LAKE" -type f -name '*.parquet' -print -quit 2>/dev/null)
if [ -n "$SAMPLE" ]; then
    MODE=$(stat -c %a "$SAMPLE")
    case "$MODE" in *[4567]) say "ok: lake files are readable by other users (mode $MODE on $(basename "$SAMPLE"))";;
        *) warn "lake files are not readable by other users (mode $MODE): the research user could not read its snapshot links.  The services' umask would have to be 022, which changes how production writes files: owner decision.";; esac
fi
ENVF="$(dirname "$LAKE")/.env"
if [ -e "$ENVF" ]; then
    case "$(stat -c %a "$ENVF")" in *[4567]) warn "$ENVF is readable by other users: run  chmod 600 $ENVF  (not done here: it is a production file)";; *) say "ok: $ENVF is not readable by other users";; esac
fi
HOMEMODE=$(stat -c %a "$(getent passwd "$SVC_USER" | cut -d: -f6)")
case "$HOMEMODE" in *[1357]) warn "the home of $SVC_USER (mode $HOMEMODE) lets other users enter it: run  chmod 750 $(getent passwd "$SVC_USER" | cut -d: -f6)  so the research user cannot reach the logs, tokens or .env (not done here).";; *) say "ok: the home of $SVC_USER is not open to other users";; esac
command -v systemctl >/dev/null || warn "no systemd: the CPU and memory limits (step 3) cannot be applied; use ulimit or cgroups by hand"

section "1. the research user and group"
run "getent group $RES_USER >/dev/null || groupadd $RES_USER"
run "id $RES_USER >/dev/null 2>&1 || useradd --create-home --shell /bin/bash --gid $RES_USER --comment 'JARVIS research (read-only)' $RES_USER"
run "passwd --lock $RES_USER"
run "gpasswd --delete $RES_USER sudo 2>/dev/null; gpasswd --delete $RES_USER $SVC_USER 2>/dev/null; true"
run "chmod 750 /home/$RES_USER"

section "2. the research root"
run "install -d -m 0755 -o root -g root $ROOT"
run "install -d -m 2750 -o $SVC_USER -g $RES_USER $ROOT/snapshots"
run "install -d -m 2700 -o $RES_USER -g $RES_USER $ROOT/work"
run "install -d -m 2700 -o $RES_USER -g $RES_USER $ROOT/work/duckdb-spill"
run "install -d -m 2700 -o $RES_USER -g $RES_USER $ROOT/runs"

section "3. limits on the research user's processes"
UID_R=$(id -u "$RES_USER" 2>/dev/null || echo '<uid of '"$RES_USER"'>')
DROPIN="/etc/systemd/system/user-${UID_R}.slice.d/50-jarvis-research.conf"
run "install -d -m 0755 /etc/systemd/system/user-${UID_R}.slice.d"
run "printf '%s\n' '[Slice]' 'CPUQuota=${CPU_QUOTA}' 'MemoryHigh=${MEM_HIGH}' 'MemoryMax=${MEM_MAX}' 'MemorySwapMax=0' 'TasksMax=${TASKS_MAX}' 'IOWeight=20' 'CPUWeight=20' > $DROPIN"
run "systemctl daemon-reload"
say "the research user's processes get at most ${CPU_QUOTA} of CPU, ${MEM_MAX} of memory (throttled from ${MEM_HIGH}), ${TASKS_MAX} tasks, no swap, low disk and CPU priority"
if command -v quotaon >/dev/null && quotaon -p "$(df --output=target "$ROOT" 2>/dev/null | tail -1)" 2>/dev/null | grep -q 'is on'; then
    say "disk quotas are on for this filesystem: set one for $RES_USER with  setquota -u $RES_USER 40G 50G 0 0 <mountpoint>"
else
    warn "disk quotas are not enabled on this filesystem, so nothing stops the research user filling the disk except DuckDB's own temp-space cap (JARVIS_RESEARCH_TEMP_GB, default 20) and the snapshot job's free-disk guard.  Keep an eye on $ROOT/work; a quota is a filesystem change (owner decision)."
fi

section "4. the research user's Python environment (from the repository copy, without secrets)"
run "install -d -m 0750 -o $RES_USER -g $RES_USER /home/$RES_USER/JARVIS"
run "rsync -a --delete --exclude='.env*' --exclude='lake' --exclude='logs' --exclude='.git' --exclude='.venv' --exclude='data/universe.local.csv' --exclude='data/*.local.*' --exclude='probe_data' --exclude='__pycache__' $REPO/ /home/$RES_USER/JARVIS/"
run "chown -R $RES_USER:$RES_USER /home/$RES_USER/JARVIS"
run "sudo -u $RES_USER bash -c 'cd ~/JARVIS && python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt'"
run "printf '%s\n' 'export JARVIS_RESEARCH_ROOT=$ROOT' 'export JARVIS_RESEARCH_MEMORY_GB=3' 'export JARVIS_RESEARCH_THREADS=2' 'export JARVIS_RESEARCH_TEMP_GB=20' 'cd ~/JARVIS 2>/dev/null' > /home/$RES_USER/.jarvis-research-env"
run "grep -q jarvis-research-env /home/$RES_USER/.profile 2>/dev/null || echo '. ~/.jarvis-research-env' >> /home/$RES_USER/.profile"
run "chown $RES_USER:$RES_USER /home/$RES_USER/.jarvis-research-env /home/$RES_USER/.profile"
say "to update the research copy of the code after a git pull:  sudo bash deploy/research/setup.sh --apply  (idempotent)"

section "5. the first snapshot, then checks as the research user"
run "sudo -u $SVC_USER bash -c 'cd $REPO && nice -n 19 ionice -c3 .venv/bin/python -m research.snapshot create --root $ROOT --lake $LAKE'"
run "sudo -u $RES_USER bash -c 'ls $ROOT/snapshots/latest | head -3'"
if [ "$APPLY" = 1 ]; then
    FAIL=0
    sudo -u "$RES_USER" bash -c "touch '$LAKE/.write-test' 2>/dev/null" && { say "FAIL: the research user can WRITE to the lake"; rm -f "$LAKE/.write-test"; FAIL=1; } || say "ok: the research user cannot write to the lake"
    [ -e "$ENVF" ] && sudo -u "$RES_USER" test -r "$ENVF" && { say "FAIL: the research user can read $ENVF"; FAIL=1; } || say "ok: the research user cannot read the secrets file"
    sudo -u "$RES_USER" test -r "$ROOT/snapshots/latest/manifest.json" && say "ok: the research user can read the latest snapshot" || { say "FAIL: the research user cannot read the snapshot"; FAIL=1; }
    sudo -u "$RES_USER" bash -c "touch '$ROOT/snapshots/latest/.write-test' 2>/dev/null" && { say "FAIL: the research user can write into a snapshot"; FAIL=1; } || say "ok: the research user cannot write into a snapshot"
    [ "$FAIL" = 0 ] || { say "STOPPED: fix the failures above before using this"; exit 1; }
fi

section "left for you"
say "* install the cron entry from deploy/crontab ('45 11 * * *' research.snapshot create), then run: python -m data_layer.cron_check"
say "* give the research user an SSH key (this script creates none):  sudo install -d -m 700 -o $RES_USER -g $RES_USER /home/$RES_USER/.ssh  and put the public key in /home/$RES_USER/.ssh/authorized_keys (mode 600)"
say "* $WARNINGS warning(s) above need a decision"
[ "$APPLY" = 1 ] || say "* this was a dry run: run again with --apply"
exit 0
