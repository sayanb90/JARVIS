# JARVIS Go-Live Runbook

**Next test: section 14 (VPS test day).** Living document. Update the **Status** column and tick the gates as each item is
proven *on the VPS*, not on a dev box. Last reviewed: 2026-10-25.

Legend: `[x]` done and verified · `[~]` built, **not yet verified in prod** · `[ ]` not started

---

## 0. Start here: from a blank VPS to data flowing

Follow this top to bottom. Every command is meant to be pasted as written; the sections it points to hold the detail and the pass/fail criteria. Nothing in it has been run on a real VPS yet: the commands are standard Ubuntu administration plus this repository's own tools (which are covered by automated tests), so expect to adjust small things and write them down in section 10.

| Step | What | Time | Who |
|---|---|---|---|
| 0.1 | Gather accounts and inputs | 30 min | you |
| 0.2 | Provision and harden the server | 30 min | you |
| 0.3 | Install the code and run the tests | 15 min | you |
| 0.4 | Configure `.env` | 10 min | you |
| 0.5 | Build the stock universe | 15 min | you |
| 0.6 | Smoke checks | 5 min | you |
| 0.7 | **Test day** (section 14) | one session | you |
| 0.8 | Go live: cron, services, backups, viewer | 45 min | you |
| 0.9 | The daily routine (the token needs you every morning unless the Analytics Token is adopted, section 13.2) | 5 min a day | you |
| 0.10 | Weekly and monthly care | | you |

### 0.1 What you need before you start
- **A VPS.** Ubuntu **24.04 LTS** (it ships Python 3.12; 22.04 ships 3.10, which is too old for this code). At least 2 vCPU, 4 GB RAM and a **100 GB SSD** (1-second bars are about 10-15 GB a year typical and up to about 130 GB worst case, section 13.5; leave room for backups if they share the machine, though they should not). A Mumbai-region server is not required (we work in 1-second bars) but keeps the connection to Upstox short.
- **A second place for backups:** another disk or a mounted bucket. A backup on the same disk is not a backup.
- **An Upstox account with an app** from the developer console: its API key, API secret and the redirect URL you set (for example `http://localhost:8000`). Without them no live data can flow (section 13.2).
- **Access to this repository from the server:** a read-only deploy key (below) or a personal access token.
- **The stock list:** the NSE index constituents CSV files for the indices you want (they carry company name, industry, symbol and **ISIN**). The ISIN is what the live feed subscribes with. Optional: an FMP API key (fundamentals; whether the free plan covers Indian stocks is unverified, O9). Optional: a domain name if you want the viewer on your phone over HTTPS.
- **A password manager** for the secrets. `.env` is deliberately not backed up.

### 0.2 Provision and harden (as root, once)
Keep your first SSH session open until the last check passes, so a typo cannot lock you out.
```bash
timedatectl set-timezone UTC                       # every time in this runbook is UTC
apt update && apt -y upgrade
apt -y install python3 python3-venv python3-pip git chrony ufw fail2ban unattended-upgrades cron logrotate
adduser --disabled-password --gecos "" jarvis
install -d -m 700 -o jarvis -g jarvis /home/jarvis/.ssh
cp ~/.ssh/authorized_keys /home/jarvis/.ssh/ && chown jarvis:jarvis /home/jarvis/.ssh/authorized_keys && chmod 600 /home/jarvis/.ssh/authorized_keys
# let jarvis restart only its own services
cat > /etc/sudoers.d/jarvis <<'EOF'
jarvis ALL=(root) NOPASSWD: /usr/bin/systemctl restart jarvis-feed, /usr/bin/systemctl restart jarvis-viewer, /usr/bin/systemctl reset-failed jarvis-feed, /usr/bin/systemctl status jarvis-feed, /usr/bin/systemctl status jarvis-viewer
EOF
chmod 440 /etc/sudoers.d/jarvis
# keys only, no root login
# named 00- on purpose: for sshd the FIRST value wins, and cloud images ship 50-cloud-init.conf with PasswordAuthentication yes
printf 'PasswordAuthentication no\nPermitRootLogin no\n' > /etc/ssh/sshd_config.d/00-jarvis.conf
systemctl reload ssh
sshd -T | grep -E '^(passwordauthentication|permitrootlogin)'     # both must say no
# firewall: SSH only (add 80 and 443 later only if the viewer goes on the internet)
ufw default deny incoming && ufw default allow outgoing && ufw allow OpenSSH && ufw --force enable
systemctl enable --now chrony fail2ban
```
**Check from a second terminal before closing the first:** `ssh jarvis@<server>` works with your key, `timedatectl | grep "Time zone"` says UTC, `chronyc tracking` shows the clock offset is a few milliseconds. The collector rejects ticks stamped more than a minute ahead of the server clock, so a wrong clock silently loses data.

### 0.3 Install the code (as `jarvis`)
```bash
# private repo: create a read-only deploy key, add the .pub under the repo's Settings > Deploy keys, then clone over SSH
ssh-keygen -t ed25519 -N "" -f ~/.ssh/jarvis_deploy && cat ~/.ssh/jarvis_deploy.pub
GIT_SSH_COMMAND="ssh -i ~/.ssh/jarvis_deploy" git clone git@github.com:sayanb90/JARVIS.git ~/JARVIS
cd ~/JARVIS && git config core.sshCommand "ssh -i ~/.ssh/jarvis_deploy"
git checkout claude/us-stock-market-analysis-MZtcm          # until the PR is merged to master
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest -m "not browser"                          # expect: every test passes (about 2 minutes)
```
If any test fails here, stop and send me the output: something about the server differs from the machine this was built on.

### 0.4 Configure `.env` (the one file for secrets and settings)
```bash
cp .env.example .env && chmod 600 .env && nano .env
```
Fill in `UPSTOX_API_KEY`, `UPSTOX_API_SECRET`, `UPSTOX_REDIRECT_URI` (exactly as in the developer console), `JARVIS_BACKUP_DIR` (the second place from 0.1), `WEBAPP_USER` and `WEBAPP_PASSWORD` (choose a long password), and `FMP_API_KEY` if you have one. Leave the `UPSTOX_*TOKEN` lines empty: the tools write them. Never commit this file (it is git-ignored).

### 0.5 Build the stock universe
`data/universe.csv` is the single list every feed reads. It ships with 10 sample stocks per market. For India, import the NSE constituents files:
```bash
# one command per file; they merge, so import Nifty 500 and any other index you want (e.g. a microcap index)
python -m data_layer.universe import-nse ~/ind_nifty500list.csv
python -m data_layer.universe check                       # lists anything the live feed or Yahoo cannot use
```
It reports rows with a missing or malformed ISIN and skips them. US, UK and EU rows are still the 10-name placeholders from section 11 (O11): replace them when you choose the indices. Start the test day with the whole list for daily prices but only `--limit 20` for the live feed, then widen (Upstox's subscription limits are unknown, Gate U1). A handful of NSE symbols are spelled differently on Yahoo; the first backfill's `verify ohlc` lists the ones with no data so you can fix them.

### 0.6 Smoke checks (no network, no data yet)
```bash
python scheduler.py --job all --dry-run                   # shows ticker counts per market, writes nothing
python -m data_layer.health                               # expect FAIL lines for "no daily OHLC": nothing is loaded yet
```

### 0.7 Test day
Do section 14 now, before turning anything on permanently. It proves the 5-year daily backfill for all four markets and one trading day of Upstox data. Use the path that suits your time:
- **During the session (best):** 14.1 to 14.5, including the live feed run.
- **Only after the close:** 14.2 (daily prices), 14.3 (token) and 14.4b (pull the whole day from Upstox's candle API). This proves the token, formats, storage and checks, but not the live WebSocket; run the live part on another day before trusting the daemon.

### 0.8 Go live (after the test day passes)
**As `jarvis`:**
```bash
cd ~/JARVIS && . .venv/bin/activate
# 1. Full daily history for the whole universe (skip if 14.2 already ran on the final universe)
python -m data_layer.yf_client --mode full --market ALL && python -m data_layer.lake --load ohlc
python -m data_layer.verify ohlc --years 5                # no FAIL lines; fix missing symbols in universe.csv and re-run

# 2. Scheduled jobs (daily prices, fundamentals, nightly backup, health reports)
crontab deploy/crontab && crontab -l
```
**As root (or your admin login: the restricted `jarvis` user cannot install system files):**
```bash
# 3. Log rotation
cp /home/jarvis/JARVIS/deploy/logrotate-jarvis /etc/logrotate.d/jarvis && logrotate -d /etc/logrotate.d/jarvis

# 4. The live collector and the viewer as services
cp /home/jarvis/JARVIS/deploy/jarvis-feed.service /home/jarvis/JARVIS/deploy/jarvis-viewer.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now jarvis-feed jarvis-viewer
systemctl status jarvis-feed jarvis-viewer --no-pager
```
**As `jarvis` again:**
```bash
# 5. First backup, then PROVE you can restore it (Gate 0)
python -m data_layer.backup run && python -m data_layer.backup verify
python -m data_layer.backup restore --snapshot $(date +%F) --to /tmp/restore-test && ls /tmp/restore-test && rm -rf /tmp/restore-test
```
Then: copy the backup folder off the machine on a schedule (`rclone sync "$JARVIS_BACKUP_DIR" remote:jarvis-backup`, or your provider's snapshots); and, if you want the viewer on your phone, put Caddy in front of it with the snippet in 16.3 (open ports 80 and 443 in `ufw` first). The feed service waits until 03:40 UTC on the next trading day by itself.

**Do not skip the order.** The feed is the one thing that cannot be redone: every trading day it is not running is 1-second history lost for good. Everything else can be re-fetched.

### 0.9 The daily routine
| Time (IST) | UTC | What happens | What you do |
|---|---|---|---|
| 08:45 | 03:15 | Cron writes the morning health report to `logs/health.log` | Glance: `tail -30 ~/JARVIS/logs/health.log`. Any FAIL or WARN is explained in the line |
| before 09:10 | before 03:40 | **Refresh the Upstox token** (it expires daily) | **Skip this step entirely if `auth check` showed the Analytics Token is accepted and you ran `--adopt`** (section 13.2) -- the collector then keeps using `UPSTOX_FEED_TOKEN` with no daily action. Otherwise: `python -m data_layer.upstox.auth login-url`, log in on your phone, then `python -m data_layer.upstox.auth exchange "<the URL you land on>"`. **No restart needed**: the collector re-reads `.env` and, if it was rejected, retries every minute until the new token works |
| 09:15-15:30 | 03:45-10:00 | The feed service collects and writes bars every 15 seconds | Nothing. To peek: `journalctl -u jarvis-feed -n 20 --no-pager` shows frames, ticks and bars written |
| 15:35 | 10:05 | The feed compacts the day into one file | |
| 17:00 | 11:30 | Cron writes the after-close health report | `python -m data_layer.health --after-close` (or read the log): bars for today, compacted, backup fresh |
| 04:00 and 05:00 (next morning) | 22:30 and 23:30 | Cron loads daily prices for all four markets, then the nightly backup | Nothing |
| any time | | The viewer | Open it, pick the date and a stock. Anything odd goes in section 10 |

If the token expires and you forget: the collector keeps retrying and logs `Upstox rejected the token`; refresh it whenever you notice and it picks up from that moment. The seconds before are lost. If you want a safety net, put a recurring phone reminder at 08:55 IST.

### 0.10 Weekly and monthly care
- **Weekly:** `python -m data_layer.backup verify`; `df -h` and compare with the estimate in 13.5; `python -m data_layer.health --probe` (checks the token against Upstox); skim `logs/scheduler.log` for errors; `git pull` only if you are deliberately updating (then re-run `python -m pytest -m "not browser"` and restart both services).
- **Monthly:** a real restore drill into a scratch directory (step 5 above); review `universe.csv` against index changes (re-import the NSE files); check the fundamentals coverage if you rely on FMP (`data/fund_meta.csv` dates).
- **Not automated yet:** alerting (the health report is a file you read, not a message that finds you), the daily token refresh unless the extended token works, and index reconstitution. Say the word when you want the first of those built.

## 1. Scope and principles

- Scope (ADR-2, amended by ADR-3): **daily OHLC for India, US, UK and EU; everything intraday, live ticks and fundamentals for India (NSE) only.** Universe lives in `data/universe.csv` (single source of truth for every feed); the `market` column is `IN`, `US`, `UK` or `EU`.
- **Ingest ticks and L1 live, store the raw ticks.** The Upstox feed (section 13) delivers ticks and top-of-book; every one is persisted as-is (**SCRUM-77**, superseding ADR-5's 1-second bar aggregation — the first live session showed real sub-second update rates, so bucketing to 1 second was discarding exchange-level detail we cannot re-fetch). Bars of any width (1s, 1m, 5m, ...) are derived from raw ticks on read by `data_layer.query.Lake` (`Lake._ticks_bars_sql`), so nothing downstream (viewer, `verify`, `health`) had to change its interface. The one pre-migration day (2026-10-30) that only has legacy 1-second bars is still readable via a fallback path.
- Free sources first (FMP free tier for fundamentals, Yahoo Finance for prices). Paid upgrades are gated on a concrete limitation, listed in section 9.
- Storage: Parquet lake queried with DuckDB (ADR-1, section 12). Partitioned by year (daily) and by day (bars); no server process, no heap ceiling.
- Prod runs on the VPS. Dev/cloud sandboxes can build and run the test suite; collectors run on the VPS.

## 2. Feed inventory

| Feed | Source | Grain | Cadence | History | Store | Status |
|---|---|---|---|---|---|---|
| Fundamentals (income, balance, cash flow, ROIC/shares) | FMP free (250 calls/day) | Annual | Rotating by staleness, ~62 tickers/day | Last 5 fiscal years | `lake/fundamentals/*.parquet` | `[~]` |
| Daily OHLC, 4 markets | Yahoo (`yfinance`) | 1 day | Every trading day after the last close (22:30 UTC) | 5 years backfill | `lake/ohlc/year=YYYY/` | `[~]` not run live (Yahoo blocked in the build sandbox) |
| Intraday bars, interim, **India only** | Yahoo (`yfinance`) | 5 min | Once, after the NSE close | About 59 days via `--backfill`, then forward (see 2.1) | `lake/bars/interval=5m/date=YYYY-MM-DD/` | `[~]` retire once the Upstox feed is live |
| **Live ticks + L1** | Upstox API **v3** WebSocket (Protobuf) | Raw tick / L1 push, full precision (SCRUM-77) | During market hours | Forward-only | `lake/ticks/`; `lake/bars/interval=1s/` kept only for the one pre-migration day | `[~]` built and tested against a fake server; not run against Upstox (section 13) |
| NSE futures and options intraday | Upstox v3 (NSE_FO) | 1-second bars + open interest | live feed and REST candles | forward collection | `lake/bars`, contract table in `lake/instruments` | `[ ]` built, not yet run live: section 17 |

### 2.1 Constraint that shapes the whole intraday plan

Yahoo serves only a short trailing window of intraday history (about 7 days of 1m, about 60 days of 5m/15m/30m). **There is no deep backfill.** Our intraday history starts the day the collector first runs, so:

- The collector must run daily without gaps. A missed week of 1m data is unrecoverable; a missed week of 5m is recoverable only inside the 60-day window (the job re-fetches the last 5 days every run, so a short outage self-heals).
- Backtests on intraday strategies cannot start until enough history accumulates. **Start the collector first; everything else can wait.**
- If a strategy needs years of intraday history on day one, that is a paid-data decision (section 9), not a code change.

### 2.2 Storage sizing (approximate, uncompressed)

Row is about 56 bytes (timestamp, 2 enumerated syms, 4 floats, long).

| Scenario | Rows/day | Per year |
|---|---|---|
| 250 stocks, 5m (about 77 bars/day) | about 19 k | about 270 MB |
| 250 stocks, 1m | about 96 k | about 1.4 GB |
| 1,000 stocks, 1m | about 385 k | about 5.5 GB |

Fits comfortably (bars are India only, so these are the upper bound). Parquet files are already ZSTD-compressed (typically several times smaller than the figures above). Keep a retention policy for the per-day CSVs (section 7).

## 3. Environments

| | Dev / cloud sandbox | VPS (prod) |
|---|---|---|
| Purpose | Build, run the automated tests | Run the collectors, the feed service, the viewer, backups |
| OS | Anything with Python 3.11+ | Ubuntu 24.04 LTS (Python 3.12) |
| Network egress to Yahoo / FMP / Upstox | Blocked or proxied (cannot test live) | Direct |
| Holds real data | No | Yes |
| Spec | | at least 2 vCPU, 4 GB RAM, 100 GB SSD, UTC clock synced by chrony; backups on a separate disk |

VPS provider and region are still open (O1); section 0.1 lists what to pick.

## 4. Phase plan and go-live gates

### Phase 0 — VPS foundation
Steps 0.1 to 0.6 in section 0 (provision, harden, install, configure, universe, smoke checks).

**Gate 0:** `python -m pytest -m "not browser"` passes on the server; the clock is UTC and synced; `python -m data_layer.universe check` is clean; **a backup has been restored into a scratch directory and read back** (0.8 step 5).

### Phase 1 — Data foundation
Steps 0.7 and 0.8 in section 0: test day first (section 14), then the full daily backfill, cron, services and backups.

**Gate 1 (data is trustworthy):**
- [ ] `verify ohlc` shows no FAIL for all four markets (section 14.2)
- [ ] 1-second bars: `verify bars --interval 1s` shows no FAIL for a full live session, and `verify compare-rest` agrees with Upstox's minute candles (section 14.5)
- [ ] No duplicate `(symbol, ts)` rows after the day is compacted
- [ ] The collector survived a real disconnect or restart without losing the day (look for `reconnecting` in `journalctl -u jarvis-feed`)
- [ ] 5 consecutive trading days where `python -m data_layer.health --after-close` shows no FAIL and nothing was fixed by hand
- [ ] Scoring: `Lake().top_by_score("IN", 10)` returns sensible results against a known-good ticker (needs fundamentals, O9)

### Phase 2 — Backtester webapp on the VPS
`backtester/` and `webapp/` are empty stubs today. Plan:

- [ ] Decide stack (open decision O3). Suggested: Python backend (FastAPI) importing `data_layer.query.Lake` in-process (read-only), thin frontend
- [ ] Data access layer: one module that queries `bars5m` and `ohlc` over IPC and returns frames; backtester never reads CSVs
- [ ] Backtest engine rules baked in from day one: signals use **bar close**, fills at **next bar open**, explicit commission and slippage parameters, no look-ahead into fundamentals (use the date a filing became public, not the fiscal period end)
- [ ] Corporate actions: stored intraday bars are adjusted as of fetch time and are **not re-adjusted** after later splits/dividends. Backtests across a split date must be flagged (see section 8)
- [ ] Strategy registry: strategies as versioned modules; each run stores strategy version, parameters, data range and results
- [ ] Deploy behind a reverse proxy (Caddy or nginx) with TLS and authentication. There is no database port to expose; the app opens the lake files read-only
- [ ] Run the app as a `systemd` service under the `jarvis` user with restart on failure
- [ ] Resource limits: cap concurrent backtests so a heavy run cannot exhaust RAM and kill the collectors

**Gate 2:** a known simple strategy (for example SMA crossover on one ticker) reproduces the same result in the webapp and in an independent calculation, and the run is stored and retrievable.

### Phase 3 — Futures and options (index products)
Built on the Upstox feed we already use (ADR-7, section 17): instrument table, contract selection, open interest on 1-second bars, REST day pull, verification, viewer. **Not yet run against the live service**; the test steps are in 17.5. Section 9 keeps the original planning questions and the vendor list, which is now only needed for deep history.

## 5. Daily operations (all times UTC, trading weekdays)

The routine you follow is in **0.9**. This section is the reference for what runs by itself.

| What | How | When (UTC) | File |
|---|---|---|---|
| Live feed to raw ticks, compaction | systemd service `jarvis-feed` (a daemon, not cron) | waits until 03:40, runs to 10:05 | `deploy/jarvis-feed.service` |
| Daily prices, four markets | cron `scheduler.py --job ohlc-daily` | 22:30 | `deploy/crontab` |
| Fundamentals rotation (India) | cron `scheduler.py --job fundamentals` | 12:00 | `deploy/crontab` |
| Nightly backup snapshot | cron `python -m data_layer.backup run` | 23:30 | `deploy/crontab` |
| Health reports | cron `python -m data_layer.health` | 03:15 and 11:30 | `deploy/crontab` |
| Viewer | systemd service `jarvis-viewer` | always | `deploy/jarvis-viewer.service` |

The Yahoo 5-minute `bars` job still exists in `scheduler.py` as a stopgap cross-check but is **not** in the crontab: the intraday base is the 1-second feed (ADR-5).

Notes:
- NSE regular session is 03:45-10:00 UTC (09:15-15:30 IST), no daylight saving. LSE and Euronext close about 16:30 UTC, NYSE at 20:00 or 21:00 UTC; the 22:30 daily-price run is after all of them.
- Different holiday calendars: a market closed that day simply returns no rows. On an NSE holiday the feed connects, receives nothing, and the health report says so; that is expected.
- The scheduler holds a lock file (`logs/scheduler.lock`). If a run was killed, delete a stale lock before re-running.

### Daily check (2 minutes)
`python -m data_layer.health --after-close` prints one PASS, WARN or FAIL line per condition: disk space, universe and ISIN coverage, freshness of daily prices per market, latest bar day and instrument count, uncompacted days, whether the feed is writing during the session, the systemd service, the last scheduler run, token present, newest backup. Exit code 1 if anything FAILs. Anything marked FAIL or WARN names what to look at.

### Failure handling
| Symptom | Likely cause | Action |
|---|---|---|
| `Upstox rejected the token` in the feed log | Token expired (daily) | Refresh it (0.9); the feed picks it up within a minute, no restart |
| Feed service `failed` or `inactive` | Crash, or systemd gave up after repeated failures | `journalctl -u jarvis-feed -n 50`; fix the cause; `sudo systemctl reset-failed jarvis-feed && sudo systemctl restart jarvis-feed` |
| Health says "live feed is NOT writing" in session | Disconnected, token rejected, NSE holiday, or no subscribable stocks | Read the log; check ISIN coverage in the health report |
| `ahead of local clock` warnings | Server clock wrong, or timestamps not in milliseconds | `chronyc tracking`; if the clock is fine, report it (Gate U1) |
| Daily-price job fails with "Yahoo returned no data" | Yahoo down or blocking | Re-run later; the job fails loudly on purpose so cron shows it |
| Some tickers missing in `verify ohlc` | Delisted or spelled differently on Yahoo | Fix the symbol in `universe.csv`, re-run `yf_client --mode full --market <m>` |
| `scheduler.lock` exists | Previous run still running or killed | Check the PID in the file; delete only if the process is gone |
| Lake load fails | Disk full or bad CSV | `df -h`; inspect the file; reload with `python -m data_layer.lake --load <what>` |
| Health: "the lake cannot be opened" | A damaged Parquet file | The message names it; restore that file from the newest backup snapshot |
| FMP returns 429/403 | Daily budget exceeded, or free plan does not cover India | Wait for reset; lower `FMP_DAILY_BUDGET`; see O9 |
| A gap in 1-second data | Collector was down | Unrecoverable (Upstox keeps no 1-second history); `python -m data_layer.upstox.candles --date <day>` can repair the **1-minute** table only. Record the gap in section 10 |

## 6. Loader commands

```bash
python -m data_layer.lake --load all                        # fundamentals + full-history OHLC
python -m data_layer.lake --load fundamentals               # upsert fundamentals tables
python -m data_layer.lake --load daily                      # upsert daily OHLC increment
python -m data_layer.lake --load bars --interval 5m         # upsert last 7 days of bar files
python -m data_layer.lake --load bars --since 2026-10-01    # rebuild bar partitions from a date
```

Reloads are idempotent: every load is an upsert on the table key, and each bar day-file holds the complete state for that market and day.

## 7. Backup and retention

```bash
python -m data_layer.backup run                     # one dated snapshot in $JARVIS_BACKUP_DIR (cron does this nightly)
python -m data_layer.backup list
python -m data_layer.backup verify [--snapshot 2026-10-27]    # every file present and every Parquet file readable
python -m data_layer.backup restore --snapshot 2026-10-27 --to /tmp/restore-test    # refuses to overwrite unless --force
```
- Snapshots are hard-linked: a file that did not change since the last snapshot costs no extra disk, so a daily snapshot adds only the new day's files. Keeps the newest 7 daily and the newest 4 Sunday snapshots.
- Included: `lake/`, `data/universe.csv`, `data/fund_meta.csv`, `data/fundamentals`, `data/ohlc`, `data/bars`. **Not** included, on purpose: `.env` (secrets: keep them in your password manager) and logs.
- Put the destination on a different disk, and copy it off the machine as well (`rclone sync` or `rsync -aH`, which keeps the hard links). The intraday history is the one dataset we cannot re-download (section 2.1): treat it as irreplaceable.
- **A backup is not real until a restore has been tested** (Gate 0), and again monthly (0.10). `verify` catches a corrupt file; only a restore proves the procedure works.
- To recover after a lost server: provision a new one (0.2 and 0.3), restore the newest snapshot into `~/JARVIS`, re-create `.env` from your password manager, run `python -m data_layer.health`, then start the services.

## 8. Known risks and limits

| Risk | Impact | Mitigation |
|---|---|---|
| Yahoo is an unofficial, unsupported source; its terms limit use and it can change or break without notice | Collector can fail silently or return bad data | Alerting on row counts; plan a paid-feed swap (section 9); keep personal use |
| Intraday retention window (above) | No deep backfill | Collect forward from day one |
| Bar timestamps are UTC; sessions differ by market and DST | Wrong session alignment in strategies | Convert at the edge only; test the DST week |
| Stored bars not re-adjusted for later splits/dividends | Spurious returns across corporate actions | Maintain a corporate-actions table; flag or re-fetch affected tickers |
| Yahoo data may be delayed or revised, especially NSE | Not suitable for live execution decisions | Research and backtest only until a licensed real-time feed exists |
| Single-writer lake | Two loaders writing the same file at once | Scheduler lock file serialises jobs; writes are atomic renames |
| FMP free tier rate limit | Fundamentals refresh slows as universe grows | Rotation by staleness; upgrade tier if the universe passes about 250 |
| Look-ahead bias in fundamentals | Inflated backtest results | Join on filing/announcement date |
| Loader and query code covered by unit tests on synthetic data only | First real fetch may expose provider quirks | Gate 1 steps 4–5 are the first real execution |

## 9. Planning entry: index futures and options intraday data

**Status: the live and after-close pipeline was built on Upstox (section 17). What remains of this planning entry is history: years of past contracts, including expired ones. Vendor names below are candidates to evaluate; nothing here is verified.**

### 9.1 Why this is not a copy of the equity feed
- Instrument count explodes: one index has many expiries times many strikes times calls and puts, and weekly expiries multiply it. Storage and request volume are orders of magnitude above equities
- Contracts are **short-lived and numerous**: you need a contract master (underlying, expiry, strike, type, lot size) and a lifecycle, not a ticker list
- Lot sizes, expiry days and strike steps change over time; the backtester must use the values in force on each date
- Useful analytics need more than OHLC: open interest, implied volatility, greeks, bid/ask. These are often a separate, paid feed
- Free equity sources like Yahoo do **not** cover this well enough for systematic work

### 9.2 Decisions to make (in order)
1. [ ] **Underlyings:** for example Nifty 50 and Bank Nifty on NSE. Pick the first one or two only
2. [ ] **Instruments:** futures only, options only, or both; weekly expiries or monthly only
3. [ ] **Data needed by the first strategy:** bars only, or bars plus OI/IV/greeks and chain snapshots? Define the strategy before buying data
4. [ ] **Granularity:** 1-second bars, same as equities (ADR-5); raw ticks only if a strategy proves it needs them
5. [ ] **History needed vs forward collection:** need years of history now (buy it) or accumulate forward (cheap, slow)?
6. [ ] **Source:** evaluate at least two options per market against the criteria below
7. [ ] **Budget:** set a monthly ceiling before evaluating vendors
8. [ ] **Execution intent:** research-only, or eventual live trading (which requires a licensed real-time feed and broker API)?

### 9.3 Source candidates to evaluate (unverified)
| Market | Candidate type | Examples to look at | What to check |
|---|---|---|---|
| India | Exchange end-of-day files | NSE bhavcopy (free, EOD only) | Good for daily F&O and OI history, not intraday |
| India | Broker APIs | **Upstox (already chosen for equities, section 13)**, Zerodha Kite Connect, Angel One SmartAPI | Intraday history depth, rate limits, cost, whether expired contracts are available |
| India | Data vendors | TrueData, Global Datafeeds | Historical F&O depth, licence for personal use, price |

Evaluation criteria: coverage of **expired** contracts, adjusted vs raw history, bar sizes available, OI/IV included, licence terms for storing and processing, API rate limits, total monthly cost, reliability.

### 9.4 Design sketch (to confirm after decisions)
- `contractMaster` table: `instId, underlying, market, kind (FUT/CE/PE), expiry, strike, lotSize, tickSize, listedDate, expiredDate`
- `fnoBars1m` table in its own Parquet tree `lake_fno/` (volume is too high to share `lake/bars/`), keyed by `instId`, with `oi` and optional `iv`, `bid`, `ask`
- Optional `chainSnapshot` table (end-of-bar chain for an underlying) if strategies need the whole surface
- Separate collector module and cron entry; do not extend the equity collector
- Sizing must be estimated from real contract counts before choosing a source (rows per day equal contracts times bars per session)

### 9.5 Exit criteria for this planning item
Decisions 1–8 recorded, one source chosen with a documented cost, a sizing estimate, and a one-week pilot plan on one underlying. Then open a build task.

## 10. Change and incident log

| Date | Event | Notes |
|---|---|---|
| 2026-10-29 | O10 resolved: the v3 feed accepts Upstox's **Analytics Token** (`UPSTOX_ANALYTICS_TOKEN`), generated by hand once a year at the developer console (no OAuth login), confirmed with `auth check` (HTTP 101, real frame received) and adopted via `auth check --adopt` -> `UPSTOX_FEED_TOKEN`. The daily browser-login refresh is no longer required day to day; re-run `check` if the collector starts rejecting it (401/403) in case the token expires sooner than the documented 1 year, or needs manual renewal before then | `data_layer/upstox/auth.py`; section 13.2, 13.3 |
| 2026-10-25 | Added `data_layer/upstox/auth.py`: login URL, code exchange, `.env` token storage, and a feed probe that tests access vs extended token. Owner must first create the Upstox developer-console app | Waiting on app credentials |
| 2026-10-25 | Upstox v3 feed adapter built (`data_layer/upstox/`): decode, instrument keys from ISIN, reconnecting WebSocket runner, daily scheduling, 8 tests incl. fake server. Protobuf module vendored from the official SDK (ADR-4) | Not yet run against Upstox |
| 2026-10-30 | Viewer redesign (SCRUM-76): sections with sub-tabs, one legend per chart, market heat map, derivatives summary/chain/OI/contract tabs, phone and tablet layouts, demo option chains | Section 16.1 |
| 2026-10-30 | Futures and options support (SCRUM-71): contract table, selection, `feed/candles --segment fo`, open interest on bars, `verify fo`, viewer open interest, demo derivatives; 46 new tests incl. `futures_ingestion` scenarios. Section 17, ADR-7 | Not yet run against Upstox |
| 2026-10-30 | Live feed relaxed to full raw-tick precision (SCRUM-77): the first live session showed genuine sub-second update rates (54% of 1-second bars folded in >1 tick), so 1-second aggregation in the live feed was dropped in favour of storing every tick. `data_layer.query.Lake` now derives bars of any width from ticks on read (`Lake._ticks_bars_sql`); viewer, `verify` and `health` interfaces unchanged. One historical day (2026-10-30 itself) keeps its legacy 1-second bars via a fallback path. Opened SCRUM-78 (widen F&O to 4 weekly expiries) as a follow-up, not yet started | Section 2.2, 13.5, 16 |
| 2026-10-26 | Operations pack: section 0 start-to-finish VPS guide, `deploy/` (systemd units, crontab, logrotate), `data_layer.backup`, `data_layer.health`, `data_layer.universe`; the live feed now re-reads the token and retries instead of exiting when it is rejected | Nothing here has run on a real VPS yet |
| 2026-10-26 | Intraday viewer webapp built (`webapp/`, `analytics/microstructure.py`): date and instrument dropdowns, price, volume, book at best, spread, volatility, market overview, synthetic demo data. Covered by unit tests, Gherkin scenarios and a real-browser test | ADR-6, section 16 |
| 2026-10-25 | Daily OHLC re-extended to India+US+UK+EU (5y, Yahoo); intraday, ticks and fundamentals stay India only; `--backfill` for deepest Yahoo intraday window; `currency` column added to universe (ADR-3). Earlier same day: scope narrowed to NSE only; US removed from universe, fetchers, scheduler, lake and tests. Upstox v2 WebSocket chosen for live tick/L1 (ADR-2); tick model, bar aggregator and segment storage built | Adapter blocked on proto/docs access (section 13) |
| 2026-10-25 | Runbook created; intraday bars collector, `bars5m` schema and loader added | Not yet run on the VPS |
| 2026-10-25 | `yf_client` universe now read from `data/universe.csv` (previously a hardcoded 20-ticker US fallback) | |

## 11. Open decisions for the owner

| # | Question | Blocks |
|---|---|---|
| O1 | VPS provider, OS and size? | Phase 0 |
| O2 | ~~q licence~~ Closed by ADR-1 (no KDB+ dependency) | — |
| O3 | Webapp stack: **decided** for the viewer (FastAPI + Plotly, ADR-6). Still open: who uses it (just you, or others), and the backtester UI | Phase 2 |
| O4 | Universe target: stay at about 250 stocks or move toward 1,000? (drives FMP tier and storage) | Phase 1 |
| O5 | First F&O underlying, and the first strategy that needs it | Phase 3 |
| O6 | Monthly data budget ceiling | Phase 3 |
| O7 | ~~Upstox v2 or v3~~ Closed: v3 (ADR-4) | — |
| O8 | Keep raw ticks/L1 as well as 1-second bars? Decide after the first live session shows the real update rate and disk use (13.5) | Section 13 |
| O9 | Fundamentals source for NSE: does the FMP free tier cover Indian tickers? Unverified, must be tested before relying on it | Phase 1 |
| O12 | F&O: which underlyings and how many strikes and expiries to collect first? Start with NIFTY, BANKNIFTY and 2-3 stocks; widen after measuring disk and tick rate (17.6) | Section 17 |
| O13 | F&O history: use the Plus plan's expired-contract OHLC, a vendor, or forward collection only? | Section 17 |
| O11 | Which UK and EU indices define the universe, and how many names (about 1,200 total was mentioned earlier)? | Phase 1 |
| O10 | ~~Token handling~~ **Resolved 2026-10-29:** the v3 feed accepts the Analytics Token (`auth check` confirmed HTTP 101 + a real frame); adopted via `auth check --adopt`. Watch for it being rejected (401/403) before its documented 1-year expiry and renew by hand at the developer console if so. Stored in `.env` (mode 600) | Section 13 |
| 2026-10-25 | Replaced KDB+ with Parquet + DuckDB (`lake.py`, `query.py`, tests); removed `schema.q`, `kdb_loader.q`, `helpers.q`. Pillar scorer ported to Python with explicit date ordering for the growth/decrease pillars | ADR-1 |

## 12. Architecture decisions

### ADR-1: Parquet lake + DuckDB instead of KDB+ (2026-10-25)
- **Context:** the project had no KDB+ subscription, so only the free 32-bit build was usable. Its 4 GB address-space limit also counts memory-mapped columns, which caps query size, and an in-VPS webapp would need a q process plus an IPC bridge from Python.
- **Decision:** store data as partitioned Parquet (ZSTD) and query it with embedded DuckDB. Collectors still write CSV staging files; `data_layer/lake.py` upserts them, `data_layer/query.py` reads.
- **Consequences:** free, no server, no heap ceiling, SQL as-of joins and window functions available to the backtester, plain Python on the VPS. We give up kdb's native tick-plant and in-memory tables.
- **Hot layer:** with aggregated bars (not ticks) today's data for 1,000 symbols at 5 minutes is a few MB, so the current day stays as a pandas/Polars frame in the collector or webapp process. Revisit if the F&O chain feed (section 9) outgrows one box: ClickHouse is the next step, not a return to KDB+.
- **Reversible:** CSV staging files remain the rebuild source for every table.

### ADR-2: NSE only, Upstox WebSocket for live tick/L1 (2026-10-25)
- **Context:** the product now targets Indian markets only. A live broker feed gives real ticks and top-of-book, which Yahoo cannot (Yahoo is delayed, unofficial and keeps only about 60 days of 5-minute history).
- **Decision:** drop US/UK/EU. Ingest Upstox API market data over WebSocket (Protobuf, binary frames), aggregate to bars in-process (1 second, per ADR-5), persist bars (and optionally raw ticks) as append-only Parquet segments.
- **Consequences:** history starts the day the feed first runs. Upstox's historical-candle API can backfill daily bars; user reports say 1-minute history is limited to a rolling window of a few months (unverified, section 13.3). Fundamentals still need a separate source (Upstox does not provide them).
- **Kept:** Yahoo daily/intraday collectors stay as an interim and cross-check source until the Upstox path is verified.

### ADR-3: Daily OHLC for four markets, intraday for India only (2026-10-25)
- **Decision:** 5 years of daily OHLC from Yahoo for India, US, UK and EU. Intraday bars, live ticks and fundamentals remain India only. Amends ADR-2.
- **Intraday depth:** Yahoo allows about 59 days of 5m and 6 of 1m; 60m bars reach about 729 days. **6 months of 5-minute history cannot come from Yahoo.** Options: take 60m for 2 years from Yahoo, rely on Upstox historical candles (reported as about six months of 1m; unverified, section 13.3), or let the live feed accumulate forward.
- **Not yet handled:** (1) UK `.L` prices are in pence (GBp), not pounds, and EU is quoted in EUR, US in USD, India in INR; no FX table exists, so cross-market return comparisons need one. (2) Yahoo prices are adjusted at fetch time and the daily job refreshes only the last 7 days, so after a split or dividend older rows disagree with new ones. (3) UK and EU universe rows are 10-name placeholders without ISINs; the real index constituents (FTSE 100/250? STOXX 600? Euro Stoxx 50?) are not chosen. (4) Yahoo was unreachable from the build sandbox, so no live fetch has run.

### ADR-4: Upstox API v3 for the live feed (2026-10-25)
- **Decision:** use the v3 market data feed (`wss://api.upstox.com/v3/feed/market-data-feed`), not v2, which community posts report as heading for deprecation. Amends ADR-2.
- **Source of truth for the protocol:** the official `upstox-python-sdk` 2.30.0 (MIT). The Upstox docs site was unreachable from the build sandbox, so endpoint, subscribe message, modes and Protobuf schema were read from the SDK's own source and generated module, which is vendored in `data_layer/upstox/proto/`.

### ADR-5: 1-second base resolution for intraday (2026-10-26)
- **Context:** the product is for building trading strategies, so a 1-minute intraday base is too coarse. An earlier default of 1 minute was never agreed with the owner.
- **Decision:** the intraday base is **1 second**, nothing coarser. Raw ticks are not the default only because of data volume; they remain an opt-in.
- **Consequences:** about 10-15 GB a year typical, up to about 130 GB worst case (13.5). 1-minute and 5-minute bars are derived with `Lake.rollup()`. Upstox's candle API (minimum 1 minute) and Yahoo (minimum 1 minute, short history) can no longer serve as the intraday source, only as cross-checks. Intraday history exists only from the first day the live collector runs, so it must run every session.
- **Open:** how often the Upstox feed actually pushes per instrument (if it is a snapshot about once a second, 1-second bars lose almost nothing versus raw ticks). The first live session settles it.

### ADR-6: FastAPI + Plotly for the intraday viewer (2026-10-26)
- **Decision:** a small FastAPI app reads the Parquet lake read-only through DuckDB and serves a single static page; charts are Plotly, with the JavaScript taken from the pip `plotly` package and served locally (no CDN, so it works offline and on a locked-down VPS). Pure maths lives in `analytics/` so the backtester can reuse it.
- **Why not a heavier stack:** one user, one VPS, data already in Parquet; no build step, no database server, nothing to keep running besides one process. A framework can replace it later without touching the maths or the lake.
- **Consequences:** one day and one instrument per request (a liquid stock at 1-second bars is about 2.4 MB of JSON, about 0.6 MB compressed); no live refresh yet (reload to see new data).

### ADR-7: Derivatives use the equity pipeline with an open-interest column (2026-10-30)
Futures and options come from the same Upstox v3 feed and REST candles as equities, are stored in the same 1-second bar tree with a nullable `oi` column, and are resolved through a history-preserving contract table. A second collector and tree were rejected: the protocol, session, bucketing, compaction and viewer are identical, and the contract table already separates the part that differs. Revisit if the number of contracts makes the shared tree slow to scan (the viewer reads one day and one symbol at a time, so this is unlikely before thousands of contracts).

## 13. Upstox live feed (tick + L1)

**Status: `[~]` Built and unit-tested end to end against a local fake server that speaks the SDK's protocol. NOT yet run against the real Upstox service (needs a valid token on a trading day). That run is Gate U1.**

### 13.1 What exists
- `data_layer/upstox/decode.py`: Protobuf `FeedResponse` to `Tick`. Handles stock/derivative full feeds (LTP, last trade time, cumulative volume `vtt`, first level bid/ask and sizes, OI), index feeds (LTP only), `ltpc` mode and `option_greeks` mode; reads market-status frames.
- `data_layer/upstox/instruments.py`: universe symbol to instrument key, `NSE_EQ|<ISIN>`, from the ISIN column. Symbols without a valid ISIN are reported and skipped. Optional validation against a locally saved instrument master.
- `data_layer/upstox/feed.py`: `FeedProcessor` (frame bytes to bars, drops last-trade-yesterday and future-timestamped ticks) and `UpstoxFeed` (WebSocket with bearer token, binary JSON subscribe in batches of 500, periodic flush and write, watchdog that reconnects after 120 s without frames, exponential backoff, fatal error on HTTP 401/403 with a "token expires daily" message). The daemon waits for each weekday session (03:40-10:05 UTC), then compacts the day. `--once` runs a single session now.
- `data_layer/upstox/candles.py`: REST candle API client for whole-day pulls, gap repair and range backfill (section 14.4b). Fills gaps only unless `--overwrite`; tags bars `src='rest'`.
- `data_layer/ticks.py`, `data_layer/tick_store.py`: tick model and storage. The live feed persists every tick as-is (SCRUM-77); `tick_store.compact_ticks_day` merges a day's segments into `lake/ticks/date=.../ticks.parquet`. `BarAggregator` (same file) is no longer used by the live feed but still backs the REST-candle path's minute bars. `data_layer/query.py`'s `Lake._ticks_bars_sql` derives bars of any width from ticks on read, queryable as `bars_1s` exactly as before; a quiet second still has no row (nothing is forward-filled).
- `Lake.rollup(width_s)` (in `data_layer/query.py`): builds 1-minute, 5-minute or any coarser bars from the 1-second ones, aligned to the 09:15 IST open. It is tested to equal bars built tick by tick at that width.
- Tests: `tests/test_upstox.py`.

### 13.2 Get a token, then run it (VPS)
One-time, by the owner: at the Upstox developer console choose **New App**, set the redirect URL (for example `http://localhost:8000`), and put the API key, API secret and redirect URL in `.env` (`UPSTOX_API_KEY`, `UPSTOX_API_SECRET`, `UPSTOX_REDIRECT_URI`). Nothing below can run without this.
```bash
python -m data_layer.upstox.auth login-url      # open the link on your phone, log in, approve
python -m data_layer.upstox.auth exchange "<the URL you were redirected to>"   # stores tokens in .env (mode 600)
python -m data_layer.upstox.auth check          # does the v3 feed accept the access token? the extended token?
python -m data_layer.upstox.auth check --adopt  # if the extended token works: collector uses it (UPSTOX_FEED_TOKEN)
```
`check` is the O10 experiment: it tests both long-lived candidates, `UPSTOX_ANALYTICS_TOKEN` (preferred -- generate it by hand once at Developer Apps -> your app -> Analytics tab -> Generate Token, paste into `.env` yourself) and `UPSTOX_EXTENDED_TOKEN` (from `exchange`, if the OAuth response carried one). **Confirmed 2026-10-29: the Analytics Token is accepted by the v3 feed**, so no daily refresh is needed as long as it keeps being accepted; otherwise the access token must be redone every trading morning (steps 1-2). Tokens are never printed or logged.
```bash
python -m data_layer.upstox.feed --once --limit 20      # smoke test on a trading day
python -m data_layer.upstox.feed                         # daemon
```
`systemd` unit (`/etc/systemd/system/jarvis-feed.service`):
```ini
[Service]
User=jarvis
WorkingDirectory=/home/jarvis/JARVIS
EnvironmentFile=/home/jarvis/JARVIS/.env
ExecStart=/home/jarvis/JARVIS/.venv/bin/python -m data_layer.upstox.feed
Restart=always
RestartSec=10
```

### 13.3 Gate U1: first real run (still to do)
- [ ] **U0:** developer-console app created, `auth exchange` succeeds, `auth check` shows which token the feed accepts and I record the extended token's real lifetime here: ______
- [ ] Token obtained and the feed accepts it (HTTP 101, not 401)
- [ ] One captured real frame saved as a test fixture; confirm `ltt` is epoch **milliseconds** (the processor warns if timestamps look wrong)
- [ ] `full` mode for the whole universe works; find the real per-connection and per-user subscription limits and set `SUBSCRIBE_BATCH` and the connection count accordingly (not documented in the SDK)
- [ ] Bars for a full session match a second source (open, high, low, close; volume differs slightly because bars are built from tick deltas)
- [ ] First bar 03:45 UTC, last bar 09:59 UTC, no duplicate `(symbol, ts)` after `compact_day`
- [ ] Behaviour on an NSE holiday (connection works, no ticks; the watchdog will reconnect every 2 minutes, harmless but noisy)
- [x] Decide O10: **the Analytics Token** (`UPSTOX_ANALYTICS_TOKEN` -> adopted into `UPSTOX_FEED_TOKEN`), confirmed 2026-10-29 by `auth check`. Still to verify over time: that it keeps being accepted for its full documented 1-year life; re-run `check` if the collector logs a 401/403.

### 13.4 Known limits
- The aggregator builds volume from cumulative-volume deltas; the first tick after connect is a baseline, so volume traded before the collector connected is not attributed to any bar. Start before 03:45 UTC.
- If the feed is down, ticks in the gap are unrecoverable from the live path. Upstox's candle API only goes down to **1 minute** (community reports: about six months of it, unverified), so it can repair or cross-check minute bars (`python -m data_layer.upstox.candles`, section 14.4b) but can **never** recreate 1-second data. Run the collector every session without gaps.
- Index instruments carry no volume.

### 13.5 Storage sizing (measured 2026-10-30, superseding the estimates below; SCRUM-77)

O8 is now resolved: the first live session (854 symbols: 500 equities + 354 subscribed F&O) showed the feed
genuinely pushes sub-second updates (54% of 1-second bars folded in more than one tick, mean 1.84/bar, up to
18 in one symbol-second) — not a once-a-second snapshot. **The live feed now stores raw ticks only** (no more
1-second bar aggregation in `data_layer/upstox/feed.py`); `data_layer/query.py` derives bars of any width from
ticks on read, so the viewer and `verify`/`health` tooling are unaffected. Measured by reconstructing that
day's real tick stream and writing it through the actual ZSTD/Parquet path:

| Dataset | Size/day (current universe) | Per year (250 trading days) |
|---|---|---|
| Raw ticks (now the only live-feed output) | about 90 MB (realistic per-tick bid/ask variability) | about 22 GB |
| 1-second bars (legacy; one historical day only, 2026-10-30, kept for continuity) | about 39 MB | about 10 GB equivalent if still produced |
| 1-minute+ bars from the REST candle API (unaffected, separate pipeline) | a few KB | negligible |

Equities, not F&O, drive this: 99.8% of ticks (3,860,976 of 3,868,926) came from the 500 equities; the 354
F&O contracts contributed only ~7,950. A VPS with 100 GB free disk comfortably covers several years at the
current universe; the lever that matters is the equity universe size, not F&O contract count (see SCRUM-78).

The original pre-launch estimates (unverified, now superseded) were:

| Scenario | Rows per day | Per year (250 days) |
|---|---|---|
| 1-second bars, typical: top 200 stocks active 40% of seconds, the other 1,000 active 5% | about 2.9 million | about 9-15 GB |
| 1-second bars, worst case: every symbol active every second | about 27 million | about 80-130 GB |
| 1-minute bars from the same universe | about 450 thousand | about 1.5-2.5 GB |

## 14. VPS test day: 5-year OHLC (US, UK, EU, India) and one day of Upstox intraday (India)

**Goal:** prove on the VPS, with real data, the two things that have only been tested offline: (A) a 5-year daily OHLC backfill for all four markets, and (B) one full NSE session of Upstox v3 ticks turned into 1-minute bars.
**Tested so far:** everything in this section's code path runs end to end against stubbed Yahoo data and a local fake Upstox server (section 15: unit tests plus Gherkin scenarios). **Not tested:** real Yahoo, real Upstox, the real token flow.
**Scope of the test universe:** `data/universe.csv` has 10 stocks per market (UK and EU rows are placeholders, section 11 O11). That is enough to prove the pipeline; widen the universe after it passes.

### 14.0 Timeline (all times UTC; IST = UTC + 5:30)

| UTC | IST | Step |
|---|---|---|
| Any time before market | any | 14.1 setup, 14.2 OHLC test (needs no token, about 15-30 min) |
| before 03:35 | 09:05 | 14.3 get and check the Upstox token |
| 03:40 | 09:10 | 14.4 start the feed (inside `tmux`) |
| 03:45 to 10:00 | 09:15 to 15:30 | session runs unattended; peek at the log |
| 10:05 | 15:35 | feed stops by itself and compacts the day |
| after about 11:00 | after 16:30 | 14.5 verify; Yahoo's daily bar for the day must exist first |

**Can only test after the close?** Use the after-close path in 14.4b: the token steps work at any hour and the whole day's bars can be pulled at once from Upstox's candle API. That proves the token, the REST format, storage and verification; it does **not** prove the live WebSocket (needs a market-hours run on another day).

First check the NSE holiday calendar for the test date (likely Monday 2026-10-26). On a holiday the connection works but no ticks arrive, so move 14.3-14.5 to the next trading day.

### 14.1 Before you start
Complete steps 0.1 to 0.6 of section 0 (server hardened, code installed and tested, `.env` filled in, universe built). For this test day you only need the Upstox app credentials in `.env`; FMP is **not** needed.

### 14.2 Test A: 5-year daily OHLC, four markets (no token needed)
```bash
python -m data_layer.yf_client --mode full --market ALL     # about 1-3 min; logs/yf_client.log
python -m data_layer.lake --load ohlc
python -m data_layer.verify ohlc --years 5
```
Quick look:
```bash
python - <<'PY'
from data_layer.query import Lake
q = Lake()
print(q.sql("SELECT market, count(*) rows, count(DISTINCT symbol) symbols, min(date) first, max(date) last FROM ohlc GROUP BY 1 ORDER BY 1"))
print(q.ohlc("SHEL.L").tail(3))      # UK closes are in PENCE (about 2,500-3,000), not pounds
PY
ls lake/ohlc                          # year=2021 ... year=2026
```
Idempotency (must not change the row count):
```bash
python -m data_layer.yf_client --mode daily --market ALL && python -m data_layer.lake --load daily
python -m data_layer.lake --load ohlc            # re-run the full load
python -m data_layer.verify ohlc | grep -i "duplicate"
```
**Pass when:**
- [ ] `verify ohlc` prints no `FAIL`; every market shows all its universe symbols with data
- [ ] 5-6 year folders under `lake/ohlc/`; about 1,200-1,270 rows per symbol (US/UK/EU more than India because of holidays)
- [ ] duplicates stay 0 after the re-runs
- [ ] 3 spot checks per market (close and volume for one date) agree with a second source such as the exchange site or a broker screen
- [ ] write down every `WARN` and why; unexplained one-day moves over 40% usually mean unadjusted splits

### 14.3 Test B1: Upstox token (before 03:35 UTC)
```bash
python -m data_layer.upstox.auth login-url        # open on your phone, log in, approve
python -m data_layer.upstox.auth exchange "<the full URL you were redirected to>"
python -m data_layer.upstox.auth check            # tells you which token the v3 feed accepts
```
Record the result of `check` in section 13.3 (Gate U0). If it says the extended token is accepted, run `check --adopt`; otherwise the access token is used and must be redone every trading morning. The exchange step must happen within a couple of minutes of the login (the code is single-use).

### 14.4 Test B2: run the feed for one session (India)
```bash
tmux new -s feed
. .venv/bin/activate
python -m data_layer.upstox.feed --once --limit 20 --dump-frames 5 2>&1 | tee logs/feed-test.log
```
`--dump-frames 5` saves the first 5 raw frames to `logs/frames/*.bin`: bring these back, they become the real test fixtures and settle whether `ltt` is in milliseconds. Detach with `Ctrl+b d`. It subscribes to the universe's India rows (10 today, the `--limit` is an upper bound) in `full` mode and stops itself at 10:05 UTC. `Ctrl+C` at any time is safe: it saves the last bars and compacts the day.

What the log should show:
- `subscribed 10 instruments in mode full` (a `N universe symbols have no usable Upstox key` warning means missing ISINs)
- `market status: {...NORMAL_OPEN...}` around 03:45
- every 30 s `frames=... ticks=... bars_written=...` with `ticks` growing
- at the end `session YYYY-MM-DD done: compacted N bars`

Red flags in the log: `ahead of local clock` (timestamp unit or VPS clock wrong), `no frames for 120s` repeatedly during market hours, `rejected the token` (expired; redo 14.3), repeated `connection lost`.

### 14.4b After-close path: pull the whole day at once (REST)
Use this if you cannot be online during the session. Needs only a token (14.3 works at any hour; at night `auth check` will say "accepted, no frame", which is normal).
```bash
python -m data_layer.upstox.candles --date 2026-10-26 --limit 20 --dump-raw 3     # one API call per instrument
python -m data_layer.verify bars --date 2026-10-26 --interval 1m                   # these REST bars are 1-minute
```
- `--dump-raw 3` saves the first three raw responses to `logs/candles/`: bring them back, they settle the exact response format.
- It only **fills gaps**: bars already collected live are kept (`--overwrite` replaces them). Bars are tagged `src = 'rest'` (live ones are `'live'`); VWAP and tick count are empty for REST bars.
- For the present trading day the tool asks the intraday endpoint first and falls back to the historical one. If both come back empty right after the close, run it again the next morning before the open, or force `--source intraday`.
- `--from 2026-09-01 --to 2026-10-23 --chunk-days 5` repairs or backfills a range (community posts say 1-minute history reaches back about six months; unverified, and per-call range limits are unknown, so the default is one day per call).
- **Pass when:** `verify bars --interval 1m` shows no FAIL, 375 minute bars for liquid names (first 03:45, last 09:59 UTC), last-bar close within 0.5% of Yahoo's daily close.
- **REST is minute resolution only.** It tests the token, the response format, storage and verification, but it cannot test 1-second data. Its second job is an independent check of the live feed on a day you also ran `feed`: `python -m data_layer.verify compare-rest --date 2026-10-26` rolls the live 1-second bars up to minutes and compares them with these REST candles (coverage, close, high, total volume).
- **Not proven by this path:** the WebSocket connection, tick decoding, and tick-to-bar aggregation. Do a live `feed --once` run in a market session before enabling the daemon.

### 14.5 Verify the intraday day (after about 11:00 UTC)
```bash
python -m data_layer.yf_client --mode daily --market IN --days-back 3 && python -m data_layer.lake --load daily   # Yahoo daily bar to cross-check against
python -m data_layer.verify bars --date 2026-10-26 --interval 1s      # use the real date
du -sh lake/bars lake
```
**Pass when:**
- [ ] `verify bars` prints no `FAIL`
- [ ] first bar within 30 seconds of 03:45 UTC, last bar in the final minute before 10:00 UTC
- [ ] liquid names (for example RELIANCE, HDFCBANK, INFY) show thousands of active seconds; the report prints min/median/max active seconds out of 22,500, and symbols silent for more than 5 minutes inside the session are a `WARN` (illiquid or feed gaps), not a failure
- [ ] the `late=` counter in the log stays near zero (a large value means updates arrive out of order)
- [ ] if you also ran 14.4b the same day: `verify compare-rest` shows minutes agreeing (coverage at least 95%, closes within 0.2%, volume ratio 0.9-1.1)
- [ ] last-bar close is within 0.5% of Yahoo's daily close for each symbol
- [ ] median volume ratio (bars / Yahoo daily) is close to 1; far from 1 means a unit or volume-delta problem
- [ ] no `seg-*.parquet` files left in `lake/ticks/date=<day>/` (only `ticks.parquet`, since SCRUM-77)
- [ ] 3 spot checks of a one-minute roll-up (`Lake().rollup(60, 'RELIANCE.NS')`: open, high, low, close) against a charting screen
- [ ] `du -sh lake/bars` after the session: record it here ______ (replaces the estimates in 13.5)

### 14.6 Results to bring back
Paste or attach: the output of both `verify` commands; the first and last 15 lines of `logs/feed-test.log`; the `auth check` output (tokens are masked); `ls lake/ohlc`; `du -sh lake`; the `logs/frames/*.bin` files; the lines of the log containing `WARN` or `ahead of local clock`; and answers to: did the extended token work, how many instruments subscribed without error, any disconnects.

### 14.7 If something goes wrong
| Symptom | Likely cause | Action |
|---|---|---|
| `yf_client` returns 0 rows or "possibly delisted" | Yahoo throttling, network, or wrong ticker | Retry later; test one ticker by hand; fix the symbol in `universe.csv` |
| UK prices look 100x too big | `.L` prices are in pence | Expected; divide by 100 for pounds. See ADR-3 |
| `exchange` fails with HTTP 4xx | Redirect URI differs from the console, or the code expired or was reused | Make the redirect URI identical in `.env` and the console; redo `login-url` |
| `check` shows `rejected HTTP 401` for both tokens | Token expired or app not authorised for market data | Redo 14.3; if it persists, report the exact message |
| Connected, no frames all day | NSE holiday, subscribed keys wrong, or the instrument master uses different keys | Check the key warning in the log; confirm the ISINs in `universe.csv`; try `--mode ltpc` |
| Ticks flow but `verify bars` finds no rows | All ticks dropped as last-trade-yesterday or in the future | Look at the `stale=` and `future=` counters in the 30 s log lines; check the VPS clock |
| Feed killed, `seg-*` files remain | Crash or `kill -9` | `python -c "from datetime import date; from data_layer.tick_store import compact_day; print(compact_day('1s', date(2026,10,26)))"` |
| Volume ratio far from 1 | Cumulative-volume baseline or units | Report it; do not tune by hand |
| Disk fills | Raw ticks enabled | Do not use `--raw-ticks` on the test day |

### 14.8 After a pass
Tick the Gate 1 and Gate U1 boxes in sections 4 and 13.3, record the extended-token result, then: widen the universe (real UK and EU constituents, O11), schedule the daily OHLC cron (section 5), and enable the `jarvis-feed` service (section 13.2). Do not enable the daemon before the intraday checks above pass.

## 17. Futures and options (Upstox NSE_FO)

### 17.1 What exists (built 2026-10-30, jira SCRUM-71)
| Piece | Where | What it does |
|---|---|---|
| Instrument table | `data_layer/upstox/fo_instruments.py`, `lake/instruments/fo.parquet` | Parses Upstox's NSE instrument file into one row per contract: key, symbol, underlying, FUT/CE/PE, expiry, strike, lot size, tick size, weekly flag, `first_seen`, `last_seen`. Contracts that vanish from later files (expired) are **kept**, so old bars stay resolvable |
| Contract symbol | same | Upstox's trading symbol with spaces as underscores, for example `NIFTY_24500_CE_27_OCT_26`, `NIFTY_FUT_27_OCT_26`. Unique per contract, valid after expiry |
| Selection | `fo_instruments select` | Nearest N futures expiries; for the nearest option expiries, K strikes either side of at-the-money (K large enough, or `strikes: all` in the watchlist, pulls the whole chain). Writes `data/fo_universe.csv` |
| Subscription watchlist | `data/fo_watchlist.csv` (tracked in git, unlike the generated `fo_universe.csv`) | One row per underlying with its own futures/option-expiry/strike window; `fo_instruments select --watchlist ...` (default path, so plain `select` with no `--underlying` uses it) runs the selection per row and combines the results. This is the "what do we subscribe to" config for the derivatives pipeline |
| Live feed | `feed --segment fo` | Same WebSocket, same 1-second bars. Bars gain `oi` (open interest at the last tick in the bar) |
| After-close pull | `candles --segment fo` | REST minute candles; the 7th field of each candle is read as open interest |
| Storage | `lake/bars/interval=1s/...` | The same tree and files as equities; `oi` is a nullable column, so old files and equity rows are unaffected |
| Verification | `verify fo --date D [--interval 1m] [--all]` | Bars present, open interest coverage and movement, books, crossed books, bar sanity |
| Viewer | `webapp` | Contracts appear in the instrument dropdown; open interest is a line on the price panel plus cards (open interest, change) and a row in the book card. `python -m webapp.demo --with-derivatives` makes a demo future and option |

### 17.2 Design decisions
- **Same pipeline, one extra column.** Derivatives use the equity tables with an `oi` column; no second collector or tree (the earlier sketch in 9.4 assumed a separate `lake_fno/`; revisit only if volume forces it, see 17.6).
- **Instrument table is history-preserving.** A contract stays in the table after it leaves the exchange file.
- **Selection is explicit and small.** You choose underlyings and window width; nothing subscribes to a whole chain by default. `data/fo_universe.csv` is a working file (git-ignored); selected contracts that have expired are skipped with a warning.
- **Watchlist-driven selection for the live pipeline.** `data/fo_watchlist.csv` is the tracked list of what to subscribe to (one row per underlying, its own futures/option-expiry/strike window, `strikes: all` for the whole chain); cron re-runs `select` from it each morning before the session so the nearest-expiry roll happens automatically (17.4, 17.6). A one-off `--underlying ...` pull still works exactly as before and ignores the watchlist.
- **At-the-money level.** `select` takes `--spot NIFTY=24500`. Without it, it uses the last close of the nearest future if bars exist in the lake, else the middle of the strike ladder (printed as a warning: only a rough proxy).
- **Session.** NSE F&O trades 09:15-15:30 IST like cash equities, so the existing session window and bar bucketing apply unchanged. Commodity or currency derivatives are out of scope.

### 17.3 Unverified assumptions (all from SDK and community material; the Upstox docs site is not reachable from the build sandbox)
1. ~~Instrument file URL ... field names unverified~~ **Confirmed 2026-10-30** against a live `import --download` + `peek --underlying NIFTY`: `segment` NSE_FO, `instrument_type` FUT/CE/PE, `instrument_key` `NSE_FO|<token>`, `trading_symbol`, `underlying_symbol`, `expiry` (epoch ms), `strike_price`, `lot_size`, `tick_size`, `weekly` all present and named exactly as assumed.
2. Futures and options frames carry open interest in `marketFF.oi` (full mode) or `firstLevelWithGreeks.oi` (option_greeks mode); an OI of zero is stored as empty. **Partially confirmed 2026-10-30**: live `oi` populated on 100% of bars for contracts that traded; a zero/empty case was not observed in the short test window.
3. For derivatives, cumulative `vtt` is in the same unit the exchange reports (contracts vs. quantity = contracts x lot size) is still open; not isolated in the 2026-10-30 test.
4. ~~REST candles carry open interest as the 7th element~~ **Confirmed 2026-10-30**: dumped raw candle rows end `..., volume, oi]`, e.g. `[..., 0, 21240]`.
5. ~~Subscription limits per connection and per plan are unknown~~ **Confirmed 2026-10-30**: 518 NSE_FO instruments (NIFTY + BANKNIFTY, nearest expiry, every strike) subscribed on one connection with no rejection; `verify fo` showed clean bars and 100%-median open-interest coverage. Still unknown: the actual ceiling above 518, and whether it differs on a different plan tier.

### 17.4 Commands
```bash
python -m data_layer.upstox.fo_instruments import --download            # or: --file NSE.json.gz (see 17.5 if the download is refused)
python -m data_layer.upstox.fo_instruments peek --file data/NSE.json.gz --underlying NIFTY -n 2
python -m data_layer.upstox.fo_instruments list --underlying NIFTY --type FUT
python -m data_layer.upstox.fo_instruments select --underlying NIFTY BANKNIFTY RELIANCE \
        --futures 2 --option-expiries 1 --strikes 5 --spot NIFTY=24500 BANKNIFTY=52000
python -m data_layer.upstox.fo_instruments select --watchlist data/fo_watchlist.csv   # tracked list; default when --underlying is omitted
python -m data_layer.upstox.feed --once --segment fo --limit 20 --dump-frames 5     # during the session (smoke test)
python -m data_layer.upstox.feed --once --segment fo                                # the real selection, no --limit
python -m data_layer.upstox.candles --segment fo --date 2026-10-30 --limit 10 --dump-raw 3   # after the close
python -m data_layer.verify fo --date 2026-10-30                          # live 1s bars; add --interval 1m for REST bars
```
Use real spot levels from the Upstox app. Prices in the examples are placeholders.

### 17.5 Test day for futures (after section 14 test B passes, token in place)
1. **Update the code:** `git checkout master && git pull`, then `pip install -r requirements.txt` (no new packages).
2. **Instrument file.** Try `fo_instruments import --download`. If it says the download was refused, download `NSE.json.gz` from that URL in a browser and run `import --file <path>`. Expect a line like `imported N contracts ({'CE': .., 'FUT': .., 'PE': ..}), M underlyings`.
3. **Check the field names:** `peek --underlying NIFTY -n 2` and compare with 17.3 item 1. Bring back that output.
4. **Select a small set:** NIFTY and one stock, `--futures 2 --option-expiries 1 --strikes 3`. About 30 contracts is plenty for a first run.
5. **During the session (09:15-15:30 IST):** `feed --once --segment fo --limit 20 --dump-frames 5 2>&1 | tee logs/fo-test.log`. Note the subscription count and any rejection text in the log.
6. **After the close:** `candles --segment fo --date <today> --limit 10 --dump-raw 3`, then `verify fo --date <today>` (live) and `verify fo --date <today> --interval 1m` (REST).
7. **Look at it:** `python -m webapp --lake lake` and open a future: open interest should move through the day.

**Pass when:**
- [x] `import` finds NSE_FO contracts and `peek` shows the field names in 17.3 -- 33,541 contracts, 224 underlyings; fields matched exactly
- [x] `select` writes `data/fo_universe.csv` with futures and options -- 518 contracts (NIFTY + BANKNIFTY, nearest expiry, every strike, via the watchlist)
- [x] the feed subscribes the contracts without error and writes bars (log shows `bars_written` rising) -- all 518 subscribed on one connection, no rejection
- [x] `verify fo` prints no FAIL: open interest present on most bars of liquid contracts and not constant all day -- no FAIL; 354 of 518 contracts traded (164 far strikes never did, expected), open-interest coverage min/median 100%
- [ ] bar volume for a liquid future is plausible against the app -- not checked against the Upstox app itself; `n_ticks`/`oi` looked sane (see below) but this box has no human next to the app to cross-check
- [x] REST candles have a seventh field and `verify fo --interval 1m` passes -- confirmed, no FAIL
- [x] record: instruments per subscribe that worked **518** (no rejection at that count), tick rate of the nearest NIFTY future **~1.3 ticks/s** (60 ticks over a ~48s sample), `du -sh lake` growth **5.9M** (short test window only -- re-measure after a full session)

**2026-10-30 test notes:** SENSEX was explicitly scoped out of this round -- it trades on BSE (segment BFO), and this pipeline only downloads/parses NSE's instrument file (`MASTER_URL`, `SEGMENT = "NSE_FO"` in `fo_instruments.py`). Adding it means a second instrument-file source with its own unverified field names; tracked as a follow-up, not started.

### 17.6 Limits and next steps
- No history: collection is forward only. The plan table shows OHLC for expired contracts as a Plus feature of the developer API; not built, endpoint and depth unknown (open decision O13).
- **BSE/BFO (SENSEX and other BSE-listed derivatives) is not supported at all.** `fo_instruments.py` only downloads and parses NSE's instrument file; there is no BSE instrument source, no BFO segment handling, and no field-name verification for it (the Upstox docs site has been unreachable from every build so far, so even NSE's field names needed a live `peek` to confirm -- 17.3 item 1). Scoped out of the 2026-10-30 round; treat as new work, not a tweak.
- Roll logic **is now semi-automated**: `data/fo_watchlist.csv` (tracked in git) plus `fo_instruments select --watchlist ...` (the default when `--underlying` is omitted) re-select the nearest expiry/full chain on demand, and the crontab entries in `deploy/crontab` run that refresh and then one `feed --once --segment fo` session every weekday morning, so the roll happens without a human re-typing the `select` command. What is still manual: widening the watchlist to new underlyings, and noticing if a refresh silently finds nothing (no alerting wired up yet -- `logs/fo-select.log` and `logs/fo-feed.log` are the only record).
- Not built: implied volatility and greeks computed locally, option-chain snapshots, a chain view in the viewer, instruments for other segments.
- Volume: NIFTY + BANKNIFTY at the nearest expiry with every strike was 518 selected contracts (354 that actually traded in the short test). At 1-second bars that is roughly the equity universe's volume again, now running on a separate daily cron invocation rather than the long-lived equity service -- measure `du -sh lake` growth after a full session before adding more underlyings or expiries.
- The viewer's overview tab and `day_overview` do not yet include open interest.

## 15. Automated tests

Run everything (no network, no credentials, no real data touched): `pip install -r requirements-dev.txt && python -m pytest --cov`. For a quick loop skip the 40-second real-browser test: `python -m pytest -m "not browser"`. Do not pass an extra `-q`: it is already in `pytest.ini` and a second one hides the final pass/fail summary line. CI runs the same on every push (Python 3.11 and 3.13) and fails below 90% coverage; current coverage is about 98%.

### 15.1 Layout
| Path | What it holds |
|---|---|
| `tests/unit/` | One file per module (futures: `test_fo_instruments.py`, `test_open_interest.py`): FMP client, Yahoo client, lake loader, query and scoring, ticks and tick store, scheduler, verify, Upstox decode/auth/feed, command-line entry points |
| `tests/features/*.feature` | Plain-English Gherkin scenarios describing whole flows (the "cucumbers") |
| `tests/bdd/` | Step definitions for the scenarios, plus `fake_upstox.py`, a local WebSocket server that speaks the Upstox v3 protocol |
| `tests/conftest.py`, `tests/helpers.py` | `world` fixture (every path redirected into a temp directory), builders for Yahoo-shaped frames and Upstox Protobuf frames |

### 15.2 Scenarios (what the business flows guarantee)
| Feature | Guarantees |
|---|---|
| `ohlc_history` | 5-year daily history for IN/US/UK/EU; reruns never duplicate; a corrected Yahoo price replaces the stored one; a failed Yahoo batch or unknown symbol is caught by verification |
| `intraday_yahoo` | Interim Yahoo bars: India only, UTC, forming bar excluded, no duplicates, deepest windows 59/6/729 days |
| `live_feed` | A full session becomes 1-second bars (4,500 per stock at one update per 5 s) with bid/ask; several updates in one second make one bar; quiet seconds get no bar; 1-second bars rolled up to a minute match Upstox's minute candles; agrees with Yahoo's daily bar; reconnects without data loss; 401/403 stop with a clear message; stale and future-stamped updates rejected; Ctrl+C saves and compacts |
| `fundamentals_rotation` | Stalest-first within the 250-call budget; a full cycle covers every stock; a failed fetch is retried first and never marked fresh; India only |
| `pillar_scoring` | Each of the 8 pillars costs exactly one point in isolation; strict boundaries; unknown or unpriced stocks score 0 |
| `upstox_day_pull` | A whole session pulled at once after the close passes verification; live bars are kept and only gaps filled; overwrite mode; unknown instrument reported not fatal; expired token stops cleanly; intraday endpoint for today, historical for earlier days |
| `webapp_viewer` | The dropdowns list dates newest first and every instrument with its bar count; choosing a date and instrument returns price, volume, book, spread and volatility series of equal length in IST; coarser widths keep the day's total volume; the overview ranks instruments; days without a book render without NaN; bad input is refused (422/404) before touching data; optional login protects everything; viewing never changes a file |
| `ops` (unit tests) | Backups: snapshot contents, hard links for unchanged files, same-day replace, 7 daily + 4 Sunday retention, verify catches missing, resized and corrupt Parquet files, restore refuses to overwrite. Health report: every PASS/WARN/FAIL condition, a damaged lake reported instead of crashing, silent feed detected in session. Universe importer: BOM, bad ISINs, duplicates, merge and replace. `deploy/` files: crontab fields valid and pointing at real jobs and modules, nothing scheduled that collides with the feed, systemd units run real modules, runbook section 0 names only commands that exist |
| `futures_ingestion` | The instrument file becomes a contract table that keeps expired contracts; selection chooses futures expiries and a strike window around the money and excludes expired contracts; a live futures session is stored under the contract symbol as 1-second bars with open interest and passes `verify fo`; the viewer reports open interest; the REST pull stores open interest; equities and futures share the lake without mixing |
| `token_setup` | Login link, token exchange stored with mode 600 and never printed, extended-token adoption only when the feed accepts it |
| `scheduler_ops` | All four markets loaded; dry run writes nothing; overlapping runs refused; US bars refused; lock released after failure; a total Yahoo outage fails the job |

### 15.2b Webapp tests
`tests/unit/test_microstructure.py` (exact values for spread, imbalance, realised volatility, summaries), `tests/unit/test_webapp.py` (lake discovery, every endpoint, input validation, caching, compression, auth, read-only, demo generator) and `tests/unit/test_webapp_browser.py`, which starts the app and drives it in headless Chromium through `tests/browser/smoke.js`: dropdowns, stacked panels, hover card, width change, overview sorting and click-through, deep links, phone width without sideways scrolling, and no console errors. The browser test is skipped automatically where Node and Playwright are not installed (it is skipped in CI).

### 15.3 What the tests do NOT prove
Real Yahoo, FMP and Upstox behaviour (field formats, limits, outages), real network timing, and the real Upstox token lifetime. Section 14 is the live check for those. The fake Upstox server encodes our reading of the protocol from the official SDK, so a mismatch with the live service would pass here and fail there; the raw frames saved by `--dump-frames` are the first thing to turn into fixtures.

### 15.4 Adding a test
New behaviour gets a unit test next to its module and, if it changes a user-visible flow, a scenario in the matching `.feature` file. Never call the real network from a test: use the `world` fixture and stub `yf_client._batch_download`, `fmp_client.FMPExtractor` or the fake Upstox server.

### 15.5 Defects found while writing these tests (all fixed)
- A complete Yahoo outage made the OHLC and bars jobs report success with zero rows; they now fail so cron alerts.
- `auth.exchange_code` bound `requests.post` at import time, so it could not be stubbed and would have hit the real API from a test.
- The collector ignored a stop signal while the feed was quiet.
- Known and left as is: `ebit` is filled from FMP's `ebitda` (characterised by `test_ebit_is_currently_sourced_from_fmp_ebitda`; no pillar uses it).

## 16. Intraday viewer (webapp)

**Status: `[~]` built and tested (unit, Gherkin, real headless Chromium) on synthetic data; not yet looked at on real collected data.**

### 16.1 What you see
Three sections in the top bar (**Equities**, **Market**, **Derivatives**) and one **trading day** picker. Each section has its own controls, a strip of key figures and **sub-tabs**. Every chart is its own panel with its own legend: click a legend entry to show or hide that series, and the choice is kept while you browse. Time charts in a section zoom together. The URL holds the full selection (section, sub-tab, instrument, contract, widths), so any view can be bookmarked or sent.

| Section | Sub-tabs |
|---|---|
| Equities (instrument, bar width 1 s to 5 min, volatility window) | **Price & volume**: last price with VWAP (best bid and ask one click away), volume per bar with the cumulative line. **Order book**: size at the best bid and ask (smoothed; every-second values one click away), book imbalance, spread in basis points, and a **book at cursor** card for the second under the pointer. **Volatility**: rolling annualised realised volatility |
| Market (show top 10/20/50/all, colour by volatility, return or spread) | **Heat map**: the most volatile instruments as tiles sized by traded value, plus "most volatile" and "biggest moves" lists. **Liquidity**: spread against volatility, bubble size = traded value. **All instruments**: sortable table. Clicking any tile, row or bubble opens that instrument |
| Derivatives (underlying, option expiry) | **Summary**: spot proxy, ATM strike, put/call OI ratio, max pain, call and put OI; futures with basis and OI change; key strikes by OI. **Option chain**: calls and puts by strike with OI bars, ATM highlighted, ATM ± 5/10/20 or all strikes. **Open interest**: OI by strike (most active 15, ATM ± 10 or all strikes, scrollable) and today's OI change by strike. **Contract**: any future or option with price and open interest, volume, spread and the book card |

Signals and other future tools are added as a sub-tab (one button and one panel) in the relevant section. Layouts are checked at phone (390 px), tablet (820 px) and desktop (1440 px) widths with no sideways scrolling. Demo data with futures and option chains: `python -m webapp.demo --with-derivatives`.

### 16.2 Definitions (so the numbers can be checked)
| Measure | Definition |
|---|---|
| spread / spread_bps | ask minus bid; spread divided by mid, times 10,000. Crossed, locked-at-zero or missing books give no value, never a wrong one |
| imbalance | (bid size minus ask size) divided by their sum, in [-1, 1]; +1 means all the size is on the bid |
| realised volatility | square root of (sum of squared log returns between consecutive bar closes, scaled to a 250-day year). Seconds with no bar add nothing (the price did not move). In the chart it is a trailing window, scaled by the time actually elapsed early in the session, and blank for the first minute |
| smoothed lines | trailing time-window mean over the same window as the volatility |

Caveat: at 1-second sampling the bid-ask bounce inflates realised volatility compared with 1-minute estimates. Compare instruments with each other, not with published volatility figures.

### 16.3 Run it
```bash
python -m webapp                               # http://127.0.0.1:8000 on ./lake (or $WEBAPP_LAKE / $JARVIS_LAKE)
python -m webapp.demo && python -m webapp --lake lake_demo    # synthetic data first, with a red "synthetic" banner
```
Access from your phone: keep the app on `127.0.0.1` and put a reverse proxy with TLS in front, and set a login in the app as well:
```
# /etc/caddy/Caddyfile  (Caddy obtains the certificate automatically)
viewer.example.com {
    reverse_proxy 127.0.0.1:8000
}
```
```ini
# /etc/systemd/system/jarvis-viewer.service
[Service]
User=jarvis
WorkingDirectory=/home/jarvis/JARVIS
EnvironmentFile=/home/jarvis/JARVIS/.env          # add WEBAPP_USER and WEBAPP_PASSWORD here
ExecStart=/home/jarvis/JARVIS/.venv/bin/python -m webapp
Restart=always
```
Without a proxy, an SSH tunnel works from a laptop: `ssh -L 8000:127.0.0.1:8000 jarvis@<vps>` then open `http://127.0.0.1:8000`.

### 16.4 Security
Read-only (it never writes to the lake; a test checks file times and sizes). Binds to `127.0.0.1` by default. If `WEBAPP_USER` and `WEBAPP_PASSWORD` are both set, every route including the static files and the chart library requires HTTP Basic auth (constant-time comparison); over plain HTTP the password is visible, so use TLS. Dates and instrument names are validated against strict patterns and passed as query parameters, never built into SQL; widths and windows must come from a fixed list.

### 16.5 Known limits and next steps
- No live refresh during the session (reload). Next: auto-refresh of the last few minutes.
- One instrument and day per request; a multi-day view and an instrument-comparison view are not built.
- Interval is the 1-second base only; days that exist only as REST minute bars (`src='rest'`) are not offered.
- It has only been looked at on synthetic data. On the VPS after the first live session, check each panel against a charting screen (runbook 14.5) and record anything that looks wrong.

