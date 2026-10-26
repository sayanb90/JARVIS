"""
Upstox OAuth helper: get a token, store it in .env, and test which token the v3 feed accepts.

One-time setup (developer console -> New App): note the API key (client_id), API secret and
the redirect URL you set, then put them in .env:
  UPSTOX_API_KEY=...   UPSTOX_API_SECRET=...   UPSTOX_REDIRECT_URI=http://localhost:8000

  python -m data_layer.upstox.auth login-url          # open the printed link, log in, approve
  python -m data_layer.upstox.auth exchange "<pasted redirect URL or just the code>"
  python -m data_layer.upstox.auth check [--adopt]    # which token does the v3 feed accept?

`exchange` stores UPSTOX_ACCESS_TOKEN and, if the response carries one, UPSTOX_EXTENDED_TOKEN.
`check` opens the feed with each token; with --adopt it writes UPSTOX_FEED_TOKEN=<extended> when
the extended token is accepted, which the collector prefers over the daily access token.
Endpoints are from the official SDK (upstox-python-sdk 2.30.0); the extended token's lifetime and
whether the feed accepts it are NOT documented there, which is what `check` finds out.
Tokens are never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests
import websockets
from dotenv import load_dotenv

from ..lake import ENV_FILE
from .feed import FEED_URL

BASE = "https://api.upstox.com"
DIALOG = f"{BASE}/v2/login/authorization/dialog"
TOKEN_URL = f"{BASE}/v2/login/authorization/token"
ENV_PATH = ENV_FILE


def mask(tok: str | None) -> str:
    return "(missing)" if not tok else f"{tok[:4]}...{tok[-4:]} ({len(tok)} chars)"


def need(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"{name} is not set in {ENV_PATH}")
    return v


def login_url(client_id: str, redirect_uri: str) -> str:
    return f"{DIALOG}?{urlencode({'client_id': client_id, 'redirect_uri': redirect_uri, 'response_type': 'code'})}"


def extract_code(text: str) -> str:
    """Accept the full redirected URL (http://localhost:8000/?code=XXXX) or the bare code."""
    text = text.strip()
    if "code=" in text:
        q = parse_qs(urlparse(text).query)
        if q.get("code"):
            return q["code"][0]
    return text


def exchange_code(code: str, client_id: str, client_secret: str, redirect_uri: str,
                  post=None) -> dict:
    post = post or requests.post                     # resolved at call time so tests can stub it
    r = post(TOKEN_URL, headers={"Accept": "application/json", "Api-Version": "2.0"},
             data={"code": code, "client_id": client_id, "client_secret": client_secret,
                   "redirect_uri": redirect_uri, "grant_type": "authorization_code"}, timeout=30)
    if r.status_code != 200:
        raise SystemExit(f"token exchange failed: HTTP {r.status_code} {r.text[:200]}")
    return r.json()


def update_env(path: Path, values: dict[str, str]) -> None:
    """Set KEY=value lines, keeping every other line; file mode 600."""
    lines = path.read_text().splitlines() if path.exists() else []
    done: set[str] = set()
    for i, ln in enumerate(lines):
        key = ln.split("=", 1)[0].strip()
        if key in values and not ln.lstrip().startswith("#"):
            lines[i] = f"{key}={values[key]}"
            done.add(key)
    lines += [f"{k}={v}" for k, v in values.items() if k not in done]
    path.write_text("\n".join(lines) + "\n")
    path.chmod(0o600)


async def probe_feed(token: str, url: str = FEED_URL, instrument_key: str | None = None,
                     wait_s: float = 6.0) -> str:
    """'rejected HTTP 401', 'accepted, no frame in 6s', or 'accepted, frame received'."""
    try:
        async with websockets.connect(url, additional_headers={"Authorization": f"Bearer {token}"},
                                      max_size=None) as ws:
            if instrument_key:
                await ws.send(json.dumps({"guid": "auth-check", "method": "sub",
                                          "data": {"instrumentKeys": [instrument_key], "mode": "ltpc"}}).encode())
            try:
                while True:
                    m = await asyncio.wait_for(ws.recv(), timeout=wait_s)
                    if isinstance(m, bytes):
                        return "accepted, frame received"
            except asyncio.TimeoutError:
                return f"accepted, no frame in {wait_s:.0f}s (normal outside market hours)"
    except websockets.exceptions.InvalidStatus as exc:
        return f"rejected HTTP {exc.response.status_code}"
    except (OSError, websockets.exceptions.WebSocketException) as exc:
        return f"connection error: {exc!r}"


def cmd_check(adopt: bool, url: str = FEED_URL) -> int:
    from .instruments import build_key_map
    keys, _ = build_key_map()
    key = next(iter(keys), None)
    results = {}
    for var in ("UPSTOX_ACCESS_TOKEN", "UPSTOX_EXTENDED_TOKEN"):
        tok = os.getenv(var, "").strip()
        results[var] = "not set" if not tok else asyncio.run(probe_feed(tok, url, key))
        print(f"{var:24s} {mask(tok):28s} -> {results[var]}")
    ext = results["UPSTOX_EXTENDED_TOKEN"]
    if ext.startswith("accepted"):
        print("\nThe v3 feed accepts the extended token. Check its lifetime in the developer console "
              "before relying on it.")
        if adopt:
            update_env(ENV_PATH, {"UPSTOX_FEED_TOKEN": os.environ["UPSTOX_EXTENDED_TOKEN"]})
            print(f"UPSTOX_FEED_TOKEN written to {ENV_PATH}; the collector will use it.")
        else:
            print("Re-run with --adopt to make the collector use it (UPSTOX_FEED_TOKEN).")
        return 0
    print("\nThe extended token is not usable for the feed; the access token must be refreshed daily.")
    return 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("login-url")
    ex = sub.add_parser("exchange")
    ex.add_argument("code", help="redirected URL or bare authorization code")
    ck = sub.add_parser("check")
    ck.add_argument("--adopt", action="store_true")
    a = ap.parse_args(argv)
    load_dotenv(ENV_PATH)
    if a.cmd == "login-url":
        print(login_url(need("UPSTOX_API_KEY"), need("UPSTOX_REDIRECT_URI")))
        print("\nOpen it, log in and approve. The browser then lands on your redirect URL "
              "(the page may fail to load, that is fine). Copy that full URL and run:\n"
              '  python -m data_layer.upstox.auth exchange "<that URL>"')
        return 0
    if a.cmd == "exchange":
        tok = exchange_code(extract_code(a.code), need("UPSTOX_API_KEY"), need("UPSTOX_API_SECRET"),
                            need("UPSTOX_REDIRECT_URI"))
        vals = {"UPSTOX_ACCESS_TOKEN": tok.get("access_token", "")}
        if tok.get("extended_token"):
            vals["UPSTOX_EXTENDED_TOKEN"] = tok["extended_token"]
        if not vals["UPSTOX_ACCESS_TOKEN"]:
            raise SystemExit("response had no access_token")
        update_env(ENV_PATH, vals)
        print(f"saved to {ENV_PATH}: " + ", ".join(f"{k}={mask(v)}" for k, v in vals.items()))
        if "UPSTOX_EXTENDED_TOKEN" not in vals:
            print("note: the response carried no extended_token")
        print("next: python -m data_layer.upstox.auth check")
        return 0
    return cmd_check(a.adopt)


if __name__ == "__main__":
    sys.exit(main())
