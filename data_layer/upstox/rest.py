"""
Thin REST client for the Upstox API, built for measuring and for unattended jobs.

  * every call goes through the shared RateLimiter
  * it never raises on an HTTP or network problem: it returns a `Resp` with status, Upstox error code and message
  * 429 and 5xx are retried with backoff (honouring Retry-After); other errors are returned at once
  * the Authorization header is never logged or stored; error text is truncated
  * per-tag statistics (requests, errors by status and code, latency, bytes) feed the probe report
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from .ratelimit import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://api.upstox.com"
RATE_HEADERS = ("retry-after", "x-ratelimit", "ratelimit")


@dataclass
class Resp:
    path: str
    ok: bool
    status: int                      # HTTP status; 0 for a network error
    data: object = None              # parsed JSON body when there is one
    error_code: str = ""             # Upstox error code, for example UDAPI1149
    error_message: str = ""
    latency_s: float = 0.0
    nbytes: int = 0
    attempts: int = 1
    rate_headers: dict = field(default_factory=dict)


class UpstoxRest:
    def __init__(self, token_provider: Callable[[], str], *, base_url: str = BASE_URL, session=None,
                 limiter: Optional[RateLimiter] = None, timeout: float = 30.0, max_attempts: int = 3,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        if session is None:
            import requests
            session = requests.Session()
        self.session, self.base_url, self.token_provider = session, base_url.rstrip("/"), token_provider
        self.limiter = limiter or RateLimiter()
        self.timeout, self.max_attempts, self.sleep = timeout, max_attempts, sleep
        self.stats: dict[str, dict] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _parse_error(body: object, text: str) -> tuple[str, str]:
        if isinstance(body, dict):
            errs = body.get("errors")
            if isinstance(errs, list) and errs and isinstance(errs[0], dict):
                e = errs[0]
                return str(e.get("errorCode") or e.get("error_code") or ""), str(e.get("message") or "")[:300]
            if body.get("message"):
                return str(body.get("code") or ""), str(body["message"])[:300]
        return "", text[:300]

    def _record(self, tag: str, r: Resp) -> None:
        with self._lock:
            s = self.stats.setdefault(tag, {"requests": 0, "ok": 0, "errors": {}, "latency_s": 0.0, "bytes": 0, "retries": 0,
                                            "rate_headers": {}})
            s["requests"] += 1
            s["ok"] += 1 if r.ok else 0
            s["latency_s"] += r.latency_s
            s["bytes"] += r.nbytes
            s["retries"] += r.attempts - 1
            if not r.ok:
                key = f"{r.status}:{r.error_code}" if r.error_code else str(r.status)
                s["errors"][key] = s["errors"].get(key, 0) + 1
            for k, v in r.rate_headers.items():
                s["rate_headers"][k] = v

    # ------------------------------------------------------------------ the call
    def get(self, path: str, params: Optional[dict] = None, *, tag: Optional[str] = None, auth: bool = True) -> Resp:
        tag = tag or path.split("?")[0]
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        headers = {"Accept": "application/json"}
        if auth:
            headers["Authorization"] = "Bearer " + self.token_provider()
        resp: Optional[Resp] = None
        for attempt in range(1, self.max_attempts + 1):
            self.limiter.acquire()
            t0 = time.monotonic()
            try:
                r = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
            except Exception as exc:                                     # noqa: BLE001 -- network problems are data here
                resp = Resp(path=path, ok=False, status=0, error_message=f"{type(exc).__name__}: {str(exc)[:200]}",
                            latency_s=time.monotonic() - t0, attempts=attempt)
                self.sleep(min(2 ** attempt, 20))
                continue
            latency = time.monotonic() - t0
            text = getattr(r, "text", "") or ""
            try:
                body = r.json()
            except Exception:                                            # noqa: BLE001
                body = None
            hdrs = {k.lower(): v for k, v in dict(getattr(r, "headers", {}) or {}).items()
                    if any(k.lower().startswith(h) for h in RATE_HEADERS)}
            ok = 200 <= r.status_code < 300 and not (isinstance(body, dict) and body.get("status") == "error")
            code, msg = ("", "") if ok else self._parse_error(body, text)
            resp = Resp(path=path, ok=ok, status=r.status_code, data=body, error_code=code, error_message=msg,
                        latency_s=latency, nbytes=len(r.content) if getattr(r, "content", None) is not None else len(text),
                        attempts=attempt, rate_headers=hdrs)
            if ok or (r.status_code != 429 and r.status_code < 500):
                break
            wait = min(float(hdrs.get("retry-after", 0) or 0) or 2 ** attempt, 60)
            log.warning("%s -> HTTP %s (attempt %d/%d), waiting %.0fs", path.split("?")[0], r.status_code, attempt,
                        self.max_attempts, wait)
            self.sleep(wait)
        assert resp is not None
        self._record(tag, resp)
        return resp

    def fetch_raw(self, url: str, tag: str = "raw") -> tuple[int, bytes]:
        """Download a public file (the instrument master).  No token, not rate limited; (status, content)."""
        try:
            r = self.session.get(url, timeout=max(self.timeout, 120.0))
            return r.status_code, r.content
        except Exception as exc:                                         # noqa: BLE001
            log.warning("download %s failed: %s", url, exc)
            return 0, b""

    def summary(self) -> dict:
        total = sum(s["requests"] for s in self.stats.values())
        return {"requests": total, "limiter_requests": self.limiter.requests, "limiter_waited_s": round(self.limiter.waited_s, 1),
                "by_tag": json.loads(json.dumps(self.stats))}
