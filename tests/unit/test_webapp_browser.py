"""Drives the real page in headless Chromium (Node + Playwright).  Skipped when those are not installed."""
import os
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest
import uvicorn

from webapp.app import create_app

SMOKE = Path(__file__).resolve().parent.parent / "browser" / "smoke.js"


def node_path():
    node = shutil.which("node")
    if not node:
        return None
    npm = shutil.which("npm")
    root = subprocess.run([npm, "root", "-g"], capture_output=True, text=True).stdout.strip() if npm else ""
    probe = subprocess.run([node, "-e", "require('playwright')"], env={**os.environ, "NODE_PATH": root}, capture_output=True)
    return (node, root) if probe.returncode == 0 else None


@pytest.mark.browser
def test_viewer_in_a_real_browser(demo_lake_fo):
    found = node_path()
    if not found:
        pytest.skip("Node + Playwright with Chromium not installed")
    node, root = found
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    server = uvicorn.Server(uvicorn.Config(create_app(demo_lake_fo), host="127.0.0.1", port=port, log_level="warning"))
    t = threading.Thread(target=server.run, daemon=True); t.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.1)
    try:
        r = subprocess.run([node, str(SMOKE), f"http://127.0.0.1:{port}"], env={**os.environ, "NODE_PATH": root},
                           capture_output=True, text=True, timeout=180)
    finally:
        server.should_exit = True; t.join(10)
    assert r.returncode == 0 and r.stdout.strip() == "OK", r.stdout + r.stderr
