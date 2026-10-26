import argparse
import os

import uvicorn


def main() -> None:
    ap = argparse.ArgumentParser(description="JARVIS intraday viewer")
    ap.add_argument("--lake", help="lake directory (default: $WEBAPP_LAKE, $JARVIS_LAKE or ./lake)")
    ap.add_argument("--host", default="127.0.0.1", help="bind address; keep 127.0.0.1 behind a reverse proxy")
    ap.add_argument("--port", type=int, default=8000)
    a = ap.parse_args()
    if a.lake:
        os.environ["WEBAPP_LAKE"] = a.lake
    uvicorn.run("webapp.app:app", host=a.host, port=a.port, log_level="info")


if __name__ == "__main__":
    main()
