"""Entry point: launch the download manager web app.

    python run.py                # http://127.0.0.1:8000
    python run.py --port 9000
"""
import argparse

import uvicorn


def main():
    p = argparse.ArgumentParser(description="Quant Data download manager")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--reload", action="store_true", help="auto-reload (dev)")
    args = p.parse_args()
    uvicorn.run("app.main:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
