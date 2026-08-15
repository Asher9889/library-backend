"""Central entry point for the SOUL library backend + LiveKit voice agent.

Starts every long-running service (FastAPI backend, LiveKit agent worker) as
managed child processes so a single command brings the whole stack up.

Usage:
    python run.py                # backend + agent worker (default)
    python run.py backend        # FastAPI backend only
    python run.py agent          # LiveKit agent worker only
    python run.py agent --prod   # agent worker in production mode (no --dev)
    python run.py --host 0.0.0.0 --port 8080   # override backend bind

Signals: Ctrl+C (SIGINT) / SIGTERM are forwarded to every child process.
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable or str(ROOT / ".venv" / "bin" / "python")


def _backend_command(host: str, port: int) -> list[str]:
    return [
        PYTHON,
        "-m",
        "uvicorn",
        "main:app",
        "--host",
        host,
        "--port",
        str(port),
    ]


def _agent_command(prod: bool) -> list[str]:
    args = [PYTHON, "-m", "livekit.agents", "start", "livekit_agent/agent.py"]
    if not prod:
        args.append("--dev")
    return args


def _spawn(name: str, command: list[str]) -> subprocess.Popen:
    print(f"[run] starting {name}: {' '.join(command)}", flush=True)
    return subprocess.Popen(
        command,
        cwd=ROOT,
        start_new_session=True,
        stdin=subprocess.DEVNULL,
    )


def _terminate(procs: dict[str, subprocess.Popen]) -> None:
    for name, proc in procs.items():
        if proc.poll() is None:
            print(f"[run] stopping {name}...", flush=True)
            try:
                os.kill(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    for proc in procs.values():
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    print("[run] all services stopped", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Start SOUL library backend + LiveKit agent.")
    parser.add_argument("services", nargs="*",
                        help="which services to start: backend, agent (default: both)")
    parser.add_argument("--prod", action="store_true", help="run the agent worker in production mode")
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"), help="backend bind host")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "7698")), help="backend bind port")
    args = parser.parse_args()

    services = args.services or ["backend", "agent"]
    for service in services:
        if service not in ("backend", "agent"):
            parser.error(f"unknown service {service!r} (choose from 'backend', 'agent')")

    procs: dict[str, subprocess.Popen] = {}
    if "backend" in services:
        procs["backend"] = _spawn("backend", _backend_command(args.host, args.port))
    if "agent" in services:
        procs["agent"] = _spawn("agent", _agent_command(args.prod))

    if not procs:
        parser.print_help()
        sys.exit(1)

    print(
        f"[run] SOUL library stack up (backend: http://{args.host}:{args.port}, "
        f"agent: {procs['agent'].pid if 'agent' in procs else 'off'})",
        flush=True,
    )

    def _signal_handler(_signum, _frame):
        _terminate(procs)
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        while True:
            time.sleep(1)
            for name, proc in procs.items():
                code = proc.poll()
                if code is not None:
                    print(f"[run] {name} exited unexpectedly (code {code}); stopping the stack", flush=True)
                    _terminate(procs)
                    sys.exit(1)
    except KeyboardInterrupt:
        _terminate(procs)


if __name__ == "__main__":
    main()
