"""Entry point for the LiveKit voice agent worker.

Usage:
    python run_agent.py dev      # development mode (registers with the LiveKit server)
    python run_agent.py start    # production mode

Equivalent commands:
    ./.venv/bin/python -m livekit.agents start livekit_agent/agent.py --dev
    ./.venv/bin/python -m livekit.agents start livekit_agent/agent.py
"""
import logging
import sys


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if mode not in ("dev", "start"):
        print("usage: python run_agent.py [dev|start]")
        sys.exit(1)

    args = ["start", "livekit_agent/agent.py"]
    if mode == "dev":
        args.append("--dev")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    import livekit.agents.__main__ as agent_cli

    sys.exit(agent_cli.main(args))


if __name__ == "__main__":
    main()
