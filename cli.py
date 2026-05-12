"""CLI entrypoint for the Hermes Discord Realtime plugin."""

from __future__ import annotations

import argparse


def register_cli(parser: argparse.ArgumentParser) -> None:
    sub = parser.add_subparsers(dest="discord_realtime_cmd")

    run = sub.add_parser("run", help="Run the Discord Realtime voice bridge")
    run.add_argument("--guild-id", type=int, required=True)
    run.add_argument("--voice-channel-id", type=int, required=True)
    run.add_argument("--allowed-user-id", type=int, action="append", required=True)
    run.add_argument("--model", default=None)
    run.add_argument("--voice", default=None)
    run.add_argument("--instructions", default=None)
    run.add_argument("--agent-toolsets", default=None)
    run.add_argument("--agent-timeout", type=float, default=180.0)
    run.add_argument("--vad-rms-threshold", type=int, default=650)
    run.add_argument("--vad-silence-seconds", type=float, default=0.75)
    run.add_argument("--vad-trailing-seconds", type=float, default=0.25)
    run.add_argument("--vad-min-seconds", type=float, default=0.25)
    run.add_argument("--volume", type=float, default=1.0)
    run.add_argument("--log-level", default="INFO")

    sub.add_parser("env", help="Check required environment variables")


def discord_realtime_command(args) -> None:
    cmd = getattr(args, "discord_realtime_cmd", None)
    if cmd == "run":
        from .hermes_discord_realtime.runtime import main

        main(args)
        return
    if cmd == "env":
        from .hermes_discord_realtime.runtime import print_env_status

        print_env_status()
        return
    print("Usage: hermes discord-realtime {run,env}")
