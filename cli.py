"""CLI entrypoint for the Hermes Discord Realtime plugin."""

from __future__ import annotations

import argparse


def register_cli(parser: argparse.ArgumentParser) -> None:
    sub = parser.add_subparsers(dest="discord_realtime_cmd")

    run = sub.add_parser("run", help="Run the Discord Realtime voice bridge")
    run.add_argument("--guild-id", type=int, default=None)
    run.add_argument("--voice-channel-id", type=int, default=None)
    run.add_argument("--allowed-user-id", type=int, action="append", default=None)
    run.add_argument("--discord-bot-token", default=None)
    run.add_argument("--openai-api-key", default=None)
    run.add_argument("--mode", choices=("turn", "duplex"), default="duplex")
    run.add_argument("--model", default=None)
    run.add_argument("--voice", default=None)
    run.add_argument("--instructions", default=None)
    run.add_argument("--agent-toolsets", default=None)
    run.add_argument("--agent-timeout", type=float, default=None)
    run.add_argument("--vad-rms-threshold", type=int, default=None)
    run.add_argument("--vad-silence-seconds", type=float, default=None)
    run.add_argument("--vad-trailing-seconds", type=float, default=None)
    run.add_argument("--vad-min-seconds", type=float, default=None)
    run.add_argument("--volume", type=float, default=None)
    run.add_argument("--log-level", default=None)

    sub.add_parser("env", help="Check required environment variables")
    sub.add_parser("doctor", help="Validate keys, Discord permissions, and runtime dependencies")
    sub.add_parser("invite-url", help="Print a Discord invite URL for the voice bot")
    sub.add_parser("setup", help="Print the recommended setup flow and env template")
    sub.add_parser("install-service", help="Install the realtime voice sidecar as a user service")
    sub.add_parser("uninstall-service", help="Remove the realtime voice sidecar user service")
    sub.add_parser("start", help="Start the realtime voice sidecar service")
    sub.add_parser("stop", help="Stop the realtime voice sidecar service")
    sub.add_parser("restart", help="Restart the realtime voice sidecar service")
    sub.add_parser("status", help="Show realtime voice sidecar service status")
    logs = sub.add_parser("logs", help="Tail realtime voice sidecar service logs")
    logs.add_argument("-n", "--lines", type=int, default=120)
    logs.add_argument("-f", "--follow", action="store_true")


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
    if cmd == "doctor":
        from .hermes_discord_realtime.runtime import doctor

        doctor()
        return
    if cmd == "invite-url":
        from .hermes_discord_realtime.runtime import print_invite_url

        print_invite_url()
        return
    if cmd == "setup":
        from .hermes_discord_realtime.runtime import print_setup

        print_setup()
        return
    if cmd == "install-service":
        from .hermes_discord_realtime.runtime import install_service

        install_service()
        return
    if cmd == "uninstall-service":
        from .hermes_discord_realtime.runtime import uninstall_service

        uninstall_service()
        return
    if cmd in {"start", "stop", "restart", "status"}:
        from .hermes_discord_realtime.runtime import service_command

        service_command(cmd)
        return
    if cmd == "logs":
        from .hermes_discord_realtime.runtime import service_logs

        service_logs(lines=args.lines, follow=args.follow)
        return
    print(
        "Usage: hermes discord-realtime "
        "{run,env,doctor,setup,invite-url,install-service,start,stop,restart,status,logs}"
    )
