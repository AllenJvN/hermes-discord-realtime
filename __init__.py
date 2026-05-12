"""Hermes Discord Realtime plugin."""

from __future__ import annotations

from .cli import discord_realtime_command, register_cli


def register(ctx) -> None:
    ctx.register_cli_command(
        name="discord-realtime",
        help="Discord OpenAI Realtime voice bridge",
        setup_fn=register_cli,
        handler_fn=discord_realtime_command,
        description=(
            "Run a duplex Discord voice bridge backed by OpenAI Realtime. "
            "Tool calls are delegated to the normal Hermes agent via one compact "
            "ask_hermes_agent function."
        ),
    )
