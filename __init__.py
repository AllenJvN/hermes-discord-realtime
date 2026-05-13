"""Hermes Discord Realtime plugin."""

from __future__ import annotations

from .hermes_discord_realtime.gateway_integration import register_gateway_hooks


def register(ctx) -> None:
    register_gateway_hooks(ctx)
