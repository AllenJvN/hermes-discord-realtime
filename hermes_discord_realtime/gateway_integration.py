"""One-bot gateway integration for Hermes Discord Realtime.

This module intentionally does not create a Discord client. It runs inside the
normal Hermes gateway process and reuses the existing Discord adapter/client so
DMs, home-channel text, slash commands, cron, and voice share one bot session.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import threading
import time
import uuid
from typing import Any, Optional

import discord

from gateway.config import Platform

from .runtime import (
    DEFAULT_INSTRUCTIONS,
    LiveVoiceReceiver,
    RealtimeDuplexSession,
    StreamingPCMAudioSource,
    _downsample_48k_stereo_to_24k_mono,
    _parse_user_ids,
    _pcm_rms,
)

log = logging.getLogger("hermes.discord_realtime.gateway")

_SESSIONS: dict[int, "GatewayRealtimeSession"] = {}


def register_gateway_hooks(ctx) -> None:
    """Register gateway command interception hooks."""
    ctx.register_hook("pre_gateway_dispatch", _pre_gateway_dispatch)
    ctx.register_command(
        name="realtime",
        handler=lambda raw_args="": "Use `/realtime join`, `/realtime leave`, or `/realtime status` in Discord.",
        description="Control OpenAI Realtime duplex voice in Discord",
        args_hint="<join|leave|status|doctor>",
    )
    ctx.register_command(
        name="rt",
        handler=lambda raw_args="": "Use `/rt join`, `/rt leave`, or `/rt status` in Discord.",
        description="Alias for /realtime",
        args_hint="<join|leave|status|doctor>",
    )


def _pre_gateway_dispatch(event, gateway, session_store=None):
    text = (getattr(event, "text", "") or "").strip()
    lowered = text.lower()
    if not (
        lowered.startswith("/realtime")
        or lowered.startswith("/rt")
        or lowered.startswith("hermes realtime")
    ):
        return None

    source = getattr(event, "source", None)
    if not source or getattr(source, "platform", None) != Platform.DISCORD:
        return None

    try:
        if not gateway._is_user_authorized(source):
            return {"action": "allow"}
    except Exception:
        return {"action": "allow"}

    try:
        asyncio.get_running_loop().create_task(_handle_realtime_command(event, gateway))
    except RuntimeError:
        log.warning("Could not schedule realtime command; no running event loop")
        return {"action": "allow"}

    return {"action": "skip", "reason": "discord realtime command handled by plugin"}


async def _handle_realtime_command(event, gateway) -> None:
    adapter = gateway.adapters.get(Platform.DISCORD)
    if not adapter or not getattr(adapter, "_client", None):
        await _reply(adapter, event, "Discord adapter is not connected.")
        return

    command, args = _parse_command_text(getattr(event, "text", "") or "")
    action = (args[0] if args else "status").lower()

    try:
        if action in {"join", "start", "on", "channel"}:
            message = await _join(event, adapter)
        elif action in {"leave", "stop", "off", "disconnect"}:
            message = await _leave(event, adapter)
        elif action in {"status", "state"}:
            message = _status(event, adapter)
        elif action in {"doctor", "check"}:
            message = await _doctor(event, adapter)
        elif action in {"help", ""}:
            message = _help()
        else:
            message = f"Unknown realtime action `{action}`.\n\n{_help()}"
    except Exception as exc:
        log.warning("Realtime command failed: %s", exc, exc_info=True)
        message = f"Realtime voice command failed: {exc}"

    await _reply(adapter, event, message)


def _parse_command_text(text: str) -> tuple[str, list[str]]:
    parts = text.strip().split()
    if not parts:
        return "", []
    if parts[0].lower() == "hermes" and len(parts) > 1 and parts[1].lower() == "realtime":
        return "realtime", parts[2:]
    command = parts[0].lstrip("/").lower()
    return command, parts[1:]


async def _reply(adapter, event, message: str) -> None:
    if not adapter:
        return
    source = event.source
    metadata = {}
    if getattr(source, "thread_id", None):
        metadata["thread_id"] = source.thread_id
    try:
        await adapter.send(
            source.chat_id,
            message,
            reply_to=getattr(source, "message_id", None),
            metadata=metadata or None,
        )
    except Exception:
        log.warning("Could not send realtime command reply", exc_info=True)


async def _join(event, adapter) -> str:
    guild_id = _get_guild_id(event)
    if not guild_id:
        return "Realtime voice only works in a Discord server."

    user_id = str(event.source.user_id)
    voice_channel = await adapter.get_user_voice_channel(guild_id, user_id)
    if not voice_channel:
        return "Join a Discord voice channel first, then run `/realtime join`."

    existing = _SESSIONS.get(guild_id)
    if existing and existing.is_connected:
        if existing.channel_id == voice_channel.id:
            return f"Realtime voice is already running in **{voice_channel.name}**. Use `/realtime leave` to stop."
        await existing.close()

    # If the built-in Hermes voice mode is already connected, clear it so the
    # realtime receiver is the only socket listener on this guild.
    try:
        if hasattr(adapter, "is_in_voice_channel") and adapter.is_in_voice_channel(guild_id):
            await adapter.leave_voice_channel(guild_id)
    except Exception:
        log.debug("Could not clear existing Hermes voice channel before realtime join", exc_info=True)

    session = GatewayRealtimeSession(
        adapter=adapter,
        guild_id=guild_id,
        text_channel_id=int(event.source.chat_id),
        voice_channel=voice_channel,
        allowed_user_ids=_allowed_user_ids(adapter),
        args=_gateway_realtime_args(),
    )
    _SESSIONS[guild_id] = session
    await session.start()
    return (
        f"Joined **{voice_channel.name}** with OpenAI Realtime duplex.\n"
        "You can interrupt while I speak. Use `/realtime leave` to disconnect."
    )


async def _leave(event, adapter) -> str:
    guild_id = _get_guild_id(event)
    if not guild_id:
        return "Realtime voice is not connected here."
    session = _SESSIONS.pop(guild_id, None)
    if not session:
        return "Realtime voice is not connected here."
    await session.close()
    return "Left realtime voice."


def _status(event, adapter) -> str:
    guild_id = _get_guild_id(event)
    if not guild_id:
        return "Realtime voice status is only available in a Discord server."
    session = _SESSIONS.get(guild_id)
    if not session or not session.is_connected:
        return "Realtime voice is not connected. Use `/realtime join` while you are in a voice channel."
    return (
        f"Realtime voice is connected to **{session.channel_name}**.\n"
        f"Allowed users: {', '.join(sorted(session.allowed_user_ids))}\n"
        f"Model: `{session.args.model}` Voice: `{session.args.voice}`"
    )


async def _doctor(event, adapter) -> str:
    guild_id = _get_guild_id(event)
    if not guild_id:
        return "Run `/realtime doctor` inside a Discord server."
    user_id = str(event.source.user_id)
    voice_channel = await adapter.get_user_voice_channel(guild_id, user_id)
    lines: list[str] = []
    if os.getenv("OPENAI_REALTIME_API_KEY"):
        lines.append("[ok] OPENAI_REALTIME_API_KEY is set")
    else:
        lines.append("[fail] OPENAI_REALTIME_API_KEY is not set")
    if voice_channel:
        lines.append(f"[ok] caller is in voice channel {voice_channel.name}")
        me = voice_channel.guild.me
        perms = voice_channel.permissions_for(me)
        for name in ("view_channel", "connect", "speak", "use_voice_activation"):
            lines.append(f"[{'ok' if getattr(perms, name) else 'fail'}] {name}={getattr(perms, name)}")
    else:
        lines.append("[fail] caller is not in a voice channel")
    lines.append(f"[ok] gateway owns Discord client as {adapter._client.user}")
    return "\n".join(lines)


def _help() -> str:
    return (
        "Realtime voice commands:\n"
        "`/realtime join` - join your current voice channel\n"
        "`/realtime leave` - disconnect cleanly\n"
        "`/realtime status` - show current state\n"
        "`/realtime doctor` - check key/permission basics"
    )


def _get_guild_id(event) -> Optional[int]:
    if getattr(event.source, "guild_id", None):
        return int(event.source.guild_id)
    raw = getattr(event, "raw_message", None)
    if raw is not None:
        if getattr(raw, "guild_id", None):
            return int(raw.guild_id)
        guild = getattr(raw, "guild", None)
        if guild:
            return int(guild.id)
    return None


def _allowed_user_ids(adapter) -> set[str]:
    try:
        ids = set(str(x) for x in _parse_user_ids(None))
        if ids:
            return ids
    except SystemExit:
        pass
    return set(str(x) for x in getattr(adapter, "_allowed_user_ids", set()) if str(x).isdigit())


def _gateway_realtime_args() -> argparse.Namespace:
    api_key = os.getenv("OPENAI_REALTIME_API_KEY") or ""
    if not api_key:
        raise RuntimeError("OPENAI_REALTIME_API_KEY is not set")
    return argparse.Namespace(
        openai_api_key=api_key,
        model=os.getenv("OPENAI_REALTIME_MODEL", "gpt-realtime"),
        voice=os.getenv("OPENAI_REALTIME_VOICE", "alloy"),
        instructions=os.getenv("OPENAI_REALTIME_INSTRUCTIONS", DEFAULT_INSTRUCTIONS),
        agent_toolsets=os.getenv("HERMES_REALTIME_AGENT_TOOLSETS", "all"),
        agent_timeout=float(os.getenv("HERMES_REALTIME_AGENT_TIMEOUT", "180")),
        vad_rms_threshold=int(os.getenv("OPENAI_REALTIME_VAD_RMS_THRESHOLD", "650")),
        vad_silence_seconds=float(os.getenv("OPENAI_REALTIME_VAD_SILENCE_SECONDS", "0.75")),
        vad_trailing_seconds=float(os.getenv("OPENAI_REALTIME_VAD_TRAILING_SECONDS", "0.25")),
        vad_min_seconds=float(os.getenv("OPENAI_REALTIME_VAD_MIN_SECONDS", "0.25")),
        volume=float(os.getenv("OPENAI_REALTIME_VOLUME", "1.0")),
    )


class GatewayRealtimeSession:
    def __init__(
        self,
        *,
        adapter,
        guild_id: int,
        text_channel_id: int,
        voice_channel,
        allowed_user_ids: set[str],
        args: argparse.Namespace,
    ) -> None:
        self.adapter = adapter
        self.guild_id = guild_id
        self.text_channel_id = text_channel_id
        self.voice_channel = voice_channel
        self.channel_id = voice_channel.id
        self.channel_name = voice_channel.name
        self.allowed_user_ids = allowed_user_ids
        self.args = args
        self.session_id = uuid.uuid4().hex[:10]
        self.voice_client: Optional[discord.VoiceClient] = None
        self.output_source: Optional[StreamingPCMAudioSource] = None
        self.receiver: Optional[LiveVoiceReceiver] = None
        self.duplex: Optional[RealtimeDuplexSession] = None
        self.keepalive_task: Optional[asyncio.Task] = None
        self.vad_task: Optional[asyncio.Task] = None
        self.stop_event = asyncio.Event()
        self._vad_lock = threading.Lock()
        self._speech_active = False
        self._speech_started_at = 0.0
        self._last_voice_at = 0.0
        self._frames_sent = 0

    @property
    def is_connected(self) -> bool:
        return bool(self.voice_client and self.voice_client.is_connected())

    async def start(self) -> None:
        log.info("Gateway realtime joining guild=%s channel=%s", self.guild_id, self.channel_name)
        self.voice_client = await self.voice_channel.connect()
        self.adapter._voice_clients[self.guild_id] = self.voice_client
        self.output_source = StreamingPCMAudioSource(log)
        self.duplex = RealtimeDuplexSession(
            api_key=self.args.openai_api_key,
            model=self.args.model,
            voice=self.args.voice,
            instructions=self.args.instructions,
            output_source=self.output_source,
            agent_toolsets=self.args.agent_toolsets,
            agent_timeout=self.args.agent_timeout,
            logger=log,
        )
        await asyncio.to_thread(self.duplex.connect)
        self.voice_client.play(self.output_source)
        self.receiver = LiveVoiceReceiver(
            self.voice_client,
            allowed_user_ids=self.allowed_user_ids,
            frame_callback=self._on_live_pcm,
            logger=log,
        )
        self.receiver.start()
        self.keepalive_task = asyncio.create_task(self._udp_keepalive_loop(), name=f"rt-keepalive-{self.guild_id}")
        self.vad_task = asyncio.create_task(self._duplex_vad_loop(), name=f"rt-vad-{self.guild_id}")
        log.info("Gateway realtime ready: guild=%s channel=%s", self.guild_id, self.channel_name)

    async def close(self) -> None:
        self.stop_event.set()
        for task in (self.keepalive_task, self.vad_task):
            if task:
                task.cancel()
        if self.receiver:
            self.receiver.stop()
            self.receiver = None
        if self.duplex:
            await asyncio.to_thread(self.duplex.close)
            self.duplex = None
        if self.output_source:
            self.output_source.stop()
            self.output_source = None
        if self.voice_client:
            try:
                if self.voice_client.is_playing():
                    self.voice_client.stop()
                if self.voice_client.is_connected():
                    await self.voice_client.disconnect(force=True)
            finally:
                if self.adapter._voice_clients.get(self.guild_id) is self.voice_client:
                    self.adapter._voice_clients.pop(self.guild_id, None)
                self.voice_client = None
        _SESSIONS.pop(self.guild_id, None)

    async def _udp_keepalive_loop(self) -> None:
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(15)
                if self.voice_client and self.voice_client.is_connected():
                    try:
                        self.voice_client._connection.send_packet(b"\xf8\xff\xfe")
                    except Exception:
                        log.debug("Discord UDP keepalive failed", exc_info=True)
        except asyncio.CancelledError:
            pass

    def _on_live_pcm(self, user_id: int, pcm_48k_stereo: bytes) -> None:
        if str(user_id) not in self.allowed_user_ids or not self.duplex:
            return

        now = time.monotonic()
        rms = _pcm_rms(pcm_48k_stereo)
        voice = rms >= self.args.vad_rms_threshold
        should_append = False
        should_cancel = False

        # Called from the Discord socket reader thread, so keep this section
        # synchronous and small.
        with self._vad_lock:
            if voice:
                if not self._speech_active:
                    self._speech_active = True
                    self._speech_started_at = now
                    log.info("speech_started: user_id=%s rms=%d", user_id, rms)
                    should_cancel = self.duplex.is_output_active() or bool(
                        self.output_source and self.output_source.has_pending_audio()
                    )
                self._last_voice_at = now
                should_append = True
            elif self._speech_active and now - self._last_voice_at <= self.args.vad_trailing_seconds:
                should_append = True

        if should_cancel:
            self.duplex.cancel_response()
        if should_append:
            self.duplex.append_audio(_downsample_48k_stereo_to_24k_mono(pcm_48k_stereo))
            self._frames_sent += 1

    async def _duplex_vad_loop(self) -> None:
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(0.05)
                should_commit = False
                duration = 0.0
                with self._vad_lock:
                    if self._speech_active:
                        now = time.monotonic()
                        silence = now - self._last_voice_at
                        duration = self._last_voice_at - self._speech_started_at
                        if silence >= self.args.vad_silence_seconds:
                            self._speech_active = False
                            should_commit = duration >= self.args.vad_min_seconds
                if should_commit and self.duplex:
                    log.info("speech_stopped: duration=%.2fs frames=%d", duration, self._frames_sent)
                    self._frames_sent = 0
                    await asyncio.to_thread(self.duplex.commit_and_respond)
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Gateway realtime VAD loop crashed")
            await self.close()
