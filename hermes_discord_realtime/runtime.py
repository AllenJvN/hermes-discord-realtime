#!/usr/bin/env python3
"""Sandbox Discord voice proof-of-concept for OpenAI Realtime.

This is intentionally isolated from the Hermes gateway:
- no systemd changes
- no slash command registration
- no Hermes tools
- one explicit guild/channel/user allowlist
- manual run only

Run with hermes-gateway stopped because Discord only allows one live bot
client per token.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
from array import array
from pathlib import Path
from typing import Any, Optional

import discord

from hermes_cli.env_loader import load_hermes_dotenv
from gateway.platforms.discord import VoiceReceiver

REALTIME_URL = "wss://api.openai.com/v1/realtime"
DEFAULT_INSTRUCTIONS = (
    "You are Hermes speaking in a Discord voice channel through OpenAI Realtime. "
    "Keep normal conversation short and natural. "
    "When the user asks you to do anything that requires Hermes capabilities "
    "(Home Assistant, git, terminals, repos, device workers, web, memory, etc.), "
    "call ask_hermes_agent with the user's request. Then briefly speak the result."
)


class RealtimeRoundTrip:
    """Small synchronous OpenAI Realtime audio-in/audio-out client."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        voice: str,
        instructions: str,
        logger: logging.Logger,
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.voice = voice
        self.instructions = instructions
        self.log = logger
        self._ws: Any = None

    def connect(self) -> None:
        from websockets.sync.client import connect

        headers = [
            ("Authorization", f"Bearer {self.api_key}"),
            ("OpenAI-Beta", "realtime=v1"),
        ]
        url = f"{REALTIME_URL}?model={self.model}"
        try:
            self._ws = connect(url, additional_headers=headers, open_timeout=20)
        except TypeError:
            self._ws = connect(url, extra_headers=headers, open_timeout=20)

        self._send(
            {
                "type": "session.update",
                "session": {
                    "modalities": ["audio", "text"],
                    "instructions": self.instructions,
                    "voice": self.voice,
                    "input_audio_format": "pcm16",
                    "output_audio_format": "pcm16",
                    "turn_detection": None,
                },
            }
        )
        self.log.info("OpenAI Realtime connected: model=%s voice=%s", self.model, self.voice)

    def close(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None

    def respond_to_discord_pcm(self, pcm_48k_stereo: bytes, timeout: float) -> tuple[bytes, str]:
        """Send one utterance to Realtime and return raw 24k mono PCM + transcript."""
        if self._ws is None:
            self.connect()

        pcm_24k_mono = _convert_raw_pcm(
            pcm_48k_stereo,
            src_rate=48000,
            src_channels=2,
            dst_rate=24000,
            dst_channels=1,
        )
        self.log.info(
            "Sending utterance to Realtime: in=%d bytes converted=%d bytes",
            len(pcm_48k_stereo),
            len(pcm_24k_mono),
        )

        self._send(
            {
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(pcm_24k_mono).decode("ascii"),
            }
        )
        self._send({"type": "input_audio_buffer.commit"})
        self._send(
            {
                "type": "response.create",
                "response": {
                    "modalities": ["audio", "text"],
                    "instructions": self.instructions,
                },
            }
        )

        started = time.monotonic()
        audio = bytearray()
        transcript_parts: list[str] = []
        saw_done = False

        while time.monotonic() - started < timeout:
            frame = self._recv(timeout=max(0.1, timeout - (time.monotonic() - started)))
            if frame is None:
                break
            ftype = frame.get("type")
            if ftype == "response.audio.delta":
                delta = frame.get("delta") or frame.get("audio") or ""
                if delta:
                    audio.extend(base64.b64decode(delta))
            elif ftype in {"response.audio_transcript.delta", "response.text.delta"}:
                transcript_parts.append(str(frame.get("delta") or ""))
            elif ftype in {"response.done", "response.completed"}:
                saw_done = True
                break
            elif ftype == "error":
                err = frame.get("error") or frame
                raise RuntimeError(f"OpenAI Realtime error: {err}")
            elif ftype in {"session.created", "session.updated", "input_audio_buffer.committed", "response.created"}:
                self.log.debug("Realtime event: %s", ftype)

        if not saw_done:
            self.log.warning("Realtime response timed out/ended before response.done")
        transcript = "".join(transcript_parts).strip()
        self.log.info("Realtime response: audio=%d bytes transcript=%r", len(audio), transcript[:160])
        return bytes(audio), transcript

    def _send(self, payload: dict[str, Any]) -> None:
        assert self._ws is not None
        self._ws.send(json.dumps(payload))

    def _recv(self, timeout: float) -> Optional[dict[str, Any]]:
        assert self._ws is not None
        try:
            raw = self._ws.recv(timeout=timeout)
        except TimeoutError:
            return None
        except TypeError:
            raw = self._ws.recv()
        if raw is None:
            return None
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return None
        return value if isinstance(value, dict) else None


class StreamingPCMAudioSource(discord.AudioSource):
    """Persistent Discord PCM source fed by OpenAI Realtime audio deltas."""

    FRAME_BYTES = 3840  # 20ms of 48kHz stereo s16le.

    def __init__(self, log: logging.Logger) -> None:
        self.log = log
        self._buffer = bytearray()
        self._lock = threading.Lock()
        self._closed = False
        self._last_audio_at = 0.0
        self._underruns = 0

    def is_opus(self) -> bool:
        return False

    def read(self) -> bytes:
        with self._lock:
            if self._closed:
                return b""
            if len(self._buffer) >= self.FRAME_BYTES:
                chunk = bytes(self._buffer[: self.FRAME_BYTES])
                del self._buffer[: self.FRAME_BYTES]
                return chunk
            self._underruns += 1
        return b"\0" * self.FRAME_BYTES

    def cleanup(self) -> None:
        self.stop()

    def stop(self) -> None:
        with self._lock:
            self._closed = True
            self._buffer.clear()

    def clear(self) -> None:
        with self._lock:
            self._buffer.clear()

    def has_pending_audio(self) -> bool:
        with self._lock:
            return bool(self._buffer)

    def enqueue_openai_pcm24(self, pcm_24k_mono: bytes) -> None:
        pcm_48k_stereo = _upsample_24k_mono_to_48k_stereo(pcm_24k_mono)
        with self._lock:
            if self._closed:
                return
            self._buffer.extend(pcm_48k_stereo)
            self._last_audio_at = time.monotonic()


class RealtimeDuplexSession:
    """OpenAI Realtime WebSocket client for live audio in/out."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        voice: str,
        instructions: str,
        output_source: StreamingPCMAudioSource,
        agent_toolsets: str,
        agent_timeout: float,
        logger: logging.Logger,
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.voice = voice
        self.instructions = instructions
        self.output_source = output_source
        self.agent_toolsets = agent_toolsets
        self.agent_timeout = agent_timeout
        self.log = logger
        self._ws: Any = None
        self._send_lock = threading.Lock()
        self._closed = threading.Event()
        self._recv_thread: Optional[threading.Thread] = None
        self._response_active = threading.Event()
        self._connected = False
        self._handled_call_ids: set[str] = set()

    def connect(self) -> None:
        from websockets.sync.client import connect

        headers = [
            ("Authorization", f"Bearer {self.api_key}"),
            ("OpenAI-Beta", "realtime=v1"),
        ]
        url = f"{REALTIME_URL}?model={self.model}"
        try:
            self._ws = connect(url, additional_headers=headers, open_timeout=20)
        except TypeError:
            self._ws = connect(url, extra_headers=headers, open_timeout=20)

        self._send(
            {
                "type": "session.update",
                "session": {
                    "modalities": ["audio", "text"],
                    "instructions": self.instructions,
                    "voice": self.voice,
                    "input_audio_format": "pcm16",
                    "output_audio_format": "pcm16",
                    "turn_detection": None,
                    "tools": [
                        {
                            "type": "function",
                            "name": "ask_hermes_agent",
                            "description": (
                                "Delegate a request to the normal Hermes agent with its configured tools. "
                                "Use for Home Assistant, git/repos, terminals, device workers, web, memory, "
                                "or any real-world action. Return the result to the user by voice."
                            ),
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "request": {
                                        "type": "string",
                                        "description": "The user's request, rewritten clearly for the Hermes agent.",
                                    }
                                },
                                "required": ["request"],
                                "additionalProperties": False,
                            },
                        }
                    ],
                    "tool_choice": "auto",
                },
            }
        )
        self._connected = True
        self._recv_thread = threading.Thread(
            target=self._recv_loop,
            name="hermes-realtime-duplex-recv",
            daemon=True,
        )
        self._recv_thread.start()
        self.log.info("OpenAI Realtime duplex connected: model=%s voice=%s", self.model, self.voice)

    def close(self) -> None:
        self._closed.set()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None

    def is_output_active(self) -> bool:
        return self._response_active.is_set()

    def append_audio(self, pcm_24k_mono: bytes) -> None:
        if not pcm_24k_mono or not self._connected:
            return
        try:
            self._send(
                {
                    "type": "input_audio_buffer.append",
                    "audio": base64.b64encode(pcm_24k_mono).decode("ascii"),
                }
            )
        except Exception as exc:
            self.log.warning("Realtime append failed: %s", exc)

    def commit_and_respond(self) -> None:
        if not self._connected:
            return
        try:
            self.log.info("input_audio_buffer.commit + response.create")
            self._send({"type": "input_audio_buffer.commit"})
            self._send({"type": "response.create", "response": {"modalities": ["audio", "text"]}})
        except Exception as exc:
            self.log.warning("Realtime commit/respond failed: %s", exc)

    def cancel_response(self) -> None:
        if not self._connected:
            return
        active = self._response_active.is_set()
        self.log.info("barge-in: %s + output queue flush", "response.cancel" if active else "local flush")
        self.output_source.clear()
        if active:
            try:
                self._send({"type": "response.cancel"})
            except Exception as exc:
                self.log.warning("Realtime cancel failed: %s", exc)
        self._response_active.clear()

    def _handle_function_call(self, *, name: str, call_id: str, arguments: str) -> None:
        if not call_id or call_id in self._handled_call_ids:
            return
        self._handled_call_ids.add(call_id)
        self.log.info("tool_call: name=%s call_id=%s arguments=%s", name, call_id, arguments or "{}")

        if name == "ask_hermes_agent":
            result = _ask_hermes_agent(
                arguments,
                toolsets=self.agent_toolsets,
                timeout=self.agent_timeout,
                log=self.log,
            )
        else:
            result = {"ok": False, "error": f"unknown tool: {name}"}

        self.log.info("tool_result: name=%s ok=%s", name, result.get("ok"))
        self._send(
            {
                "type": "conversation.item.create",
                "item": {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": json.dumps(result),
                },
            }
        )
        self._send({"type": "response.create", "response": {"modalities": ["audio", "text"]}})

    def _send(self, payload: dict[str, Any]) -> None:
        assert self._ws is not None
        with self._send_lock:
            self._ws.send(json.dumps(payload))

    def _recv_loop(self) -> None:
        assert self._ws is not None
        while not self._closed.is_set():
            try:
                raw = self._ws.recv()
            except Exception as exc:
                if not self._closed.is_set():
                    self.log.warning("Realtime recv loop ended: %s", exc)
                return
            try:
                frame = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
            except (TypeError, ValueError):
                continue
            if not isinstance(frame, dict):
                continue
            ftype = frame.get("type")
            if ftype == "response.created":
                self._response_active.set()
                self.log.info("response.created")
            elif ftype == "response.audio.delta":
                delta = frame.get("delta") or frame.get("audio") or ""
                if delta:
                    try:
                        self.output_source.enqueue_openai_pcm24(base64.b64decode(delta))
                    except Exception as exc:
                        self.log.warning("Could not enqueue realtime audio: %s", exc)
            elif ftype in {"response.audio_transcript.delta", "response.text.delta"}:
                text = str(frame.get("delta") or "")
                if text:
                    self.log.debug("transcript.delta: %s", text)
            elif ftype in {"response.done", "response.completed", "response.cancelled"}:
                response = frame.get("response") or {}
                for item in response.get("output") or []:
                    if isinstance(item, dict) and item.get("type") == "function_call":
                        self._handle_function_call(
                            name=str(item.get("name") or ""),
                            call_id=str(item.get("call_id") or ""),
                            arguments=str(item.get("arguments") or "{}"),
                        )
                self._response_active.clear()
                self.log.info("%s", ftype)
            elif ftype == "response.output_item.done":
                item = frame.get("item") or {}
                if isinstance(item, dict) and item.get("type") == "function_call":
                    self._handle_function_call(
                        name=str(item.get("name") or ""),
                        call_id=str(item.get("call_id") or ""),
                        arguments=str(item.get("arguments") or "{}"),
                    )
            elif ftype == "response.function_call_arguments.done":
                self._handle_function_call(
                    name=str(frame.get("name") or ""),
                    call_id=str(frame.get("call_id") or ""),
                    arguments=str(frame.get("arguments") or "{}"),
                )
            elif ftype == "error":
                self.log.error("Realtime error: %s", frame.get("error") or frame)
            elif ftype in {"session.created", "session.updated", "input_audio_buffer.committed"}:
                self.log.debug("Realtime event: %s", ftype)


class LiveVoiceReceiver(VoiceReceiver):
    """VoiceReceiver variant that emits decoded PCM frames immediately."""

    def __init__(self, voice_client, allowed_user_ids: set, frame_callback, logger: logging.Logger):
        super().__init__(voice_client, allowed_user_ids=allowed_user_ids)
        self._frame_callback = frame_callback
        self._log = logger

    def _on_packet(self, data: bytes):
        if not self._running:
            return
        if len(data) < 16:
            return
        if (data[0] >> 6) != 2 or (data[1] & 0x7F) != 0x78:
            return

        first_byte = data[0]
        _, _, seq, _timestamp, ssrc = struct.unpack_from(">BBHII", data, 0)
        if ssrc == self._bot_ssrc:
            return

        cc = first_byte & 0x0F
        has_extension = bool(first_byte & 0x10)
        has_padding = bool(first_byte & 0x20)
        header_size = 12 + (4 * cc) + (4 if has_extension else 0)
        if len(data) < header_size + 4:
            return

        ext_data_len = 0
        if has_extension:
            ext_preamble_offset = 12 + (4 * cc)
            ext_words = struct.unpack_from(">H", data, ext_preamble_offset + 2)[0]
            ext_data_len = ext_words * 4

        header = bytes(data[:header_size])
        payload_with_nonce = data[header_size:]
        if len(payload_with_nonce) < 4:
            return
        nonce = bytearray(24)
        nonce[:4] = payload_with_nonce[-4:]
        encrypted = bytes(payload_with_nonce[:-4])

        try:
            import nacl.secret

            box = nacl.secret.Aead(self._secret_key)
            decrypted = box.decrypt(encrypted, header, bytes(nonce))
        except Exception as exc:
            self._log.debug("NaCl decrypt failed for seq=%s: %s", seq, exc)
            return

        if ext_data_len and len(decrypted) > ext_data_len:
            decrypted = decrypted[ext_data_len:]

        if has_padding:
            if not decrypted:
                return
            pad_len = decrypted[-1]
            if pad_len == 0 or pad_len > len(decrypted):
                return
            decrypted = decrypted[:-pad_len]
            if not decrypted:
                return

        with self._lock:
            user_id = self._ssrc_to_user.get(ssrc, 0)
        if not user_id:
            user_id = self._infer_user_for_ssrc(ssrc)
        if not user_id or (self._allowed_user_ids and str(user_id) not in self._allowed_user_ids):
            return

        if self._dave_session:
            try:
                import davey

                decrypted = self._dave_session.decrypt(user_id, davey.MediaType.audio, decrypted)
            except Exception as exc:
                if "Unencrypted" not in str(exc):
                    self._log.debug("DAVE decrypt failed for ssrc=%d: %s", ssrc, exc)
                    return

        try:
            if ssrc not in self._decoders:
                self._decoders[ssrc] = discord.opus.Decoder()
            pcm = self._decoders[ssrc].decode(decrypted)
        except Exception as exc:
            self._log.debug("Opus decode error for ssrc=%d: %s", ssrc, exc)
            return

        self._frame_callback(user_id, pcm)


class RealtimeDiscordSandbox(discord.Client):
    def __init__(self, args: argparse.Namespace, log: logging.Logger) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.voice_states = True
        intents.members = True
        super().__init__(intents=intents)
        self.args = args
        self.log = log
        self.voice_client: Optional[discord.VoiceClient] = None
        self.receiver: Optional[VoiceReceiver] = None
        self.listen_task: Optional[asyncio.Task] = None
        self.keepalive_task: Optional[asyncio.Task] = None
        self.vad_task: Optional[asyncio.Task] = None
        self.stop_event = asyncio.Event()
        self.realtime = RealtimeRoundTrip(
            api_key=args.openai_api_key,
            model=args.model,
            voice=args.voice,
            instructions=args.instructions,
            logger=log,
        )
        self.allowed_user_ids = {str(uid) for uid in args.allowed_user_id}
        self._processing = asyncio.Lock()
        self._joined = False
        self.output_source: Optional[StreamingPCMAudioSource] = None
        self.duplex: Optional[RealtimeDuplexSession] = None
        self._vad_lock = threading.Lock()
        self._speech_active = False
        self._speech_started_at = 0.0
        self._last_voice_at = 0.0
        self._frames_sent = 0

    async def on_ready(self) -> None:
        if self._joined:
            return
        self._joined = True
        assert self.user is not None
        self.log.info("Discord logged in as %s (%s)", self.user, self.user.id)
        guild = self.get_guild(self.args.guild_id)
        if guild is None:
            raise RuntimeError(f"Guild {self.args.guild_id} not found or bot is not a member")
        channel = guild.get_channel(self.args.voice_channel_id)
        if channel is None:
            raise RuntimeError(f"Voice channel {self.args.voice_channel_id} not found in guild {guild.name}")
        if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
            raise RuntimeError(f"Channel {channel} is not a voice/stage channel")

        self.log.info("Joining voice channel: %s / %s", guild.name, channel.name)
        self.voice_client = await channel.connect()
        if self.args.mode == "duplex":
            self.output_source = StreamingPCMAudioSource(self.log)
            self.duplex = RealtimeDuplexSession(
                api_key=self.args.openai_api_key,
                model=self.args.model,
                voice=self.args.voice,
                instructions=self.args.instructions,
                output_source=self.output_source,
                agent_toolsets=self.args.agent_toolsets,
                agent_timeout=self.args.agent_timeout,
                logger=self.log,
            )
            self.duplex.connect()
            self.voice_client.play(self.output_source)
            self.receiver = LiveVoiceReceiver(
                self.voice_client,
                allowed_user_ids=self.allowed_user_ids,
                frame_callback=self._on_live_pcm,
                logger=self.log,
            )
            self.receiver.start()
            self.vad_task = asyncio.create_task(self._duplex_vad_loop(), name="discord-realtime-vad")
            self.log.info("Duplex mode ready. Interrupt while Hermes speaks to test barge-in.")
        else:
            self.receiver = VoiceReceiver(self.voice_client, allowed_user_ids=self.allowed_user_ids)
            self.receiver.start()
            self.realtime.connect()
            self.listen_task = asyncio.create_task(self._listen_loop(), name="discord-realtime-listen")
        self.keepalive_task = asyncio.create_task(self._udp_keepalive_loop(), name="discord-realtime-keepalive")
        self.log.info("Ready. Speak in Discord as one of: %s", ", ".join(sorted(self.allowed_user_ids)))

    async def close(self) -> None:
        self.stop_event.set()
        for task in (self.listen_task, self.keepalive_task, self.vad_task):
            if task:
                task.cancel()
        if self.receiver:
            self.receiver.stop()
            self.receiver = None
        if self.duplex:
            self.duplex.close()
            self.duplex = None
        if self.output_source:
            self.output_source.stop()
            self.output_source = None
        if self.voice_client and self.voice_client.is_playing():
            self.voice_client.stop()
        if self.voice_client and self.voice_client.is_connected():
            await self.voice_client.disconnect(force=True)
        self.realtime.close()
        await super().close()

    async def _udp_keepalive_loop(self) -> None:
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(15)
                if self.voice_client and self.voice_client.is_connected():
                    try:
                        self.voice_client._connection.send_packet(b"\xf8\xff\xfe")
                    except Exception:
                        self.log.debug("Discord UDP keepalive failed", exc_info=True)
        except asyncio.CancelledError:
            pass

    async def _listen_loop(self) -> None:
        assert self.receiver is not None
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(0.2)
                completed = self.receiver.check_silence()
                for user_id, pcm_data in completed:
                    if str(user_id) not in self.allowed_user_ids:
                        self.log.info("Ignoring voice from unauthorized user_id=%s", user_id)
                        continue
                    if self._processing.locked():
                        self.log.info("Ignoring overlapping utterance from user_id=%s while response is in flight", user_id)
                        continue
                    asyncio.create_task(self._handle_utterance(user_id, pcm_data))
        except asyncio.CancelledError:
            pass
        except Exception:
            self.log.exception("Listen loop crashed")
            await self.close()

    def _on_live_pcm(self, user_id: int, pcm_48k_stereo: bytes) -> None:
        if str(user_id) not in self.allowed_user_ids or not self.duplex:
            return

        now = time.monotonic()
        rms = _pcm_rms(pcm_48k_stereo)
        voice = rms >= self.args.vad_rms_threshold
        should_append = False
        should_cancel = False

        with self._vad_lock:
            if voice:
                if not self._speech_active:
                    self._speech_active = True
                    self._speech_started_at = now
                    self.log.info("speech_started: user_id=%s rms=%d", user_id, rms)
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
            pcm_24k_mono = _downsample_48k_stereo_to_24k_mono(pcm_48k_stereo)
            self.duplex.append_audio(pcm_24k_mono)
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
                    self.log.info("speech_stopped: duration=%.2fs frames=%d", duration, self._frames_sent)
                    self._frames_sent = 0
                    await asyncio.to_thread(self.duplex.commit_and_respond)
        except asyncio.CancelledError:
            pass
        except Exception:
            self.log.exception("Duplex VAD loop crashed")
            await self.close()

    async def _handle_utterance(self, user_id: int, pcm_data: bytes) -> None:
        async with self._processing:
            if not self.voice_client or not self.voice_client.is_connected():
                return
            assert self.receiver is not None
            self.log.info("Utterance captured: user_id=%s bytes=%d", user_id, len(pcm_data))
            self.receiver.pause()
            try:
                audio_pcm, _transcript = await asyncio.to_thread(
                    self.realtime.respond_to_discord_pcm,
                    pcm_data,
                    self.args.response_timeout,
                )
                if not audio_pcm:
                    self.log.warning("No audio returned by Realtime")
                    return
                wav_path = await asyncio.to_thread(_pcm24_to_wav_file, audio_pcm)
                try:
                    await self._play_wav(wav_path)
                finally:
                    try:
                        os.unlink(wav_path)
                    except OSError:
                        pass
            except Exception:
                self.log.exception("Realtime utterance failed")
            finally:
                self.receiver.resume()

    async def _play_wav(self, wav_path: str) -> None:
        assert self.voice_client is not None
        while self.voice_client.is_playing():
            await asyncio.sleep(0.05)
        done = asyncio.Event()
        loop = asyncio.get_running_loop()

        def after(error: Optional[Exception]) -> None:
            if error:
                self.log.error("Discord playback error: %s", error)
            loop.call_soon_threadsafe(done.set)

        source = discord.FFmpegPCMAudio(wav_path)
        source = discord.PCMVolumeTransformer(source, volume=self.args.volume)
        self.voice_client.play(source, after=after)
        await asyncio.wait_for(done.wait(), timeout=self.args.playback_timeout)


def _pcm_rms(pcm_s16le: bytes) -> int:
    if not pcm_s16le:
        return 0
    samples = memoryview(pcm_s16le).cast("h")
    if not samples:
        return 0
    total = 0
    step = max(1, len(samples) // 2048)
    count = 0
    for idx in range(0, len(samples), step):
        sample = int(samples[idx])
        total += sample * sample
        count += 1
    return int((total / max(1, count)) ** 0.5)


def _downsample_48k_stereo_to_24k_mono(pcm_48k_stereo: bytes) -> bytes:
    samples = memoryview(pcm_48k_stereo).cast("h")
    out = array("h")
    # Two stereo frames at 48kHz become one mono frame at 24kHz.
    for idx in range(0, len(samples) - 3, 4):
        left = int(samples[idx])
        right = int(samples[idx + 1])
        out.append(max(-32768, min(32767, (left + right) // 2)))
    return out.tobytes()


def _upsample_24k_mono_to_48k_stereo(pcm_24k_mono: bytes) -> bytes:
    samples = memoryview(pcm_24k_mono).cast("h")
    out = array("h")
    for sample in samples:
        value = int(sample)
        # Duplicate once for 24k -> 48k, and duplicate channels for stereo.
        out.extend((value, value, value, value))
    return out.tobytes()


def _ask_hermes_agent(
    arguments: str,
    *,
    toolsets: str,
    timeout: float,
    log: logging.Logger,
) -> dict[str, Any]:
    try:
        parsed = json.loads(arguments or "{}")
    except ValueError:
        parsed = {}
    request = str(parsed.get("request") or arguments or "").strip()
    if not request:
        return {"ok": False, "error": "ask_hermes_agent requires a request string"}

    prompt = (
        "You are Hermes handling a request that arrived through realtime Discord voice.\n"
        "Use your normal tools when needed. Keep the final response concise and spoken-friendly.\n\n"
        f"Voice user request: {request}"
    )
    cmd = [
        sys.executable,
        "-m",
        "hermes_cli.main",
        "-z",
        prompt,
        "-t",
        toolsets,
    ]
    env = os.environ.copy()
    env["HERMES_YOLO_MODE"] = "1"
    env["HERMES_ACCEPT_HOOKS"] = "1"
    try:
        log.info("ask_hermes_agent: toolsets=%s request=%r", toolsets, request[:200])
        proc = subprocess.run(
            cmd,
            cwd=os.getenv("HERMES_REALTIME_AGENT_CWD") or os.getcwd(),
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "request": request, "error": f"Hermes agent timed out after {timeout:.0f}s"}
    except Exception as exc:
        return {"ok": False, "request": request, "error": str(exc)}

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0:
        return {
            "ok": False,
            "request": request,
            "returncode": proc.returncode,
            "stdout": stdout[-2000:],
            "stderr": stderr[-2000:],
        }
    return {"ok": True, "request": request, "response": stdout[-4000:]}


def _convert_raw_pcm(
    pcm: bytes,
    *,
    src_rate: int,
    src_channels: int,
    dst_rate: int,
    dst_channels: int,
) -> bytes:
    proc = subprocess.run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "s16le",
            "-ar",
            str(src_rate),
            "-ac",
            str(src_channels),
            "-i",
            "pipe:0",
            "-f",
            "s16le",
            "-ar",
            str(dst_rate),
            "-ac",
            str(dst_channels),
            "pipe:1",
        ],
        input=pcm,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        timeout=15,
    )
    return proc.stdout


def _pcm24_to_wav_file(pcm_24k_mono: bytes) -> str:
    raw = tempfile.NamedTemporaryFile(prefix="hermes_rt_out_", suffix=".pcm", delete=False)
    wav_path = raw.name[:-4] + ".wav"
    try:
        raw.write(pcm_24k_mono)
        raw.close()
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "s16le",
                "-ar",
                "24000",
                "-ac",
                "1",
                "-i",
                raw.name,
                "-ar",
                "48000",
                "-ac",
                "2",
                wav_path,
            ],
            check=True,
            timeout=15,
        )
        return wav_path
    finally:
        try:
            os.unlink(raw.name)
        except OSError:
            pass


def _load_args() -> argparse.Namespace:
    load_hermes_dotenv()
    parser = argparse.ArgumentParser(description="Hermes Discord/OpenAI Realtime voice bridge")
    parser.add_argument("--guild-id", type=int, required=True)
    parser.add_argument("--voice-channel-id", type=int, required=True)
    parser.add_argument("--allowed-user-id", type=int, action="append", required=True)
    parser.add_argument("--mode", choices=("turn", "duplex"), default="turn")
    parser.add_argument("--model", default=os.getenv("OPENAI_REALTIME_MODEL", "gpt-realtime"))
    parser.add_argument("--voice", default=os.getenv("OPENAI_REALTIME_VOICE", "alloy"))
    parser.add_argument("--instructions", default=os.getenv("OPENAI_REALTIME_INSTRUCTIONS", DEFAULT_INSTRUCTIONS))
    parser.add_argument("--agent-toolsets", default=os.getenv("HERMES_REALTIME_AGENT_TOOLSETS", "all"))
    parser.add_argument("--agent-timeout", type=float, default=float(os.getenv("HERMES_REALTIME_AGENT_TIMEOUT", "180")))
    parser.add_argument("--response-timeout", type=float, default=30.0)
    parser.add_argument("--playback-timeout", type=float, default=120.0)
    parser.add_argument("--volume", type=float, default=1.0)
    parser.add_argument("--vad-rms-threshold", type=int, default=650)
    parser.add_argument("--vad-silence-seconds", type=float, default=0.75)
    parser.add_argument("--vad-trailing-seconds", type=float, default=0.25)
    parser.add_argument("--vad-min-seconds", type=float, default=0.25)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--openai-api-key", default=os.getenv("OPENAI_REALTIME_API_KEY", "") or os.getenv("OPENAI_API_KEY", ""))
    parser.add_argument("--discord-bot-token", default=os.getenv("DISCORD_BOT_TOKEN", ""))
    args = parser.parse_args()
    if not args.openai_api_key:
        parser.error("OPENAI_REALTIME_API_KEY or OPENAI_API_KEY is not set in the environment or ~/.hermes/.env")
    if not args.discord_bot_token:
        parser.error("DISCORD_BOT_TOKEN is not set in the environment or ~/.hermes/.env")
    return args


async def _amain(args: argparse.Namespace | None = None) -> None:
    args = args or _load_args()
    load_hermes_dotenv()
    if getattr(args, "model", None) is None:
        args.model = os.getenv("OPENAI_REALTIME_MODEL", "gpt-realtime")
    if getattr(args, "voice", None) is None:
        args.voice = os.getenv("OPENAI_REALTIME_VOICE", "alloy")
    if getattr(args, "instructions", None) is None:
        args.instructions = os.getenv("OPENAI_REALTIME_INSTRUCTIONS", DEFAULT_INSTRUCTIONS)
    if getattr(args, "agent_toolsets", None) is None:
        args.agent_toolsets = os.getenv("HERMES_REALTIME_AGENT_TOOLSETS", "all")
    args.openai_api_key = os.getenv("OPENAI_REALTIME_API_KEY", "") or os.getenv("OPENAI_API_KEY", "")
    args.discord_bot_token = os.getenv("DISCORD_BOT_TOKEN", "")
    if not args.openai_api_key:
        raise SystemExit("OPENAI_REALTIME_API_KEY or OPENAI_API_KEY is not set")
    if not args.discord_bot_token:
        raise SystemExit("DISCORD_BOT_TOKEN is not set")
    if not hasattr(args, "mode"):
        args.mode = "duplex"
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        force=True,
    )
    log = logging.getLogger("hermes.discord_realtime")
    log.info("Starting Discord Realtime voice bridge. Stop hermes-gateway first when reusing the same bot token.")
    client = RealtimeDiscordSandbox(args, log)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(client.close()))
        except NotImplementedError:
            pass

    try:
        await client.start(args.discord_bot_token)
    finally:
        await client.close()
        log.info("Sandbox stopped")


def main(args: argparse.Namespace | None = None) -> None:
    asyncio.run(_amain(args))


def print_env_status() -> None:
    load_hermes_dotenv()
    for name in ("DISCORD_BOT_TOKEN", "OPENAI_REALTIME_API_KEY", "OPENAI_API_KEY"):
        value = os.getenv(name, "")
        prefix = value[:7] if value else ""
        print(f"{name}: set={bool(value)} len={len(value)} prefix={prefix}")


if __name__ == "__main__":
    main()
