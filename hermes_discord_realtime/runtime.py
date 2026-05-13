#!/usr/bin/env python3
"""Runtime helpers for the Hermes Discord Realtime gateway plugin.

This module deliberately contains no Discord client. Hermes owns Discord through
`hermes-gateway`, and this plugin reuses that existing client for duplex voice.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import struct
import subprocess
import sys
import threading
import time
from array import array
from typing import Any, Optional

import discord

from gateway.platforms.discord import VoiceReceiver

REALTIME_URL = "wss://api.openai.com/v1/realtime"
DEFAULT_INSTRUCTIONS = (
    "You are Hermes speaking in a Discord voice channel through OpenAI Realtime. "
    "Keep normal conversation short and natural. "
    "When the user asks you to do anything that requires Hermes capabilities "
    "(Home Assistant, git, terminals, repos, device workers, web, memory, etc.), "
    "call ask_hermes_agent with the user's request. Then briefly speak the result."
)


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
        if self._recv_thread and self._recv_thread.is_alive():
            self._recv_thread.join(timeout=2)
        self._connected = False

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


def _env_value(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name, "")
        if value:
            return value
    return default


def _parse_int(value: Any, *, name: str) -> int:
    if isinstance(value, int):
        return value
    if value is None or str(value).strip() == "":
        raise SystemExit(f"{name} is required")
    try:
        return int(str(value).strip())
    except ValueError as exc:
        raise SystemExit(f"{name} must be a Discord snowflake/integer, got {value!r}") from exc


def _parse_user_ids(values: Any) -> list[int]:
    raw: list[str] = []
    if values:
        raw.extend(str(v) for v in values)
    env_value = _env_value("DISCORD_ALLOWED_USERS")
    if env_value:
        raw.extend(part.strip() for part in env_value.replace(";", ",").split(","))
    user_ids: list[int] = []
    for item in raw:
        if not item:
            continue
        for part in str(item).replace(";", ",").split(","):
            part = part.strip()
            if part:
                user_ids.append(_parse_int(part, name="allowed user id"))
    seen: set[int] = set()
    deduped: list[int] = []
    for user_id in user_ids:
        if user_id not in seen:
            seen.add(user_id)
            deduped.append(user_id)
    if not deduped:
        raise SystemExit("At least one allowed user is required. Set DISCORD_ALLOWED_USERS.")
    return deduped
