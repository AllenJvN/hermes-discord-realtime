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
import uuid
from array import array
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Optional

import discord

from gateway.platforms.discord import VoiceReceiver

REALTIME_URL = "wss://api.openai.com/v1/realtime"
DEFAULT_INSTRUCTIONS = (
    "You are Hermes speaking in a Discord voice channel through OpenAI Realtime. "
    "Keep conversation short and natural. For Home Assistant, git, terminals, "
    "repos, device workers, web, memory, or other Hermes capabilities, call "
    "ask_hermes_agent. Use response_policy='ack_only' for actions that only need "
    "an acknowledgement, like turning on a light. Use response_policy='speak_result' "
    "for questions, status checks, searches, lists, summaries, or anything where "
    "the user needs the result spoken back. After the tool returns, briefly "
    "acknowledge that Hermes is working; do not claim success until a later "
    "message explicitly says it succeeded."
)

ACK_RESPONSE_INSTRUCTIONS = (
    "Briefly acknowledge that Hermes has started working on the request. "
    "Use one natural sentence. Do not claim it is done, answered, or successful."
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
        self._background_executor = ThreadPoolExecutor(
            max_workers=_env_int("HERMES_REALTIME_BACKGROUND_WORKERS", default=2, minimum=1, maximum=8),
            thread_name_prefix="hermes-realtime-job",
        )
        self._jobs_lock = threading.Lock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._max_active_jobs = _env_int("HERMES_REALTIME_MAX_ACTIVE_JOBS", default=4, minimum=1, maximum=32)
        self._max_retained_jobs = _env_int("HERMES_REALTIME_MAX_RETAINED_JOBS", default=20, minimum=1, maximum=200)

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
                                "Delegate to the normal Hermes agent. Choose ack_only for actions that can "
                                "stay silent on success, and speak_result for questions, searches, status, "
                                "lists, summaries, or any information the user needs spoken back."
                            ),
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "request": {
                                        "type": "string",
                                        "description": "The user's request, rewritten clearly for the Hermes agent.",
                                    },
                                    "response_policy": {
                                        "type": "string",
                                        "enum": ["ack_only", "speak_result"],
                                        "description": (
                                            "ack_only for action commands; speak_result for questions, status, "
                                            "searches, lists, summaries, or other returned information."
                                        ),
                                    },
                                },
                                "required": ["request", "response_policy"],
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
        self._background_executor.shutdown(wait=False, cancel_futures=True)
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
            result = self._start_background_hermes_job(arguments)
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
        response: dict[str, Any] = {"modalities": ["audio", "text"]}
        if name == "ask_hermes_agent" and result.get("ok"):
            response["instructions"] = ACK_RESPONSE_INSTRUCTIONS
        self._send({"type": "response.create", "response": response})
        if name == "ask_hermes_agent" and result.get("job_id"):
            self._mark_job_ack_sent(str(result["job_id"]))

    def _start_background_hermes_job(self, arguments: str) -> dict[str, Any]:
        parsed = _parse_agent_arguments(arguments)
        request = parsed["request"]
        response_policy = parsed["response_policy"]
        if not request:
            return {"ok": False, "error": "ask_hermes_agent requires a request string"}

        with self._jobs_lock:
            self._cleanup_jobs_locked()
            active_jobs = sum(1 for job in self._jobs.values() if job.get("status") in {"queued", "running"})
            if active_jobs >= self._max_active_jobs:
                return {
                    "ok": False,
                    "request": request,
                    "error": (
                        "Hermes already has several realtime voice jobs running. "
                        "Please wait a moment and try again."
                    ),
                }

            job_id = uuid.uuid4().hex[:10]
            self._jobs[job_id] = {
                "id": job_id,
                "request": request,
                "status": "queued",
                "created_at": time.monotonic(),
                "ack_sent": False,
                "response_policy": response_policy,
                "future": None,
            }
            future = self._background_executor.submit(
                _ask_hermes_agent,
                arguments,
                toolsets=self.agent_toolsets,
                timeout=self.agent_timeout,
                log=self.log,
            )
            self._jobs[job_id]["future"] = future

        self.log.info(
            "background_job_started: id=%s response_policy=%s request=%r",
            job_id,
            response_policy,
            request[:200],
        )
        future.add_done_callback(
            lambda completed: threading.Thread(
                target=self._finish_background_job,
                args=(job_id, completed),
                name=f"hermes-realtime-job-finish-{job_id}",
                daemon=True,
            ).start()
        )
        return {
            "ok": True,
            "job_id": job_id,
            "status": "started",
            "request": request,
            "response_policy": response_policy,
            "message": "Background Hermes task started. Acknowledge the attempt briefly.",
        }

    def _finish_background_job(self, job_id: str, future: Future) -> None:
        try:
            result = future.result()
        except Exception as exc:
            result = {"ok": False, "error": str(exc)}

        with self._jobs_lock:
            job = self._jobs.get(job_id)
            response_policy = str(job.get("response_policy") if job else "") or "speak_result"
            if job is not None:
                job["status"] = "succeeded" if result.get("ok") else "failed"
                job["finished_at"] = time.monotonic()
                job["result"] = _clip_result(result)
            self._cleanup_jobs_locked()

        self.log.info(
            "background_job_finished: id=%s ok=%s response_policy=%s",
            job_id,
            result.get("ok"),
            response_policy,
        )
        if self._closed.is_set() or not self._connected:
            return
        if result.get("ok") and response_policy == "ack_only" and not _result_needs_clarification(result):
            return
        self._wait_for_job_ack(job_id, timeout=2.0)
        self._speak_background_followup(job_id, result, response_policy=response_policy)

    def _speak_background_followup(
        self,
        job_id: str,
        result: dict[str, Any],
        *,
        response_policy: str,
    ) -> None:
        if result.get("ok") and _result_needs_clarification(result):
            instruction = (
                "Hermes needs a quick clarification before it can finish a background task. "
                f"Briefly ask the user this, in one sentence: {_result_message(result)}"
            )
        elif result.get("ok"):
            instruction = (
                "A background Hermes task produced an answer the user needs to hear. "
                "Briefly speak the useful result in a natural voice response. "
                f"Job {job_id}: {_result_message(result)}"
            )
        else:
            instruction = (
                "A background Hermes task failed or could not complete. Briefly tell the user "
                f"what happened in one sentence and avoid technical detail unless necessary. "
                f"Job {job_id}: {_result_message(result)}"
            )
        try:
            self._wait_until_voice_idle(timeout=4.0)
            if self._closed.is_set() or not self._connected:
                return
            self._send(
                {
                    "type": "response.create",
                    "response": {
                        "modalities": ["audio", "text"],
                        "instructions": instruction[:1200],
                    },
                }
            )
        except Exception as exc:
            self.log.warning("Could not speak background follow-up: %s", exc)

    def _mark_job_ack_sent(self, job_id: str) -> None:
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job["ack_sent"] = True

    def _wait_for_job_ack(self, job_id: str, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not self._closed.is_set():
            with self._jobs_lock:
                job = self._jobs.get(job_id)
                if job is None or job.get("ack_sent"):
                    return
            time.sleep(0.05)

    def _wait_until_voice_idle(self, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not self._closed.is_set():
            pending_audio = bool(self.output_source and self.output_source.has_pending_audio())
            if not self._response_active.is_set() and not pending_audio:
                return
            time.sleep(0.1)

    def _cleanup_jobs_locked(self) -> None:
        if len(self._jobs) <= self._max_retained_jobs:
            return
        completed = [
            (job.get("finished_at", job.get("created_at", 0.0)), job_id)
            for job_id, job in self._jobs.items()
            if job.get("status") in {"succeeded", "failed"}
        ]
        completed.sort()
        remove_count = max(0, len(self._jobs) - self._max_retained_jobs)
        for _, job_id in completed[:remove_count]:
            self._jobs.pop(job_id, None)

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
                error = frame.get("error") or frame
                if isinstance(error, dict) and error.get("code") == "response_cancel_not_active":
                    self.log.info("Realtime cancel ignored: no active response")
                else:
                    self.log.error("Realtime error: %s", error)
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


def _parse_agent_arguments(arguments: str) -> dict[str, str]:
    try:
        parsed = json.loads(arguments or "{}")
    except ValueError:
        parsed = {}
    request = str(parsed.get("request") or arguments or "").strip()
    response_policy = str(parsed.get("response_policy") or "").strip()
    if response_policy not in {"ack_only", "speak_result"}:
        response_policy = _infer_response_policy(request)
    elif response_policy == "ack_only" and _request_looks_informational(request):
        response_policy = "speak_result"
    return {"request": request, "response_policy": response_policy}


def _ask_hermes_agent(
    arguments: str,
    *,
    toolsets: str,
    timeout: float,
    log: logging.Logger,
) -> dict[str, Any]:
    parsed = _parse_agent_arguments(arguments)
    request = parsed["request"]
    response_policy = parsed["response_policy"]
    if not request:
        return {"ok": False, "error": "ask_hermes_agent requires a request string"}

    prompt = (
        "You are Hermes handling a request that arrived through realtime Discord voice.\n"
        "Use your normal tools when needed. This run is happening in the background, "
        "after the voice assistant has already acknowledged the user.\n\n"
        "Voice-mode execution policy:\n"
        f"- Response policy: {response_policy}.\n"
        "- For Home Assistant or device-control actions, attempt the requested action directly.\n"
        "- Do not verify final state, query status, or call follow-up check tools after an action "
        "unless the user explicitly asked to check, verify, confirm, or report status.\n"
        "- If response_policy is speak_result, answer the user's question or report the requested "
        "status/search/list/result clearly in your final response.\n"
        "- If response_policy is ack_only, keep the final response short because it will usually "
        "be logged but not spoken unless there is an error or clarification needed.\n"
        "- Do not claim certainty for device actions; report only what you attempted or any error.\n"
        "- If the request is ambiguous or unsafe, ask a concise clarification question.\n"
        "- Keep the final response concise and spoken-friendly.\n\n"
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
    return {"ok": True, "request": request, "response_policy": response_policy, "response": stdout[-4000:]}


def _env_int(name: str, *, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _clip_result(result: dict[str, Any]) -> dict[str, Any]:
    clipped: dict[str, Any] = {}
    for key, value in result.items():
        if key in {"stdout", "stderr", "response", "error"}:
            clipped[key] = str(value)[-1200:]
        elif key != "future":
            clipped[key] = value
    return clipped


def _result_message(result: dict[str, Any]) -> str:
    for key in ("error", "response", "stderr", "stdout"):
        value = str(result.get(key) or "").strip()
        if value:
            return value[-800:]
    return "Hermes could not complete the request."


def _result_needs_clarification(result: dict[str, Any]) -> bool:
    if not result.get("ok"):
        return True
    response = str(result.get("response") or "").strip()
    if "?" not in response:
        return False
    lowered = response.lower()
    clarification_markers = (
        "which ",
        "what ",
        "where ",
        "who ",
        "need ",
        "clarify",
        "could you",
        "can you",
        "please specify",
    )
    return any(marker in lowered for marker in clarification_markers)


def _infer_response_policy(request: str) -> str:
    if not request.strip():
        return "speak_result"
    if _request_looks_informational(request):
        return "speak_result"
    if _request_looks_actionable(request):
        return "ack_only"
    return "speak_result"


def _request_looks_informational(request: str) -> bool:
    text = f" {request.lower().strip()} "
    informational_markers = (
        "?",
        " what ",
        " what's ",
        " whats ",
        " which ",
        " who ",
        " where ",
        " when ",
        " why ",
        " how ",
        " tell me ",
        " show me ",
        " list ",
        " search ",
        " find ",
        " look up ",
        " status ",
        " state ",
        " running ",
        " report ",
        " summarize ",
        " summary ",
        " check ",
        " verify ",
        " confirm ",
        " is ",
        " are ",
        " do we ",
        " can you see ",
    )
    return any(marker in text for marker in informational_markers)


def _request_looks_actionable(request: str) -> bool:
    text = f" {request.lower().strip()} "
    actionable_markers = (
        " turn on ",
        " turn off ",
        " toggle ",
        " set ",
        " start ",
        " stop ",
        " restart ",
        " open ",
        " close ",
        " run ",
        " create ",
        " update ",
        " change ",
        " send ",
    )
    return any(marker in text for marker in actionable_markers)


def _looks_like_openai_platform_key(value: str) -> bool:
    key = (value or "").strip()
    return key.startswith(("sk-", "sk-proj-", "sk-svcacct-")) and len(key) > 20


def _resolve_openai_realtime_api_key() -> tuple[str, str]:
    """Resolve a Platform API key usable with OpenAI Realtime.

    Hermes' `openai-codex` auth is a ChatGPT/Codex OAuth credential, not an
    OpenAI Platform API key. Realtime expects Platform Bearer auth, so only
    explicit Platform-looking env vars are accepted here.
    """
    realtime_key = os.getenv("OPENAI_REALTIME_API_KEY", "").strip()
    if realtime_key:
        return realtime_key, "OPENAI_REALTIME_API_KEY"

    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    if _looks_like_openai_platform_key(openai_key):
        return openai_key, "OPENAI_API_KEY"

    return "", ""


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
