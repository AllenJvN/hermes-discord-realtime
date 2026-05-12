# Hermes Discord Realtime

Duplex OpenAI Realtime voice for Hermes Agent in Discord voice channels.

This plugin lets the existing Hermes Discord bot join a voice channel, listen
with low-latency audio, speak through OpenAI Realtime, and delegate real actions
to the normal Hermes agent through one compact tool call: `ask_hermes_agent`.

## What It Does

- Streams Discord voice audio into OpenAI Realtime.
- Streams OpenAI audio deltas back into Discord as they arrive.
- Supports barge-in: interrupt Hermes while it is speaking.
- Gives voice access to normal Hermes capabilities by delegating to `hermes -z`.
- Keeps the Realtime tool surface tiny instead of exposing every Hermes tool to
  the Realtime model directly.

## Requirements

- Hermes Agent installed on the machine running the plugin.
- Existing Discord bot token in `DISCORD_BOT_TOKEN`.
- OpenAI Platform Realtime key in `OPENAI_REALTIME_API_KEY`.
- `ffmpeg` and Discord voice dependencies available in the Hermes environment.
- If reusing the same Discord bot token as `hermes-gateway`, stop the gateway
  while this voice bridge is running.

## Install

```bash
hermes plugins install AllenJvN/hermes-discord-realtime --enable
```

Check env:

```bash
hermes discord-realtime env
```

## Run

```bash
systemctl --user stop hermes-gateway

hermes discord-realtime run \
  --guild-id <discord_server_id> \
  --voice-channel-id <voice_channel_id> \
  --allowed-user-id <your_discord_user_id>
```

For Allen's current homelab test channel:

```bash
hermes discord-realtime run \
  --guild-id 1210302880897310820 \
  --voice-channel-id 1503132822892707861 \
  --allowed-user-id 691556762846887968
```

Rollback:

```bash
Ctrl+C
systemctl --user start hermes-gateway
```

## Tool Calling Model

OpenAI Realtime gets a single function:

```text
ask_hermes_agent(request)
```

That function runs the normal Hermes agent with configurable toolsets. By
default it uses:

```text
all
```

Override with:

```bash
hermes discord-realtime run ... --agent-toolsets hermes-discord,homeassistant,device_worker,device_coding
```

This means Home Assistant, repos, terminals, device workers, browser tools, and
future Hermes capabilities remain owned by Hermes rather than duplicated inside
the Realtime session.

## Safety Notes

- This is powerful. Voice can trigger whatever the configured Hermes agent
  toolsets can do.
- Restrict `--allowed-user-id` to trusted Discord users.
- Do not expose any worker or voice bridge ports publicly.
- Prefer a separate Discord bot token for long-term production so normal
  `hermes-gateway` can keep running.

## Current Status

This was promoted from a working Hermes homelab prototype. Verified paths:

- Discord voice capture
- OpenAI Realtime duplex audio
- Barge-in interruption
- Realtime function call
- Home Assistant action through the prototype

Next hardening work:

- package as a managed systemd user service
- separate production Voice Lab bot support
- richer status command
- conversation/session persistence for voice turns
