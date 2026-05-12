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
- Discord Developer Mode enabled so you can copy IDs for server, voice channel,
  and allowed users.

## Install

```bash
hermes plugins install AllenJvN/hermes-discord-realtime --enable
```

Check env:

```bash
hermes discord-realtime env
```

Expected output should show `DISCORD_BOT_TOKEN` and
`OPENAI_REALTIME_API_KEY` as set. It is okay if `OPENAI_API_KEY` is unset or is
used for another provider; Realtime should use `OPENAI_REALTIME_API_KEY`.

## Get Discord IDs

In Discord, enable Developer Mode:

```text
User Settings -> Advanced -> Developer Mode
```

Then copy:

- Server ID: right-click the Discord server icon, then `Copy Server ID`.
- Voice channel ID: right-click the voice channel, then `Copy Channel ID`.
- User ID: right-click your user profile, then `Copy User ID`.

If right-click is awkward on mobile, get the IDs from desktop first and reuse
them in your command.

## Run

Join the Discord voice channel yourself first. Then run the bridge on the
Hermes machine.

If you are reusing the same Discord bot token as the normal Hermes gateway:

```bash
systemctl --user stop hermes-gateway

hermes discord-realtime run \
  --guild-id <discord_server_id> \
  --voice-channel-id <voice_channel_id> \
  --allowed-user-id <your_discord_user_id>
```

If you use a separate Discord bot token for voice, set `DISCORD_BOT_TOKEN` for
that shell or service and you do not need to stop `hermes-gateway`.

Example with placeholder IDs:

```bash
hermes discord-realtime run \
  --guild-id 123456789012345678 \
  --voice-channel-id 234567890123456789 \
  --allowed-user-id 345678901234567890
```

## Test It

Basic duplex voice:

```text
Hermes, tell me a long story about a fox and keep going until I stop you.
```

Then interrupt while Hermes is speaking:

```text
Stop. Summarize it in one sentence.
```

Tool calling through Hermes:

```text
Hermes, turn on the living room light.
```

The model should call `ask_hermes_agent`, the normal Hermes agent should use
its configured tools, and Hermes should speak the result.

Rollback:

```bash
Ctrl+C
systemctl --user start hermes-gateway
```

Confirm normal Discord Hermes is back:

```bash
systemctl --user status hermes-gateway
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
- Start with a narrow `--agent-toolsets` value if you do not want full Hermes
  capability from voice.
- Do not expose any worker or voice bridge ports publicly.
- Prefer a separate Discord bot token for long-term production so normal
  `hermes-gateway` can keep running.

## Troubleshooting

- `Improper token has been passed`: check `DISCORD_BOT_TOKEN`.
- `invalid_api_key`: check `OPENAI_REALTIME_API_KEY`; ChatGPT/Codex backend
  auth is not the same as an OpenAI Platform Realtime key.
- Bot does not join: confirm the bot is invited to the server and has Connect
  and Speak permissions for the voice channel.
- You speak but nothing happens: confirm your Discord user ID is included in
  `--allowed-user-id`.
- Normal Hermes Discord stopped responding: restart `hermes-gateway` after
  stopping this bridge.

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
