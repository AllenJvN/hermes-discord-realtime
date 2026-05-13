# Hermes Discord Realtime

OpenAI Realtime duplex voice for the existing Hermes Discord bot.

This is a one-bot Hermes gateway plugin. `hermes-gateway` keeps ownership of
Discord text, DMs, home-channel messages, cron output, Home Assistant events,
and the Discord connection itself. This plugin adds `/realtime` commands that
join the voice channel you are already in.

Realtime sees one tool:

```text
ask_hermes_agent(request)
```

Hermes remains responsible for the actual work: Home Assistant, repos,
terminals, device workers, memory, web, and any other configured tools.

## Install

```bash
hermes plugins install AllenJvN/hermes-discord-realtime --enable
hermes gateway restart
```

Required `~/.hermes/.env` values:

```bash
DISCORD_BOT_TOKEN=<existing_hermes_bot_token>
DISCORD_ALLOWED_USERS=<your_discord_user_id>
DISCORD_HOME_CHANNEL=<hermes_home_text_channel_id>
HERMES_REALTIME_AGENT_TOOLSETS=all
```

For OpenAI Realtime, the plugin uses `OPENAI_REALTIME_API_KEY` when set. If it
is not set, it reuses `OPENAI_API_KEY` when that value looks like an OpenAI
Platform key. Hermes `openai-codex` OAuth login is not enough by itself because
Realtime uses OpenAI Platform API authentication.

If Discord's slash picker is stale after install, type `/realtime doctor` as a
normal text message. The gateway hook handles both native slash commands and
plain text commands.

## Discord Permissions

The Hermes bot needs these permissions in the text channel where you run
`/realtime`:

```text
View Channel
Send Messages
Read Message History
Use Application Commands
```

It needs these permissions in the voice channel:

```text
View Channel
Connect
Speak
Use Voice Activity
```

Private Discord channels usually need explicit channel-level allows for the
bot role.

## Use

Join a Discord voice channel, then run:

```text
/realtime doctor
/realtime join
```

Disconnect cleanly:

```text
/realtime leave
```

Other commands:

```text
/realtime status
/rt join
/rt leave
```

## Test

Basic duplex:

```text
Hermes, tell me a long story about a fox and keep going until I stop you.
```

Barge-in:

```text
Stop. Summarize it in one sentence.
```

Hermes tool calling:

```text
Hermes, turn on the living room light.
```

The same Discord bot should keep responding to DMs or `hermes-home` while voice
is active.

## Tuning

Defaults are usually fine. Override only when needed:

```bash
OPENAI_REALTIME_MODEL=gpt-realtime
OPENAI_REALTIME_VOICE=alloy
OPENAI_REALTIME_VAD_RMS_THRESHOLD=650
OPENAI_REALTIME_VAD_SILENCE_SECONDS=0.75
HERMES_REALTIME_AGENT_TIMEOUT=180
HERMES_REALTIME_AGENT_TOOLSETS=homeassistant,device_worker,device_coding
```

## Troubleshooting

- `/realtime` is unknown: restart `hermes-gateway`; if the slash picker is stale, type `/realtime doctor` as plain text.
- `Set OPENAI_REALTIME_API_KEY or a valid OpenAI Platform OPENAI_API_KEY`: add one of those keys to `~/.hermes/.env` and restart the gateway.
- `Join a Discord voice channel first`: join voice before running `/realtime join`.
- `403 Missing Access`: fix text or voice channel permissions for the bot role.
- You speak but nothing happens: confirm your Discord user ID is in `DISCORD_ALLOWED_USERS`.
- Hermes voice works but text does not: confirm `hermes-gateway` is active and the bot can read/send in `DISCORD_HOME_CHANNEL`.
- The bot stays in voice: run `/realtime leave`; kicking the bot disconnects Discord but bypasses the plugin's clean shutdown path.

## Development

See `AGENTS.md` for maintainer notes and homelab test workflow.
