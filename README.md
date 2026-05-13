# Hermes Discord Realtime

OpenAI Realtime duplex voice for the existing Hermes Discord bot.

This plugin is intentionally a one-bot integration. The normal
`hermes-gateway` process keeps ownership of Discord DMs, the home channel,
slash commands, cron output, Home Assistant events, and voice. The plugin loads
inside that gateway and adds `/realtime` commands that join the voice channel
you are already in.

Realtime gets one compact tool:

```text
ask_hermes_agent(request)
```

That keeps the voice model small and fast while Hermes remains responsible for
real tools: Home Assistant, repos, terminals, device workers, memory, web, and
other configured capabilities.

## Architecture

```text
Discord
  one Hermes bot token
        |
hermes-gateway.service
  normal Hermes text + platform runtime
        |
hermes-discord-realtime plugin
  /realtime join | leave | status | doctor
        |
OpenAI Realtime + ask_hermes_agent
```

Only `hermes-gateway` owns Discord; there is no extra process to keep alive.

## Requirements

- Hermes Agent installed and already working with Discord.
- `DISCORD_BOT_TOKEN` for the existing Hermes bot.
- `DISCORD_ALLOWED_USERS` set to trusted Discord user IDs.
- `OPENAI_REALTIME_API_KEY` set to an OpenAI Platform key with Realtime access.
- `ffmpeg` available on the Hermes host.
- Discord channel permissions for text replies and voice.

## Install

```bash
hermes plugins install AllenJvN/hermes-discord-realtime --enable
hermes gateway restart
```

If Discord's slash picker does not refresh immediately, type the command as
plain text. The gateway hook handles both forms:

```text
/realtime doctor
```

## Configure

Add or confirm these values in `~/.hermes/.env`:

```bash
DISCORD_BOT_TOKEN=<existing_hermes_bot_token>
DISCORD_ALLOWED_USERS=<your_discord_user_id>
DISCORD_HOME_CHANNEL=<hermes_home_text_channel_id>
OPENAI_REALTIME_API_KEY=<openai_platform_key>
HERMES_REALTIME_AGENT_TOOLSETS=all
```

Optional tuning:

```bash
OPENAI_REALTIME_MODEL=gpt-realtime
OPENAI_REALTIME_VOICE=alloy
OPENAI_REALTIME_VAD_RMS_THRESHOLD=650
OPENAI_REALTIME_VAD_SILENCE_SECONDS=0.75
HERMES_REALTIME_AGENT_TIMEOUT=180
```

## Discord Permissions

For the text channel where you run `/realtime`, allow the Hermes bot or its
role:

```text
View Channel
Send Messages
Read Message History
Use Application Commands
```

For the voice channel, allow:

```text
View Channel
Connect
Speak
Use Voice Activity
```

Private channels usually need explicit channel-level allows even when the bot
was invited with the right global permissions.

## Use

Join a Discord voice channel yourself, then run this in a text channel where
Hermes can read and reply:

```text
/realtime doctor
/realtime join
```

Speak naturally. Hermes should answer in voice, and you can interrupt while it
is talking.

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

## Test Prompts

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

The Realtime model should call `ask_hermes_agent`, Hermes should run the normal
agent/tool flow, and the same Discord bot should continue responding to text
DMs or `hermes-home`.

## Toolsets

By default, the voice bridge allows Hermes to use all configured toolsets:

```bash
HERMES_REALTIME_AGENT_TOOLSETS=all
```

For a tighter setup:

```bash
HERMES_REALTIME_AGENT_TOOLSETS=homeassistant,device_worker,device_coding
```

Realtime still sees only `ask_hermes_agent`; this setting controls what the
Hermes agent may use behind that bridge.

## Troubleshooting

- `/realtime` is unknown: restart `hermes-gateway`; if the slash picker is stale, type `/realtime doctor` as plain text.
- `OPENAI_REALTIME_API_KEY is not set`: add an OpenAI Platform key to `~/.hermes/.env` and restart the gateway.
- `Join a Discord voice channel first`: your Discord user must be in a voice channel before `/realtime join`.
- `403 Missing Access`: fix channel-level permissions for the bot role.
- Hermes voice works but text does not: confirm `hermes-gateway` is active and the bot can view/send/read in `DISCORD_HOME_CHANNEL`.
- You speak but nothing happens: confirm your Discord user ID is in `DISCORD_ALLOWED_USERS`.
- The bot stays in voice: run `/realtime leave`; kicking the bot from Discord disconnects the socket but does not give the plugin a clean shutdown signal.

## Development

See `AGENTS.md` for maintainer notes, homelab test workflow, and repo sync
expectations.
