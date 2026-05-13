# Hermes Discord Realtime

Duplex OpenAI Realtime voice for Hermes Agent in Discord voice channels.

This plugin adds a Discord voice sidecar for Hermes. The normal
`hermes-gateway` keeps handling text, DMs, home channels, cron delivery, slash
commands, and platform events. The realtime sidecar joins a voice channel,
streams audio to OpenAI Realtime, speaks back into Discord, supports barge-in,
and delegates real work to Hermes through one compact tool:

```text
ask_hermes_agent(request)
```

## Architecture

Recommended production layout:

```text
hermes-gateway.service
  normal Hermes Discord/text gateway
  uses DISCORD_BOT_TOKEN

hermes-discord-realtime.service
  voice-only Discord sidecar
  uses DISCORD_REALTIME_BOT_TOKEN
  calls Hermes via ask_hermes_agent
```

This avoids the Discord session conflict that happens when two processes log in
with the same bot token. Same-token lab testing is still supported, but you must
stop `hermes-gateway` while the voice bridge is running.

## Requirements

- Hermes Agent installed and working.
- A Discord server where you can invite bots and edit channel permissions.
- A normal Hermes Discord bot for text, if you use `hermes-gateway`.
- A separate Discord bot for voice, recommended for production.
- OpenAI Platform Realtime key in `OPENAI_REALTIME_API_KEY`.
- `ffmpeg` available on the Hermes host.
- Discord Developer Mode enabled so you can copy server/channel/user IDs.

## Install

```bash
hermes plugins install AllenJvN/hermes-discord-realtime --enable
```

Check the command surface:

```bash
hermes discord-realtime setup
hermes discord-realtime env
```

## Create The Voice Bot

In the Discord Developer Portal:

1. Create an application, for example `Hermes Voice`.
2. Add a bot.
3. Copy the bot token.
4. Copy the application/client ID.
5. Put them in `~/.hermes/.env`:

```bash
DISCORD_REALTIME_BOT_TOKEN=<voice_bot_token>
DISCORD_REALTIME_CLIENT_ID=<voice_bot_client_id>
```

Print an invite URL:

```bash
hermes discord-realtime invite-url
```

Invite the voice bot to your server.

Minimum permissions:

```text
View Channels
Connect
Speak
Use Voice Activity
Send Messages
Read Message History
```

## Configure IDs

Enable Discord Developer Mode:

```text
User Settings -> Advanced -> Developer Mode
```

Copy IDs and add them to `~/.hermes/.env`:

```bash
DISCORD_REALTIME_GUILD_ID=<server_id>
DISCORD_REALTIME_VOICE_CHANNEL_ID=<voice_channel_id>
DISCORD_REALTIME_ALLOWED_USERS=<your_discord_user_id>
OPENAI_REALTIME_API_KEY=<openai_platform_key>
HERMES_REALTIME_AGENT_TOOLSETS=all
```

If the voice channel is private, explicitly allow the voice bot or its role:

```text
View Channel
Connect
Speak
Use Voice Activity
```

## Validate

Run the doctor before starting the service:

```bash
hermes discord-realtime doctor
```

The doctor checks:

- required env vars
- `ffmpeg`
- Python voice dependencies
- bot membership in the Discord server
- voice channel permissions
- allowed user configuration

Fix every `[fail]` before continuing. `[warn]` items are usually safe but worth
reading.

## Run As A Service

Install and start the sidecar:

```bash
hermes discord-realtime install-service
hermes discord-realtime start
```

Inspect:

```bash
hermes discord-realtime status
hermes discord-realtime logs
```

Stop cleanly:

```bash
hermes discord-realtime stop
```

This is the preferred way to make the bot leave voice. Do not kick it unless
you are intentionally testing Discord permission recovery.

## Manual Run

For foreground testing:

```bash
hermes discord-realtime run
```

You can also override config:

```bash
hermes discord-realtime run \
  --guild-id 123456789012345678 \
  --voice-channel-id 234567890123456789 \
  --allowed-user-id 345678901234567890
```

Same-token lab mode:

```bash
systemctl --user stop hermes-gateway

hermes discord-realtime run \
  --guild-id <server_id> \
  --voice-channel-id <voice_channel_id> \
  --allowed-user-id <your_discord_user_id> \
  --discord-bot-token "$DISCORD_BOT_TOKEN"

systemctl --user start hermes-gateway
```

Do not use same-token mode for normal operation.

## Test Prompts

Basic duplex:

```text
Hermes, tell me a long story about a fox and keep going until I stop you.
```

Interrupt while Hermes is speaking:

```text
Stop. Summarize it in one sentence.
```

Tool calling through Hermes:

```text
Hermes, turn on the living room light.
```

The Realtime model should call `ask_hermes_agent`, Hermes should use its normal
tools, and the voice bot should speak the result.

## Tool Calling Model

OpenAI Realtime gets exactly one tool:

```text
ask_hermes_agent(request)
```

The plugin runs a normal Hermes agent turn with configurable toolsets. The
default is:

```bash
HERMES_REALTIME_AGENT_TOOLSETS=all
```

For a tighter setup:

```bash
HERMES_REALTIME_AGENT_TOOLSETS=homeassistant,device_worker,device_coding
```

This keeps the Realtime context small and lets Hermes remain the owner of real
tools, safety behavior, memory, repos, terminals, Home Assistant, and device
workers.

## Security Model

- Voice access is powerful because it can call the normal Hermes agent.
- Restrict `DISCORD_REALTIME_ALLOWED_USERS` to trusted users.
- Prefer a separate voice bot token.
- Keep both Discord bots private to your server.
- Do not expose any local worker ports publicly.
- Built-in checks are guardrails, not a sandbox.

## Troubleshooting

- `403 Missing Access`: the bot is not in the server, cannot view the channel,
  or lacks channel-specific permissions.
- Bot joins voice but text Hermes is silent: make sure `hermes-gateway` is
  running and the text bot can see/send/read in the configured home channel.
- Normal Hermes stops responding during voice: you are probably using same-token
  mode; switch to `DISCORD_REALTIME_BOT_TOKEN`.
- You speak but nothing happens: confirm your Discord user ID is in
  `DISCORD_REALTIME_ALLOWED_USERS`.
- `invalid_api_key`: use an OpenAI Platform key in `OPENAI_REALTIME_API_KEY`.
- No audio: confirm `ffmpeg` and PyNaCl are installed in the Hermes environment.

## Commands

```bash
hermes discord-realtime setup
hermes discord-realtime env
hermes discord-realtime invite-url
hermes discord-realtime doctor
hermes discord-realtime run
hermes discord-realtime install-service
hermes discord-realtime start
hermes discord-realtime stop
hermes discord-realtime restart
hermes discord-realtime status
hermes discord-realtime logs
hermes discord-realtime uninstall-service
```
