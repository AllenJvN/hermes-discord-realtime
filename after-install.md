# Hermes Discord Realtime Installed

Recommended production flow:

```bash
hermes discord-realtime setup
hermes discord-realtime env
hermes discord-realtime invite-url
hermes discord-realtime doctor
hermes discord-realtime install-service
hermes discord-realtime start
```

Use a separate voice bot token in `DISCORD_REALTIME_BOT_TOKEN` so the normal
`hermes-gateway` keeps handling DMs, `hermes-home`, cron, and slash commands.

Required `~/.hermes/.env` values:

```bash
DISCORD_REALTIME_BOT_TOKEN=<voice_bot_token>
DISCORD_REALTIME_CLIENT_ID=<voice_bot_client_id>
DISCORD_REALTIME_GUILD_ID=<server_id>
DISCORD_REALTIME_VOICE_CHANNEL_ID=<voice_channel_id>
DISCORD_REALTIME_ALLOWED_USERS=<your_discord_user_id>
OPENAI_REALTIME_API_KEY=<openai_platform_key>
HERMES_REALTIME_AGENT_TOOLSETS=all
```

To make the bot leave voice cleanly:

```bash
hermes discord-realtime stop
```
