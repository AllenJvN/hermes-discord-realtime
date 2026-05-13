# Hermes Discord Realtime Installed

Restart the normal Hermes gateway so it loads the plugin:

```bash
hermes gateway restart
```

Required `~/.hermes/.env` values:

```bash
DISCORD_BOT_TOKEN=<existing_hermes_bot_token>
DISCORD_ALLOWED_USERS=<your_discord_user_id>
OPENAI_REALTIME_API_KEY=<openai_platform_key>
HERMES_REALTIME_AGENT_TOOLSETS=all
```

In Discord, join a voice channel and run:

```text
/realtime doctor
/realtime join
```

To make Hermes leave voice cleanly:

```text
/realtime leave
```

This plugin runs inside `hermes-gateway`; restart the gateway after config
changes.
