# Hermes Discord Realtime Installed

Restart the normal Hermes gateway so it loads the plugin:

```bash
hermes gateway restart
```

Required `~/.hermes/.env` values:

```bash
DISCORD_BOT_TOKEN=<existing_hermes_bot_token>
DISCORD_ALLOWED_USERS=<your_discord_user_id>
DISCORD_HOME_CHANNEL=<hermes_home_text_channel_id>
HERMES_REALTIME_AGENT_TOOLSETS=all
```

The plugin uses `OPENAI_REALTIME_API_KEY` when set, otherwise it reuses a valid
OpenAI Platform `OPENAI_API_KEY`.

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
