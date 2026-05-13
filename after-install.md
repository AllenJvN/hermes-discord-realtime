# Hermes Discord Realtime Installed

Restart the normal Hermes gateway so it loads the plugin:

```bash
hermes gateway restart
```

The normal Hermes Discord gateway should already be working. Confirm these
`~/.hermes/.env` values exist:

```bash
DISCORD_BOT_TOKEN=<existing_hermes_bot_token>
DISCORD_ALLOWED_USERS=<your_discord_user_id>
DISCORD_HOME_CHANNEL=<hermes_home_text_channel_id>
OPENAI_REALTIME_API_KEY=<openai_platform_api_key>
HERMES_REALTIME_AGENT_TOOLSETS=all
```

The plugin uses `OPENAI_REALTIME_API_KEY` when set, otherwise it reuses a valid
OpenAI Platform `OPENAI_API_KEY`. Hermes `openai-codex` OAuth is not enough by
itself for Realtime voice.

In Discord, join a voice channel and run:

```text
/realtime doctor
/realtime join
```

Voice tool calls acknowledge immediately and run Hermes in the background.
Action commands stay silent on success; questions, status checks, searches, and
other information requests speak the result when ready. Action requests avoid
extra verification unless you explicitly ask for status.

During longer tool calls, Hermes posts a compact progress card in the Discord
voice channel chat and edits it as work continues. Set
`HERMES_REALTIME_PROGRESS_TEXT=false` to disable those cards.

To make Hermes leave voice cleanly:

```text
/realtime leave
```

This plugin runs inside `hermes-gateway`; restart the gateway after config
changes.
