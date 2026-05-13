# Agent Notes

This repo is the public plugin source for the Hermes Discord Realtime voice
bridge.

Development rules:

- Keep Realtime's exposed tool surface compact. Prefer one bridge tool,
  `ask_hermes_agent`, over mirroring every Hermes tool schema.
- Keep production Hermes core changes out of this repo unless the plugin API
  truly cannot support the behavior.
- Test against the homelab Hermes VM before pushing user-facing changes when
  possible.
- If copying changes into `/home/allen/.hermes/plugins/hermes-discord-realtime`
  or `/home/allen/.hermes/hermes-agent/experiments`, keep this GitHub repo in
  sync afterward.
- Never commit `.env`, Discord tokens, OpenAI keys, Home Assistant tokens, or
  voice recordings.

Production v1 is a separate voice sidecar bot using
`DISCORD_REALTIME_BOT_TOKEN`. The normal `hermes-gateway` should stay running
with `DISCORD_BOT_TOKEN` for text, DMs, home channels, cron, and slash
commands. Same-token mode is only for temporary lab testing and requires
stopping `hermes-gateway`.
