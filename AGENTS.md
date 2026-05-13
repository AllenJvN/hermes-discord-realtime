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
- If copying changes into `/home/allen/.hermes/plugins/hermes-discord-realtime`,
  keep this GitHub repo in sync afterward.
- Never commit `.env`, Discord tokens, OpenAI keys, Home Assistant tokens, or
  voice recordings.

Production v1 is one-bot gateway mode only. The plugin registers a
`pre_gateway_dispatch` hook and `/realtime` commands; it reuses the existing
Discord adapter/client inside `hermes-gateway` and must not start a second
Discord client. Test by restarting `hermes-gateway`, then using
`/realtime doctor`, `/realtime join`, and `/realtime leave` from Discord while
confirming DMs/text still work.
