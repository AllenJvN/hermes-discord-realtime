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

Known homelab test IDs:

- guild: `1210302880897310820`
- voice channel: `1503132822892707861`
- allowed user: `691556762846887968`

The current bridge reuses the existing Hermes Discord bot token, so
`hermes-gateway` must be stopped while running it. A separate Discord bot token
is the preferred next step for production.
