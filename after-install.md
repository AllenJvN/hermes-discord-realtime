# Hermes Discord Realtime Installed

Run:

```bash
hermes discord-realtime env
```

Enable Discord Developer Mode and copy:

- server ID
- voice channel ID
- your Discord user ID

Join your target Discord voice channel yourself first. If you are reusing the
same Discord bot token as the normal Hermes gateway, stop the gateway and start
the bridge:

```bash
systemctl --user stop hermes-gateway

hermes discord-realtime run \
  --guild-id <guild_id> \
  --voice-channel-id <voice_channel_id> \
  --allowed-user-id <discord_user_id>
```

Try:

```text
Hermes, tell me a long story about a fox and keep going until I stop you.
```

Interrupt with:

```text
Stop. Summarize it in one sentence.
```

Restart normal Hermes Discord afterwards:

```bash
Ctrl+C
systemctl --user start hermes-gateway
```
