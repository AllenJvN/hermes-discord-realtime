# Hermes Discord Realtime Installed

Run:

```bash
hermes discord-realtime env
```

Then join your target Discord voice channel and start the bridge:

```bash
systemctl --user stop hermes-gateway

hermes discord-realtime run \
  --guild-id <guild_id> \
  --voice-channel-id <voice_channel_id> \
  --allowed-user-id <discord_user_id>
```

Restart normal Hermes Discord afterwards:

```bash
systemctl --user start hermes-gateway
```
