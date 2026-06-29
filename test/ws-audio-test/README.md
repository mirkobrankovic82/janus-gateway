# ws-audio-test

CLI to validate Janus AudioBridge **plain RTP** and core **RTP-over-WebSocket** (`rtpws`).

## Prerequisites

- Janus built with `--enable-websockets` (libwebsockets)
- AudioBridge plugin enabled
- Core media listener enabled in `janus.jcfg`:

```jcfg
rtpws: {
    enable_rtp_ws = true
    port = 8190
    path = "/rtp-ws"
    # public_url = "wss://media.example.com"   # optional; returned in join response
    # secure = true
    # cert_pem = "/path/to/cert.pem"
    # cert_key = "/path/to/key.pem"
}
```

- Room allows WS participants (`allow_ws_participants = true`, or let the CLI create the room with that flag)
- HMAC token if `token_auth_secret` is configured (`export TOKEN=...`)

## Build

```bash
cd test/ws-audio-test
go build -o bin/ws-audio-test .
```

## Commands

| Command | What it tests |
|---------|----------------|
| `signaling` | Room create + join; prints `websocket_media.url` or `rtp` details |
| `plain-rtp` | UDP RTP **in** (tone → Janus) and **out** (mix → client) with counters |
| `ws-stream` | Join with `media=websocket`, then WS binary RTP **in** and **out** |
| `ws-e2e` | Plain-RTP feeder + WS participant; verifies **outbound** mix over WS |

### Quick run

```bash
export JANUS_HTTP=http://127.0.0.1:8088/janus
export TOKEN=your-hmac-token   # if token auth is enabled
export ROOM=ws-audio-test

# Signaling only
go run . signaling --janus-http "$JANUS_HTTP" --token "$TOKEN" --room "$ROOM" --media websocket

# WS bidirectional (auto-join)
go run . ws-stream --janus-http "$JANUS_HTTP" --token "$TOKEN" --room "$ROOM" --tone --expect-rx 50

# Full outbound mix test: plain-RTP talks, WS leg receives mix
go run . ws-e2e --janus-http "$JANUS_HTTP" --token "$TOKEN" --room "$ROOM" --local-ip 127.0.0.1 --expect-rx 50
```

`ws-stream` also accepts `--ws-url` directly if you already have a `websocket_media.url` from a prior join.

### Bidirectional checks

All streaming modes track `tx` / `rx` packet counts and bytes. Use `--expect-rx N` to fail if Janus did not deliver at least `N` inbound RTP packets (outbound leg).

```bash
# Plain RTP both ways
go run . plain-rtp \
  --janus-http "$JANUS_HTTP" --token "$TOKEN" --room "$ROOM" --local-ip 127.0.0.1 \
  --tone --expect-rx 50

# WS media both ways (auto-join)
go run . ws-stream \
  --janus-http "$JANUS_HTTP" --token "$TOKEN" --room "$ROOM" \
  --tone --expect-rx 50

# Or connect to an existing media URL
go run . ws-stream \
  --ws-url 'ws://127.0.0.1:8190/rtp-ws?sid=...' \
  --tone --expect-rx 50
```

## Protocol

1. **Signaling**: normal Janus API — `join` with `"media": "websocket"`; AudioBridge returns `websocket_media.url`.
2. **Media**: connect to that URL; server sends JSON `call_info` (text frame).
3. Client and server exchange **binary** WebSocket frames containing full RTP packets (12+ bytes).
