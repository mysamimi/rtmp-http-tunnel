# Deploying the server on a VPS

A single small VPS is enough: the server never transcodes (`-c copy`), so 1 vCPU, 1 GB RAM and 10-20 GB disk handle a stream or two. What matters is that the VPS is **reachable from where your client runs** and has a stable uplink. Test that before relying on it (see "Checks").

These steps assume Debian/Ubuntu and a user with sudo.

## 1. Install FFmpeg and build the binary

```bash
sudo apt update && sudo apt install -y ffmpeg golang-go   # or install Go 1.20+ from go.dev
git clone https://github.com/mysamimi/rtmp-http-tunnel.git
cd rtmp-http-tunnel && go build -o rtmp-http-tunnel .
```

Use the native FFmpeg on the server (`"use_docker": false`); Docker is not needed here. (The RTMP *listen* limitation of some FFmpeg builds only affects the client.)

## 2. Create a service user and directories

```bash
sudo useradd --system --home /opt/rtmp-http-tunnel --shell /usr/sbin/nologin tunnel
sudo mkdir -p /opt/rtmp-http-tunnel/tmp
sudo cp rtmp-http-tunnel /opt/rtmp-http-tunnel/
```

## 3. TLS certificate

Without a domain, create a self-signed certificate and pin it in the client:

```bash
cd /opt/rtmp-http-tunnel
sudo openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout key.pem -out cert.pem -days 825 -subj "/CN=YOUR-SERVER-IP"
sudo openssl x509 -in cert.pem -noout -fingerprint -sha256
```

Put the fingerprint (colons are fine) into the client's `tls_pin_sha256`. With a domain and a Let's Encrypt certificate you can skip the pin, but the domain must resolve correctly from the client's network.

## 4. Configuration

Generate the secrets and write `/opt/rtmp-http-tunnel/config.json`:

```bash
openssl rand -hex 32   # auth_token
openssl rand -hex 16   # obfuscation_key
```

```json
{
  "mode": "server",
  "server": {
    "listen_addr": ":443",
    "auth_token": "<auth_token>",
    "obfuscation_key": "<obfuscation_key>",
    "buffer_duration": 60,
    "rebuffer_duration": 4,
    "chunk_duration": 2,
    "inactive_session_timeout": 120,
    "max_buffered_chunks": 90,
    "tls_cert_file": "/opt/rtmp-http-tunnel/cert.pem",
    "tls_key_file": "/opt/rtmp-http-tunnel/key.pem",
    "use_docker": false,
    "streams": [
      {
        "path": "main",
        "target_rtmp_url": "rtmp://a.rtmp.youtube.com/live2/YOUR-STREAM-KEY",
        "temp_dir": "/opt/rtmp-http-tunnel/tmp/main"
      }
    ]
  }
}
```

```bash
sudo chown -R tunnel:tunnel /opt/rtmp-http-tunnel
sudo chmod 600 /opt/rtmp-http-tunnel/config.json /opt/rtmp-http-tunnel/key.pem   # the stream key lives here
```

`buffer_duration` is both the start-up delay and the longest client outage the stream can ride out. Nothing is sent to YouTube until the buffer is full.

The config is read once at start: **restart the service after every change** (including a new stream key).

## 5. systemd service

`/etc/systemd/system/rtmp-http-tunnel.service`:

```ini
[Unit]
Description=rtmp-http-tunnel server
After=network-online.target
Wants=network-online.target

[Service]
User=tunnel
WorkingDirectory=/opt/rtmp-http-tunnel
ExecStart=/opt/rtmp-http-tunnel/rtmp-http-tunnel -mode server -config /opt/rtmp-http-tunnel/config.json
AmbientCapabilities=CAP_NET_BIND_SERVICE
Restart=always
RestartSec=3
NoNewPrivileges=true
ProtectSystem=full
ReadWritePaths=/opt/rtmp-http-tunnel

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now rtmp-http-tunnel
sudo journalctl -u rtmp-http-tunnel -f
```

## 6. Firewall

```bash
sudo ufw allow 22/tcp     # keep SSH reachable first
sudo ufw allow 443/tcp
sudo ufw enable
```

## 7. Client side

On the machine next to your encoder (see the main README for all options):

```json
{
  "mode": "client",
  "client": {
    "server_url": "https://YOUR-SERVER-IP",
    "auth_token": "<same auth_token>",
    "obfuscation_key": "<same obfuscation_key>",
    "tls_pin_sha256": "<fingerprint from step 3>",
    "concurrency": 4,
    "chunk_duration": 2,
    "use_docker": true,
    "streams": [
      { "stream_path": "main", "input_url": "rtmp://0.0.0.0:1935", "temp_dir": "./tmp_client/main" }
    ]
  }
}
```

`stream_path` must equal the server's `path`. In OBS set the keyframe interval to **2 seconds** and use H.264 + AAC. The stream key in OBS is irrelevant; the real key only exists in the server config.

## Checks

Before the first stream, from the client's network:

```bash
curl -vk https://YOUR-SERVER-IP/                       # expect a TLS handshake and "404 page not found"
curl -sk -H "Authorization: Bearer <auth_token>" https://YOUR-SERVER-IP/s/main/health
# {"status":"ok","buffer_duration":60,"chunk_duration":2}
```

A `404` on the second command means a wrong token, `stream_path` or URL. The server answers every unauthenticated request with a plain 404 on purpose.

## What the logs should show (server)

1. `[Receiver] Successfully received chunk ...` - uploads are arriving.
2. `[Buffer] Buffering before output starts: N/30 chunks` - counting up; nothing reaches YouTube yet.
3. `[Buffer] Buffer filled ... Starting streaming` then `[Streamer] Starting FFmpeg restreamer output to: rtmp://a.rtmp.youtube.com/live2/ab***`.
4. FFmpeg output. Errors such as `Input/output error` or `Server error` mean YouTube rejected the connection; the server also logs a warning with a checklist if FFmpeg stops within 10 seconds.

YouTube's dashboard may take another 10-30 seconds to show "receiving data". If the old key keeps working after you changed the config, the service was not restarted.

## If YouTube says "no data"

- Wait for the buffer to fill (step 2 above) plus about 30 seconds.
- Confirm the service was restarted after editing the key, and the log shows the new key prefix.
- In YouTube Studio the stream must be created and live streaming must be enabled on the channel (new channels can need up to 24 hours).
- Check the URL form: `rtmp://a.rtmp.youtube.com/live2/<key>` with no spaces.
