# rtmp-http-tunnel

`rtmp-http-tunnel` is a lightweight, highly resilient open-source streaming relay tool written in Go. It is designed to bypass network throttling, Deep Packet Inspection (DPI), and high-jitter network environments (such as restricted internet connections) when live streaming to platforms like YouTube, Twitch, or Aparat.

## The Problem
Standard RTMP streaming works over a single, persistent TCP connection (usually port 1935). On unstable or highly censored networks, this single connection is easily throttled, suffers from packet loss (causing stream disconnects), or is blocked entirely by firewalls. Furthermore, network bonding (combining multiple WAN connections) cannot easily distribute a single raw RTMP stream dynamically.

## The Solution
`rtmp-http-tunnel` solves this by splitting your local RTMP stream into small, obfuscated HLS segments (typically 2 seconds each) and uploading them concurrently using a pool of workers over secure HTTPS (port 443). 

The remote server buffers these segments (e.g., for 60 seconds) to absorb packet loss and network jitter, reconstructs them sequentially, and pipes them back into a continuous RTMP stream directed at YouTube/Twitch.

```
+----------+          +-------------------------+          +-------------------------+          +-----------------+
|  OBS /   |  RTMP    |      Local Client       |  HTTPS   |      Remote Server      |  RTMP    |  YouTube/Twitch |
|  Encoder | -------> | (RTMP Ingest -> Chunks) | =======> | (Jitter Buffer -> RTMP) | -------> |   Ingest Server |
+----------+ (Port    +-------------------------+  POSTs   +-------------------------+ (Port    +-----------------+
              1935)                               (Port                                 1935)
                                                   443)
```

---

## Features
- **Multi-Stream Support (Multi-Client & Multi-Server):** Relay multiple independent streams simultaneously over the same server port by routing them to dynamic path endpoints (e.g. `/s/stream_abc/upload`).
- **Smart Expiry Policy:** The client queries the server's `/health` endpoint to discover buffer duration configurations dynamically. It discards obsolete chunks local-side (and avoids uploading them) if they exceed the server's window to conserve bandwidth.
- **Background Inactive Session Cleanup:** The server runs a background routine to safely terminate inactive stream session processes, releasing ports, docker containers, and cleaning up temporary disk assets.
- **Dockerized FFmpeg Integration:** Run video processing inside lightweight containers. No need to install and configure local FFmpeg builds on either the client or the server if Docker is available.
- **Concurrent Chunk Uploads:** Leverages a worker pool to upload segments in parallel, naturally utilizing multi-connection load balancers or software network bonding.
- **DPI-proof XOR Obfuscation:** Scrambles chunk bytes with a simple key on the application layer to bypass Deep Packet Inspection (DPI) that targets video headers.
- **MPEG-TS Integrity Verification:** Automatically validates decrypted packets on arrival to instantly warn you if there is an `obfuscation_key` mismatch.
- **Resilient Jitter Buffering:** The server buffers a configurable sliding window of chunks (e.g., 60s) before feeding them to the ingest platform, absorbing up to 60s of complete client-side disconnections without dropping the target stream.
- **Graceful Shutdown:** Implements proper OS signal capturing. Closing the client or server via `Ctrl+C` immediately stops subprocesses, shuts down HTTP listeners, releases ports, and terminates Docker containers cleanly.

---

## Requirements
- **Go** (1.20 or later)
- **Option A (Recommended):** **Docker** running on both Client and Server machines (the app will manage `linuxserver/ffmpeg` containers automatically).
- **Option B (Native):** **FFmpeg** installed and added to the PATH on both machines. Note: On the client side, your FFmpeg build must support RTMP listening (`-listen 1`). FFmpeg builds with `librtmp` enabled may fail to listen; standard Homebrew or Linux package manager builds are recommended.

---

## Configuration

Copy `config.json.example` to `config.json` and adjust parameters:

```json
{
  "mode": "client",
  "client": {
    "server_url": "https://your-server-ip:8443",
    "auth_token": "a-strong-random-shared-secret-token",
    "obfuscation_key": "some-secret-xor-obfuscation-key",
    "concurrency": 4,
    "chunk_duration": 2,
    "use_docker": true,
    "docker_image": "linuxserver/ffmpeg",
    "max_upload_retries": 5,
    "tls_pin_sha256": "hex-sha256-of-the-server-certificate",
    "streams": [
      {
        "stream_path": "stream_abc123",
        "input_url": "rtmp://0.0.0.0:1935",
        "temp_dir": "./tmp_client/stream_abc123"
      },
      {
        "stream_path": "stream_xyz456",
        "input_url": "srt://0.0.0.0:9000",
        "temp_dir": "./tmp_client/stream_xyz456"
      }
    ]
  },
  "server": {
    "listen_addr": ":8443",
    "auth_token": "a-strong-random-shared-secret-token",
    "obfuscation_key": "some-secret-xor-obfuscation-key",
    "buffer_duration": 60,
    "rebuffer_duration": 4,
    "chunk_duration": 2,
    "inactive_session_timeout": 120,
    "tls_cert_file": "/etc/letsencrypt/live/your-server-ip/fullchain.pem",
    "tls_key_file": "/etc/letsencrypt/live/your-server-ip/privkey.pem",
    "use_docker": true,
    "docker_image": "linuxserver/ffmpeg",
    "streams": [
      {
        "path": "stream_abc123",
        "target_rtmp_url": "rtmp://a.rtmp.youtube.com/live2/key-for-stream-1",
        "temp_dir": "./tmp_server/stream_abc123"
      },
      {
        "path": "stream_xyz456",
        "target_rtmp_url": "rtmp://a.rtmp.youtube.com/live2/key-for-stream-2",
        "temp_dir": "./tmp_server/stream_xyz456"
      }
    ]
  }
}
```

### Key Parameters:
- `"concurrency"`: Number of concurrent HTTP uploader workers per client stream.
- `"max_upload_retries"`: Number of times client attempts uploading a chunk before skipping/discarding.
- `"inactive_session_timeout"`: Server-side inactivity timeout (seconds). If no chunk uploads are received for a stream within this duration, the server terminates its streamer process and cleans up.
- `"tls_pin_sha256"` (Client): Hex SHA-256 fingerprint of the server's (leaf) certificate. When set, only that exact certificate is accepted, which is the recommended way to use a self-signed certificate. Get it on the server with:
  ```bash
  openssl x509 -in fullchain.pem -noout -fingerprint -sha256
  ```
  (colons and upper case are accepted).
- `"tls_ca_file"` (Client): Path to a PEM file with a private CA to trust instead of the system roots.
- `"insecure_skip_verify"` (Client): Disables server certificate verification entirely. Off by default; avoid it, since anyone on the path can then read the auth token and stream.
- `"auth_token"`: Required shared bearer token. The client and server values must match.
- `"obfuscation_key"`: Required shared XOR key. The client and server values must match.
- `"buffer_duration"` and `"chunk_duration"`: On the server, `buffer_duration` must be greater than `chunk_duration`; otherwise the server refuses to start.
- `"rebuffer_duration"`: Optional fast-resume buffer after a stream has already started once. Defaults to roughly two chunks when omitted. Set it to a small value like `4` to resume quickly after a client restart while keeping a larger initial `buffer_duration`.
- `"streams"` (Client): Array of local streaming pipelines, defining:
  - `"stream_path"`: Server route identifier (`/s/{stream_path}/upload`).
  - `"input_url"`: Universal input URL. Supported formats:
    - RTMP (Listener): `rtmp://0.0.0.0:1935` (zero IP host binds listener)
    - RTMP (Pull/Connect): `rtmp://192.168.1.100:1935/live/app`
    - SRT (Listener): `srt://0.0.0.0:9000` (auto-appends `?mode=listener`)
    - SRT (Caller/Pull): `srt://192.168.1.100:9000` (auto-appends `?mode=caller`)
    - UDP (Listener): `udp://0.0.0.0:1234` (auto-appends `?listen`)
    - UDP (Multicast Pull): `udp://239.1.1.1:1234`
    - HTTP/HLS: `http://example.com/stream.m3u8`
  - `"temp_dir"`: Path for localized chunking assets.
- `"streams"` (Server): Array of static registered path definitions.
  - `"path"`: Expected URL route identifier.
  - `"target_rtmp_url"`: Outbound RTMP ingest server (e.g. YouTube RTMP URL + Key).
  - `"temp_dir"`: Path for localized server buffered chunks.

### Startup Validation

The application requires a valid configuration file. If `config.json` is missing or invalid, startup fails instead of falling back to unsafe defaults.

Validation checks include:
- `mode` must be `client` or `server` (or overridden with `-mode`).
- Client `server_url` must be `http` or `https`.
- `auth_token`, `obfuscation_key`, stream paths, input URLs, target URLs, and temp directories must be present.
- Stream paths may contain only letters, numbers, `.`, `_`, and `-`; slashes are not allowed because paths are used in `/s/{stream_path}/...` routes.
- Stream paths must be unique within the selected client or server configuration.
- TLS certificate and key paths must be configured together.

---

## Usage

### 1. Running the Server (VPS / Data Center)
Ensure your firewall allows traffic on the listening port (e.g., `8443`). Run:
```bash
go run main.go -mode server -config config.json
```

### 2. Running the Client (Local Machine)
Run the client program:
```bash
go run main.go -mode client -config config.json
```

### 3. Start Streaming
Configure your encoder(s) (e.g., OBS) to stream to their respective ports.
For example, for the first stream:
- **Server:** `rtmp://127.0.0.1:1935/live/app`
- **Stream Key:** (can be left blank or filled with any dummy key)

For the second stream:
- **Server:** `rtmp://127.0.0.1:1936/live/app`
- **Stream Key:** (can be left blank or filled with any dummy key)

Once OBS starts streaming, the client segments, obfuscates, and relays the chunks to `/s/{stream_path}/upload`. The server buffers them in isolations and forwards the reconstructed stream to the matching `target_rtmp_url`.

---

## Safety Limits

- Upload requests are capped at 50 MB per chunk. Oversized chunks are rejected with HTTP `413` so corrupted/truncated media is not accepted silently.
- The client discards chunks that are too old for the server buffer window. If the server health endpoint is unreachable, the client uses a conservative fallback window.
- The client verifies the server's TLS certificate by default. For a self-signed certificate set `tls_pin_sha256` (or `tls_ca_file`); `insecure_skip_verify` exists only as an explicit opt-out. Plain HTTP is supported but unencrypted, so use HTTPS for real deployments.
- Requests with a missing or wrong `auth_token`, unknown paths and the global `/health` without a token all get the same plain `404 page not found` response, so the server does not advertise itself to probes. Consequently a `404` seen by the client can mean a wrong token, a wrong `stream_path` or a wrong `server_url`.

---

## Troubleshooting

### Missing or Invalid Config
If startup fails with `Invalid configuration`, fix the field named in the log. The application does not run with placeholder defaults.

### TLS Errors (`x509: certificate signed by unknown authority`, `fingerprint ... does not match`)
The server certificate is verified by default. Set `tls_pin_sha256` to the server certificate's fingerprint (see Key Parameters). After renewing a certificate, update the pin, unless the key and certificate are reused.

### Decryption Warning
If the server logs:
`[Receiver] WARNING: Decrypted chunk does not start with MPEG-TS sync byte (0x47, 'G')...`
It means the server was unable to decrypt the incoming chunk. Double-check that `"obfuscation_key"` is identical in both the client and server configuration.

### Port Conflicts
If you receive a `port is already allocated` or `bind: address already in use` error:
- On the server: Stop any existing server processes running in the background:
  ```bash
  killall rtmp-http-tunnel
  ```
- On the client (Docker): Clean up any orphaned containers that are still holding the port:
  ```bash
  docker rm -f $(docker ps -a -q --filter name=rtmp-http-tunnel-client-)
  ```
