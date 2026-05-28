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
    "streams": [
      {
        "stream_path": "stream_abc123",
        "local_rtmp_addr": "127.0.0.1:1935",
        "temp_dir": "./tmp_client/stream_abc123"
      },
      {
        "stream_path": "stream_xyz456",
        "local_rtmp_addr": "127.0.0.1:1936",
        "temp_dir": "./tmp_client/stream_xyz456"
      }
    ]
  },
  "server": {
    "listen_addr": ":8443",
    "auth_token": "a-strong-random-shared-secret-token",
    "obfuscation_key": "some-secret-xor-obfuscation-key",
    "buffer_duration": 60,
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
- `"streams"` (Client): Array of local streaming pipelines, defining:
  - `"stream_path"`: Server route identifier (`/s/{stream_path}/upload`).
  - `"local_rtmp_addr"`: Local port OBS/Encoder will connect to.
  - `"temp_dir"`: Path for localized chunking assets.
- `"streams"` (Server): Array of static registered path definitions.
  - `"path"`: Expected URL route identifier.
  - `"target_rtmp_url"`: Outbound RTMP ingest server (e.g. YouTube RTMP URL + Key).
  - `"temp_dir"`: Path for localized server buffered chunks.

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

## Troubleshooting

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
