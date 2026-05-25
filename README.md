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
    "local_rtmp_addr": "127.0.0.1:1935",
    "server_url": "https://your-server-ip:8443",
    "auth_token": "a-strong-random-shared-secret-token",
    "concurrency": 4,
    "chunk_duration": 2,
    "obfuscation_key": "some-secret-xor-obfuscation-key",
    "temp_dir": "./tmp_client",
    "use_docker": true,
    "docker_image": "linuxserver/ffmpeg"
  },
  "server": {
    "listen_addr": ":8443",
    "auth_token": "a-strong-random-shared-secret-token",
    "obfuscation_key": "some-secret-xor-obfuscation-key",
    "buffer_duration": 60,
    "chunk_duration": 2,
    "target_rtmp_url": "rtmp://a.rtmp.youtube.com/live2/xxxx-xxxx-xxxx-xxxx-xxxx",
    "temp_dir": "./tmp_server",
    "tls_cert_file": "/etc/letsencrypt/live/your-server-ip/fullchain.pem",
    "tls_key_file": "/etc/letsencrypt/live/your-server-ip/privkey.pem",
    "use_docker": true,
    "docker_image": "linuxserver/ffmpeg"
  }
}
```

### Key Parameters:
- `"use_docker"`: Set to `true` to use Dockerized FFmpeg, or `false` to use local host FFmpeg.
- `"obfuscation_key"`: A shared key used to XOR-encrypt chunk data. Must be **identical** on both the client and server.
- `"auth_token"`: The authorization bearer token used by the server to validate requests.

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
Configure your encoder (e.g., OBS) to stream to:
- **Server:** `rtmp://127.0.0.1:1935/live/app`
- **Stream Key:** (can be left blank or filled with any dummy key)

Once OBS starts streaming, the client will segment the video, obfuscate the chunks, and upload them. The server will buffer them and push the reconstructed stream to the configured `target_rtmp_url` (YouTube).

---

## Troubleshooting

### Decryption Warning
If the server logs:
`[Receiver] WARNING: Decrypted chunk does not start with MPEG-TS sync byte (0x47, 'G')...`
It means the server was unable to decrypt the incoming chunk. Double-check that `"obfuscation_key"` is identical in both the client and server configuration, and that the server successfully loaded its `config.json` file.

### Port Conflicts
If you receive a `port is already allocated` or `bind: address already in use` error:
- On the server: Stop any existing server processes running in the background:
  ```bash
  killall rtmp-http-tunnel
  ```
- On the client (Docker): Clean up any orphaned containers that are still holding the port:
  ```bash
  docker rm -f $(docker ps -a -q --filter ancestor=linuxserver/ffmpeg)
  ```
