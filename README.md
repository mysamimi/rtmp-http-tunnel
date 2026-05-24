# rtmp-http-tunnel

`rtmp-http-tunnel` is a lightweight, open-source streaming relay tool designed to bypass network throttling, Deep Packet Inspection (DPI), and high-jitter network environments (such as restricted internet connections) when live streaming to platforms like YouTube, Twitch, or Aparat.

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
- **Concurrent Chunk Uploads:** Leverages a worker pool to upload segments in parallel, naturally utilizing multi-connection load balancers or software network bonding.
- **XOR Obfuscation:** Encrypts chunk bytes with a simple key on the application layer to bypass Deep Packet Inspection (DPI) that targets video headers.
- **Resilient Jitter Buffering:** The server buffers a configurable sliding window of chunks (e.g., 60s) before feeding them to the ingest platform, absorbing up to 60s of complete client-side disconnections without dropping the target stream.
- **Lightweight Go Implementation:** Static binary with zero heavy external library dependencies.
- **Automatic Recovery:** Client retries failed uploads with exponential backoff; Server recovers from empty buffers by pausing and re-buffering dynamically.

---

## Requirements
- **Go** (1.20 or later)
- **FFmpeg** installed and added to the PATH on both Client and Server machines.
  - Client uses FFmpeg for HLS segmentation.
  - Server uses FFmpeg for RTMP streaming output.

---

## Configuration & Usage

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/yourusername/rtmp-http-tunnel.git
   cd rtmp-http-tunnel
   ```

2. **Prepare configuration:**
   Copy `config.json.example` to `config.json` and adjust parameters.
   ```bash
   cp config.json.example config.json
   ```

3. **Running the Server (VPS / Data Center):**
   Ensure your firewall allows traffic on the listening port (e.g., `8443`). Run:
   ```bash
   go run main.go -mode server -config config.json
   ```

4. **Running the Client (Local Machine):**
   Run the client program:
   ```bash
   go run main.go -mode client -config config.json
   ```

5. **Start Streaming:**
   Configure your encoder (e.g., OBS) to stream to:
   - **Stream Type:** Custom...
   - **Server:** `rtmp://127.0.0.1:1935`
   - **Stream Key:** `app` (or any string)

Once OBS starts streaming, the client will segment the video, upload the encrypted chunks, and the server will buffer and push the reconstructed stream to the configured `target_rtmp_url` (YouTube).

---

## Technical Details

### Security
To protect the server endpoint, all POST requests must supply the correct shared token in the headers:
```
Authorization: Bearer <auth_token>
```
Additionally, chunks are XOR-scrambled using the `obfuscation_key` before transmission and descrambled on arrival.

### Network Jitter Compensation
If the client's network upload speed drops, the server's buffer drains. If it drains completely, the server pauses feeding FFmpeg, waits for the buffer to fill back up to the threshold, and resumes. Meanwhile, FFmpeg handles small timeline offsets gracefully without dropping the RTMP push connection.
