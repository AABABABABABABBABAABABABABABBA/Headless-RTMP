# headless-rtmp

A small, fast RTMP relay server written in Go. One static binary, no dependencies, designed to sit on a VPS and run 24/7.

Accepts RTMP publishers (OBS, FFmpeg, anything that speaks `rtmp://`) and fans the stream out to any number of RTMP players.

## Why

Most RTMP servers are heavy node processes or nginx-rtmp forks. This one is a single Go binary, ~9 MB, no state on disk, minimal RAM, and designed to be hands-off on a small VPS. Build it, drop the systemd unit, forget about it.

## Features

- RTMP 1.0 publish and play (handshake + chunk stream + AMF0 commands)
- Per-stream GOP cache so late joiners see video immediately
- Stream-key auth (optional)
- Per-player bounded queue — slow players get dropped frames, never slow the publisher down
- Graceful shutdown
- HTTP endpoints: `/healthz`, `/stats`, `/streams`
- systemd unit with hardening
- Dockerfile

Scope is deliberately small: ingest and relay. No HLS, no DASH, no transcoding.

## Quick start

```
go build -o rtmpd ./cmd/rtmpd
./rtmpd
```

Publish:

```
ffmpeg -re -i input.mp4 -c copy -f flv rtmp://localhost:1935/live/mystream
```

Play:

```
ffplay rtmp://localhost:1935/live/mystream
```

With a publish key:

```
PUBLISH_KEY=secret123 ./rtmpd
# publisher uses:
rtmp://host:1935/live/mystream?key=secret123
```

## Config

Everything is env vars (or a `KEY=value` file passed with `-config`). Defaults in parens.

| var             | default | meaning                                           |
|-----------------|---------|---------------------------------------------------|
| `RTMP_ADDR`     | `:1935` | RTMP bind address                                 |
| `HTTP_ADDR`     | `:8080` | admin HTTP bind address                           |
| `RTMP_APP`      | `live`  | only this app name is accepted                    |
| `PUBLISH_KEY`   | empty   | if set, publishers must pass `?key=...`           |
| `MAX_PUBLISHERS`| `128`   | advisory cap                                      |
| `MAX_PLAYERS`   | `4096`  | advisory cap                                      |
| `READ_TIMEOUT`  | `30`    | seconds                                           |
| `WRITE_TIMEOUT` | `30`    | seconds                                           |
| `CHUNK_SIZE`    | `4096`  | outgoing chunk size                               |
| `GOP_CACHE`     | `true`  | cache most recent GOP for late joiners            |

## HTTP endpoints

- `GET /healthz` — `ok`
- `GET /streams` — JSON list of active streams
- `GET /stats` — streams + goroutine/heap counters

## VPS install

```
git clone https://github.com/aababababababbabaabababababba/headless-rtmp.git
cd headless-rtmp
sudo ./deploy/install.sh
sudo vim /etc/rtmpd.env
sudo systemctl restart rtmpd
journalctl -u rtmpd -f
```

Open `1935/tcp` in your firewall for RTMP. Keep `8080` closed to the internet or bind it to `127.0.0.1` (default in `rtmpd.env.example`).

## Docker

```
docker build -t headless-rtmp .
docker run -d --name rtmpd \
    -p 1935:1935 -p 127.0.0.1:8080:8080 \
    -e PUBLISH_KEY=secret123 \
    headless-rtmp
```

## Tests

```
go test -race ./...
```

Includes unit tests for the chunk reader/writer and AMF0 codec plus end-to-end integration tests that run a real server and verify publish→play actually delivers frames.

## Verified against

- FFmpeg 7 (publisher and player)
- OBS Studio handshake pattern

## Layout

```
cmd/rtmpd/              entry point
internal/config/        env + file config
internal/hub/           stream pub/sub routing, GOP cache
internal/rtmp/          RTMP handshake, chunks, AMF, connection state
internal/httpapi/       admin endpoints
deploy/                 systemd unit + install script
```
