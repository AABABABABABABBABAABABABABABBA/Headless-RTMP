#!/usr/bin/env bash
set -euo pipefail

# install headless rtmp as a systemd service
# run as root on a fresh debian/ubuntu vps

BIN=/usr/local/bin/rtmpd
UNIT=/etc/systemd/system/rtmpd.service
ENVF=/etc/rtmpd.env
SRC=$(cd "$(dirname "$0")/.." && pwd)

if ! id rtmpd >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin rtmpd
fi

cd "$SRC"
go build -trimpath -ldflags="-s -w" -o "$BIN" ./cmd/rtmpd
chmod 755 "$BIN"

cp deploy/rtmpd.service "$UNIT"
chmod 644 "$UNIT"

if [ ! -f "$ENVF" ]; then
    cp deploy/rtmpd.env.example "$ENVF"
    chmod 640 "$ENVF"
    chown root:rtmpd "$ENVF"
    echo "created $ENVF — edit it then run: systemctl restart rtmpd"
fi

systemctl daemon-reload
systemctl enable rtmpd
systemctl restart rtmpd
sleep 1
systemctl --no-pager status rtmpd
