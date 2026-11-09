# Core Splitter

[MeshCore](https://meshcore.io) **client multiplexer with cache**. Speaks the
companion protocol on both sides: it connects to a physical radio (serial,
BLE, or TCP) and serves a TCP interface to multiple concurrent companion
clients. Contacts, channels, SELF_INFO, DEVICE_INFO and BATTERY are cached in
SQLite so cold reads don't hit the radio. Every client sees the same state as
the physical radio — the proxy acts as a **faithful mirror**.

## How it works

```
┌────────┐             ┌────────────────┐                    ┌──────────┐
│ Client │◄───────────►│  Core Splitter │◄──────────────────►│ Physical │
│ A      │     TCP     │  (proxy)       │   TCP/serial/BLE   │ Radio    │
├────────┤   0x3c/3e   │                │      meshcore      │ Node     │
│ Client │             │  ┌──────────┐  │                    │          │
│ B      │             │  │  SQLite  │  │                    │          │
└────────┘             │  │  cache   │  │                    │          │
                       │  └──────────┘  │                    │          │
                       └────────────────┘                    └──────────┘
```

1. Proxy connects to the physical radio and syncs contacts and channels
   into SQLite at startup (and again after each reconnect). The sync
   commands are queued through the same FIFO as client commands so their
   responses never interleave with a real client request.
2. Opens a TCP server for companion clients to connect to.
3. Client commands are served from cache when possible (SELF_INFO,
   DEVICE_INFO, BATTERY, GET_CONTACTS, GET_CHANNEL); the rest are
   serialized through a FIFO queue and forwarded one at a time to the
   radio. The radio's reply is routed back to the originating client.
   If the radio disconnects, any in-flight or queued client request is
   failed immediately with ERROR, every client connection is closed and
   the TCP frontend stops listening until the radio is back. MeshCore
   has no "device unavailable" frame, so the proxy behaves like an
   absent node rather than serving a radio it doesn't have.
4. Outgoing messages (SEND_MSG, SEND_CHAN_MSG): once the radio accepts
   the message (MSG_SENT for SEND_MSG, OK for SEND_CHAN_MSG), a
   synthetic CONTACT_MSG_RECV / CHANNEL_MSG_RECV is echoed to the other
   clients so they see the message as if it had arrived over LoRa. If
   the radio rejects it or is unreachable, the originator gets the
   ERROR and nothing is echoed or persisted.
5. Incoming LoRa traffic: the radio pushes MESSAGES_WAITING (0x83) to
   every client; the first one to poll with GET_MSG retrieves the
   frame and the proxy fans it out to the rest, so every client sees
   inbound messages even though only one poll consumed it.
6. Push events from the radio (ADVERTISEMENT, NEW_ADVERT, LOG_DATA,
   ...) go to everybody. When a push or an accepted command changes a
   contact on the radio (ADVERTISEMENT, PATH_UPDATE, ADD_UPDATE_CONTACT,
   RESET_PATH, IMPORT_CONTACT) the proxy queues a re-read of it so the
   cache keeps serving the radio's current frame. Late LoRa-delivery
   ACKs (0x82) are routed back to the original sender by `ack_code`.
7. Missed messages: every message clients see (incoming, or echoed
   from another client's send) is stored. A client is identified by
   the frontend port it connects to plus the app name in its
   APP_START (the app version is ignored, so updates keep the
   history). When a known client reconnects, the messages it missed
   are announced with MESSAGES_WAITING and served by its GET_MSG polls
   before the radio is asked, exactly as a radio returns messages
   queued while the app was away. Its own messages are never replayed
   to it. Two copies of the same app (e.g. two phones with the
   official app) look identical on one port; give each its own port
   with `--tcp-frontend-port 5000,5001`. What a never-seen client gets
   is set with `CORESPLITTER_NEW_CLIENT_BACKLOG`.

## Requirements

- **Rust edition 2024** (requires Rust ≥ 1.85)
- **Linux** (primary target)
- **macOS**: reported working with the serial backend on macOS 26.5
  (rustc 1.95.0) without code changes; see
  [#1](https://github.com/ogarcia/coresplitter/issues/1). Not tested
  in CI.

## Build

The default build excludes BLE support to avoid pulling in system
dependencies (`dbus-1` headers):

```bash
cargo build --release
```

The binary is at `target/release/coresplitter`.

### BLE support

BLE is behind the optional `ble` feature and requires `dbus-1`
development headers:

```bash
# Debian / Ubuntu
sudo apt install libdbus-1-dev pkg-config
cargo build --release --features ble

# Alpine
apk add dbus-dev
cargo build --release --features ble

# Fedora
sudo dnf install dbus-devel pkgconf-pkg-config
cargo build --release --features ble
```

## Usage

### TCP backend (development / production)

```bash
cargo run --release -- \
  --tcp-backend-host 192.168.1.100 \
  --tcp-backend-port 5050 \
  --tcp-frontend-port 5001 \
  --data-dir /var/lib/coresplitter
```

### Serial backend

```bash
cargo run --release -- \
  --serial /dev/ttyUSB0 \
  --baud 115200 \
  --tcp-frontend-port 5001
```

### BLE backend (requires Bluetooth hardware)

Requires building with `--features ble` (see [BLE support](#ble-support)):

```bash
cargo run --release --features ble -- \
  --ble 00:11:22:33:44:55 \
  --ble-pin 123456
```

### Environment variables

| Variable | Description |
|---|---|
| `CORESPLITTER_SERIAL_PORT` | Serial port path |
| `CORESPLITTER_BAUD_RATE` | Baud rate (default: 115200) |
| `CORESPLITTER_BLE_ADDRESS` | BLE device address |
| `CORESPLITTER_BLE_PIN` | BLE pairing PIN (default: 123456) |
| `CORESPLITTER_TCP_BACKEND_HOST` | Radio TCP host |
| `CORESPLITTER_TCP_BACKEND_PORT` | Backend TCP port (default: 5000) |
| `CORESPLITTER_TCP_FRONTEND_HOST` | Frontend TCP bind (default: 0.0.0.0) |
| `CORESPLITTER_TCP_FRONTEND_PORT` | Frontend TCP port(s), comma separated (default: 5000) |
| `CORESPLITTER_DATA_DIR` | Data directory (default: ./data) |
| `CORESPLITTER_LOG_LEVEL` | Log level: off, error, warn, info, debug, verbose |
| `CORESPLITTER_LOG_JSON` | Enable JSON event logging |
| `CORESPLITTER_RECORD_RADIO_RX` | Record all raw radio RX to database |
| `CORESPLITTER_MESSAGES_RETENTION_DAYS` | Days to keep stored messages, 0 = forever (default: 0) |
| `CORESPLITTER_RAW_RX_RETENTION_DAYS` | Days to keep raw_rx recordings, 0 = forever (default: 7) |
| `CORESPLITTER_CLIENTS_RETENTION_DAYS` | Days a client may stay away before it is forgotten, 0 = never (default: 180) |
| `CORESPLITTER_NEW_CLIENT_BACKLOG` | Messages replayed to a never-seen client: 0 = none, N = last N messages, Nh / Nd = last N hours / days (default: 0) |
| `CORESPLITTER_CONTACTS_RETENTION_DAYS` | Days a contact may be missing from the radio before it leaves the cache, 0 = never (default: 0) |

### Database maintenance

At startup and every 24 hours the proxy purges the SQLite database
according to the retention settings above, then returns the freed space
with an incremental vacuum.

- **Messages** are kept forever unless a retention is set; they then
  expire by local arrival time, not by the sender's timestamp, which
  can be arbitrarily wrong.
  A client offline for longer than the retention misses what was
  purged meanwhile.
- **Clients** (the identities used to replay missed messages) are
  forgotten after 180 days without connecting. One that comes back
  later is treated as new and gets `CORESPLITTER_NEW_CLIENT_BACKLOG`
  instead of everything it missed.
- **Contacts** are kept by default even when the radio no longer
  reports them: the cache is a safeguard against a node losing its
  contacts (firmware reflash, factory reset). With a retention set, a
  contact is only dropped if it was missing from the last full contact
  sync *and* the radio hasn't reported it for that many days. In that
  mode the maintenance pass also triggers a full sync, so that "last
  sync" stays recent on long-running proxies.

### Running as a systemd service

Install the binary as `/usr/local/bin/coresplitter` and create
`/etc/systemd/system/coresplitter.service`:

```ini
[Unit]
Description=MeshCore client multiplexer with cache
Wants=network-online.target
After=network-online.target

[Service]
Type=exec
ExecStart=/usr/local/bin/coresplitter
EnvironmentFile=-/etc/coresplitter.conf
Environment=CORESPLITTER_DATA_DIR=/var/lib/coresplitter
DynamicUser=yes
StateDirectory=coresplitter
# Serial backend: grant access to the port (dialout on Debian/Ubuntu,
# uucp on Arch)
#SupplementaryGroups=dialout
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

The configuration goes in `/etc/coresplitter.conf` using the
[environment variables](#environment-variables) above, for example:

```bash
CORESPLITTER_SERIAL_PORT=/dev/ttyUSB0
CORESPLITTER_TCP_FRONTEND_PORT=5000,5001
CORESPLITTER_MESSAGES_RETENTION_DAYS=365
```

Then enable it and follow the log:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now coresplitter
journalctl -u coresplitter -f
```

`DynamicUser` runs the proxy as a throwaway unprivileged user, and
`StateDirectory` gives it a persistent `/var/lib/coresplitter` for the
database. There is no need to restart the service when the radio goes
away: the proxy keeps retrying the connection on its own and stops
cleanly on `systemctl stop`.

## Test suite

### Integration tests

Uses a fake radio (`tools/fake_radio.py`) and a real proxy deployed on
ephemeral ports. Requires Python 3.12+ with `uv`.

```bash
uv run tools/test_proxy.py
```

All tests run sequentially. Each cycle spawns a fresh proxy and fake radio,
runs the tests, and cleans up.

### Current tests (17 cases, 30 assertions, all PASS)

| # | Test | Description |
|---|------|-------------|
| 1 | Multi-client concurrent | Two clients both receive SELF_INFO; one sends a message, the other receives the broadcast |
| 2 | SET_CHANNEL + persistence | Creates a channel, verifies SQLite persistence after reconnection |
| 3 | Injected channel msg via GET_MSG | Client waits for 0x83, polls with GET_MSG, gets CHANNEL_MSG_RECV (0x08) |
| 4 | Injected contact msg via GET_MSG | Same as #3 for CONTACT_MSG_RECV (0x07) |
| 5 | GET_MESSAGE polling | Client waits for 0x83, sends GET_MESSAGE, receives queued messages |
| 6 | Multiple messages in order | FIFO polling: two consecutive messages have different content |
| 7 | Long message (>133 chars) | Two clients: one sends long text, the other receives the intact broadcast |
| 8 | Contact send (0x02) | Two clients: one sends SEND_MSG, the other receives the raw payload broadcast |
| 9 | GET_MSG reply fans out | Two clients; one polls via GET_MSG, the other sees the same frame via broadcast |
| 10 | REMOVE_CONTACT purges cache | Removes a contact; the next cached GET_CONTACTS no longer lists it |
| 11 | Cached GET_CONTACTS mirrors radio | Cached CONTACT frames keep flags, path and lastmod; the `since` filter, START count and END lastmod follow the firmware |
| 12 | ADD_UPDATE_CONTACT refreshes cache | After the radio accepts a contact update, the cached list shows its new name, path and lastmod |
| 13 | Rejected send not echoed | SEND_CHAN_MSG to an unknown channel gets ERROR and no echo reaches the other client |
| 14 | Missed messages replayed | A client that was offline gets the message it missed through GET_MSG on reconnect, and only once |
| 15 | Own messages not replayed | A client that sends and disconnects does not get its own message back on reconnect |
| 16 | Same app on two ports | Two copies of the same app on different ports each get the missed message |
| 17 | TCP reconnection | Kills fake radio: clients are closed and new connections refused; after reconnect GET_CHANNEL works |

### Fake radio

```bash
# TCP mode
uv run tools/fake_radio.py --port 5050

# TCP mode with simulated incoming messages
uv run tools/fake_radio.py --port 5050 --inject

# Serial PTY mode
uv run tools/fake_radio.py --serial
```

## Container

A multi-stage `Containerfile` is included. Built locally, it compiles the
binary from source:

```bash
docker build -t coresplitter -f Containerfile .
docker run --rm coresplitter --help
```

CI compiles static musl binaries for amd64 and arm64 on native runners
(with a cargo cache) and injects them with `--build-arg BUILDER=prebuilt`,
publishing a multi-arch image to `ghcr.io/ogarcia/coresplitter`. Base image
versions live in `versions.env`.

### Serial device in a rootless container

To use a radio on USB from rootless Podman, pass the device in and keep
your supplementary groups. Serial ports are usually `root:uucp` (Arch) or
`root:dialout` (Debian/Ubuntu) with mode `0660`, and rootless Podman drops
your groups inside the container by default, so even if your user is in
that group the proxy gets `failed to open serial port: Permission denied`.
`--group-add keep-groups` (crun runtime) keeps them.

As a Quadlet user unit (`~/.config/containers/systemd/coresplitter.container`):

```ini
[Unit]
Description=MeshCore client multiplexer with cache

[Container]
Image=ghcr.io/ogarcia/coresplitter:master
# The by-id link survives the port coming back as ttyUSB1
AddDevice=/dev/serial/by-id/your-device-id:/dev/ttyUSB0
GroupAdd=keep-groups
Environment=CORESPLITTER_SERIAL_PORT=/dev/ttyUSB0
PublishPort=5000:5000
Volume=coresplitter.volume:/var/lib/coresplitter

[Service]
Restart=always

[Install]
WantedBy=default.target
```

With plain `podman run`: `--device /dev/ttyUSB0 --group-add keep-groups`.

`ls -l /dev/serial/by-id/` lists your device ids. If the port still
can't be opened, run the container as root inside (`User=0` and
`Group=0`, or `--user 0:0`); Docker may need it, since it has no
`keep-groups`.

## Releases

Pushing a bare semver tag (`1.2.3`) publishes a GitHub release with static
binaries for `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`.

## Project status

- **TCP backend**: tested against a real radio (Heltec V3, firmware v1.15.0)
- **Serial backend**: implemented, pending hardware test
- **BLE backend**: implemented, pending hardware test
- **Tests**: 30/30 PASS (17 cases) on TCP backend against fake radio
