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
   failed immediately with ERROR.
4. Outgoing messages (SEND_MSG, SEND_CHAN_MSG): after the radio
   accepts the forward, a synthetic CONTACT_MSG_RECV / CHANNEL_MSG_RECV
   is echoed to the other clients so they see the message as if it
   had arrived over LoRa. If the radio is unreachable the originator
   gets an ERROR and nothing is echoed or persisted.
5. Incoming LoRa traffic: the radio pushes MESSAGES_WAITING (0x83) to
   every client; the first one to poll with GET_MSG retrieves the
   frame and the proxy fans it out to the rest, so every client sees
   inbound messages even though only one poll consumed it.
6. Push events from the radio (ADVERTISEMENT, NEW_ADVERT, LOG_DATA,
   ...) go to everybody. Late LoRa-delivery ACKs (0x82) are routed
   back to the original sender by `ack_code`.

## Requirements

- **Rust edition 2024** (requires Rust ≥ 1.85)
- **Linux** (single target)

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
| `CORESPLITTER_TCP_FRONTEND_PORT` | Frontend TCP port (default: 5000) |
| `CORESPLITTER_DATA_DIR` | Data directory (default: ./data) |
| `CORESPLITTER_LOG_LEVEL` | Log level: off, error, warn, info, debug, verbose |
| `CORESPLITTER_LOG_JSON` | Enable JSON event logging |
| `CORESPLITTER_RECORD_RADIO_RX` | Record all raw radio RX to database |

## Test suite

### Integration tests

Uses a fake radio (`tools/fake_radio.py`) and a real proxy deployed on
ephemeral ports. Requires Python 3.12+ with `uv`.

```bash
uv run tools/test_proxy.py
```

All tests run sequentially. Each cycle spawns a fresh proxy and fake radio,
runs the tests, and cleans up.

### Current tests (10 cases, 17 assertions, all PASS)

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
| 10 | TCP reconnection | Kills fake radio, waits for reconnect, verifies GET_CHANNEL works |

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

A multi-stage `Containerfile` is included:

```bash
docker build -t coresplitter -f Containerfile .
docker run --rm coresplitter --help
```

## Project status

- **TCP backend**: tested against a real radio (Heltec V3, firmware v1.15.0)
- **Serial backend**: implemented, pending hardware test
- **BLE backend**: implemented, pending hardware test
- **Tests**: 17/17 PASS (10 cases) on TCP backend against fake radio
