# Core Splitter

Virtual node proxy for [MeshCore](https://meshcore.io). Connects to a physical
radio (serial, BLE, or TCP), syncs contacts and channels into SQLite, and
serves a TCP interface to multiple concurrent clients. Every client sees the
same state as the physical radio — the proxy acts as a **faithful mirror**.

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

1. Proxy connects to the physical radio and syncs contacts/channels to SQLite
2. Opens a TCP server for clients to connect to
3. Client commands served from cache when possible (GET_CONTACTS,
   GET_CHANNEL) or forwarded to the radio
4. Outgoing messages (SEND_MSG, SEND_CHAN_MSG) are broadcast to other clients
   immediately and forwarded to the radio
5. Incoming messages from the radio are cached in SQLite and broadcast to all
   clients

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
|---|---|---|
| `CORESPLITTER_SERIAL_PORT` | Serial port path |
| `CORESPLITTER_BAUD_RATE` | Baud rate (default: 115200) |
| `CORESPLITTER_BLE_ADDRESS` | BLE device address |
| `CORESPLITTER_BLE_PIN` | BLE pairing PIN (default: 123456) |
| `CORESPLITTER_TCP_BACKEND_HOST` | Radio TCP host |
| `CORESPLITTER_TCP_BACKEND_PORT` | Backend TCP port (default: 5000) |
| `CORESPLITTER_TCP_FRONTEND_HOST` | Frontend TCP bind (default: 0.0.0.0) |
| `CORESPLITTER_TCP_FRONTEND_PORT` | Frontend TCP port (default: 5000) |
| `CORESPLITTER_NODE_NAME` | Virtual node name (defaults to physical radio's name) |
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

### Current tests (15 tests, all PASS)

| # | Test | Description |
|---|------|-------------|
| 1 | Multi-client concurrent | Two clients both receive SELF_INFO; one sends a message, the other receives the broadcast |
| 2 | SET_CHANNEL + persistence | Creates a channel, verifies SQLite persistence after reconnection |
| 3 | Injected messages (0x08) | Periodic injector sends CHANNEL_MSG_RECV, client receives it via broadcast |
| 5 | Contact message (0x07) | Injector sends CONTACT_MSG_RECV, client receives it via broadcast |
| 6 | GET_MESSAGE polling | Client waits for 0x83, sends GET_MESSAGE, receives queued messages |
| 7 | Multiple messages in order | FIFO polling: two consecutive messages have different content |
| 8 | Long message (>133 chars) | Two clients: one sends long text, the other receives the intact broadcast |
| 9 | Contact send (0x02) | Two clients: one sends SEND_MSG, the other receives the raw payload broadcast |
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
- **Tests**: 15/15 PASS on TCP backend against fake radio
