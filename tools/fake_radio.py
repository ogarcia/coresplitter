#!/usr/bin/env python3
"""Fake MeshCore radio emulator for development and testing.

Usage:
    uv run tools/fake_radio.py --port 5050           # TCP mode
    uv run tools/fake_radio.py --serial              # Serial PTY mode
    uv run tools/fake_radio.py --port 5050 --inject  # + inject incoming msgs

Speaks the MeshCore companion protocol (0x3c/0x3e framing). Responds to
commands with realistic canned data. Optionally injects fake incoming
messages on a timer.
"""

import argparse
import asyncio
import fcntl
import os
import random
import struct
import sys
import termios
import time
import tty

HOST = "0.0.0.0"
PORT = 5050

# ---------------------------------------------------------------------------
# Default test data
# ---------------------------------------------------------------------------

DEFAULT_CONTACTS = [
    {
        "key": bytes.fromhex("aabbccdd0011aabbccdd0011aabbccdd0011aabbccdd0011aabbccdd0011aabb"),
        "type": 0,
        "name": "Alpha Test",
        "lat": 43.3623,
        "lon": -8.4115,
        "last_advert": 0,
    },
    {
        "key": bytes.fromhex("1122334455661122334455661122334455661122334455661122334455661122"),
        "type": 0,
        "name": "Bravo Test",
        "lat": 43.3700,
        "lon": -8.4200,
        "last_advert": 0,
    },
]

DEFAULT_CHANNELS = [
    {"idx": 0, "name": "Public", "psk": b"\x00" * 16},
    {"idx": 1, "name": "Pruebas", "psk": b"\x00" * 16},
    {"idx": 2, "name": "Dev", "psk": random.randbytes(16)},
]

# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------

CMD_FRAME = 0x3C
RESP_FRAME = 0x3E


def encode_response(payload: bytes) -> bytes:
    return bytes([RESP_FRAME]) + struct.pack("<H", len(payload)) + payload


def decode_frame(data: bytes):
    """Return (header, payload) or None."""
    if len(data) < 3:
        return None
    hdr = data[0]
    (plen,) = struct.unpack("<H", data[1:3])
    if len(data) < 3 + plen:
        return None
    return hdr, data[3 : 3 + plen]


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------

def respond_ok(extra: bytes = b"") -> bytes:
    return encode_response(b"\x00" + extra)


def respond_error(code: int = 1) -> bytes:
    return encode_response(bytes([0x01, code]))


def respond_self_info() -> bytes:
    node_name = b"FakeRadio\x00"
    pk = bytes.fromhex("deadbeefcafebabedeadbeefcafebabedeadbeefcafebabedeadbeefcafebabe")
    lat = int(43.3623 * 1_000_000)
    lon = int(-8.4115 * 1_000_000)
    freq = int(868_175_000 / 1000)
    bw = int(125_000 / 1000)

    payload = bytearray()
    payload.append(0x05)
    payload.append(1)
    payload.append(20)
    payload.append(1)
    payload.extend(pk)
    payload.extend(struct.pack("<i", lat))
    payload.extend(struct.pack("<i", lon))
    payload.extend(b"\x00" * 4)
    payload.extend(struct.pack("<I", freq))
    payload.extend(struct.pack("<I", bw))
    payload.extend(node_name)
    return encode_response(bytes(payload))


def respond_contacts() -> list[bytes]:
    frames: list[bytes] = []
    n = len(DEFAULT_CONTACTS)
    start = bytearray([0x02])
    start.extend(struct.pack("<I", n))
    frames.append(encode_response(bytes(start)))

    for c in DEFAULT_CONTACTS:
        entry = bytearray()
        entry.append(0x03)
        entry.extend(c["key"])
        entry.append(c["type"])
        entry.append(0)  # flags
        entry.append(0)  # path_len
        entry.extend(b"\x00" * 64)  # path
        name_b = c["name"].encode("utf-8")[:32].ljust(32, b"\x00")
        entry.extend(name_b)
        entry.extend(struct.pack("<I", c["last_advert"]))
        entry.extend(struct.pack("<i", int(c["lat"] * 1_000_000)))
        entry.extend(struct.pack("<i", int(c["lon"] * 1_000_000)))
        entry.extend(b"\x00" * 4)  # last_mod
        frames.append(encode_response(bytes(entry)))

    end = bytearray([0x04])
    end.extend(struct.pack("<I", int(time.time())))
    frames.append(encode_response(bytes(end)))
    return frames


def respond_device_info() -> bytes:
    payload = bytearray()
    payload.append(0x0D)
    payload.append(11)
    payload.append(175)
    payload.append(40)
    payload.extend(b"\x00" * 4)
    payload.extend(b"19 Apr 2026\x00\x00\x00\x00\x00")
    payload.extend(b"Heltec V3\x00" + b"\x00" * 30)
    return encode_response(bytes(payload))


def respond_channel_info(idx: int, name: str, psk: bytes = b"\x00" * 16) -> bytes:
    payload = bytearray()
    payload.append(0x12)
    payload.append(idx)
    name_b = name.encode("utf-8")[:32].ljust(32, b"\x00")
    payload.extend(name_b)
    secret = psk[:16].ljust(16, b"\x00")
    payload.extend(secret)
    return encode_response(bytes(payload))


def respond_battery() -> bytes:
    payload = bytearray()
    payload.append(0x0C)
    payload.extend(struct.pack("<H", 4100))
    return encode_response(bytes(payload))


def respond_current_time() -> bytes:
    payload = bytearray()
    payload.append(0x09)
    payload.extend(struct.pack("<I", int(time.time())))
    return encode_response(bytes(payload))


def respond_msg_sent() -> bytes:
    payload = bytearray()
    payload.append(0x06)
    payload.append(0)
    payload.extend(b"\x00" * 4)
    payload.extend(struct.pack("<I", 30000))
    return encode_response(bytes(payload))


def respond_login() -> bytes:
    return encode_response(b"\x85")


def make_chan_msg(channel: int, text: str) -> bytes:
    payload = bytearray()
    payload.append(0x08)
    payload.append(channel)
    payload.append(0)
    payload.append(0)
    payload.extend(struct.pack("<I", int(time.time())))
    payload.extend(text.encode("utf-8"))
    return encode_response(bytes(payload))


# ---------------------------------------------------------------------------
# Writer abstraction (TCP stream or serial fd)
# ---------------------------------------------------------------------------

class TcpWriter:
    def __init__(self, writer: asyncio.StreamWriter):
        self._w = writer

    def write(self, data: bytes) -> None:
        self._w.write(data)

    async def drain(self) -> None:
        await self._w.drain()

    def close(self) -> None:
        self._w.close()


class SerialWriter:
    def __init__(self, fd: int):
        self.fd = fd

    def write(self, data: bytes) -> None:
        os.write(self.fd, data)

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        os.close(self.fd)


# ---------------------------------------------------------------------------
# Fake Radio
# ---------------------------------------------------------------------------

def make_contact_msg(from_key: bytes, text: str) -> bytes:
    payload = bytearray()
    payload.append(0x07)
    payload.extend(from_key[:6].ljust(6, b"\x00"))
    payload.append(0)
    payload.append(1)
    payload.extend(struct.pack("<I", int(time.time())))
    payload.extend(text.encode("utf-8"))
    return encode_response(bytes(payload))


def make_messages_waiting() -> bytes:
    return encode_response(b"\x83")


class FakeRadio:
    def __init__(self, inject: bool = False):
        self.channels: list[dict] = list(DEFAULT_CHANNELS)
        self.inject = inject
        # Per-writer message queues (writer fd/id → list of framed messages)
        self._msg_queues: dict[int, list[bytes]] = {}
        self._next_queue_id = 0

    def _queue_for_writer(self, w) -> int:
        qid = id(w)
        if qid not in self._msg_queues:
            self._msg_queues[qid] = []
        return qid

    def _push_msg(self, w, framed: bytes):
        qid = self._queue_for_writer(w)
        self._msg_queues[qid].append(framed)

    def _pop_msg(self, w) -> bytes | None:
        qid = self._queue_for_writer(w)
        q = self._msg_queues[qid]
        return q.pop(0) if q else None

    # -- TCP handler -------------------------------------------------------

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        print(f"[+] TCP conexión desde {addr}")
        w = TcpWriter(writer)

        inject_task = None
        if self.inject:
            inject_task = asyncio.create_task(self._injector(w))

        buf = b""
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break

                buf += data
                while True:
                    result = decode_frame(buf)
                    if result is None:
                        break
                    hdr, payload = result
                    buf = buf[3 + len(payload):]
                    if hdr != CMD_FRAME:
                        print(f"  [!] Header inesperado: 0x{hdr:02x}")
                        continue
                    await self._handle_command(payload, w)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            pass
        finally:
            if inject_task:
                inject_task.cancel()
            w.close()
        print(f"[-] TCP desconectado {addr}")

    # -- Serial handler ----------------------------------------------------

    async def run_serial(self):
        master_fd, slave_fd = os.openpty()
        slave_path = os.ttyname(slave_fd)

        fl = fcntl.fcntl(master_fd, fcntl.F_GETFL)
        fcntl.fcntl(master_fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        tty.setraw(slave_fd)

        print(f"[+] Serial PTY creado: {slave_path}", flush=True)
        print(f"    Conecta el proxy con: --serial {slave_path}", flush=True)

        w = SerialWriter(master_fd)
        loop = asyncio.get_running_loop()

        inject_task = None
        if self.inject:
            inject_task = asyncio.create_task(self._injector(w))

        buf = bytearray()

        def on_read():
            nonlocal buf
            try:
                data = os.read(master_fd, 4096)
                if not data:
                    print("  [serial] EOF on master")
                    loop.remove_reader(master_fd)
                    return
            except BlockingIOError:
                return
            except OSError as e:
                print(f"  [serial] OSError: {e}")
                loop.remove_reader(master_fd)
                return

            print(f"  [serial] read {len(data)}B: {data.hex()[:60]}", flush=True)
            buf.extend(data)

            buf.extend(data)
            while True:
                result = decode_frame(bytes(buf))
                if result is None:
                    break
                hdr, payload = result
                buf = buf[3 + len(payload):]
                if hdr != CMD_FRAME:
                    print(f"  [!] Header inesperado: 0x{hdr:02x}")
                    continue
                asyncio.create_task(self._handle_command(payload, w))

        loop.add_reader(master_fd, on_read)

        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            if inject_task:
                inject_task.cancel()
            loop.remove_reader(master_fd)
            os.close(master_fd)
            os.close(slave_fd)
            print("[-] Serial PTY cerrado")

    # -- Command dispatch --------------------------------------------------

    async def _handle_command(self, payload: bytes, w):
        if not payload:
            return
        code = payload[0]
        rest = payload[1:]
        cmd_name = CMD_NAMES.get(code, f"0x{code:02x}")
        print(f"  -> {cmd_name} ({len(payload)}B)", flush=True)

        match code:
            case 0x01:
                w.write(respond_self_info())
                await w.drain()

            case 0x02:
                w.write(respond_msg_sent())
                await w.drain()

            case 0x03:
                w.write(respond_msg_sent())
                await w.drain()

            case 0x04:
                for frame in respond_contacts():
                    w.write(frame)
                await w.drain()

            case 0x05:
                w.write(respond_current_time())
                await w.drain()

            case 0x06:
                w.write(respond_ok())
                await w.drain()

            case 0x07:
                w.write(respond_ok())
                await w.drain()

            case 0x08:
                w.write(respond_ok())
                await w.drain()

            case 0x0A:
                msg = self._pop_msg(w)
                if msg is not None:
                    w.write(msg)
                else:
                    w.write(encode_response(b"\x0A"))
                await w.drain()

            case 0x0B:
                w.write(respond_ok())
                await w.drain()

            case 0x0C:
                w.write(respond_ok())
                await w.drain()

            case 0x0E:
                w.write(respond_ok())
                await w.drain()

            case 0x14:
                w.write(respond_battery())
                await w.drain()

            case 0x16:
                w.write(respond_device_info())
                await w.drain()

            case 0x1A:
                w.write(respond_login())
                await w.drain()

            case 0x1F:
                idx = rest[0] if rest else 0
                ch = next((c for c in self.channels if c["idx"] == idx), None)
                if ch:
                    w.write(respond_channel_info(idx, ch["name"], ch["psk"]))
                else:
                    w.write(respond_channel_info(idx, ""))
                await w.drain()

            case 0x20:
                if len(rest) >= 49:
                    idx = rest[0]
                    name = rest[1:33].rstrip(b"\x00").decode("utf-8", errors="replace")
                    psk = rest[33:49]
                    existing = next((c for c in self.channels if c["idx"] == idx), None)
                    if existing:
                        existing["name"] = name
                        existing["psk"] = psk
                    else:
                        self.channels.append({"idx": idx, "name": name, "psk": psk})
                    print(f"     -> Canal {idx}: name={name!r}")
                w.write(respond_ok())
                await w.drain()

            case 0x25:
                w.write(respond_ok())
                await w.drain()

            case 0x38:
                w.write(encode_response(b"\x18" + b"\x00" * 20))
                await w.drain()

            case _:
                print(f"     -> comando desconocido, respondiendo ERROR")
                w.write(respond_error(1))
                await w.drain()

    # -- Injection helpers -------------------------------------------------

    async def _injector(self, w):
        items: list[bytes] = [
            make_chan_msg(0, "Keep alive from fake radio"),
            make_contact_msg(b"\xAA\xBB\xCC\xDD\xEE\xFF", "Hello from Alpha"),
            make_chan_msg(0, "Test message on Public"),
            make_chan_msg(0, "Alerta: temperatura alta"),
            make_chan_msg(0, "A" * 200),
        ]
        try:
            n = 0
            while True:
                await asyncio.sleep(6)
                if n >= len(items):
                    n = 0
                framed = items[n]
                w.write(framed)
                self._push_msg(w, framed)
                w.write(make_messages_waiting())
                await w.drain()
                print(f"  <- INJECT item[{n}]: {framed[3:10].hex()}...")
                n += 1
        except asyncio.CancelledError:
            pass

    async def _delayed_inject(self, w, msg: bytes, delay: float):
        await asyncio.sleep(delay)
        try:
            # Push to GET_MESSAGE queue
            self._push_msg(w, msg)
            # Send as direct push for broadcast/caching
            w.write(msg)
            await w.drain()
            print(f"  <- INJECT (delayed): {msg[3:10].hex()}")
        except Exception:
            pass


CMD_NAMES = {
    0x01: "AppStart",
    0x02: "SendMsg",
    0x03: "SendChanMsg",
    0x04: "GetContacts",
    0x05: "GetTime",
    0x06: "SetTime",
    0x07: "SendAdvert",
    0x08: "SetName",
    0x09: "UpdateContact",
    0x0A: "GetMsg",
    0x0B: "SetRadio",
    0x0C: "SetTxPower",
    0x0D: "ResetPath",
    0x0E: "SetCoords",
    0x14: "GetBattery",
    0x16: "DeviceQuery",
    0x1A: "SendLogin",
    0x1F: "GetChannel",
    0x20: "SetChannel",
    0x25: "SetDevicePin",
    0x38: "GetStats",
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    ap = argparse.ArgumentParser(description="Fake MeshCore radio for testing")
    ap.add_argument("--port", type=int, default=None, help="TCP port (default: 5050)")
    ap.add_argument("--serial", action="store_true", help="Serial PTY mode instead of TCP")
    ap.add_argument("--inject", action="store_true", help="Inject fake incoming messages")
    args = ap.parse_args()

    if args.serial:
        radio = FakeRadio(inject=args.inject)
        print("Fake radio en modo SERIAL (PTY)  (inject={})".format("on" if args.inject else "off"))
        print("  Canales:", [c["name"] for c in DEFAULT_CHANNELS])
        print("  Contactos:", [c["name"] for c in DEFAULT_CONTACTS])
        await radio.run_serial()
    else:
        port = args.port or PORT
        radio = FakeRadio(inject=args.inject)
        server = await asyncio.start_server(radio.handle_client, HOST, port)

        addr = ", ".join(str(s.getsockname()) for s in server.sockets)
        print(f"Fake radio escuchando en {addr}  (inject={'on' if args.inject else 'off'})")
        print(f"  Canales: {[c['name'] for c in DEFAULT_CHANNELS]}")
        print(f"  Contactos: {[c['name'] for c in DEFAULT_CONTACTS]}")

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
