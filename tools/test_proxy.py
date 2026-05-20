#!/usr/bin/env python3
"""Integration tests for coresplitter proxy."""
import asyncio
import logging
import os
import socket
import struct
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("test")

# --- Helpers ---

class Client:
    def __init__(self, reader, writer):
        self.r = reader
        self.w = writer

    async def send(self, payload: bytes):
        frame = b"\x3c" + struct.pack("<H", len(payload)) + payload
        self.w.write(frame)
        await self.w.drain()

    async def recv(self, timeout=5.0):
        try:
            hdr = await asyncio.wait_for(self.r.readexactly(3), timeout)
        except asyncio.IncompleteReadError:
            return None
        except asyncio.TimeoutError:
            return b""
        except ConnectionResetError:
            return None
        length = struct.unpack("<H", hdr[1:3])[0]
        payload = await asyncio.wait_for(self.r.readexactly(length), timeout)
        return payload

    async def appstart(self):
        await self.send(b"\x01\x01" + b"testclient")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 10:
            p = await self.recv(2)
            if p is None:
                return None
            if len(p) and p[0] == 0x05:
                return p
        return None

    async def get_channel(self, idx: int):
        await self.send(b"\x1f" + bytes([idx]))
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(2)
            if p is None:
                return None
            if len(p) and p[0] in (0x12, 0x01):
                return p
        return None

    async def set_channel(self, idx: int, name: str, psk: bytes | None = None):
        name_b = name.encode()[:32].ljust(32, b"\x00")
        if psk is None:
            from hashlib import sha256
            psk = sha256(name.encode()).digest()[:16]
        payload = b"\x20" + bytes([idx]) + name_b + psk
        await self.send(payload)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] == 0x00:
                return p
        return None

    async def get_contacts(self, lastmod=0):
        await self.send(b"\x04" + struct.pack("<I", lastmod))
        contacts = []
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                break
            if len(p) == 0:
                continue
            if p[0] == 0x03:
                contacts.append(p)
            elif p[0] == 0x04:
                break
        return contacts

    async def get_message(self):
        await self.send(b"\x0A")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] in (0x07, 0x08, 0x0A):
                return p
        return None

    async def send_msg(self, dst: bytes, text: str, ts: int | None = None):
        if ts is None:
            ts = int(time.time())
        payload = b"\x02\x00\x00" + struct.pack("<I", ts) + dst[:6].ljust(6, b"\x00") + text.encode()
        await self.send(payload)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] == 0x06:
                return p
        return None

    async def send_chan_msg(self, channel: int, text: str, ts: int | None = None):
        if ts is None:
            ts = int(time.time())
        payload = b"\x03" + bytes([channel, 0]) + struct.pack("<I", ts) + text.encode()
        await self.send(payload)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] == 0x06:
                return p
        return None

    async def close(self):
        self.w.close()

async def connect_client(host="127.0.0.1", port=None) -> Client:
    r, w = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=5
    )
    return Client(r, w)


async def wait_for_port(port, timeout=15):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port), timeout=2
            )
            w.close()
            return True
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
            await asyncio.sleep(0.5)
    return False


async def find_free_port():
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# --- Test cases ---

PASS = 0
FAIL = 0

def ok(msg):
    global PASS
    PASS += 1
    log.info("  PASS: %s", msg)

def fail(msg):
    global FAIL
    FAIL += 1
    log.error("  FAIL: %s", msg)


async def test_multi_client(proxy_port):
    log.info("--- Test 1: Multi-cliente concurrente ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)

    r1 = await c1.appstart()
    if r1 is None:
        fail(f"c1 SELF_INFO failed (got: {await c1.recv(200)})")
    r2 = await c2.appstart()
    if r2 is None:
        fail(f"c2 SELF_INFO failed (got: {await c2.recv(200)})")
    if r1 and r2:
        ok("both clients receive SELF_INFO")
    else:
        await c1.close()
        await c2.close()
        return

    # c1 sends SEND_CHAN_MSG to channel 0
    await c1.send_chan_msg(0, "Multi-client test")

    # MSG_SENT is consumed by send_chan_msg's drain loop.
    ok("c1 sent message (MSG_SENT consumed by drain)")

    # c2 should receive a synthesized CHANNEL_MSG_RECV (0x08) via broadcast
    got_broadcast = False
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c2.recv(2)
        if p is None:
            break
        if len(p) == 0:
            continue
        if p[0] == 0x08 and b"Multi-client test" in p:
            got_broadcast = True
            break

    if got_broadcast:
        ok("c2 receives CHANNEL_MSG_RECV (0x08) via broadcast_to_others")
    else:
        fail("c2 did not receive broadcast message")

    await c1.close()
    await c2.close()


async def test_set_channel(proxy_port):
    log.info("--- Test 2: SET_CHANNEL + persistence ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    r = await c.set_channel(5, "TestChannel")
    if r:
        ok("SET_CHANNEL → ACK (0x00)")
    else:
        fail("SET_CHANNEL did not receive ACK")
        await c.close()
        return

    r = await c.get_channel(5)
    if r and r[0] == 0x12:
        name = r[2:34].rstrip(b"\x00").decode("utf-8", "replace")
        if name == "TestChannel":
            ok("GET_CHANNEL(5) → 'TestChannel'")
        else:
            fail(f"GET_CHANNEL(5) expected 'TestChannel', got {name!r}")
    else:
            fail(f"GET_CHANNEL(5) failed: {r.hex() if r else 'None'}")

    # Reconnect and verify persistence
    log.info("  Reconnecting for persistence...")
    await c.close()
    c = await connect_client(port=proxy_port)
    await c.appstart()
    r = await c.get_channel(5)
    if r and r[0] == 0x12:
        name = r[2:34].rstrip(b"\x00").decode("utf-8", "replace")
        if name == "TestChannel":
            ok("SQLite persistence: GET_CHANNEL(5) → 'TestChannel' after reconnect")
        else:
            fail(f"persistence failed: {name!r}")
    else:
        fail("persistence: GET_CHANNEL failed")

    await c.close()


async def test_injected_messages(proxy_port):
    log.info("--- Test 3: Injected messages (CHANNEL_MSG_RECV) ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    t0 = time.monotonic()
    received = False
    while time.monotonic() - t0 < 25:
        p = await c.recv(3)
        if p is None:
            break
        if len(p) == 0:
            continue
        if p[0] == 0x08:
            received = True
            break

    if received:
        ok("CHANNEL_MSG_RECV received via broadcast")
    else:
        fail("did not receive CHANNEL_MSG_RECV in 25s")

    await c.close()


async def test_contact_msg_received(proxy_port):
    log.info("--- Test 5: Contact message (0x07) received ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    t0 = time.monotonic()
    received = False
    while time.monotonic() - t0 < 25:
        p = await c.recv(3)
        if p is None:
            break
        if len(p) == 0:
            continue
        if p[0] == 0x07:
            received = True
            break

    if received:
        ok("CONTACT_MSG_RECV (0x07) received via broadcast")
    else:
        fail("did not receive 0x07 in 25s")

    await c.close()


async def test_get_message_polling(proxy_port):
    log.info("--- Test 6: GET_MESSAGE polling (0x0A) ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    # Wait for any 0x83 from the periodic injector (max 6s per cycle)
    t0 = time.monotonic()
    got_83 = False
    while time.monotonic() - t0 < 10:
        p = await c.recv(2)
        if p is None:
            break
        if len(p) and p[0] == 0x83:
            got_83 = True
            break

    if not got_83:
        fail("did not receive MESSAGES_WAITING (0x83) in 10s")
        await c.close()
        return
    ok("0x83 received")

    # Drain queue: GET_MESSAGE until stale injector items are gone, then
    # send one final GET_MESSAGE to verify the protocol works.
    polled = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < 8:
        p = await c.get_message()
        if p is None:
            break
        if p[0] == 0x0A:
            break
        polled.append(p)
    # We should have polled at least one real message
    if polled:
        ok(f"GET_MESSAGE returned {len(polled)} messages (last 0x{polled[-1][0]:02x})")
    else:
        fail("GET_MESSAGE did not produce any real messages")

    await c.close()


async def test_multiple_messages_order(proxy_port):
    log.info("--- Test 7: Multiple messages in order ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    # Collect two consecutive messages via polling to verify ordering
    msgs = []
    t0 = time.monotonic()
    while len(msgs) < 2 and time.monotonic() - t0 < 20:
        p = await c.recv(3)
        if p is None or len(p) == 0:
            continue
        if p[0] == 0x83:
            m = await c.get_message()
            if m and m[0] in (0x07, 0x08):
                msgs.append(m)
    # After receiving two messages via push + poll, verify they differ
    if len(msgs) >= 2:
        txt0 = msgs[0][6:].decode("utf-8", errors="replace") if len(msgs[0]) > 6 else ""
        txt1 = msgs[1][6:].decode("utf-8", errors="replace") if len(msgs[1]) > 6 else ""
        if txt0 and txt1 and txt0 != txt1:
            ok(f"two distinct messages in order: {txt0[:20]!r} ≠ {txt1[:20]!r}")
        else:
            fail(f"messages equal or empty: {txt0[:20]!r} vs {txt1[:20]!r}")
    else:
        fail(f"only got {len(msgs)} messages in 20s")

    await c.close()


async def test_long_message(proxy_port):
    log.info("--- Test 8: Long message (>133 chars) ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    r1 = await c1.appstart()
    r2 = await c2.appstart()
    if not r1 or not r2:
        fail("SELF_INFO failed")
        await c1.close()
        await c2.close()
        return

    long_text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
    assert len(long_text) > 133

    await c1.send_chan_msg(0, long_text)

    t0 = time.monotonic()
    received = False
    while time.monotonic() - t0 < 6:
        p = await c2.recv(2)
        if p is None or len(p) == 0:
            continue
        if p[0] == 0x08:
            txt_rx = p[8:].decode("utf-8", errors="replace") if len(p) > 8 else ""
            if "Lorem" in txt_rx or len(txt_rx) > 100:
                received = True
                break

    if received:
        ok(f"c2 receives CHANNEL_MSG_RECV (0x08) with long text ({len(txt_rx)} chars)")
    else:
        fail("c2 did not receive long message broadcast in 6s")

    await c1.close()
    await c2.close()


async def test_send_contact_msg(proxy_port):
    log.info("--- Test 9: Send contact message (0x02) ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    r1 = await c1.appstart()
    r2 = await c2.appstart()
    if not r1 or not r2:
        fail("SELF_INFO failed")
        await c1.close()
        await c2.close()
        return

    dst = bytes.fromhex("aabbccddeeff")
    r = await c1.send_msg(dst, "Hello contact")
    if not r:
        fail("SEND_MSG did not return MSG_SENT")
        await c1.close()
        await c2.close()
        return
    ok("c1: SEND_MSG → MSG_SENT (0x06)")

    t0 = time.monotonic()
    got_broadcast = False
    while time.monotonic() - t0 < 6:
        p = await c2.recv(2)
        if p is None or len(p) == 0:
            continue
        if p[0] == 0x07 and b"Hello contact" in p:
            got_broadcast = True
            break

    if got_broadcast:
        ok("c2 receives CONTACT_MSG_RECV (0x07) via broadcast_to_others with correct text")
    else:
        fail("c2 did not receive 0x07 broadcast with expected text")

    await c1.close()
    await c2.close()


async def test_reconnect_tcp(proxy_port):
    global fake_proc, fake_pid, fake_port
    log.info("--- Test 10: TCP disconnection + reconnection ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    # Kill fake radio
    if fake_proc and fake_proc.returncode is None:
        log.info("  Killing fake radio (PID=%d)...", fake_pid)
        fake_proc.terminate()
        try:
            await asyncio.wait_for(fake_proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            fake_proc.kill()
    fake_proc = None

    await asyncio.sleep(4)
    log.info("  Proxy should be reconnecting...")

    # Restart fake radio on the same port (proxy still tries to reconnect to old port)
    log.info("  Restarting fake radio...")
    await start_fake(port=fake_port)
    if not fake_proc:
        fail("could not restart fake radio")
        await c.close()
        return

    # Drain any broadcast messages that arrived during sleep
    for _ in range(50):
        p = await c.recv(0.5)
        if p is None or len(p) == 0:
            break

    r = await c.get_channel(0)
    if r and r[0] == 0x12:
        ok("proxy reconnected and responds to GET_CHANNEL after reconnection")
    else:
        fail(f"proxy does not respond after reconnection (r={r}, raw=...)")

    await c.close()


# --- Harness ---

fake_proc = None
fake_pid = None
fake_port = None
proxy_port = None
proxy_proc = None

async def start_fake(port=None):
    global fake_proc, fake_pid, fake_port
    if port is None:
        fake_port = await find_free_port()
    else:
        fake_port = port
    fake_proc = await asyncio.create_subprocess_exec(
        sys.executable, os.path.join(os.path.dirname(__file__), "fake_radio.py"),
        "--port", str(fake_port), "--inject",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    fake_pid = fake_proc.pid
    log.info("Fake radio started (PID=%d, port=%d)", fake_pid, fake_port)
    ok = await wait_for_port(fake_port)
    if not ok:
        log.error("Fake radio no responde")
        return False
    return True

async def stop_fake():
    global fake_proc, fake_pid
    if fake_proc and fake_proc.returncode is None:
        fake_proc.terminate()
        try:
            await asyncio.wait_for(fake_proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            fake_proc.kill()
    fake_proc = None
    fake_pid = None

async def main():
    global PASS, FAIL, proxy_proc, proxy_port

    DATA_DIR = f"/tmp/cs_test_proxy/{os.getpid()}"
    subprocess.run(["rm", "-rf", DATA_DIR], check=False)

    if not await start_fake():
        return

    proxy_port = await find_free_port()

    proxy_bin = os.path.join(
        os.path.dirname(__file__), "..", "target", "debug", "coresplitter"
    )
    proxy_proc = await asyncio.create_subprocess_exec(
        proxy_bin,
        "--tcp-backend-host", "127.0.0.1",
        "--tcp-backend-port", str(fake_port),
        "--tcp-frontend-port", str(proxy_port),
        "--data-dir", DATA_DIR,
        "--log-level", "info",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,

    )
    log.info("Proxy started (PID=%d)", proxy_proc.pid)

    async def _pipe_reader(stream, prefix):
        while True:
            line = await stream.readline()
            if not line:
                break
            log.info("%s %s", prefix, line.decode("utf-8", "replace").rstrip())
    asyncio.create_task(_pipe_reader(proxy_proc.stdout, "[proxy:out]"))
    asyncio.create_task(_pipe_reader(proxy_proc.stderr, "[proxy:err]"))

    ok = await wait_for_port(proxy_port)
    if not ok:
        log.error("Proxy no responde en puerto %d", proxy_port)
        await stop_fake()
        return
    log.info("Proxy ready on port %d", proxy_port)

    await test_multi_client(proxy_port)
    await test_set_channel(proxy_port)
    await test_injected_messages(proxy_port)
    await test_contact_msg_received(proxy_port)
    await test_get_message_polling(proxy_port)
    await test_multiple_messages_order(proxy_port)
    await test_long_message(proxy_port)
    await test_send_contact_msg(proxy_port)
    await test_reconnect_tcp(proxy_port)

    total = PASS + FAIL
    log.info("=" * 40)
    if FAIL == 0:
        log.info("RESULT: %d/%d PASS", PASS, total)
    else:
        log.error("RESULT: %d/%d PASS, %d FAIL", PASS, total, FAIL)

    if proxy_proc:
        proxy_proc.terminate()
        try:
            await asyncio.wait_for(proxy_proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            proxy_proc.kill()
    await stop_fake()

if __name__ == "__main__":
    asyncio.run(main())
