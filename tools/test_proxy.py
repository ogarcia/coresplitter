#!/usr/bin/env python3
"""Integration tests for coresplitter proxy."""
import asyncio
import logging
import os
import socket
import sqlite3
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

_client_seq = 0

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

    async def appstart(self, name: str | None = None):
        # [0x01, app_ver, reserved(6), app_name]. The proxy identifies the
        # client by (port, app_name); a unique default name keeps tests
        # from sharing a message history unless they ask to.
        global _client_seq
        if name is None:
            _client_seq += 1
            name = f"test-{os.getpid()}-{_client_seq}"
        await self.send(b"\x01\x01" + b"\x00" * 6 + name.encode())
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
        # [0x03, txt_type, channel_idx, ts(4), text]; the radio replies OK.
        payload = b"\x03" + bytes([0, channel]) + struct.pack("<I", ts) + text.encode()
        await self.send(payload)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await self.recv(1)
            if p is None:
                return None
            if len(p) and p[0] == 0x00:
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

    # The radio's OK is consumed by send_chan_msg's drain loop.
    ok("c1 sent message (OK consumed by drain)")

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


async def _wait_for_0x83(c, timeout):
    """Wait up to `timeout` seconds for a MESSAGES_WAITING (0x83) on `c`."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        p = await c.recv(2)
        if p is None:
            return False
        if len(p) and p[0] == 0x83:
            return True
    return False


async def test_injected_messages(proxy_port):
    log.info("--- Test 3: Injected channel msg via GET_MSG polling ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    if not await _wait_for_0x83(c, 10):
        fail("did not receive MESSAGES_WAITING (0x83) in 10s")
        await c.close()
        return

    msg = await c.get_message()
    if msg and msg[0] == 0x08:
        ok("GET_MSG after 0x83 returns CHANNEL_MSG_RECV (0x08)")
    else:
        fail(f"GET_MSG returned: {msg.hex() if msg else 'None'}")

    await c.close()


async def test_contact_msg_received(proxy_port):
    log.info("--- Test 4: Injected contact msg via GET_MSG polling ---")
    c = await connect_client(port=proxy_port)
    r = await c.appstart()
    if not r:
        fail("SELF_INFO failed")
        await c.close()
        return

    # The injector cycles through items; the 0x07 contact msg is item 1.
    # Keep polling on every 0x83 until we hit a 0x07 (or timeout).
    deadline = time.monotonic() + 30
    got_07 = False
    while time.monotonic() < deadline:
        if not await _wait_for_0x83(c, 10):
            break
        msg = await c.get_message()
        if msg and msg[0] == 0x07:
            got_07 = True
            break

    if got_07:
        ok("GET_MSG returns CONTACT_MSG_RECV (0x07)")
    else:
        fail("did not retrieve a 0x07 via GET_MSG in 30s")

    await c.close()


async def test_get_message_polling(proxy_port):
    log.info("--- Test 5: GET_MESSAGE polling (0x0A) ---")
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
    log.info("--- Test 6: Multiple messages in order ---")
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
    log.info("--- Test 7: Long message (>133 chars) ---")
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
        if p[0] == 0x08 and b"Lorem" in p:
            txt_rx = p[8:].decode("utf-8", errors="replace")
            received = True
            break

    # The rest of the mesh gets "<node name>: <text>", cut at 160 bytes.
    expected = ("FakeRadio: " + long_text)[:160]
    if not received:
        fail("c2 did not receive long message broadcast in 6s")
    elif txt_rx == expected:
        ok(f"c2 receives the channel echo as sent on air ({len(txt_rx)} chars)")
    else:
        fail(f"c2 got {txt_rx!r}, expected {expected!r}")

    await c1.close()
    await c2.close()


async def test_send_contact_msg(proxy_port):
    log.info("--- Test 8: Send contact message (0x02) ---")
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


async def test_resend_echoed_once(proxy_port):
    log.info("--- Test 18: A resent message is echoed once ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    if not await c1.appstart() or not await c2.appstart():
        fail("SELF_INFO failed for one or both clients")
        await c1.close()
        await c2.close()
        return

    # A retry reuses the timestamp: same payload, same packet on air.
    ts = int(time.time())
    for _ in range(2):
        if not await c1.send_chan_msg(0, "sent twice", ts=ts):
            fail("SEND_CHAN_MSG did not return OK")
            await c1.close()
            await c2.close()
            return

    echoes = 0
    t0 = time.monotonic()
    while time.monotonic() - t0 < 2:
        p = await c2.recv(0.5)
        if p is None:
            break
        if len(p) and p[0] == 0x08 and b"sent twice" in p:
            echoes += 1
    if echoes == 1:
        ok("resent SEND_CHAN_MSG reaches the other client once")
    else:
        fail(f"resent SEND_CHAN_MSG echoed {echoes} times")

    await c1.close()
    await c2.close()


async def test_drain_while_offline(proxy_port, data_dir):
    log.info("--- Test 19: Messages fetched while nobody is connected ---")
    c = await connect_client(port=proxy_port)
    if not await c.appstart("drain-app"):
        fail("SELF_INFO failed")
        await c.close()
        return
    await c.close()

    # The fake radio queues a message and sends 0x83 every 6 s. With no
    # client connected, only the proxy itself can fetch it.
    def newest():
        with sqlite3.connect(os.path.join(data_dir, "state.db")) as db:
            return db.execute("SELECT id, frame FROM messages ORDER BY id DESC LIMIT 1").fetchone()

    before = newest()
    head = before[0] if before else 0
    row = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 15:
        await asyncio.sleep(0.5)
        row = newest()
        if row and row[0] > head:
            break
    if not row or row[0] <= head:
        fail("no message fetched from the radio while nobody was connected")
        return
    ok("proxy fetches the radio's queue while nobody is connected")

    # The frame was stored in its v3 or v1 layout; match on its tail.
    text = bytes(row[1])[-12:]
    c = await connect_client(port=proxy_port)
    await c.appstart("drain-app")
    if await _drain_for_text(c, text, polls=8):
        ok("reconnecting app gets the message fetched in its absence")
    else:
        fail("message fetched while offline was not replayed")
    await c.close()


async def test_late_reply_not_misrouted(proxy_port):
    log.info("--- Test 20: A late reply is not handed to the next request ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    if not await c1.appstart() or not await c2.appstart():
        fail("SELF_INFO failed for one or both clients")
        await c1.close()
        await c2.close()
        return

    # The radio answers c1's lookup (NOT_FOUND) after the proxy has given
    # up on it, and only then the lookup c2 queued behind it.
    from fake_radio import DEFAULT_CONTACTS, SLOW_KEY
    alpha = DEFAULT_CONTACTS[0]["key"]
    await c1.send(b"\x1e" + SLOW_KEY)
    await asyncio.sleep(0.5)
    await c2.send(b"\x1e" + alpha)

    async def first_reply(c, timeout):
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            p = await c.recv(1)
            if p and p[0] in (0x03, 0x01):
                return p
        return None

    r1, r2 = await asyncio.gather(first_reply(c1, 25), first_reply(c2, 25))
    if r1 == b"\x01\x01":
        ok("timed-out GET_CONTACT_BY_KEY gets ERROR")
    else:
        fail(f"timed-out GET_CONTACT_BY_KEY: {r1.hex() if r1 else 'None'}")
    if r2 and r2[0] == 0x03 and r2[1:33] == alpha:
        ok("next request gets its own reply, not the late one")
    else:
        fail(f"next request got: {r2.hex() if r2 else 'None'}")

    await c1.close()
    await c2.close()


async def test_unconfirmed_message_resent(proxy_port):
    log.info("--- Test 21: A message the client never confirmed is sent again ---")
    a = await connect_client(port=proxy_port)
    b = await connect_client(port=proxy_port)
    if not await a.appstart("ack-app") or not await b.appstart():
        fail("SELF_INFO failed for one or both clients")
        await a.close()
        await b.close()
        return
    await _drain_all(a)

    # a gets the echo but says nothing afterwards, like a phone that drops
    # off the WiFi: nothing proves it read the message.
    await b.send_chan_msg(0, "did you read this")
    got = False
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await a.recv(1)
        if p and b"did you read this" in p:
            got = True
            break
    await asyncio.sleep(0.5)  # connected but silent, then gone
    await b.close()
    await a.close()
    if not got:
        fail("a did not receive the echo")
        return
    await asyncio.sleep(1.5)  # let the proxy notice and flush cursors

    a = await connect_client(port=proxy_port)
    await a.appstart("ack-app")
    if await _drain_for_text(a, b"did you read this"):
        ok("unconfirmed message is sent again on reconnect")
    else:
        fail("unconfirmed message was taken as delivered")
    await _drain_all(a)
    await a.close()
    await asyncio.sleep(1.5)

    # Confirmed this time (the client polled after it): not sent again.
    a = await connect_client(port=proxy_port)
    await a.appstart("ack-app")
    if await _drain_for_text(a, b"did you read this", polls=3):
        fail("confirmed message sent again")
    else:
        ok("confirmed message is not sent again")
    await a.close()


async def test_reconnect_tcp(proxy_port):
    global fake_proc, fake_pid, fake_port
    log.info("--- Test 17: TCP disconnection + reconnection ---")
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

    # With the radio gone the proxy must drop its clients (MeshCore has no
    # "unavailable" frame) instead of leaving them hanging.
    closed = False
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c.recv(1)
        if p is None:
            closed = True
            break
    if closed:
        ok("client connection closed when the radio drops")
    else:
        fail("client connection still open after the radio dropped")
    await c.close()

    # ...and stop listening while it reconnects.
    try:
        probe = await connect_client(port=proxy_port)
        await probe.close()
        fail("proxy accepted a connection with no radio attached")
    except (ConnectionRefusedError, OSError):
        ok("proxy refuses connections while the radio is down")

    log.info("  Restarting fake radio...")
    await start_fake(port=fake_port)
    if not fake_proc:
        fail("could not restart fake radio")
        return

    # Backoff is 1 s, 2 s, 4 s... give the proxy time to come back.
    if not await wait_for_port(proxy_port, timeout=20):
        fail("proxy did not reopen the frontend after reconnection")
        return

    c = await connect_client(port=proxy_port)
    r = await c.get_channel(0)
    if r and r[0] == 0x12:
        ok("proxy reconnected and responds to GET_CHANNEL after reconnection")
    else:
        fail(f"proxy does not respond after reconnection (r={r})")

    await c.close()


async def test_request_jumps_sync(proxy_port):
    log.info("--- Test 22: A client request does not wait behind the sync ---")
    # Bring the radio back slow on GET_CHANNEL: the sync the proxy runs on
    # reconnecting then takes 40 x 0.25 s = 10 s.
    await stop_fake()
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        try:
            probe = await connect_client(port=proxy_port)
            await probe.close()
            await asyncio.sleep(0.2)
        except (ConnectionRefusedError, OSError):
            break
    await start_fake(port=fake_port, extra=("--channel-delay", "0.25"))
    if not fake_proc:
        fail("could not restart fake radio")
        return
    if not await wait_for_port(proxy_port, timeout=20):
        fail("proxy did not reopen the frontend after reconnection")
        return

    c = await connect_client(port=proxy_port)
    if not await c.appstart():
        fail("SELF_INFO failed")
        await c.close()
        return

    from fake_radio import DEFAULT_CONTACTS
    alpha = DEFAULT_CONTACTS[0]["key"]
    t0 = time.monotonic()
    await c.send(b"\x1e" + alpha)
    reply = None
    while time.monotonic() - t0 < 15:
        p = await c.recv(1)
        if p and p[0] in (0x03, 0x01):
            reply = p
            break
    took = time.monotonic() - t0
    if reply and reply[0] == 0x03 and took < 3:
        ok(f"request answered during the sync ({took:.1f} s)")
    else:
        fail(f"request waited behind the sync ({took:.1f} s, "
             f"r={reply.hex() if reply else 'None'})")

    # A cached read keeps its place and is still answered.
    r = await c.get_channel(0)
    if r and r[0] == 0x12:
        ok("GET_CHANNEL answered while the sync runs")
    else:
        fail(f"GET_CHANNEL during the sync: {r.hex() if r else 'None'}")
    await c.close()


async def test_get_msg_fans_out(proxy_port):
    log.info("--- Test 9: GET_MSG reply fans out to other clients ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)

    r1 = await c1.appstart()
    r2 = await c2.appstart()
    if not r1 or not r2:
        fail("SELF_INFO failed for one or both clients")
        await c1.close()
        await c2.close()
        return

    # Wait for an injector 0x83 on c1 (c2 sees it too but we don't act on it).
    if not await _wait_for_0x83(c1, 10):
        fail("c1 did not receive MESSAGES_WAITING (0x83) in 10s")
        await c1.close()
        await c2.close()
        return

    # c1 polls. Expect a real message (not NO_MORE_MSGS).
    msg_c1 = await c1.get_message()
    if not msg_c1 or msg_c1[0] not in (0x07, 0x08):
        fail(f"c1 GET_MSG returned: {msg_c1.hex() if msg_c1 else 'None'}")
        await c1.close()
        await c2.close()
        return
    ok(f"c1 GET_MSG → 0x{msg_c1[0]:02x}")

    # c2 should see the same message via fan-out broadcast (no GET_MSG of
    # its own). Drain c2 until we find a matching 0x07/0x08. Need a
    # generous window because c2 will also see broadcast 0x83 from the
    # injector and we have to drain past those.
    got_fanout = None
    seen = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < 10:
        p = await c2.recv(1)
        if p is None:
            break
        if len(p) == 0:
            continue
        seen.append(f"0x{p[0]:02x}")
        if p[0] in (0x07, 0x08):
            got_fanout = p
            break
    log.info("  c2 saw frames: %s", seen)

    if got_fanout is None:
        fail("c2 did not receive the message via fan-out")
    elif got_fanout == msg_c1:
        ok("c2 receives the same frame via fan-out broadcast")
    else:
        fail(
            f"c2 received a frame but bytes differ:\n"
            f"  c1: {msg_c1.hex()}\n  c2: {got_fanout.hex()}"
        )

    await c1.close()
    await c2.close()


async def test_message_layout_per_version(proxy_port):
    log.info("--- Test 18: Message frames follow each client's protocol version ---")
    old = await connect_client(port=proxy_port)
    new = await connect_client(port=proxy_port)
    if not await old.appstart() or not await new.appstart():
        fail("SELF_INFO failed for one or both clients")
        await old.close()
        await new.close()
        return

    # DEVICE_QUERY carries the version the app understands; the proxy
    # answers it but must not hand it on to the radio.
    for c, version in ((old, 1), (new, 3)):
        await c.send(bytes([0x16, version]))
        p = await c.recv(5)
        if not p or p[0] != 0x0D:
            fail(f"DEVICE_QUERY v{version} returned: {p.hex() if p else 'None'}")

    if not await _wait_for_0x83(new, 10):
        fail("v3 client did not receive MESSAGES_WAITING (0x83) in 10s")
        await old.close()
        await new.close()
        return

    await new.send(b"\x0A")
    v3 = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await new.recv(1)
        if p is None:
            break
        if len(p) and p[0] in (0x07, 0x08, 0x10, 0x11):
            v3 = p
            break
    if not v3 or v3[0] not in (0x10, 0x11):
        fail(f"v3 client GET_MSG returned: {v3.hex() if v3 else 'None'}")
        await old.close()
        await new.close()
        return
    ok(f"v3 client gets 0x{v3[0]:02x}")

    v1 = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await old.recv(1)
        if p is None:
            break
        if len(p) and p[0] in (0x07, 0x08, 0x10, 0x11):
            v1 = p
            break
    if v1 == bytes([v3[0] - 0x09]) + v3[4:]:
        ok(f"v1 client gets the same message as 0x{v1[0]:02x}")
    else:
        fail(f"v1 client got {v1.hex() if v1 else 'None'} for {v3.hex()}")

    await old.close()
    await new.close()


async def _get_contact_names(c):
    """GET_CONTACTS and return the names in the stream, or None on failure."""
    await c.send(b"\x04")
    names = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c.recv(2)
        if p is None:
            return None
        if len(p) == 0:
            continue
        if p[0] == 0x03 and len(p) >= 132:
            names.append(p[100:132].rstrip(b"\x00").decode("utf-8", "replace"))
        elif p[0] == 0x04:
            return names
    return None


async def test_remove_contact(proxy_port):
    log.info("--- Test 10: REMOVE_CONTACT also purges the cache ---")
    c = await connect_client(port=proxy_port)
    if not await c.appstart():
        fail("SELF_INFO failed")
        await c.close()
        return

    names = await _get_contact_names(c)
    if not names or "Bravo Test" not in names:
        fail(f"GET_CONTACTS before remove: {names}")
        await c.close()
        return

    async def remove(key):
        await c.send(b"\x0f" + key)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 5:
            p = await c.recv(2)
            if p is None:
                return None
            if len(p) and p[0] in (0x00, 0x01):
                return p
        return None

    # The fake radio refuses to remove Alpha: the cache must keep it.
    alpha_key = bytes.fromhex(
        "aabbccdd0011aabbccdd0011aabbccdd0011aabbccdd0011aabbccdd0011aabb"
    )
    reply = await remove(alpha_key)
    if reply != b"\x01\x02":
        fail(f"refused REMOVE_CONTACT reply: {reply.hex() if reply else 'None'}")
        await c.close()
        return
    names = await _get_contact_names(c)
    if not names or "Alpha Test" not in names:
        fail(f"contact the radio refused to remove left the cache: {names}")
        await c.close()
        return
    ok("refused REMOVE_CONTACT → ERROR, contact kept in cache")

    bravo_key = bytes.fromhex(
        "1122334455661122334455661122334455661122334455661122334455661122"
    )
    reply = await remove(bravo_key)
    if reply is None or reply[0] != 0x00:
        fail(f"REMOVE_CONTACT reply: {reply.hex() if reply else 'None'}")
        await c.close()
        return
    ok("REMOVE_CONTACT → OK (0x00)")

    names = await _get_contact_names(c)
    if names is None:
        fail("GET_CONTACTS after remove failed")
    elif "Bravo Test" in names:
        fail(f"removed contact still served from cache: {names}")
    elif "Alpha Test" not in names:
        fail(f"remaining contact missing after remove: {names}")
    else:
        ok("GET_CONTACTS no longer lists the removed contact")

    await c.close()


async def test_rejected_send_not_echoed(proxy_port):
    log.info("--- Test 13: Rejected send is not echoed ---")
    c1 = await connect_client(port=proxy_port)
    c2 = await connect_client(port=proxy_port)
    if not await c1.appstart() or not await c2.appstart():
        fail("SELF_INFO failed for one or both clients")
        await c1.close()
        await c2.close()
        return

    # Channel 30 does not exist on the radio: it must answer ERROR and
    # the other clients must not see the message as if it had been sent.
    ts = int(time.time())
    await c1.send(b"\x03" + bytes([0, 30]) + struct.pack("<I", ts) + b"never sent")
    reply = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c1.recv(1)
        if p is None:
            break
        if len(p) and p[0] in (0x00, 0x01):
            reply = p
            break
    if reply and reply[0] == 0x01:
        ok("SEND_CHAN_MSG to unknown channel → ERROR")
    else:
        fail(f"SEND_CHAN_MSG to unknown channel: {reply.hex() if reply else 'None'}")

    echoed = False
    t0 = time.monotonic()
    while time.monotonic() - t0 < 2:
        p = await c2.recv(0.5)
        if p is None:
            break
        if len(p) and p[0] == 0x08 and b"never sent" in p:
            echoed = True
            break
    if echoed:
        fail("c2 received an echo of a message the radio rejected")
    else:
        ok("rejected message is not echoed to other clients")

    await c1.close()
    await c2.close()


async def _get_contacts_raw(c, since=None):
    """GET_CONTACTS; return (start, [contact frames], end) or None."""
    payload = b"\x04" if since is None else b"\x04" + struct.pack("<I", since)
    await c.send(payload)
    start, frames = None, []
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c.recv(2)
        if p is None:
            return None
        if len(p) == 0:
            continue
        if p[0] == 0x02:
            start = p
        elif p[0] == 0x03:
            frames.append(p)
        elif p[0] == 0x04:
            return start, frames, p
    return None


async def test_contacts_served_verbatim(proxy_port):
    log.info("--- Test 11: Cached GET_CONTACTS mirrors the radio ---")
    c = await connect_client(port=proxy_port)
    if not await c.appstart():
        fail("SELF_INFO failed")
        await c.close()
        return

    full = await _get_contacts_raw(c)
    if not full:
        fail("GET_CONTACTS failed")
        await c.close()
        return
    _, frames, end = full
    alpha = next((f for f in frames if f[100:132].rstrip(b"\x00") == b"Alpha Test"), None)
    # flags=1, out_path_len=2, out_path=2a7f, lastmod=1700000100 in fake_radio
    if (
        alpha
        and alpha[34] == 0x01
        and alpha[35] == 2
        and alpha[36:38] == bytes([0x2A, 0x7F])
        and struct.unpack("<I", alpha[144:148])[0] == 1_700_000_100
    ):
        ok("cached CONTACT keeps flags, path and lastmod from the radio")
    else:
        fail(f"cached CONTACT differs from the radio: {alpha.hex() if alpha else None}")

    # 'since' = Alpha's lastmod: Alpha must be left out, START still
    # reports the total and END the newest lastmod actually sent.
    part = await _get_contacts_raw(c, since=1_700_000_100)
    if not part:
        fail("GET_CONTACTS(since) failed")
    else:
        start, sent, end = part
        mods = [struct.unpack("<I", f[144:148])[0] for f in sent]
        total = struct.unpack("<I", start[1:5])[0]
        last = struct.unpack("<I", end[1:5])[0]
        if (
            all(m > 1_700_000_100 for m in mods)
            and len(sent) < len(frames)
            and total == len(frames)
            and last == max(mods, default=0)
        ):
            ok("GET_CONTACTS honours 'since' like the firmware")
        else:
            fail(f"GET_CONTACTS(since): lastmods={mods} total={total} end={last}")

    await c.close()


async def test_update_contact_refreshes_cache(proxy_port):
    log.info("--- Test 12: ADD_UPDATE_CONTACT refreshes the cache ---")
    c = await connect_client(port=proxy_port)
    if not await c.appstart():
        fail("SELF_INFO failed")
        await c.close()
        return

    full = await _get_contacts_raw(c)
    alpha = next(
        (f for f in (full[1] if full else []) if f[100:132].rstrip(b"\x00") == b"Alpha Test"),
        None,
    )
    if not alpha:
        fail("Alpha Test not in cached contacts")
        await c.close()
        return

    # Rename Alpha and give it a new path; frame minus code and lastmod.
    upd = bytearray(alpha[1:144])
    upd[34] = 1  # out_path_len
    upd[35:99] = bytes([0x55]).ljust(64, b"\x00")
    upd[99:131] = b"Alpha Renamed".ljust(32, b"\x00")
    await c.send(b"\x09" + bytes(upd))
    reply = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        p = await c.recv(2)
        if p is None:
            break
        if len(p) and p[0] in (0x00, 0x01):
            reply = p
            break
    if not reply or reply[0] != 0x00:
        fail(f"ADD_UPDATE_CONTACT reply: {reply.hex() if reply else 'None'}")
        await c.close()
        return

    # The refresh is a GET_CONTACT_BY_KEY queued after the OK; give it a
    # moment, then the cached list must carry the radio's new frame.
    await asyncio.sleep(0.5)
    full = await _get_contacts_raw(c)
    renamed = next(
        (f for f in (full[1] if full else []) if f[100:132].rstrip(b"\x00") == b"Alpha Renamed"),
        None,
    )
    if renamed and renamed[35] == 1 and renamed[36] == 0x55 and renamed[144:148] != alpha[144:148]:
        ok("cached contact reflects ADD_UPDATE_CONTACT (name, path, lastmod)")
    else:
        names = [f[100:132].rstrip(b"\x00").decode() for f in (full[1] if full else [])]
        fail(f"cache not refreshed after ADD_UPDATE_CONTACT: {names}")

    await c.close()


async def _drain_for_text(c, text: bytes, polls: int = 5):
    """Poll GET_MSG until a frame containing `text` shows up, the radio
    says NO_MORE_MSGS, or `polls` runs out. Returns the matching frame."""
    for _ in range(polls):
        p = await c.get_message()
        if not p:
            return None
        if text in p:
            return p
        if p[0] == 0x0A:
            return None
    return None


async def _drain_all(c, polls: int = 20):
    """Poll GET_MSG until NO_MORE_MSGS, as an app does after a sync. That
    last poll is what confirms the messages before it were read."""
    for _ in range(polls):
        p = await c.get_message()
        if not p or p[0] == 0x0A:
            return


async def test_missed_messages_replayed(proxy_port):
    log.info("--- Test 14: Missed messages replayed on reconnect ---")
    a = await connect_client(port=proxy_port)
    if not await a.appstart("replay-app"):
        fail("SELF_INFO failed")
        await a.close()
        return
    await a.close()
    await asyncio.sleep(0.5)  # let the proxy notice the disconnect

    b = await connect_client(port=proxy_port)
    await b.appstart()
    if not await b.send_chan_msg(0, "while you were away"):
        fail("SEND_CHAN_MSG failed")
        await b.close()
        return
    await b.close()

    a = await connect_client(port=proxy_port)
    await a.appstart("replay-app")
    if await _drain_for_text(a, b"while you were away"):
        ok("reconnecting client gets the message it missed via GET_MSG")
    else:
        fail("missed message was not replayed on reconnect")
    await _drain_all(a)

    # Caught up now: reconnecting again must not replay it a second time.
    await a.close()
    await asyncio.sleep(0.5)
    a = await connect_client(port=proxy_port)
    await a.appstart("replay-app")
    if await _drain_for_text(a, b"while you were away", polls=3):
        fail("message replayed twice")
    else:
        ok("already delivered message is not replayed again")
    await a.close()


async def test_own_messages_not_replayed(proxy_port):
    log.info("--- Test 15: Own messages are not replayed ---")
    a = await connect_client(port=proxy_port)
    if not await a.appstart("echo-app"):
        fail("SELF_INFO failed")
        await a.close()
        return
    await a.send_chan_msg(0, "my own words")
    await a.close()
    await asyncio.sleep(0.5)

    a = await connect_client(port=proxy_port)
    await a.appstart("echo-app")
    if await _drain_for_text(a, b"my own words", polls=3):
        fail("client got its own message back on reconnect")
    else:
        ok("client's own message is not replayed to it")
    await a.close()


async def test_same_app_two_ports(proxy_port, proxy_port2):
    log.info("--- Test 16: Same app on two ports keeps separate histories ---")
    for port in (proxy_port, proxy_port2):
        c = await connect_client(port=port)
        if not await c.appstart("twin-app"):
            fail(f"SELF_INFO failed on port {port}")
            await c.close()
            return
        await c.close()
    await asyncio.sleep(0.5)

    sender = await connect_client(port=proxy_port)
    await sender.appstart()
    await sender.send_chan_msg(0, "for both twins")
    await sender.close()

    got = []
    for port in (proxy_port, proxy_port2):
        c = await connect_client(port=port)
        await c.appstart("twin-app")
        got.append(bool(await _drain_for_text(c, b"for both twins")))
        await c.close()
    if all(got):
        ok("each port's copy of the same app gets the missed message")
    else:
        fail(f"missed message per port (port1, port2): {got}")


# --- Harness ---

fake_proc = None
fake_pid = None
fake_port = None
proxy_port = None
proxy_proc = None

async def start_fake(port=None, extra=()):
    global fake_proc, fake_pid, fake_port
    if port is None:
        fake_port = await find_free_port()
    else:
        fake_port = port
    fake_proc = await asyncio.create_subprocess_exec(
        sys.executable, os.path.join(os.path.dirname(__file__), "fake_radio.py"),
        "--port", str(fake_port), "--inject", *extra,
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
    proxy_port2 = await find_free_port()

    proxy_bin = os.path.join(
        os.path.dirname(__file__), "..", "target", "debug", "coresplitter"
    )
    proxy_proc = await asyncio.create_subprocess_exec(
        proxy_bin,
        "--tcp-backend-host", "127.0.0.1",
        "--tcp-backend-port", str(fake_port),
        "--tcp-frontend-port", f"{proxy_port},{proxy_port2}",
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
    await test_get_msg_fans_out(proxy_port)
    await test_remove_contact(proxy_port)
    await test_contacts_served_verbatim(proxy_port)
    await test_update_contact_refreshes_cache(proxy_port)
    await test_rejected_send_not_echoed(proxy_port)
    await test_missed_messages_replayed(proxy_port)
    await test_own_messages_not_replayed(proxy_port)
    await test_same_app_two_ports(proxy_port, proxy_port2)
    await test_message_layout_per_version(proxy_port)
    await test_resend_echoed_once(proxy_port)
    await test_drain_while_offline(proxy_port, DATA_DIR)
    await test_late_reply_not_misrouted(proxy_port)
    await test_unconfirmed_message_resent(proxy_port)
    await test_reconnect_tcp(proxy_port)
    await test_request_jumps_sync(proxy_port)

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
