#!/usr/bin/env python3
"""Simula la secuencia exacta del cliente flutter para depurar GET_CONTACTS."""
import socket, struct, sys, time

HOST = sys.argv[1] if len(sys.argv) > 1 else "192.168.1.10"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5050

s = socket.create_connection((HOST, PORT), timeout=15)

def send(data):
    frame = b"\x3c" + struct.pack("<H", len(data)) + data
    s.sendall(frame)

def recv(timeout=5):
    s.settimeout(timeout)
    h = s.recv(3)
    if not h or len(h) < 3:
        return None
    assert h[0] == 0x3e, f"bad header {h[0]:#x}"
    plen = struct.unpack("<H", h[1:3])[0]
    return s.recv(plen, socket.MSG_WAITALL)

# --- Secuencia flutter real ---

# 1. DEVICE_QUERY
send(bytes([0x16]))
r = recv()
print(f"DEVICE_QUERY -> {len(r)}B code=0x{r[0]:02x}" if r else "DEVICE_QUERY -> timeout")

# 2. APPSTART
send(bytes([0x01, 0x01]) + b"testclient")
r = recv()
print(f"APPSTART    -> {len(r)}B code=0x{r[0]:02x}" if r else "APPSTART -> timeout")

# 3. SET_FLOOD_SCOPE (0x1C)
send(bytes([0x1C, 0x02]))
r = recv()
print(f"SET_FLOOD   -> {len(r)}B code=0x{r[0]:02x}" if r else "SET_FLOOD -> timeout")

# 4. GET_BATTERY (0x18)
send(bytes([0x18]))
r = recv()
print(f"GET_BATTERY -> {len(r)}B code=0x{r[0]:02x}" if r else "GET_BATTERY -> timeout")

# 5. SET_TIME (0x23)
import time as _time
now = int(_time.time())
send(bytes([0x23]) + struct.pack("<I", now))
r = recv()
print(f"SET_TIME    -> {len(r)}B code=0x{r[0]:02x}" if r else "SET_TIME -> timeout")

# 6. GET_CONTACTS
send(bytes([0x04]))
print("GET_CONTACTS enviado, recibiendo...")
count = 0
contact_bodies = []
while True:
    r = recv(3)
    if r is None:
        break
    count += 1
    code = r[0]
    labels = {2: "START", 3: "CONTACT", 4: "END", 5: "SELF_INFO",
              6: "MSG_SENT", 0x0D: "DEVICE_INFO", 0x12: "CHANNEL_INFO",
              0x07: "CONTACT_MSG_RECV", 0x08: "CHANNEL_MSG_RECV",
              0x83: "MSGS_WAITING", 0x0A: "NO_MORE_MSGS",
              0x01: "ERROR"}
    label = labels.get(code, f"0x{code:02x}")
    extra = ""
    if code == 3:
        contact_bodies.append(r)
        extra = f" pk={r[1:33].hex()[:8]}..."
    print(f"  [{len(r):3}B] code=0x{code:02x} {label}{extra}")
    if code == 4:
        break
    if count > 200:
        print("  [corte por límite]")
        break

print(f"\nTotal: {count} respuestas ({len(contact_bodies)} contact bodies)")
s.close()
