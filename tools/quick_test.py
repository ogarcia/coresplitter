#!/usr/bin/env python3
"""Prueba rápida de GET_CONTACTS contra el proxy."""
import socket, struct, sys

HOST = sys.argv[1] if len(sys.argv) > 1 else "192.168.1.10"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5050

s = socket.create_connection((HOST, PORT), timeout=10)

def send(data):
    frame = b"\x3c" + struct.pack("<H", len(data)) + data
    s.sendall(frame)

def recv(timeout=3):
    s.settimeout(timeout)
    h = s.recv(3)
    if not h or len(h) < 3:
        return None
    assert h[0] == 0x3e, f"header={h[0]:#x}"
    plen = struct.unpack("<H", h[1:3])[0]
    return s.recv(plen, socket.MSG_WAITALL)

# AppStart
send(bytes([0x01, 0x01]) + b"testclient")
r = recv()
print(f"SELF_INFO: {len(r)}B, code={r[0]:#x}")

# GetContacts
send(bytes([0x04]))
print("GET_CONTACTS enviado, recibiendo...")
count = 0
while True:
    r = recv(2)
    if r is None:
        break
    count += 1
    code = r[0]
    name = {2: "CONTACT_START", 3: "CONTACT", 4: "CONTACT_END", 0x12: "CHANNEL_INFO"}.get(code, f"0x{code:02x}")
    print(f"  [{len(r)}B] {name}")
    if code == 4:
        break
    if count > 200:
        break

print(f"Total: {count} respuestas")
s.close()
