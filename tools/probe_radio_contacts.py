#!/usr/bin/env python3
"""Prueba GET_CONTACTS contra radio real con más tiempo."""
import socket, struct, sys, time

HOST = "192.168.1.161"
PORT = 5000

s = socket.create_connection((HOST, PORT), timeout=30)

def send(data):
    frame = b"\x3c" + struct.pack("<H", len(data)) + data
    s.sendall(frame)
    print(f"  Sent {len(data)}B: {data.hex()}")

def recv(timeout=10):
    s.settimeout(timeout)
    h = s.recv(3)
    if not h or len(h) < 3:
        print(f"  recv header failed: got {len(h) if h else 0}B")
        return None
    if h[0] != 0x3e:
        print(f"  bad header byte={h[0]:#x} h={h.hex()}")
        return None
    plen = struct.unpack("<H", h[1:3])[0]
    if plen == 0:
        return b""
    body = s.recv(plen, socket.MSG_WAITALL)
    print(f"  <- [{plen}B] code=0x{body[0]:02x} {body.hex()[:80]}")
    return body

# Paso 1: AppStart
print("1. APPSTART")
send(bytes([0x01, 0x01]) + b"probe")
r = recv(5)
if r is None:
    print("  No response, trying again...")
    r = recv(5)
if r and len(r) > 2:
    name = r[46:].rstrip(b'\x00').decode('utf-8', errors='replace')
    print(f"  SELF_INFO: name={name}")

# Paso 2: GET_CONTACTS
print("\n2. GET_CONTACTS")
send(bytes([0x04]))
time.sleep(1)

# Leer respuestas
count = 0
contact_sizes = set()
while count < 250:
    r = recv(5)
    if r is None:
        break
    count += 1
    if r[0] == 3:
        contact_sizes.add(len(r))
    if r[0] == 4:
        print(f"  CONTACT_END after {count} messages")
        break

print(f"\nTotal: {count} mensajes")
print(f"Contact sizes: {contact_sizes}")
s.close()
