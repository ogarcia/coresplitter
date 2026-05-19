#!/usr/bin/env python3
"""Analiza formato CONTACT de la radio real."""
import socket, struct, sys

HOST = "192.168.1.161"
PORT = 5000

s = socket.create_connection((HOST, PORT), timeout=30)

def send(data):
    s.sendall(b"\x3c" + struct.pack("<H", len(data)) + data)

def recv(timeout=5):
    s.settimeout(timeout)
    h = s.recv(3)
    if not h or len(h) < 3:
        return None
    assert h[0] == 0x3e
    plen = struct.unpack("<H", h[1:3])[0]
    return s.recv(plen, socket.MSG_WAITALL)

# AppStart
send(bytes([0x01, 0x01]) + b"probe")
recv(5)

# GET_CONTACTS
send(bytes([0x04]))

# Read first contact
while True:
    r = recv(3)
    if r is None:
        break
    if r[0] == 3:
        # Print full analysis of first contact
        print(f"Total: {len(r)} bytes")
        print(f"Full hex ({len(r)}B): {r.hex()}")

        # Parse
        code = r[0]
        pk = r[1:33]
        ctype = r[33]
        rsv = r[34:36]
        print(f"\ncode={code:#x}")
        print(f"pk={pk.hex()}")
        print(f"type={ctype}")
        print(f"reserved={rsv.hex()}")

        # Find name - search for null-terminated string after offset 36
        for name_start in range(36, min(110, len(r))):
            end = name_start
            while end < len(r) and r[end] != 0:
                end += 1
            candidate = r[name_start:end]
            if len(candidate) >= 3 and all(32 <= b < 127 for b in candidate):
                print(f"  Potential name at offset {name_start}: '{candidate.decode()}' ({end-name_start} chars, null at {end})")

        # Print name field at offset 101 (proxy's expected offset)
        if len(r) >= 133:
            print(f"\n  name@101: '{r[101:133].rstrip(b'\\x00').decode('utf-8', errors='replace')}'")
        if len(r) >= 134:
            print(f"  name@102: '{r[102:134].rstrip(b'\\x00').decode('utf-8', errors='replace')}'")

        # Print last fields
        print(f"\n  last_advert@133: {struct.unpack('<I', r[133:137])[0] if len(r)>=137 else 'N/A'}")
        print(f"  lat@137: {struct.unpack('<i', r[137:141])[0] / 1e6 if len(r)>=141 else 'N/A'}")
        print(f"  lon@141: {struct.unpack('<i', r[141:145])[0] / 1e6 if len(r)>=145 else 'N/A'}")

        # Print extra bytes beyond 145
        if len(r) > 145:
            print(f"\n  Extra bytes beyond 145: {r[145:].hex()} ({len(r)-145} bytes)")

        break
    if r[0] == 4:
        print(f"CONTACT_END without finding contact?")
        break

s.close()
