#!/usr/bin/env python3
"""Conecta contra la radio física y muestra el hex de CHANNEL_INFO."""
import socket, struct, sys, time

HOST = sys.argv[1] if len(sys.argv) > 1 else "192.168.1.161"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5000

s = socket.create_connection((HOST, PORT), timeout=10)

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

# AppStart
send(bytes([0x01, 0x01]) + b"test_probe")
r = recv()
print(f"SELF_INFO: {len(r)}B")

# Query channels 0..3 and show raw hex
for idx in range(4):
    send(bytes([0x1F, idx]))
    r = recv(3)
    if r is None:
        print(f"CH {idx}: timeout")
        continue
    print(f"CH {idx}: {len(r)}B hex={r.hex()}")
    # Try to decode name at offset 2 (32 bytes)
    if len(r) >= 34:
        name_field = r[2:34]
        # Find first null
        null_pos = name_field.find(b'\x00')
        name = name_field[:null_pos].decode('utf-8', errors='replace') if null_pos >= 0 else name_field.decode('utf-8', errors='replace')
        print(f"  name@2: '{name}' (null at {null_pos})")
    # Also try offset 3
    if len(r) >= 35:
        name_field2 = r[3:35]
        null_pos2 = name_field2.find(b'\x00')
        name2 = name_field2[:null_pos2].decode('utf-8', errors='replace') if null_pos2 >= 0 else name_field2.decode('utf-8', errors='replace')
        print(f"  name@3: '{name2}' (null at {null_pos2})")
    # Show ASCII
    print(f"  ascii: {''.join(chr(b) if 32 <= b < 127 else '.' for b in r)}")

s.close()
