#!/usr/bin/env python3
"""Compara GET_CONTACTS contra radio real vs proxy."""
import socket, struct, sys, time

def connect(host, port):
    s = socket.create_connection((host, port), timeout=15)
    return s

def send(s, data):
    frame = b"\x3c" + struct.pack("<H", len(data)) + data
    s.sendall(frame)

def recv(s, timeout=5):
    s.settimeout(timeout)
    h = s.recv(3)
    if not h or len(h) < 3:
        return None
    assert h[0] == 0x3e, f"bad header {h[0]:#x}"
    plen = struct.unpack("<H", h[1:3])[0]
    return s.recv(plen, socket.MSG_WAITALL)

def drain(s, timeout=1, max_msgs=300):
    """Lee todas las respuestas disponibles."""
    msgs = []
    while len(msgs) < max_msgs:
        r = recv(s, timeout)
        if r is None:
            break
        msgs.append(r)
    return msgs

# Probar contra radio real
print("=== RADIO REAL (192.168.1.161:5000) ===")
sr = connect("192.168.1.161", 5000)
send(sr, bytes([0x01, 0x01]) + b"probe")
si = recv(sr)
print(f"SELF_INFO: {len(si)}B")

# GET_CONTACTS a la radio
send(sr, bytes([0x04]))
print("GET_CONTACTS enviado...")
t0 = time.time()
radio_msgs = drain(sr, timeout=3, max_msgs=200)
t1 = time.time()
print(f"Recibidos {len(radio_msgs)} mensajes en {t1-t0:.2f}s")
for m in radio_msgs[:5]:
    print(f"  [{len(m):3}B] code=0x{m[0]:02x} hex={m.hex()[:60]}...")
if len(radio_msgs) > 5:
    print(f"  ... y {len(radio_msgs)-5} más")

# Contact entry sizes
contact_sizes = [len(m) for m in radio_msgs if m[0] == 3]
print(f"Contact entries: {len(contact_sizes)}, sizes={set(contact_sizes)}")
sr.close()

print()

# Probar contra proxy
print("=== PROXY (192.168.1.10:5050) ===")
sp = connect("192.168.1.10", 5050)
send(sp, bytes([0x01, 0x01]) + b"probe")
si = recv(sp)
print(f"SELF_INFO: {len(si)}B")

send(sp, bytes([0x04]))
print("GET_CONTACTS enviado...")
t0 = time.time()
proxy_msgs = drain(sp, timeout=2, max_msgs=200)
t1 = time.time()
print(f"Recibidos {len(proxy_msgs)} mensajes en {t1-t0:.2f}s")
for m in proxy_msgs[:5]:
    print(f"  [{len(m):3}B] code=0x{m[0]:02x} hex={m.hex()[:60]}...")
if len(proxy_msgs) > 5:
    print(f"  ... y {len(proxy_msgs)-5} más")

contact_sizes_p = [len(m) for m in proxy_msgs if m[0] == 3]
print(f"Contact entries: {len(contact_sizes_p)}, sizes={set(contact_sizes_p)}")
sp.close()
