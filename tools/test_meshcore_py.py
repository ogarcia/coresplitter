#!/usr/bin/env python3
"""Test meshcore-py client against proxy."""
import asyncio, sys
from meshcore import TCPConnection, MeshCore, EventType

HOST = sys.argv[1] if len(sys.argv) > 1 else "192.168.1.10"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5050

async def main():
    cx = TCPConnection(HOST, PORT)
    await cx.connect()
    print(f"Conectado a {HOST}:{PORT}")

    mc = MeshCore(cx, debug=False)
    await mc.connect()
    print("MeshCore conectado")

    contacts_event = asyncio.Event()
    contacts_data = []

    def on_contacts(ev):
        contacts_data.append(ev)
        contacts_event.set()

    mc.subscribe(EventType.CONTACTS, on_contacts)
    mc.subscribe(EventType.SELF_INFO, lambda ev: print(f"SELF_INFO: {ev}"))
    mc.subscribe(EventType.DEVICE_INFO, lambda ev: print(f"DEVICE_INFO: {ev}"))
    mc.subscribe(EventType.OK, lambda ev: print(f"OK: {ev}"))
    mc.subscribe(EventType.ERROR, lambda ev: print(f"ERROR: {ev}"))

    ok = await mc.ensure_contacts()
    print(f"ensure_contacts returned {ok}")

    try:
        await asyncio.wait_for(contacts_event.wait(), timeout=15)
        print(f"\nContactos recibidos: {len(contacts_data)} eventos")
        print(f"Contactos en objeto: {len(mc.contacts)}")
        for name, c in list(mc.contacts.items())[:5]:
            print(f"  {name}: pk={c.public_key.hex()[:16]}...")
        if len(mc.contacts) > 5:
            print(f"  ... y {len(mc.contacts)-5} más")
    except asyncio.TimeoutError:
        print("TIMEOUT esperando contactos!")
        print(f"mc.contacts tiene {len(mc.contacts)} entradas")
        print(f"mc._contacts tiene {len(mc._contacts) if hasattr(mc, '_contacts') else 'N/A'}")

    await mc.disconnect()

asyncio.run(main())
