"""Message headers — metadata alongside the payload.

Like HTTP, NATS messages can carry string key/value headers separate
from the binary payload. Typical uses: content types, trace/correlation
ids, and protocol-level features (JetStream uses headers such as
Nats-Msg-Id for deduplication, shown later in this primer).
"""

import asyncio
import uuid

from nats.aio.msg import Msg

from _nats_config import connect


async def main() -> None:
    nc = await connect()

    async def handler(msg: Msg) -> None:
        print(f"payload: {msg.data.decode()}")
        print("headers:")
        for key, value in (msg.headers or {}).items():
            print(f"  {key}: {value}")

    await nc.subscribe("events", cb=handler)

    print("=== message with headers ===")
    await nc.publish(
        "events",
        b'{"kind": "signup"}',
        headers={
            "Content-Type": "application/json",
            "Trace-Id": str(uuid.uuid4()),
            "Source": "example-06",
        },
    )
    await nc.flush()
    await asyncio.sleep(0.2)

    print("=== headers also work with request-reply ===")

    async def echo_headers(msg: Msg) -> None:
        await msg.respond(msg.headers["X-Request"].upper().encode())

    await nc.subscribe("echo", cb=echo_headers)
    reply = await nc.request(
        "echo", b"", headers={"X-Request": "shout this"}, timeout=1.0
    )
    print("reply built from request header:", reply.data.decode())

    await nc.drain()


asyncio.run(main())
