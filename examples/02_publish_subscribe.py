"""Publish/subscribe — the core NATS messaging pattern.

Publishers send messages to a subject; every active subscriber on that
subject receives a copy. Delivery is fire-and-forget: if nobody is
subscribed when a message is published, it is simply gone (JetStream
adds persistence later in this primer). Messages are byte payloads.
"""

import asyncio

from nats.aio.msg import Msg

from _nats_config import connect


async def main() -> None:
    nc = await connect()
    received: list[str] = []

    async def handler(msg: Msg) -> None:
        text = msg.data.decode()
        received.append(text)
        print(f"received on [{msg.subject}]: {text}")

    sub = await nc.subscribe("greetings", cb=handler)

    print("=== publish to a live subscriber ===")
    for name in ["Alice", "Bob", "Carol"]:
        await nc.publish("greetings", f"Hello, {name}!".encode())
    await nc.flush()
    await asyncio.sleep(0.2)
    print("messages received:", len(received))

    print("=== publish with nobody listening ===")
    await sub.unsubscribe()
    await nc.publish("greetings", b"Anyone there?")
    await nc.flush()
    await asyncio.sleep(0.2)
    print("messages received:", len(received), "(fire-and-forget: it is lost)")

    await nc.drain()


asyncio.run(main())
