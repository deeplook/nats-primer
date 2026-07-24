"""Request-reply — RPC on top of pub/sub.

nc.request() publishes a message with an auto-generated, ephemeral
reply subject and waits for the first response. A responder simply
subscribes to the request subject and calls msg.respond(). If no
responder is running, the server answers immediately with a
"no responders" error instead of letting the request time out.
"""

import asyncio

import nats.errors
from _nats_config import connect
from nats.aio.msg import Msg


async def main() -> None:
    nc = await connect()

    async def responder(msg: Msg) -> None:
        name = msg.data.decode()
        await msg.respond(f"Hello, {name}!".encode())

    sub = await nc.subscribe("greet", cb=responder)

    print("=== simple request ===")
    reply = await nc.request("greet", b"world", timeout=1.0)
    print("reply:", reply.data.decode())

    print("=== concurrent requests ===")
    replies = await asyncio.gather(
        *(
            nc.request("greet", name.encode(), timeout=1.0)
            for name in ["Alice", "Bob", "Carol"]
        )
    )
    for r in replies:
        print("reply:", r.data.decode())

    print("=== request without a responder ===")
    await sub.unsubscribe()
    try:
        await nc.request("greet", b"anyone?", timeout=1.0)
    except nats.errors.NoRespondersError:
        print("no responders available (failed fast, no timeout needed)")

    await nc.drain()


asyncio.run(main())
