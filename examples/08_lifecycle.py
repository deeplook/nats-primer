"""Connection lifecycle — reconnects, callbacks, flush and drain.

NATS clients survive server restarts: they buffer published messages
while disconnected and replay subscriptions on reconnect. This example
starts its own dedicated server (regardless of NATS_URL), registers
lifecycle callbacks, then restarts the server on the same port to watch
disconnect/reconnect fire. It ends with drain(), the graceful shutdown
that waits for in-flight messages before closing.
"""

import asyncio
import time

import nats
from _nats_config import free_port, start_server


async def main() -> None:
    port = free_port()
    proc, url = start_server(port=port, jetstream=False)
    events: list[str] = []

    async def disconnected_cb() -> None:
        events.append("disconnected")
        print("callback: disconnected")

    async def reconnected_cb() -> None:
        events.append("reconnected")
        print("callback: reconnected")

    async def closed_cb() -> None:
        events.append("closed")
        print("callback: closed")

    async def error_cb(exc: Exception) -> None:
        print(f"callback: error ({type(exc).__name__})")

    nc = await nats.connect(
        url,
        disconnected_cb=disconnected_cb,
        reconnected_cb=reconnected_cb,
        closed_cb=closed_cb,
        error_cb=error_cb,
        reconnect_time_wait=0.5,
        max_reconnect_attempts=20,
    )
    print("=== connected ===")
    print("url:", nc.connected_url.geturl())

    print("=== killing the server ===")
    proc.terminate()
    proc.wait(timeout=5)
    await asyncio.sleep(1.0)
    print("is_connected:", nc.is_connected)

    print("=== restarting the server on the same port ===")
    start_server(port=port, jetstream=False)
    deadline = time.monotonic() + 10
    while not nc.is_connected and time.monotonic() < deadline:
        await asyncio.sleep(0.2)
    print("is_connected:", nc.is_connected)

    print("=== graceful shutdown with drain ===")
    await nc.drain()
    await asyncio.sleep(0.2)
    print("events observed:", events)
    print("(closing fires one final disconnected before closed)")


asyncio.run(main())
