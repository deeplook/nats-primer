"""Connecting — the first step of every NATS application.

A NATS client opens a single TCP connection to a server and multiplexes
all publishing and subscribing over it. On connect the server sends an
INFO block describing itself (version, limits, cluster membership).
The helper in _nats_config starts a throwaway local server unless the
NATS_URL environment variable points to an existing one.
"""

import asyncio
import time

from _nats_config import connect


async def main() -> None:
    nc = await connect()

    print("connected url:", nc.connected_url.geturl())
    print("server version:", nc.connected_server_version)
    print("max payload:", nc.max_payload, "bytes")
    print("client id:", nc.client_id)

    start = time.perf_counter()
    await nc.flush()
    print(f"round-trip time: {(time.perf_counter() - start) * 1000:.2f} ms")

    print("stats:", nc.stats)
    await nc.close()
    print("closed, is_connected:", nc.is_connected)


asyncio.run(main())
