"""Stream administration — inspecting, purging, and mirroring streams.

The JetStream context doubles as an admin API: list streams, inspect
their state, update configuration, purge messages by subject, and set
up mirrors — read-only copies of a stream that stay in sync, used for
scaling out readers or migrating data.
"""

import asyncio

from nats.js.api import StreamConfig, StreamSource

from _nats_config import connect


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()
    await js.add_stream(name="ORDERS", subjects=["orders.*"])
    for i in range(4):
        await js.publish("orders.eu", f"eu-{i}".encode())
    for i in range(2):
        await js.publish("orders.us", f"us-{i}".encode())

    print("=== stream info ===")
    info = await js.stream_info("ORDERS")
    print(
        f"messages: {info.state.messages}, "
        f"first seq: {info.state.first_seq}, last seq: {info.state.last_seq}"
    )

    print("=== a mirror stays in sync automatically ===")
    await js.add_stream(
        StreamConfig(name="ORDERS-MIRROR", mirror=StreamSource(name="ORDERS"))
    )
    await asyncio.sleep(0.5)
    minfo = await js.stream_info("ORDERS-MIRROR")
    print(f"mirror holds {minfo.state.messages} messages")

    print("=== updating stream limits ===")
    config = info.config
    config.max_msgs = 100
    await js.update_stream(config)
    print("max_msgs now:", (await js.stream_info("ORDERS")).config.max_msgs)

    print("=== purging by subject ===")
    await js.purge_stream("ORDERS", subject="orders.eu")
    info = await js.stream_info("ORDERS")
    print(f"after purging orders.eu: {info.state.messages} messages left")

    print("=== listing all streams ===")
    for s in await js.streams_info():
        print(f"{s.config.name}: {s.state.messages} messages")

    for name in ["ORDERS-MIRROR", "ORDERS"]:
        await js.delete_stream(name)
    await nc.drain()


asyncio.run(main())
