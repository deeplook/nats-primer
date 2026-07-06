"""Ordered consumers — strict, gap-free stream reading.

An ordered consumer is an ephemeral push consumer managed entirely by
the client: no acks (the server never redelivers), single-threaded
in-order delivery, and if a gap in sequence numbers is detected the
client silently recreates the consumer at the right position. It is
the right tool for read-only replays where order matters more than
work sharing — think event sourcing or cache rebuilding.
"""

import asyncio

from _nats_config import connect


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()
    await js.add_stream(name="LEDGER", subjects=["ledger.*"])
    for i in range(1, 11):
        await js.publish("ledger.tx", f"tx-{i:03d}".encode())

    print("=== ordered consumer: strictly sequential ===")
    sub = await js.subscribe("ledger.*", ordered_consumer=True)
    received: list[int] = []
    for _ in range(10):
        msg = await sub.next_msg(timeout=2)
        received.append(msg.metadata.sequence.stream)
    print("stream sequences:", received)
    print("in order, no gaps:", received == sorted(received) == list(range(1, 11)))

    print("=== it is ephemeral and flow-controlled ===")
    cinfo = await sub.consumer_info()
    print("durable name:", cinfo.config.durable_name)
    print("ack policy:", cinfo.config.ack_policy)
    print("flow control:", cinfo.config.flow_control)

    await sub.unsubscribe()
    await js.delete_stream("LEDGER")
    await nc.drain()


asyncio.run(main())
