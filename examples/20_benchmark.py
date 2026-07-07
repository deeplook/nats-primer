"""Throughput — what persistence costs.

A rough, single-machine comparison of publish throughput: core NATS
fire-and-forget, JetStream with synchronous acks (one round trip per
message), and JetStream with concurrent publishes (acks awaited in
batches). Absolute numbers depend on the machine; the *ratios* are the
lesson: persistence costs an ack round trip, and pipelining wins most
of it back.
"""

import asyncio
import time

from _nats_config import connect


def report(label: str, count: int, seconds: float) -> None:
    print(
        f"{label:<36} {count:>6} msgs in {seconds:6.2f}s "
        f"-> {count / seconds:>10,.0f} msgs/s"
    )


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()
    await js.add_stream(name="BENCH", subjects=["bench.*"])
    payload = b"x" * 128

    print("=== core NATS: fire-and-forget ===")
    count = 20_000
    start = time.perf_counter()
    for _ in range(count):
        await nc.publish("bench.core", payload)
    await nc.flush()
    report("core publish", count, time.perf_counter() - start)

    print("=== JetStream: synchronous acks ===")
    count = 2_000
    start = time.perf_counter()
    for _ in range(count):
        await js.publish("bench.js", payload)
    report("jetstream publish (await each ack)", count, time.perf_counter() - start)

    print("=== JetStream: concurrent publishes ===")
    count = 2_000
    batch = 200
    start = time.perf_counter()
    for _ in range(count // batch):
        await asyncio.gather(*(js.publish("bench.js", payload) for _ in range(batch)))
    report(
        f"jetstream publish (batches of {batch})", count, time.perf_counter() - start
    )

    await js.delete_stream("BENCH")
    await nc.drain()


asyncio.run(main())
