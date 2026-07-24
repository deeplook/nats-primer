"""Queue groups — load balancing across subscribers.

Subscribers that join the same queue group share the work: each message
on the subject is delivered to exactly one (randomly chosen) member of
the group instead of to all of them. This gives horizontal scaling of
workers with no extra infrastructure. Subscribers outside the group
still receive every message.
"""

import asyncio
from collections import Counter

from _nats_config import connect
from nats.aio.msg import Msg


async def main() -> None:
    nc = await connect()
    counts: Counter[str] = Counter()

    def make_worker(name: str):
        async def handler(msg: Msg) -> None:
            counts[name] += 1

        return handler

    for name in ["worker-1", "worker-2", "worker-3"]:
        await nc.subscribe("jobs", queue="processors", cb=make_worker(name))
    await nc.subscribe("jobs", cb=make_worker("auditor"))  # not in the group

    total = 30
    print(f"=== publishing {total} jobs ===")
    for i in range(total):
        await nc.publish("jobs", f"job-{i}".encode())
    await nc.flush()
    await asyncio.sleep(0.3)

    print("=== distribution ===")
    for name in sorted(counts):
        print(f"{name}: {counts[name]}")
    group_total = sum(counts[w] for w in counts if w.startswith("worker"))
    print(f"group members handled {group_total} jobs between them,")
    print(f"the auditor outside the group saw all {counts['auditor']}")

    await nc.drain()


asyncio.run(main())
