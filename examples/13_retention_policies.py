"""Retention policies — limits, interest, and work-queue streams.

A stream's retention policy decides when messages are removed:
LIMITS (default) keeps everything up to configured size/count/age caps.
INTEREST keeps a message only until every bound consumer has acked it.
WORK_QUEUE deletes a message as soon as *one* consumer acks it — a
persistent task queue where each task is handled exactly once.
"""

import asyncio

from nats.js.api import RetentionPolicy

from _nats_config import connect


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()

    async def count(stream: str) -> int:
        return (await js.stream_info(stream)).state.messages

    print("=== LIMITS: keep the last N messages ===")
    await js.add_stream(
        name="LOGS", subjects=["logs.*"], retention=RetentionPolicy.LIMITS, max_msgs=3
    )
    for i in range(6):
        await js.publish("logs.app", f"line-{i}".encode())
    print(f"published 6, stream keeps {await count('LOGS')} (max_msgs=3)")

    print("=== INTEREST: keep until all consumers acked ===")
    await js.add_stream(
        name="ALERTS", subjects=["alerts.*"], retention=RetentionPolicy.INTEREST
    )
    await js.publish("alerts.cpu", b"ignored")
    print(f"no consumers bound -> {await count('ALERTS')} messages retained")
    psub = await js.pull_subscribe("alerts.*", durable="pager")
    await js.publish("alerts.disk", b"disk full")
    print(f"consumer bound, before ack: {await count('ALERTS')} message")
    for msg in await psub.fetch(1, timeout=2):
        await msg.ack_sync()
    await asyncio.sleep(0.2)
    print(f"after ack: {await count('ALERTS')} messages")

    print("=== WORK_QUEUE: each message consumed exactly once ===")
    await js.add_stream(
        name="QUEUE", subjects=["queue.*"], retention=RetentionPolicy.WORK_QUEUE
    )
    for i in range(3):
        await js.publish("queue.jobs", f"job-{i}".encode())
    print(f"queued: {await count('QUEUE')} jobs")
    psub = await js.pull_subscribe("queue.*", durable="worker")
    for msg in await psub.fetch(3, timeout=2):
        print("worker took:", msg.data.decode())
        await msg.ack_sync()
    await asyncio.sleep(0.2)
    print(f"remaining in queue: {await count('QUEUE')}")

    for stream in ["LOGS", "ALERTS", "QUEUE"]:
        await js.delete_stream(stream)
    await nc.drain()


asyncio.run(main())
