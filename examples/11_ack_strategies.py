"""Acknowledgement strategies — ack, nak, term and redelivery.

JetStream redelivers a message until the consumer settles it:
ack() marks it done, nak(delay) asks for redelivery (optionally after a
backoff), term() drops it permanently (e.g. a poison message), and
in_progress() extends the ack deadline for slow work. The metadata on
each message shows how often it has been delivered.
"""

import asyncio

from nats.js.api import ConsumerConfig

from _nats_config import connect


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()
    await js.add_stream(name="JOBS", subjects=["jobs.*"])
    await js.publish("jobs.a", b"good job")
    await js.publish("jobs.b", b"flaky job")
    await js.publish("jobs.c", b"poison job")

    psub = await js.pull_subscribe(
        "jobs.*",
        durable="handlers",
        config=ConsumerConfig(ack_wait=1.0, max_deliver=5),
    )

    print("=== first delivery ===")
    for msg in await psub.fetch(3, timeout=2):
        deliveries = msg.metadata.num_delivered
        text = msg.data.decode()
        print(f"got '{text}' (delivery #{deliveries})")
        if text == "good job":
            await msg.ack()
            print("  -> ack: done")
        elif text == "flaky job":
            await msg.nak(delay=0.5)
            print("  -> nak with 0.5s delay: try me again later")
        else:
            await msg.term()
            print("  -> term: never redeliver this one")

    print("=== redelivery ===")
    await asyncio.sleep(0.7)
    msgs = await psub.fetch(3, timeout=2)
    for msg in msgs:
        print(f"got '{msg.data.decode()}' again "
              f"(delivery #{msg.metadata.num_delivered})")
        await msg.in_progress()  # extend the deadline for slow work
        await asyncio.sleep(0.2)
        await msg.ack()
        print("  -> in_progress, then ack")
    print(f"only the nak'd message came back ({len(msgs)} message)")

    await js.delete_stream("JOBS")
    await nc.drain()


asyncio.run(main())
