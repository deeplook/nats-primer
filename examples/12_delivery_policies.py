"""Delivery policies — replaying a stream from any point.

Each consumer chooses where in the stream it starts: ALL replays from
the beginning, LAST starts with the most recent message, NEW only sees
messages published after it was created, BY_START_SEQUENCE starts at a
given sequence number, and LAST_PER_SUBJECT gives the latest message
for every subject — a materialized snapshot.
"""

import asyncio

from nats.js.api import ConsumerConfig, DeliverPolicy

from _nats_config import connect


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()
    await js.add_stream(name="TICKER", subjects=["ticker.*"])
    prices = [
        ("ticker.AAPL", "101"),
        ("ticker.MSFT", "202"),
        ("ticker.AAPL", "103"),
        ("ticker.MSFT", "204"),
        ("ticker.AAPL", "105"),
    ]
    for subject, price in prices:
        await js.publish(subject, price.encode())

    async def read(name: str, config: ConsumerConfig) -> None:
        psub = await js.pull_subscribe("ticker.*", durable=name, config=config)
        try:
            msgs = await psub.fetch(5, timeout=1)
        except Exception:
            msgs = []
        summary = [f"{m.subject.split('.')[1]}={m.data.decode()}" for m in msgs]
        print(f"{name:<18} -> {summary}")
        for m in msgs:
            await m.ack()

    print("=== the stream holds 5 messages ===")
    await read("all", ConsumerConfig(deliver_policy=DeliverPolicy.ALL))
    await read("last", ConsumerConfig(deliver_policy=DeliverPolicy.LAST))
    await read(
        "from-seq-4",
        ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_SEQUENCE, opt_start_seq=4),
    )
    await read(
        "last-per-subject",
        ConsumerConfig(deliver_policy=DeliverPolicy.LAST_PER_SUBJECT),
    )

    print("=== NEW ignores history, sees only what comes next ===")
    psub = await js.pull_subscribe(
        "ticker.*",
        durable="new",
        config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW),
    )
    await js.publish("ticker.AAPL", b"106")
    msgs = await psub.fetch(5, timeout=1)
    print(
        "new              ->",
        [f"{m.subject.split('.')[1]}={m.data.decode()}" for m in msgs],
    )

    await js.delete_stream("TICKER")
    await nc.drain()


asyncio.run(main())
