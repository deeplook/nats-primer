"""Exactly-once semantics — deduplication and double-acking.

Distributed systems retry, so the same message may be published twice.
JetStream deduplicates on the publisher side: messages carrying the
same Nats-Msg-Id header within the stream's duplicate_window are
dropped and acked with duplicate=True. On the consumer side,
ack_sync() waits for the server to confirm the ack, closing the gap
where a plain ack could be lost and cause a redelivery.
"""

import asyncio

from _nats_config import connect


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()
    await js.add_stream(name="PAYMENTS", subjects=["payments.*"], duplicate_window=60)

    print("=== publisher-side dedupe with Nats-Msg-Id ===")
    for attempt in range(1, 4):
        ack = await js.publish(
            "payments.eur", b"invoice-42: 99.00", headers={"Nats-Msg-Id": "invoice-42"}
        )
        print(f"attempt {attempt}: stream seq {ack.seq}, duplicate={ack.duplicate}")
    state = (await js.stream_info("PAYMENTS")).state
    print(f"3 publishes, but the stream stores {state.messages} message")

    print("=== a different id is a different message ===")
    ack = await js.publish(
        "payments.eur", b"invoice-43: 12.50", headers={"Nats-Msg-Id": "invoice-43"}
    )
    print(f"stored at seq {ack.seq}, duplicate={ack.duplicate}")

    print("=== consumer-side: double-ack with ack_sync ===")
    psub = await js.pull_subscribe("payments.*", durable="ledger")
    for msg in await psub.fetch(2, timeout=2):
        await msg.ack_sync()  # returns only once the server confirmed
        print(f"confirmed ack for: {msg.data.decode()}")

    await js.delete_stream("PAYMENTS")
    await nc.drain()


asyncio.run(main())
