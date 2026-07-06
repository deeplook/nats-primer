"""Durable pull consumers — fetching messages in batches.

A consumer is a stateful view on a stream: the server tracks which
messages it has delivered and acknowledged. A *durable* consumer keeps
that cursor across client restarts, identified by its name. With pull
consumers the client controls the pace, fetching batches when it is
ready — the natural fit for scalable workers.
"""

import asyncio

import nats.errors

from _nats_config import connect


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()
    await js.add_stream(name="TASKS", subjects=["tasks.*"])
    for i in range(7):
        await js.publish("tasks.todo", f"task-{i}".encode())

    print("=== fetch in batches ===")
    psub = await js.pull_subscribe("tasks.*", durable="workers")
    for batch_no in range(1, 3):
        msgs = await psub.fetch(3, timeout=2)
        print(f"batch {batch_no}:", [m.data.decode() for m in msgs])
        for msg in msgs:
            await msg.ack()

    print("=== the durable cursor survives the client ===")
    await psub.unsubscribe()
    psub = await js.pull_subscribe("tasks.*", durable="workers")
    msgs = await psub.fetch(3, timeout=2)
    print("after 'restart', continues at:", [m.data.decode() for m in msgs])
    for msg in msgs:
        await msg.ack()

    print("=== fetch on a drained stream times out ===")
    try:
        await psub.fetch(1, timeout=1)
    except nats.errors.TimeoutError:
        print("no more messages (TimeoutError)")

    print("=== consumer bookkeeping ===")
    cinfo = await js.consumer_info("TASKS", "workers")
    print(f"delivered: {cinfo.delivered.consumer_seq}, "
          f"pending: {cinfo.num_pending}")

    # The timed-out fetch can leave a late status message in the pull
    # subscription's inbox, which would stall drain() until its timeout.
    await psub.unsubscribe()
    await js.delete_stream("TASKS")
    await nc.drain()


asyncio.run(main())
