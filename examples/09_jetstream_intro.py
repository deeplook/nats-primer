"""JetStream streams — persistent messaging with publish acks.

Core NATS is fire-and-forget: example 02 showed messages vanishing when
nobody listens. JetStream adds persistence: a stream captures every
message published to its subjects and stores it on disk. js.publish()
waits for the server to acknowledge the write, and consumers can read
the messages later — including ones sent before they existed.
"""

import asyncio

from _nats_config import connect


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()

    print("=== creating a stream ===")
    info = await js.add_stream(name="EVENTS", subjects=["events.>"])
    print("stream:", info.config.name, "subjects:", info.config.subjects)

    print("=== publishing with acknowledgement ===")
    for i in range(5):
        ack = await js.publish("events.click", f"event-{i}".encode())
        print(f"stored in stream {ack.stream} at sequence {ack.seq}")

    print("=== messages persist without any subscriber ===")
    state = (await js.stream_info("EVENTS")).state
    print(f"stream holds {state.messages} messages ({state.bytes} bytes)")

    print("=== a late consumer still gets everything ===")
    psub = await js.pull_subscribe("events.>", durable="late-reader")
    msgs = await psub.fetch(5, timeout=2)
    for msg in msgs:
        print(f"read [{msg.subject}] seq {msg.metadata.sequence.stream}:",
              msg.data.decode())
        await msg.ack()

    await js.delete_stream("EVENTS")
    await nc.drain()


asyncio.run(main())
