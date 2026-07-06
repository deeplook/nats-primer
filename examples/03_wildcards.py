"""Subject hierarchies and wildcards.

Subjects are dot-separated token hierarchies like "sensors.berlin.temp".
Subscriptions can use two wildcards: "*" matches exactly one token,
">" matches one or more trailing tokens. This lets subscribers slice a
message stream by topic without any broker-side configuration.
"""

import asyncio

from nats.aio.msg import Msg

from _nats_config import connect


async def main() -> None:
    nc = await connect()

    def make_handler(label: str):
        async def handler(msg: Msg) -> None:
            print(f"{label:<24} got [{msg.subject}]: {msg.data.decode()}")

        return handler

    await nc.subscribe("sensors.*.temp", cb=make_handler("sensors.*.temp"))
    await nc.subscribe("sensors.berlin.*", cb=make_handler("sensors.berlin.*"))
    await nc.subscribe("sensors.>", cb=make_handler("sensors.>"))

    messages = {
        "sensors.berlin.temp": b"21.5",
        "sensors.tokyo.temp": b"28.0",
        "sensors.berlin.humidity": b"40",
        "sensors.berlin.roof.wind": b"12",
    }
    for subject, data in messages.items():
        print(f"=== publishing {subject} ===")
        await nc.publish(subject, data)
        await nc.flush()
        await asyncio.sleep(0.2)

    await nc.drain()


asyncio.run(main())
