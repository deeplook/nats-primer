"""Structured payloads — sending JSON and dataclasses over NATS.

NATS payloads are raw bytes; serialization is the application's job.
The most common choice is JSON. This example round-trips a Python
dataclass through a subject, a pattern that generalizes to any codec
(msgpack, protobuf, ...). Higher-level frameworks like FastStream
(example 23) automate this with Pydantic models.
"""

import asyncio
import json
from dataclasses import asdict, dataclass

from nats.aio.msg import Msg

from _nats_config import connect


@dataclass
class Order:
    id: int
    item: str
    qty: int
    price: float


def encode(order: Order) -> bytes:
    return json.dumps(asdict(order)).encode()


def decode(data: bytes) -> Order:
    return Order(**json.loads(data))


async def main() -> None:
    nc = await connect()

    async def handle_order(msg: Msg) -> None:
        order = decode(msg.data)
        total = order.qty * order.price
        print(f"processing {order} -> total {total:.2f}")

    await nc.subscribe("orders.new", cb=handle_order)

    print("=== publishing dataclasses as JSON ===")
    orders = [
        Order(id=1, item="widget", qty=3, price=9.99),
        Order(id=2, item="gadget", qty=1, price=24.50),
    ]
    for order in orders:
        await nc.publish("orders.new", encode(order))
    await nc.flush()
    await asyncio.sleep(0.2)

    print("=== the wire format is plain JSON bytes ===")
    print(encode(orders[0]).decode())

    await nc.drain()


asyncio.run(main())
