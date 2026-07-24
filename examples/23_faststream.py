"""FastStream — a declarative framework on top of NATS.

After doing everything by hand in examples 01-22, this capstone shows
the framework route: FastStream turns subscribers into decorated
functions, validates payloads with Pydantic models, and can serve
auto-generated AsyncAPI docs. Under the hood it drives the same
nats-py client used throughout this primer.
"""

import asyncio

from _nats_config import server_url
from faststream import ExceptionMiddleware
from faststream.nats import NatsBroker
from pydantic import BaseModel, PositiveInt, ValidationError


class Order(BaseModel):
    id: int
    item: str
    qty: PositiveInt


exc_middleware = ExceptionMiddleware()


@exc_middleware.add_handler(ValidationError)
def on_bad_payload(exc: ValidationError) -> None:
    """Without this, FastStream logs the full traceback for every
    undeliverable message; here we reduce it to a one-line rejection."""
    print(f"rejected: {exc.errors()[0]['loc']} - {exc.errors()[0]['msg']}")


broker = NatsBroker(server_url(), middlewares=(exc_middleware,))


@broker.subscriber("orders.new")
async def handle_order(order: Order) -> None:
    print(f"validated order: {order.qty} x {order.item} (id={order.id})")


@broker.subscriber("orders.rpc")
async def quote(order: Order) -> str:
    return f"order {order.id} accepted"


async def main() -> None:
    await broker.start()

    print("=== dicts are parsed and validated into the model ===")
    await broker.publish({"id": 1, "item": "widget", "qty": 3}, "orders.new")
    await asyncio.sleep(0.3)

    print("=== invalid payloads are rejected, not delivered ===")
    await broker.publish({"id": 2, "item": "gadget", "qty": -5}, "orders.new")
    await asyncio.sleep(0.3)
    print("(qty=-5 failed PositiveInt validation; handler never ran)")

    print("=== request-reply: return values become responses ===")
    reply = await broker.request(
        {"id": 3, "item": "gizmo", "qty": 1}, "orders.rpc", timeout=2
    )
    print("reply:", await reply.decode())

    await broker.stop()


asyncio.run(main())
