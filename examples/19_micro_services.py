"""Microservices — the nats.micro framework.

nats.micro wraps request-reply with service conventions: named
endpoints in queue groups (so instances load-balance automatically),
error responses, and built-in observability. Every service answers on
$SRV.PING / $SRV.INFO / $SRV.STATS subjects, so discovery and
monitoring need no extra infrastructure.
"""

import asyncio
import json

from nats import micro
from nats.micro import Request

from _nats_config import connect


async def main() -> None:
    nc = await connect()

    async def add(req: Request) -> None:
        numbers = json.loads(req.data)
        await req.respond(str(sum(numbers)).encode())

    async def divide(req: Request) -> None:
        a, b = json.loads(req.data)
        if b == 0:
            await req.respond_error("400", "division by zero")
            return
        await req.respond(str(a / b).encode())

    svc = await micro.add_service(
        nc,
        name="calculator",
        version="1.0.0",
        description="Toy arithmetic service",
    )
    calc = svc.add_group(name="calc")
    await calc.add_endpoint(name="add", handler=add)
    await calc.add_endpoint(name="divide", handler=divide)

    print("=== calling endpoints ===")
    reply = await nc.request("calc.add", json.dumps([1, 2, 3, 4]).encode())
    print("calc.add [1,2,3,4] ->", reply.data.decode())
    reply = await nc.request("calc.divide", json.dumps([10, 4]).encode())
    print("calc.divide [10,4] ->", reply.data.decode())

    print("=== structured error responses ===")
    reply = await nc.request("calc.divide", json.dumps([1, 0]).encode())
    print(
        "error code:",
        reply.headers.get("Nats-Service-Error-Code"),
        "-",
        reply.headers.get("Nats-Service-Error"),
    )

    print("=== discovery: any client can ping all services ===")
    reply = await nc.request("$SRV.PING", b"", timeout=1.0)
    ping = json.loads(reply.data)
    print(f"found service '{ping['name']}' version {ping['version']}, id {ping['id']}")

    print("=== built-in per-endpoint stats ===")
    for ep in svc.stats().endpoints:
        avg_ms = ep.average_processing_time / 1e6
        print(
            f"{ep.name}: {ep.num_requests} requests, "
            f"{ep.num_errors} errors, avg {avg_ms:.2f} ms"
        )

    await svc.stop()
    await nc.drain()


asyncio.run(main())
