"""Scatter-gather — one request, many replies.

nc.request() returns only the first answer. To poll a whole fleet,
publish the request yourself with an explicit reply inbox and keep
reading that inbox until a deadline. Every subscriber on the subject
responds, and the requester gathers all answers — service discovery
and quorum patterns work exactly like this.
"""

import asyncio

import nats.errors
from nats.aio.msg import Msg

from _nats_config import connect


async def main() -> None:
    nc = await connect()

    def make_node(name: str, load: int):
        async def handler(msg: Msg) -> None:
            await msg.respond(f"{name}: load={load}".encode())

        return handler

    for name, load in [("node-a", 3), ("node-b", 7), ("node-c", 1)]:
        await nc.subscribe("cluster.status", cb=make_node(name, load))

    print("=== plain request gets only the fastest reply ===")
    reply = await nc.request("cluster.status", b"ping", timeout=1.0)
    print("first reply:", reply.data.decode())

    print("=== scatter-gather collects them all ===")
    inbox = nc.new_inbox()
    replies = await nc.subscribe(inbox)
    await nc.publish("cluster.status", b"ping", reply=inbox)

    answers: list[str] = []
    while True:
        try:
            msg = await replies.next_msg(timeout=0.5)
        except nats.errors.TimeoutError:
            break
        answers.append(msg.data.decode())
    for answer in sorted(answers):
        print("gathered:", answer)
    print(f"got {len(answers)} replies from one request")

    await nc.drain()


asyncio.run(main())
