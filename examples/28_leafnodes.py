"""Leaf nodes — extending a NATS system to the edge.

A leaf node is a full nats-server that connects *outbound* to a hub
and bridges traffic between its local clients and the wider system.
Unlike cluster routes (a mesh of peers, example 22), the leaf owns its
local traffic and only forwards subjects with remote interest — the
standard topology for edge locations, factories, or per-tenant
servers behind firewalls.
"""

import asyncio
import tempfile
from pathlib import Path

import nats
from _nats_config import free_port, start_server

HUB_CONFIG = """
leafnodes {{
  port: {leaf_port}
}}
"""

LEAF_CONFIG = """
leafnodes {{
  remotes = [ {{ url: "nats://127.0.0.1:{leaf_port}" }} ]
}}
"""


def write_config(text: str) -> Path:
    conf = Path(tempfile.mkstemp(suffix=".conf")[1])
    conf.write_text(text)
    return conf


async def main() -> None:
    print("=== starting a hub and a leaf node ===")
    leaf_port = free_port()
    hub_conf = write_config(HUB_CONFIG.format(leaf_port=leaf_port))
    leaf_conf = write_config(LEAF_CONFIG.format(leaf_port=leaf_port))
    _, hub_url = start_server("-c", str(hub_conf), jetstream=False)
    _, leaf_url = start_server("-c", str(leaf_conf), jetstream=False)
    print(f"hub on {hub_url}, accepting leaf connections on {leaf_port}")
    print(f"leaf on {leaf_url}, dialing out to the hub")
    await asyncio.sleep(0.5)  # let the leaf connection establish

    cloud = await nats.connect(hub_url)
    edge = await nats.connect(leaf_url)

    print("=== edge to cloud ===")
    sub = await cloud.subscribe("telemetry.>")
    await cloud.flush()
    await asyncio.sleep(0.3)  # interest must propagate to the leaf
    await edge.publish("telemetry.plant1.temp", b"73.2")
    msg = await sub.next_msg(timeout=2)
    print(f"cloud received [{msg.subject}]: {msg.data.decode()}")

    print("=== cloud to edge (request-reply across the leaf) ===")
    cmd_sub = await edge.subscribe("commands.plant1")

    async def responder() -> None:
        msg = await cmd_sub.next_msg(timeout=3)
        await msg.respond(b"valve closed")

    task = asyncio.get_running_loop().create_task(responder())
    await edge.flush()
    await asyncio.sleep(0.3)
    reply = await cloud.request("commands.plant1", b"close valve", timeout=2)
    print("cloud got reply from the edge:", reply.data.decode())
    await task

    await cloud.drain()
    await edge.drain()
    for conf in (hub_conf, leaf_conf):
        conf.unlink()


asyncio.run(main())
