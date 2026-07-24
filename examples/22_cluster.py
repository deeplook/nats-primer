"""Clustering — three servers, one logical bus, client failover.

NATS servers mesh into clusters via routes: point new nodes at any
existing one and gossip spreads the full topology. Clients connected
to different nodes exchange messages transparently, and when their
node dies they fail over to another. This example starts a 3-node
cluster locally, sends a message across nodes, then kills one node to
watch a client reconnect.
"""

import asyncio

import nats
from _nats_config import free_port, start_server


async def main() -> None:
    print("=== starting a 3-node cluster ===")
    seed_route = free_port()
    procs = []
    urls = []
    for i in range(3):
        cluster_port = seed_route if i == 0 else free_port()
        proc, url = start_server(
            "--cluster_name",
            "primer",
            "--cluster",
            f"nats://127.0.0.1:{cluster_port}",
            "--routes",
            f"nats://127.0.0.1:{seed_route}",
            jetstream=False,
        )
        procs.append(proc)
        urls.append(url)
        print(f"node-{i} listening on {url}")
    await asyncio.sleep(1.0)  # let route gossip settle

    print("=== messages flow across nodes ===")
    reconnected = asyncio.Event()

    async def reconnected_cb() -> None:
        reconnected.set()

    async def error_cb(exc: Exception) -> None:
        pass  # keep the output clean while node-0 goes down

    nc0 = await nats.connect(
        urls[0],
        reconnected_cb=reconnected_cb,
        error_cb=error_cb,
        reconnect_time_wait=0.5,
    )
    nc2 = await nats.connect(urls[2])
    sub = await nc2.subscribe("cross.node")
    await nc2.flush()
    await asyncio.sleep(0.5)  # let the subscription propagate via routes
    await nc0.publish("cross.node", b"published on node-0")
    msg = await sub.next_msg(timeout=2)
    print("received on node-2:", msg.data.decode())

    print("=== clients discover the whole cluster via gossip ===")
    discovered = nc0.servers
    print(f"client connected to 1 node, knows about {len(discovered)}")

    print("=== failover: killing node-0 ===")
    procs[0].terminate()
    procs[0].wait(timeout=5)
    await asyncio.wait_for(reconnected.wait(), timeout=10)
    print("client failed over to:", nc0.connected_url.geturl())

    await nc0.publish("cross.node", b"still alive after failover")
    msg = await sub.next_msg(timeout=2)
    print("received on node-2:", msg.data.decode())

    await nc0.close()
    await nc2.close()


asyncio.run(main())
