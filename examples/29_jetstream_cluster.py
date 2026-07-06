"""Replicated JetStream — streams that survive server loss.

Example 22 clustered core NATS; this one clusters JetStream. With
three JetStream-enabled nodes, a stream created with num_replicas=3
keeps a Raft-replicated copy on every node. Killing one node leaves
the stream writable and readable — the remaining replicas elect a new
leader and carry on. This is how NATS provides HA persistence.
"""

import asyncio
import tempfile

import nats

from _nats_config import free_port, start_server


async def main() -> None:
    print("=== starting a 3-node JetStream cluster ===")
    seed_route = free_port()
    procs = {}
    urls = []
    for i in range(3):
        name = f"node-{i}"
        cluster_port = seed_route if i == 0 else free_port()
        proc, url = start_server(
            "--name", name,
            "--cluster_name", "primer",
            "--cluster", f"nats://127.0.0.1:{cluster_port}",
            "--routes", f"nats://127.0.0.1:{seed_route}",
            "-js", "-sd", tempfile.mkdtemp(prefix=f"nats-primer-{name}-"),
            jetstream=False,  # we pass the JetStream flags ourselves
        )
        procs[name] = proc
        urls.append(url)
        print(f"{name} on {url}")

    nc = await nats.connect(servers=urls, reconnect_time_wait=0.5)
    js = nc.jetstream(timeout=10)

    async def eventually(operation):
        """Retry a JetStream API call while the cluster elects leaders."""
        deadline = asyncio.get_running_loop().time() + 30
        while True:
            try:
                return await operation()
            except Exception:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.5)

    print("=== creating a stream with 3 replicas ===")
    # the cluster needs a moment to elect a meta leader first
    await eventually(lambda: js.add_stream(
        name="CRITICAL", subjects=["critical.*"], num_replicas=3))
    info = await js.stream_info("CRITICAL")
    replicas = [info.cluster.leader] + [r.name for r in info.cluster.replicas]
    print("stream leader:", info.cluster.leader)
    print("replica set:", sorted(replicas))

    for i in range(5):
        await js.publish("critical.data", f"record-{i}".encode())
    print("published 5 messages")

    print("=== killing a follower replica ===")
    victim = next(r.name for r in info.cluster.replicas)
    procs[victim].terminate()
    procs[victim].wait(timeout=5)
    print(f"{victim} is gone")

    print("=== the stream keeps working ===")
    # leader re-election takes a moment, so retry through the window
    ack = await eventually(lambda: js.publish(
        "critical.data", b"record-after-failure", timeout=5))
    print(f"publish still acked at seq {ack.seq}")

    psub = await eventually(lambda: js.pull_subscribe(
        "critical.*", durable="reader", stream="CRITICAL"))
    msgs = await eventually(lambda: psub.fetch(6, timeout=5))
    print(f"read back all {len(msgs)} messages, last one:",
          msgs[-1].data.decode())
    for msg in msgs:
        await msg.ack()

    info = await eventually(lambda: js.stream_info("CRITICAL"))
    peers = [f"{r.name}{' (offline)' if r.offline else ''}"
             for r in info.cluster.replicas or []]
    print("current leader:", info.cluster.leader, "- peers:", peers)

    await nc.close()


asyncio.run(main())
