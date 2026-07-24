"""Key-Value store — a distributed KV built on JetStream.

A KV bucket is a stream in disguise: every put is a message, so you
get revisions, per-key history, deletes as tombstones, and live
watchers for free. It behaves like a small etcd/Consul for config,
feature flags, or service state.
"""

import asyncio

from _nats_config import connect
from nats.js.errors import KeyNotFoundError


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()
    kv = await js.create_key_value(bucket="CONFIG", history=5)

    print("=== put / get ===")
    rev = await kv.put("app.color", b"blue")
    print("put app.color=blue -> revision", rev)
    entry = await kv.get("app.color")
    print(f"get app.color -> {entry.value.decode()} (revision {entry.revision})")

    print("=== updates create revisions, history keeps them ===")
    await kv.put("app.color", b"green")
    await kv.put("app.color", b"red")
    for e in await kv.history("app.color"):
        print(f"revision {e.revision}: {e.value.decode()}")

    print("=== watching a key for live changes ===")
    watcher = await kv.watchall()
    await watcher.updates(timeout=1)  # skip initial values marker

    async def change_settings() -> None:
        await kv.put("app.color", b"purple")
        await kv.put("app.timeout", b"30")
        await kv.delete("app.timeout")

    asyncio.get_running_loop().create_task(change_settings())
    for _ in range(3):
        e = await watcher.updates(timeout=2)
        if e is not None:
            op = e.operation or "PUT"
            value = e.value.decode() if e.value else ""
            print(f"watch: {op} {e.key} {value}")
    await watcher.stop()

    print("=== deleted keys raise KeyNotFoundError ===")
    try:
        await kv.get("app.timeout")
    except KeyNotFoundError:
        print("app.timeout is gone")

    print("bucket status:", (await kv.status()).values, "values")
    await js.delete_key_value("CONFIG")
    await nc.drain()


asyncio.run(main())
