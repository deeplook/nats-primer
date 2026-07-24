"""Object store — storing blobs larger than the message limit.

NATS messages are capped (1 MB by default), so JetStream's object
store chunks large payloads across many messages and reassembles them
on read, with names, metadata and digests. Think of it as a minimal
S3 for data that flows through your messaging system. This example
writes the retrieved copy to out/.
"""

import asyncio
import hashlib
import os
from pathlib import Path

from _nats_config import connect
from nats.js.errors import NotFoundError

OUT_DIR = Path(__file__).resolve().parent.parent / "out"


async def main() -> None:
    nc = await connect()
    js = nc.jetstream()
    obs = await js.create_object_store("FILES")

    print("=== storing a 4 MB blob (larger than max payload) ===")
    blob = os.urandom(4 * 1024 * 1024)
    print("max message payload:", nc.max_payload, "bytes")
    info = await obs.put("random.bin", blob)
    print(f"stored '{info.name}': {info.size} bytes in {info.chunks} chunks")

    print("=== listing objects ===")
    for entry in await obs.list():
        print(f"{entry.name}: {entry.size} bytes, digest {entry.digest}")

    print("=== retrieving and verifying ===")
    result = await obs.get("random.bin")
    same = hashlib.sha256(result.data).digest() == hashlib.sha256(blob).digest()
    print(f"read back {len(result.data)} bytes, content identical: {same}")

    OUT_DIR.mkdir(exist_ok=True)
    target = OUT_DIR / "random.bin"
    target.write_bytes(result.data)
    print("wrote copy to", target)

    print("=== deleting ===")
    await obs.delete("random.bin")
    try:
        remaining = await obs.list(ignore_deletes=True)
    except NotFoundError:  # an empty store has nothing to list
        remaining = []
    print("objects left:", len(remaining))

    await js.delete_object_store("FILES")
    await nc.drain()


asyncio.run(main())
