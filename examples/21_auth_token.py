"""Authentication — a token-protected server.

NATS supports several auth schemes (tokens, user/password, NKeys, JWTs).
This example starts its own server requiring a shared token: anonymous
connections are rejected with an authorization violation, while clients
presenting the token get in. The same connect(token=...) shape applies
to the richer schemes.
"""

import asyncio

import nats
import nats.errors

from _nats_config import start_server

TOKEN = "s3cr3t"


async def quiet_error_cb(exc: Exception) -> None:
    """Auth failures also reach the client's error callback; the default
    one prints a full traceback, which we handle via the raised error."""


async def main() -> None:
    _, url = start_server("--auth", TOKEN, jetstream=False)
    print("server started with --auth <token>")

    print("=== connecting without credentials ===")
    try:
        await nats.connect(
            url, allow_reconnect=False, error_cb=quiet_error_cb
        )
    except nats.errors.Error as exc:
        print(f"rejected: {exc}")

    print("=== connecting with the wrong token ===")
    try:
        await nats.connect(
            url, token="wrong", allow_reconnect=False, error_cb=quiet_error_cb
        )
    except nats.errors.Error as exc:
        print(f"rejected: {exc}")

    print("=== connecting with the right token ===")
    nc = await nats.connect(url, token=TOKEN)
    print("connected:", nc.is_connected)

    sub = await nc.subscribe("secure.topic")
    await nc.publish("secure.topic", b"members only")
    msg = await sub.next_msg(timeout=1)
    print("received:", msg.data.decode())

    await nc.drain()


asyncio.run(main())
