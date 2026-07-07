"""TLS — encrypting client connections.

Production NATS runs with TLS. This example generates a throwaway
self-signed certificate with openssl, starts a TLS-only server, and
shows both failure modes: a client that does not trust the certificate
is rejected during the handshake, while a client whose SSL context
trusts it connects — and everything after that works unchanged.
"""

import asyncio
import logging
import ssl
import subprocess
import tempfile
from pathlib import Path

import nats

from _nats_config import free_port, start_server

# On close, asyncio warns that nats-py's transport returns True from
# eof_received(), which has no effect over SSL — harmless, so hide it.
logging.getLogger("asyncio").setLevel(logging.ERROR)


def make_self_signed_cert(directory: Path) -> tuple[Path, Path]:
    """Create key.pem/cert.pem valid for 127.0.0.1, one day."""
    key, cert = directory / "key.pem", directory / "cert.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=IP:127.0.0.1,DNS:localhost",
        ],
        check=True,
        capture_output=True,
    )
    return key, cert


async def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="nats-primer-tls-"))
    key, cert = make_self_signed_cert(workdir)
    print("generated self-signed certificate for 127.0.0.1")

    port = free_port()
    start_server(
        "--tls",
        "--tlscert",
        str(cert),
        "--tlskey",
        str(key),
        port=port,
        jetstream=False,
    )
    url = f"tls://127.0.0.1:{port}"
    print("server started in TLS-only mode")

    async def quiet_error_cb(exc: Exception) -> None:
        pass  # the rejection is reported below, keep stderr clean

    print("=== an untrusting client is rejected in the handshake ===")
    try:
        await nats.connect(url, allow_reconnect=False, error_cb=quiet_error_cb)
    except Exception as exc:
        print(f"rejected: {type(exc).__name__}: {exc}")

    print("=== trusting the certificate ===")
    ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    ctx.load_verify_locations(str(cert))
    nc = await nats.connect(url, tls=ctx)
    print("connected over TLS:", nc.is_connected)

    sub = await nc.subscribe("secure")
    await nc.publish("secure", b"encrypted on the wire")
    msg = await sub.next_msg(timeout=1)
    print("received:", msg.data.decode())

    await nc.drain()


asyncio.run(main())
