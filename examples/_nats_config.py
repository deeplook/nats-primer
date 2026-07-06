"""Shared NATS helper used by all examples in this directory.

If the environment variable NATS_URL is set, connect() uses that server
(it should be started with JetStream enabled, i.e. `nats-server -js`).
Otherwise a throwaway local nats-server is started on a random free port
with JetStream storage in a temporary directory, and torn down when the
script exits. This keeps every example self-contained: no manual server
setup is needed to run them.
"""

import atexit
import os
import shutil
import socket
import subprocess
import tempfile
import time

import nats
from nats.aio.client import Client

_shared_server: subprocess.Popen | None = None
_shared_url: str | None = None


def free_port() -> int:
    """Ask the OS for an unused TCP port."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_until_ready(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"nats-server did not become ready on port {port}")


def start_server(
    *extra_args: str, port: int | None = None, jetstream: bool = True
) -> tuple[subprocess.Popen, str]:
    """Start a throwaway nats-server; it is killed when the script exits.

    Returns the process and the client URL. Examples that need special
    server flags (auth, clustering) call this directly with extra_args;
    everything else goes through connect().
    """
    if shutil.which("nats-server") is None:
        raise RuntimeError(
            "nats-server not found on PATH — see the README prerequisites"
        )
    port = port if port is not None else free_port()
    cmd = ["nats-server", "-a", "127.0.0.1", "-p", str(port)]
    store_dir: str | None = None
    if jetstream:
        store_dir = tempfile.mkdtemp(prefix="nats-primer-js-")
        cmd += ["-js", "-sd", store_dir]
    cmd += list(extra_args)
    proc = subprocess.Popen(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    def cleanup() -> None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        if store_dir is not None:
            shutil.rmtree(store_dir, ignore_errors=True)

    atexit.register(cleanup)
    _wait_until_ready(port)
    return proc, f"nats://127.0.0.1:{port}"


def server_url() -> str:
    """URL of the server examples talk to, starting one if needed."""
    global _shared_server, _shared_url
    env_url = os.environ.get("NATS_URL")
    if env_url:
        return env_url
    if _shared_url is None:
        _shared_server, _shared_url = start_server()
    return _shared_url


async def connect(**options) -> Client:
    """Connect to the shared server (NATS_URL or an auto-started one)."""
    return await nats.connect(server_url(), **options)
