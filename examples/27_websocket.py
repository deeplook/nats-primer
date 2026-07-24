"""WebSocket — NATS from browser-friendly environments.

The server can expose the NATS protocol over WebSocket alongside the
regular TCP listener, which is how browser clients (nats.ws) and
restrictive networks reach a NATS deployment. nats-py speaks it too:
connect with a ws:// URL (aiohttp required) and the entire client API
is identical — here a TCP client and a WebSocket client exchange
messages over the same server.
"""

import asyncio
import tempfile
from pathlib import Path

import nats
from _nats_config import free_port, start_server

CONFIG = """
websocket {{
  port: {ws_port}
  no_tls: true
}}
"""


async def main() -> None:
    ws_port = free_port()
    conf = Path(tempfile.mkstemp(suffix=".conf")[1])
    conf.write_text(CONFIG.format(ws_port=ws_port))
    _, tcp_url = start_server("-c", str(conf), jetstream=False)
    ws_url = f"ws://127.0.0.1:{ws_port}"
    print(f"server listens on {tcp_url} (TCP) and {ws_url} (WebSocket)")

    nc_tcp = await nats.connect(tcp_url)
    nc_ws = await nats.connect(ws_url)
    print("websocket client connected:", nc_ws.is_connected)

    print("=== transports interoperate transparently ===")
    sub = await nc_ws.subscribe("chat")
    await nc_ws.flush()
    await nc_tcp.publish("chat", b"hello from TCP")
    msg = await sub.next_msg(timeout=1)
    print("websocket client received:", msg.data.decode())

    reply_sub = await nc_tcp.subscribe("ping")

    async def responder() -> None:
        msg = await reply_sub.next_msg(timeout=2)
        await msg.respond(b"pong from TCP")

    task = asyncio.get_running_loop().create_task(responder())
    await nc_tcp.flush()
    reply = await nc_ws.request("ping", b"ping from WebSocket", timeout=2)
    print("websocket client got reply:", reply.data.decode())
    await task

    await nc_tcp.drain()
    await nc_ws.drain()
    conf.unlink()


asyncio.run(main())
