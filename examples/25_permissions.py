"""Users and permissions — authorization beyond authentication.

Example 21 asked "who are you?"; this one adds "what may you do?".
A server config file defines users with per-subject publish and
subscribe permissions. NATS enforces them asynchronously: a forbidden
publish is not an exception on the call, but a permissions violation
reported through the connection's error callback.
"""

import asyncio
import tempfile
from pathlib import Path

import nats
import nats.errors
from _nats_config import free_port, start_server

CONFIG = """
authorization {
  users = [
    {
      user: service
      password: s3rvice
      permissions: {publish: ">", subscribe: ">"}
    }
    {
      user: sensor
      password: s3nsor
      permissions: {
        publish: "telemetry.>"
        subscribe: "commands.sensor.*"
      }
    }
  ]
}
"""


async def main() -> None:
    conf = Path(tempfile.mkstemp(suffix=".conf")[1])
    conf.write_text(CONFIG)
    port = free_port()
    _, url = start_server("-c", str(conf), port=port, jetstream=False)
    print("server started with two users: 'service' (all), 'sensor' (limited)")

    violations: list[str] = []

    async def error_cb(exc: Exception) -> None:
        violations.append(str(exc))

    service = await nats.connect(url, user="service", password="s3rvice")
    sensor = await nats.connect(
        url, user="sensor", password="s3nsor", error_cb=error_cb
    )

    print("=== the sensor may publish telemetry ===")
    sub = await service.subscribe("telemetry.>")
    await service.flush()  # make sure the server registered the interest
    await sensor.publish("telemetry.temp", b"21.5")
    msg = await sub.next_msg(timeout=1)
    print(f"service received [{msg.subject}]: {msg.data.decode()}")

    print("=== but not administrative subjects ===")
    await sensor.publish("admin.shutdown", b"muahaha")
    await sensor.flush()
    await asyncio.sleep(0.3)
    print("violation reported via error_cb:", violations[-1])

    print("=== nor subscribe outside its allowance ===")
    await sensor.subscribe("admin.>")
    await sensor.flush()
    await asyncio.sleep(0.3)
    print("violation reported via error_cb:", violations[-1])

    await service.drain()
    await sensor.drain()
    conf.unlink()


asyncio.run(main())
