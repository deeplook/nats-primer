# NATS Primer

[![CI](https://github.com/deeplook/nats-primer/actions/workflows/ci.yml/badge.svg)](https://github.com/deeplook/nats-primer/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![NATS](https://img.shields.io/badge/NATS-2.x-27AAE1.svg?logo=natsdotio)](https://nats.io)

A collection of small, self-contained Python scripts for learning
[NATS](https://nats.io) on a local machine — from core publish/subscribe to
JetStream persistence, key-value and object stores, microservices,
authentication, and clustering.

Every script starts its own throwaway `nats-server` on a random port and
tears it down on exit, so there is nothing to set up or clean up between
examples. To run against an existing server instead, set the `NATS_URL`
environment variable (start that server with JetStream enabled: `nats-server -js`).

## Prerequisites

### 1. uv

macOS / Linux:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows:
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Python 3.12

```bash
uv python install 3.12
```

### 3. nats-server

macOS:
```bash
brew install nats-server
```

Linux:
```bash
curl -sf https://binaries.nats.dev/nats-io/nats-server/v2@latest | PREFIX=~/.local/bin sh
```

Windows:
```powershell
choco install nats-server   # or: scoop install nats-server
```

## Installation

```bash
uv sync
```

That's it — no configuration files, no running services needed.

## Modules

Run any script with `uv run examples/<file>`. They are designed to be read
in order — each builds on concepts introduced by the previous.

### Core NATS

| File | Topic |
|------|-------|
| `examples/01_connect.py` | Connecting — server info, round-trip time, clean close |
| `examples/02_publish_subscribe.py` | Core pub/sub — fire-and-forget messaging |
| `examples/03_wildcards.py` | Subject hierarchies and `*` / `>` wildcards |
| `examples/04_queue_groups.py` | Queue groups — load balancing across subscribers |
| `examples/05_request_reply.py` | Request-reply — RPC on top of pub/sub, with timeouts |
| `examples/06_headers.py` | Message headers — metadata alongside the payload |
| `examples/07_json_payloads.py` | Structured payloads — JSON and dataclasses over NATS |
| `examples/08_lifecycle.py` | Connection lifecycle — reconnects, callbacks, flush and drain |

### JetStream

| File | Topic |
|------|-------|
| `examples/09_jetstream_intro.py` | Streams — persistent messaging with publish acks |
| `examples/10_pull_consumers.py` | Durable pull consumers — fetching messages in batches |
| `examples/11_ack_strategies.py` | Ack, nak, term, in-progress — redelivery semantics |
| `examples/12_delivery_policies.py` | Replaying a stream from any point |
| `examples/13_retention_policies.py` | Limits, interest, and work-queue streams |
| `examples/14_exactly_once.py` | Deduplication via `Nats-Msg-Id` and double-acking |
| `examples/15_key_value.py` | Key-Value store — revisions, history, live watchers |
| `examples/16_object_store.py` | Object store — blobs larger than the message limit |
| `examples/17_stream_management.py` | Stream admin — info, purge, updates, mirrors |

### Advanced

| File | Topic |
|------|-------|
| `examples/18_scatter_gather.py` | Scatter-gather — one request, many replies |
| `examples/19_micro_services.py` | Microservices — endpoints, discovery, stats with `nats.micro` |
| `examples/20_benchmark.py` | Throughput — core NATS vs JetStream publish costs |
| `examples/21_auth_token.py` | Authentication — a token-protected server |
| `examples/22_cluster.py` | Clustering — a 3-node cluster and client failover |
| `examples/23_faststream.py` | FastStream — declarative subscribers with Pydantic validation |

## Running all tests

```bash
uv run python -m pytest -v
```

The test suite runs every example as a subprocess and checks that it exits
cleanly. Each test starts its own isolated server, so no setup is needed
beyond having `nats-server` on the PATH.

## Notes

- `examples/_nats_config.py` is the shared helper: `connect()` uses
  `NATS_URL` if set, otherwise auto-starts a local server with JetStream
  enabled on a random free port.
- `08_lifecycle.py`, `21_auth_token.py`, and `22_cluster.py` always start
  their own dedicated servers (they kill or reconfigure them), regardless
  of `NATS_URL`.
- `16_object_store.py` writes a retrieved blob to `out/` (gitignored).
- `20_benchmark.py` prints machine-dependent numbers; the ratios between
  the three variants are the point.
- `22_cluster.py` starts three `nats-server` processes at once.

## Further reading

- [NATS documentation](https://docs.nats.io)
- [nats-py — Python client](https://github.com/nats-io/nats.py)
- [NATS by Example](https://natsbyexample.com)
- [FastStream documentation](https://faststream.airt.ai)
