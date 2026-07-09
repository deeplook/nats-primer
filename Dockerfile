# Ships nats-server and openssl alongside the Python environment, so every
# example runs as designed: each still auto-starts its own throwaway server
# inside the container. uv manages Python 3.12 (from .python-version).
FROM python:3.12-slim

# nats-server (on PATH, as the examples expect) plus openssl for the TLS
# example's self-signed certificate.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl openssl \
    && curl -sf https://binaries.nats.dev/nats-io/nats-server/v2@latest | PREFIX=/usr/local/bin sh \
    && apt-get purge -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# uv, copied from its official distroless image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Use the locked environment as-is; never re-resolve at runtime.
ENV UV_FROZEN=1

WORKDIR /app
COPY . .
RUN uv sync

# `uv run <script>` is the same command used on the host.
ENTRYPOINT ["uv", "run"]
CMD ["examples/01_connect.py"]
