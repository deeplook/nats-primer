"""Session-scoped NATS server shared across all example tests."""

import os
from collections.abc import Generator

import pytest

from examples._nats_config import start_server


@pytest.fixture(scope="session")
def nats_server() -> Generator[str, None, None]:
    proc, url = start_server()
    os.environ["NATS_URL"] = url
    yield url
    proc.terminate()
    proc.wait(timeout=5)
    os.environ.pop("NATS_URL", None)
