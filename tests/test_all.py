"""Smoke test: every example module must run cleanly end to end.

Each example is executed as a subprocess. The session-scoped nats_server
fixture starts one shared nats-server and sets NATS_URL so that examples
using connect() from _nats_config pick it up automatically. Examples that
call start_server() directly (auth, TLS, cluster, websocket, leafnode) manage
their own server and ignore NATS_URL — they work correctly either way.
The only requirement is a nats-server binary on PATH.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
MODULES = sorted(EXAMPLES_DIR.glob("[0-9][0-9]_*.py"))


@pytest.mark.parametrize("module", MODULES, ids=lambda p: p.name)
def test_example_runs_cleanly(module: Path, nats_server: str) -> None:
    env = os.environ.copy()
    result = subprocess.run(
        [sys.executable, str(module)],
        capture_output=True,
        text=True,
        cwd=EXAMPLES_DIR,
        env=env,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"{module.name} exited with {result.returncode}\n"
        f"--- stdout (tail) ---\n{result.stdout[-3000:]}\n"
        f"--- stderr (tail) ---\n{result.stderr[-3000:]}"
    )
    # nats-py and faststream report async failures (e.g. DrainTimeoutError,
    # handler exceptions) via callbacks/logging and still exit 0 — treat
    # that noise as failure; no example intentionally prints a traceback.
    for marker in ("nats: encountered error", "Traceback"):
        assert marker not in result.stderr, (
            f"{module.name} wrote {marker!r} to stderr\n"
            f"--- stderr (tail) ---\n{result.stderr[-3000:]}"
        )
