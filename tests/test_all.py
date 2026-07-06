"""Smoke test: every example module must run cleanly end to end.

Each example is executed as a fresh subprocess with NATS_URL removed
from the environment, so it starts (and tears down) its own throwaway
nats-server. The only requirement here is a nats-server binary on PATH.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
MODULES = sorted(EXAMPLES_DIR.glob("[0-9][0-9]_*.py"))


@pytest.mark.parametrize("module", MODULES, ids=lambda p: p.name)
def test_example_runs_cleanly(module: Path) -> None:
    env = os.environ.copy()
    env.pop("NATS_URL", None)  # force examples onto their own server
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
