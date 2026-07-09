.DEFAULT_GOAL := help

EXAMPLE ?= examples/01_connect.py
IMAGE ?= nats-primer

.PHONY: help install format lint test test-v run nats-version docker-build docker-run docker-test compose-run compose-down check-all clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-14s %s\n", $$1, $$2}'

install:  ## Install all dependencies
	uv sync --all-groups

format:  ## Auto-format and fix lint issues
	uv run --with ruff ruff format examples tests
	uv run --with ruff ruff check --select E9,F63,F7,F82 --fix examples tests

lint:  ## Run critical ruff checks (syntax, undefined names)
	uv run --with ruff ruff check --select E9,F63,F7,F82 examples tests

test:  ## Run the test suite
	uv run python -m pytest

test-v:  ## Run the test suite verbosely
	uv run python -m pytest -v

run:  ## Run one example (override with EXAMPLE=examples/02_publish_subscribe.py)
	uv run $(EXAMPLE)

nats-version:  ## Show the nats-server version used by examples/tests
	nats-server --version

docker-build:  ## Build the Docker image
	docker build -t $(IMAGE) .

docker-run:  ## Run one example in Docker (override with EXAMPLE=...)
	docker run --rm $(IMAGE) $(EXAMPLE)

docker-test:  ## Run the test suite in Docker
	docker run --rm $(IMAGE) python -m pytest

compose-run:  ## Run one example against a standalone JetStream server
	docker compose run --rm primer $(EXAMPLE)

compose-down:  ## Stop the compose stack and remove its volume
	docker compose down -v

check-all: install format lint test clean  ## Run format, lint, test, and clean
	@echo "All checks passed!"

clean:  ## Remove generated outputs and caches
	rm -rf dist build *.egg-info
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	rm -rf htmlcov .coverage coverage.xml
	rm -rf out
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name .DS_Store -exec rm {} +
