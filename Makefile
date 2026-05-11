# ftth-compete — common dev commands
# Windows: run via `make <target>` if you have GNU Make, or copy individual lines into PowerShell.

.PHONY: install sync test lint type fmt run refresh smoke clean help

help:
	@echo "Targets:"
	@echo "  sync     - uv sync (install deps from lockfile)"
	@echo "  test     - run pytest"
	@echo "  lint     - ruff check"
	@echo "  type     - mypy strict"
	@echo "  fmt      - ruff format"
	@echo "  run      - launch streamlit dashboard"
	@echo "  refresh  - download/refresh BDC + IAS + ACS + TIGER datasets"
	@echo "  smoke    - end-to-end smoke test against Evans CO + Plano TX + Brooklyn NY"
	@echo "  clean    - remove caches (keeps raw downloads)"

sync:
	uv sync

test:
	uv run pytest

lint:
	uv run ruff check .

type:
	uv run mypy src

fmt:
	uv run ruff format .

run:
	uv run streamlit run src/ftth_compete/ui/app.py

refresh:
	uv run python -m ftth_compete.pipelines.refresh_all

smoke:
	uv run python -m ftth_compete.cli market "Evans, CO"
	uv run python -m ftth_compete.cli market "Plano, TX"
	uv run python -m ftth_compete.cli market "Brooklyn, NY"

clean:
	powershell -Command "Remove-Item -Recurse -Force .pytest_cache,.mypy_cache,.ruff_cache -ErrorAction SilentlyContinue"
