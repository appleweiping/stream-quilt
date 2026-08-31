# Contributing

Stream alignment fails at boundaries, so focused changes and explicit semantics matter more than a
large diff.

## Setup

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows: .venv\Scripts\activate
python -m pip install -e ".[dev]"
```

Run all gates before opening a pull request:

```bash
python -m ruff check src tests
python -m ruff format --check src tests
python -m coverage run -m pytest
python -m coverage report
stream-quilt demo --output demo-output --write-input
```

## Expectations

- Add tests at exact timestamps when changing boundary behavior.
- Keep offline alignment deterministic under input permutation.
- Document watermark, clock, or gap-definition changes in `docs/architecture.md`.
- Preserve event arrival order in `replay`; do not silently sort it.
- Do not introduce wall-clock reads, telemetry, remote fetches, or model downloads.
- Update the changelog for user-visible behavior.

Open an issue before changing the meaning of `late_policy`, required streams, or half-open windows.
By contributing, you agree that your work is licensed under the MIT License.
