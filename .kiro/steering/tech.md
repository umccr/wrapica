# Tech Stack

## Language & Runtime
- Python >=3.12, <3.15

## Build System
- setuptools (>=61.0) with pyproject.toml
- `python3 -m build` for packaging

## Core Dependencies
- `libica` (>=3.2, <4) — low-level ICAv2 OpenAPI client
- `boto3` — AWS credential operations
- `fastapi` — utility usage (not serving)
- `PyJWT` — access token decoding/validation
- `ruamel.yaml` — YAML config handling
- `pandas` — tabular data operations
- `cwl_utils` — CWL workflow parsing
- `websocket_client` — real-time log streaming
- `beautifulsoup4` — HTML parsing utilities
- `binaryornot` — file type detection
- `pydantic` — UUID4 type validation
- `verboselogs` — enhanced logging

## Dev / Optional Dependencies
- `pytest`, `pytest-mock` — testing
- `sphinx`, `sphinx-rtd-theme`, `sphinx_autodoc_typehints` — docs
- `twine` — PyPI publishing

## Common Commands

```bash
# Install the package locally (editable or standard)
pip install .

# Build the distribution
make build_package

# Build documentation
make build_docs

# Quick docs build (no dependency install)
make build_docs_quick

# Run tests (when test extras installed)
pip install .[test]
pytest

# Publish to PyPI
make push_pypi

# Publish to Test PyPI
make push_test_pypi
```

## Configuration at Runtime
- `ICAV2_ACCESS_TOKEN` env var — bearer token for API auth
- `ICAV2_BASE_URL` env var — API base URL (defaults to `https://ica.illumina.com/ica/rest`)
- `ICAV2_PROJECT_ID` env var — default project context
