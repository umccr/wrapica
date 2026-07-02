# Project Structure

```
wrapica/
├── src/
│   └── wrapica/              # Main package
│       ├── enums/            # Enum classes (BundleStatus, Data, AnalysisStorageSize, etc.)
│       ├── literals/         # Literal type aliases for type checking
│       ├── libica_models/    # Re-exports of libica.openapi.v3.models for convenient imports
│       ├── libica_exceptions/# Custom exception wrappers
│       ├── utils/            # Shared utilities
│       │   ├── configuration.py  # ICAv2 auth, token, and Configuration setup
│       │   ├── globals.py        # Constants (URLs, page sizes, regex patterns)
│       │   ├── logger.py         # Logging setup (verboselogs)
│       │   ├── miscell.py        # UUID/URI format checks
│       │   ├── cwl_helpers.py    # CWL workflow utilities
│       │   ├── nextflow_helpers.py # Nextflow utilities
│       │   ├── subprocess_handler.py
│       │   └── websocket_*.py    # WebSocket streaming helpers
│       ├── project/          # Project-level operations
│       ├── project_data/     # Data CRUD, URI conversion, presigning
│       ├── project_pipelines/# Pipeline management
│       ├── project_analysis/ # Analysis launch, monitoring, logs
│       ├── bundle/           # Bundle operations
│       ├── data/             # Non-project data operations
│       ├── job/              # Job management
│       ├── pipelines/        # Non-project pipeline operations
│       ├── region/           # Region lookups
│       ├── storage_configuration/
│       ├── storage_credentials/
│       ├── tenant/           # Tenant operations
│       └── user/             # User operations
├── docs/                     # Sphinx documentation source
├── dev-scripts/              # Developer helper scripts (version bumping)
├── .github/workflows/        # CI/CD (build, release, publish to PyPI)
├── pyproject.toml            # Package metadata and dependencies
└── Makefile                  # Common build/publish/docs commands
```

## Module conventions

Each domain module (e.g. `project_data/`) follows this pattern:
- `__init__.py` — re-exports all public functions and relevant libica model classes; defines `__all__`
- `functions/<module>_functions.py` — implementation file containing the actual function definitions

## Code style notes
- Functions use snake_case with descriptive names (e.g. `get_project_data_obj_from_project_id_and_path`)
- Type hints used throughout; prefer `Literal` types (from `literals/`) for API string parameters and `Enum` classes (from `enums/`) for internal logic
- Docstrings include `:param:`, `:return:`, `:raises:`, and `.. code-block:: python` examples
- Imports are grouped: standard library → third-party (libica) → local (relative)
- `libica.openapi.v3` models are re-exported via `libica_models` and individual module `__init__.py` files for user convenience
- Global configuration accessed via `get_icav2_configuration()` singleton pattern
- Logging via `verboselogs` logger obtained from `utils.logger`
