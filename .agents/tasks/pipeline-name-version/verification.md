# Verification: wrapica pipeline `code` -> `name`/`version` migration

This records the commands run to verify the implementation and their results. All commands
were run from the worktree root
`/home/alexiswl/GitHub/UMCCR/wrapica/.worktrees/pipeline-name-version` on branch
`migrate/pipeline-name-version`.

Environment note: a globally-installed, unrelated pytest plugin
(`orcabus_pipeline_test_utils`) breaks pytest collection on this machine under plugin
autoload, so tests are run with `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 ... -p pytest_mock`, as
documented in `plan.md`. This is an environment quirk, not a project issue.

## 1. libica 3.3 model/signature confirmation (installed-source reality)

Command:

```
python3 -c "from libica.openapi.v3.models import PipelineV4; print(list(PipelineV4.model_fields.keys())); import inspect; from libica.openapi.v3.api.project_pipeline_api import ProjectPipelineApi; print([p for p in inspect.signature(ProjectPipelineApi.create_cwl_pipeline).parameters if p in ('code','name','version')]); print([p for p in inspect.signature(ProjectPipelineApi.create_nextflow_pipeline).parameters if p in ('code','name','version')])"
```

Result (confirmed, drives the implementation):
- `PipelineV4` fields include both `code` AND `name`, `version`, `pipeline_group`,
  `latest_version_in_group`, `id`.
- `create_cwl_pipeline` accepts `code`, `name`, `version` (all optional).
- `create_nextflow_pipeline` accepts `code`, `name`, `version` (all optional).

So the migration adds `name`/`version` support without inventing fields, and keeps the
deprecated `code=` passthrough for one release.

## 2. Package imports cleanly

Command:

```
PYTHONPATH=src/ python3 -c "import wrapica.project_pipelines, wrapica.pipelines"
```

Result: PASS (no output / exit 0).

New names importable from both package roots:

```
PYTHONPATH=src/ python3 -c "from wrapica.pipelines import get_pipeline_obj_from_pipeline_name, coerce_pipeline_id_or_name_to_pipeline_obj, coerce_pipeline_id_or_name_to_pipeline_id; from wrapica.project_pipelines import get_project_pipeline_obj_from_pipeline_name, get_project_pipeline_id_from_pipeline_name, coerce_pipeline_id_or_name_to_project_pipeline_obj; print('ok')"
```

Result: prints `ok`. PASS.

## 3. Test baseline (before changes)

```
PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/ -q -p pytest_mock
```

Result: `177 passed, 1 warning` (the one warning is an unrelated dateutil DeprecationWarning
from a dependency).

## 4. Full test suite (after changes)

```
PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/ -q -p pytest_mock
```

Result: `210 passed, 1 warning` (177 baseline + 33 new tests). PASS.

New/updated tests:
- `tests/test_pipelines.py`: `TestGetPipelineObjFromPipelineName` (empty-name ValueError with
  no API call, exact version, single-match-when-flag-false, latest default, not-found,
  ambiguous zero-flagged, ambiguous multi-flagged with group ids in message, fetch-all
  no-short-circuit); `TestCoercePipelineIdOrNameToPipelineObj` (UUID vs name delegation for
  both obj and id); `TestCoercePipelineIdOrCodeToPipelineObj` rewritten to assert the
  deprecated aliases emit `DeprecationWarning` AND delegate to their `_or_name` resolver;
  `TestDownloadPipelineToZip` mock updated to name/version.
- `tests/test_error_handling.py`: `TestGetPipelineObjFromPipelineCodeStopIteration` wrapped in
  `pytest.warns(DeprecationWarning)`; added `TestGetPipelineObjFromPipelineNameValueError`.
- `tests/test_project_pipelines.py`: `TestGetProjectPipelineObjFromPipelineName` (incl.
  `pipeline_group is None` case and multi-flagged group ids), `TestGetProjectPipelineIdFromPipelineName`,
  `TestCoercePipelineIdOrNameToProjectPipelineObj`,
  `TestCoercePipelineIdOrCodeToProjectPipelineObjDeprecated` (delegation + warns),
  `TestCreatePipelineIdentifierValidation` (name->name=/version=, code->code=+warns,
  both->ValueError no-API-call, neither->ValueError, for CWL and Nextflow terminal create
  functions); existing `TestGetProjectPipelineObjFromPipelineCode` wrapped in `pytest.warns`;
  `TestCreateCwlWorkflowFromZip` switched to `pipeline_name=`/`pipeline_version=` with a
  retained deprecated-`pipeline_code=` case.

## 5. Docs build

```
PYTHONPATH=src/ sphinx-build docs/ docs/_build/
```

Result: `build succeeded`. The only warning is the pre-existing
`html_static_path entry '_static' does not exist`, unrelated to this change. The new
`*_from_pipeline_name` / `coerce_pipeline_id_or_name_*` members and the retained deprecated
`*_code` members all render (added by hand to the `:members:` lists in
`docs/wrapica/pipelines.rst` and `docs/wrapica/project_pipelines.rst`).

## 6. Scope check — remaining pipeline-identifier `.code` references

```
grep -rn "pipeline_code|_from_pipeline_code|_or_code|.pipeline.code|pipeline_iter.code|pipeline_obj.code" src/wrapica/pipelines src/wrapica/project_pipelines
```

Result: every surviving pipeline-identifier `code` reference is one of:
- the deprecated `*_code` lookup/coerce shims (and their docstring examples, which
  intentionally illustrate the deprecated code path),
- their re-exports in the two `__init__.py` files and `.rst` member lists (kept for the
  deprecation window),
- the deprecated `pipeline_code` kwarg wiring in the five create functions (and the shared
  `_resolve_pipeline_create_identifier_kwargs` helper), plus the stable zip-layout fallback
  `pipeline_zip_name = pipeline_name or pipeline_code` in
  `create_nextflow_pipeline_from_nf_core_zip`,
- a commented-out FIXME block referencing `pipeline_code.split("__")`.

Out-of-scope `.code` sites were left untouched: `AnalysisInput/Output.code`, `region.code`
(bundle_functions.py ~line 345), `PipelineConfigurationParameter.code`.

## What could not be verified

- Live ICAv2 API calls require credentials (`ICAV2_ACCESS_TOKEN` etc.) and are out of CI's
  reach; all resolver/coerce/create behaviour is verified via mocked unit tests. In
  particular the tenant-scoped `get_pipeline_obj_from_pipeline_name` O(N) per-id fetch and the
  real `latest_version_in_group` grouping semantics are only meaningfully exercised against a
  live tenant. The forced v4 `Accept` header on `list_project_pipelines` is asserted
  structurally (header set in the same ApiClient block as `get_project_pipeline_obj`) but its
  effect on content negotiation is a server-side behaviour not exercised offline.
