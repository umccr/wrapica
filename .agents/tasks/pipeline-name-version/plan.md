# Implementation Plan: migrate wrapica pipeline identification from `code` to `name`/`version`

Derived from `design.md` (revision round 2) + `design-review.md` (round 3, verdict
CHANGES_REQUESTED, folded into the MUST-FIX items below). The design phase is complete;
this plan does not re-decide architecture. Code against the **installed libica 3.3**, which
was verified in exploration:

- `PipelineV4` keeps a **required** `code: str` AND adds Optional `name` (inner `min_length=1`),
  `version`, `pipeline_group` (`PipelineGroupIdentifier`, id-only, container itself Optional),
  `latest_version_in_group` (`Optional[bool]`; `None` and `False` both real).
- `PipelineV3` (tenant list endpoint `PipelineApi.get_pipelines()`) is **code-only**; there is
  **no v4 tenant list endpoint**. Only `get_pipeline(id)` returns `PipelineV4`.
- `ProjectPipelineApi.get_project_pipelines()` returns `ProjectPipelineListV4` at runtime.
- `ProjectPipelineApi.create_cwl_pipeline` and `create_nextflow_pipeline` both accept
  `code=`, `name=`, `version=` (all optional) — confirmed via `inspect.signature`.

## Conventions to follow in every item
- snake_case function names; grouped imports (stdlib -> libica -> local); docstrings with
  `:param:`/`:return:`/`:rtype:`/`:raises:` and a `.. code-block:: python` example.
- Add `import warnings` to the stdlib import group of each functions module that emits a
  deprecation. Emit `warnings.warn(..., DeprecationWarning, stacklevel=2)`.
- Keep deprecated names exported in `__init__.py` `__all__` and in the `.rst` `:members:`
  lists for the deprecation window.
- Do NOT touch out-of-scope `.code`: `AnalysisInput/Output.code`, `region.code`
  (bundle_functions.py ~line 345), pipeline configuration-parameter `.code`.

## Verification commands (discovered during exploration — use these exact invocations)
The repo's `make test` runs `PYTHONPATH=src/ pytest tests/`, but a globally-installed,
unrelated pytest plugin (`orcabus_pipeline_test_utils`) fails to import on this machine and
aborts collection under plugin autoload. The reliable invocation that reproduces a green
baseline (177 passed) is:

```
cd <WORKTREE>
PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/ -q -p pytest_mock
```

(Per-file: append the path, e.g. `tests/test_pipelines.py`.) If `make test` works in the
execution environment because that stray plugin is absent, prefer `make test`; otherwise use
the invocation above. Docs build: `make build_docs_quick` (i.e. `sphinx-build docs/ docs/_build/`).

`<WORKTREE>` = `/home/alexiswl/GitHub/UMCCR/wrapica/.worktrees/pipeline-name-version`.

---

## Work items

- [ ] 1. Tenant-scoped resolvers + deprecation shims in `pipelines_functions.py`.
      Add `import warnings` to the stdlib import group. Add two NEW functions:
      `get_pipeline_obj_from_pipeline_name(pipeline_name, pipeline_version=None) -> PipelineV4`
      and the ordered-algorithm helper logic (Decision 1a/1b). Implementation: raise
      `ValueError("pipeline_name must be a non-empty string")` up front if `not pipeline_name`
      (MUST-FIX 3); then eagerly build `[get_pipeline_obj_from_pipeline_id(p.id) for p in
      list_all_pipelines()]` (no short-circuit — needed for "latest" semantics), filter by
      `.name == pipeline_name`, then apply the ordered algorithm: explicit version -> exact
      `.version` match (0 -> ValueError, >1 -> defensive ValueError); version omitted -> 0
      matches ValueError, 1 match return it regardless of flag, else exactly one
      `latest_version_in_group is True` returns else ValueError listing versions and (for the
      >1-flagged case) the distinct `getattr(p.pipeline_group, "id", None)` values. Docstring
      must prominently recommend the project-scoped helpers (O(N) per-call ApiClient cost,
      MUST-FIX 4) — reusing one ApiClient across the loop is an acceptable trivial optimization,
      otherwise leave as-is and note it. Add NEW
      `coerce_pipeline_id_or_name_to_pipeline_obj(pipeline_id_or_name, pipeline_version=None) -> PipelineV4`
      and `coerce_pipeline_id_or_name_to_pipeline_id(pipeline_id_or_name, pipeline_version=None) -> Union[UUID4, str]`
      (UUID branch unchanged; non-UUID branch calls the name resolver). Convert the existing
      `get_pipeline_obj_from_pipeline_code` to a shim: emit `DeprecationWarning` then keep its
      current `PipelineV3.code` filter and keep re-raising `StopIteration` (unchanged contract,
      `-> PipelineType` hint unchanged). Convert `coerce_pipeline_id_or_code_to_pipeline_obj`
      (keep `-> PipelineType`) and `coerce_pipeline_id_or_code_to_pipeline_id` (keep
      `-> Union[UUID4, str]`) to shims that emit a resolution-change `DeprecationWarning`
      (text: non-UUID string now resolves by NAME not code; pass a UUID or migrate to the
      `_or_name` function) and **delegate to their `_or_name` resolver** (not the old code path).
      Add new docstrings with `Name`/`Version` print examples.
      Files: src/wrapica/pipelines/functions/pipelines_functions.py
      Verify: `PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/test_pipelines.py -q -p pytest_mock` still passes (existing coerce test currently patches `get_pipeline_obj_from_pipeline_code`; item 6 updates it — until then this item must not break import, so confirm the module imports: `PYTHONPATH=src/ python3 -c "import wrapica.pipelines"`).

- [ ] 2. `download_pipeline_to_zip` temp-dir name (Decision 1c).
      Replace `pipeline_code = pipeline_obj.code` with a name/version-derived, id-fallback name:
      `pipeline_dir_name = pipeline_obj.name or str(pipeline_obj.id)`; if `pipeline_obj.version`
      is not None, `pipeline_dir_name = f"{pipeline_dir_name}__{pipeline_obj.version}"`; use it
      for `output_dir`. Cosmetic only; must never raise.
      Files: src/wrapica/pipelines/functions/pipelines_functions.py
      Verify: `PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/test_pipelines.py::TestDownloadPipelineToZip -q -p pytest_mock` passes (mock sets `.name`/`.version`/`.id`).

- [ ] 3. Also update the `Code: {pipeline_obj.code}` print lines in the remaining
      `pipelines_functions.py` docstrings that return pipeline objects
      (`get_pipeline_obj_from_pipeline_id` example line, `list_all_pipelines` example
      `Pipeline: {pipeline.code}`) to print `Name`/`Version`. Leave the deprecated
      `*_code` shims' own examples referencing code (they intentionally illustrate the code path).
      Files: src/wrapica/pipelines/functions/pipelines_functions.py
      Verify: `make build_docs_quick` builds without new warnings/errors.

- [ ] 4. Project-scoped resolvers + deprecation shims in `project_pipelines_functions.py`.
      Add `import warnings` to the stdlib import group. Add NEW
      `get_project_pipeline_obj_from_pipeline_name(project_id, pipeline_name, pipeline_version=None) -> ProjectPipelineV4`
      implementing the SAME ordered algorithm as item 1 over `list_project_pipelines(project_id)`
      filtering `pp.pipeline.name` (empty-name ValueError up front, MUST-FIX 3; `is True` flag
      check; `getattr(pp.pipeline.pipeline_group, "id", None)` guard in the ambiguity message).
      Add NEW `get_project_pipeline_id_from_pipeline_name(...) -> str` returning
      `str(obj.pipeline.id)`. Add NEW
      `coerce_pipeline_id_or_name_to_project_pipeline_obj(pipeline_id_or_name, pipeline_version=None) -> ProjectPipelineV4`
      (UUID branch -> `get_project_pipeline_obj(project_id, ...)`; non-UUID -> name resolver;
      resolves `project_id` via `from ...project import get_project_id` like the existing alias).
      Convert `get_project_pipeline_obj_from_pipeline_code` to a shim: emit `DeprecationWarning`,
      keep current `.pipeline.code` filter and existing `-> ProjectPipeline` hint UNCHANGED
      (MUST-FIX 1). Convert `coerce_pipeline_id_or_code_to_project_pipeline_obj` to a shim:
      keep its existing `-> PipelineType` return hint UNCHANGED (NOT `ProjectPipelineV4` —
      MUST-FIX 1), emit a resolution-change `DeprecationWarning`, and delegate to
      `coerce_pipeline_id_or_name_to_project_pipeline_obj`. `get_project_pipeline_id_from_pipeline_code`
      stays a thin wrapper over the deprecated obj getter (its `DeprecationWarning` comes
      through that call; add its own warn if the shim is called directly — match the pattern
      used for the other shims). New docstrings print `Name`/`Version`.
      Files: src/wrapica/project_pipelines/functions/project_pipelines_functions.py
      Verify: `PYTHONPATH=src/ python3 -c "import wrapica.project_pipelines"` imports cleanly; `PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/test_project_pipelines.py -q -p pytest_mock` (TestGetProjectPipelineObjFromPipelineCode still passes under the shim; item 7 adds the `pytest.warns` wrapper).

- [ ] 5. Harden `list_project_pipelines` (Decision 1a / Finding 5) and update its docstring.
      Add the forced v4 Accept header (`api_client.set_default_header(header_name="Accept",
      header_value="application/vnd.illumina.v4+json")`) mirroring `get_project_pipeline_obj`;
      change the return hint `List[ProjectPipeline]` -> `List[ProjectPipelineV4]`; update the
      docstring example `Code: {pipeline.pipeline.code}` -> print `Name`/`Version`. Do NOT add a
      v4 header to tenant `list_all_pipelines` — it must stay on the v3 Accept header (Finding
      R2-6; the tenant resolver reads only `.id` off its v3 items).
      Files: src/wrapica/project_pipelines/functions/project_pipelines_functions.py
      Verify: `PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/test_project_pipelines.py::TestListProjectPipelines -q -p pytest_mock` passes.

- [ ] 6. Migrate the five CREATE functions (Decision 5 / MUST-FIX 2).
      For each of `create_cwl_project_pipeline` (~1941), `create_cwl_workflow_from_zip` (~2115),
      `create_nextflow_pipeline_from_zip` (~2215), `create_nextflow_pipeline_from_nf_core_zip`
      (~2317), `create_nextflow_project_pipeline` (~2493): add `pipeline_name: Optional[str] = None`
      as the leading identifier (positional slot where `pipeline_code` was) and a new
      `pipeline_version: Optional[str] = None`; move `pipeline_code` to a trailing
      `Optional[str] = None` keyword. Enforce in this exact order inside each: (a) both
      `pipeline_name` and `pipeline_code` not None -> `ValueError("Provide either pipeline_name
      (preferred) or the deprecated pipeline_code, not both")`; (b) only `pipeline_code` ->
      `DeprecationWarning` and pass `code=pipeline_code` to libica (no name/version); (c) only
      `pipeline_name` -> pass `name=pipeline_name` and, if `pipeline_version` is not None,
      `version=pipeline_version` (no `code=`); (d) neither -> `ValueError`. Wire the chosen
      kwargs into `create_cwl_pipeline` (~2095) / `create_nextflow_pipeline` (~2626) calls.
      In `create_nextflow_pipeline_from_nf_core_zip` replace the internal `pipeline_code` uses
      for zip arcnames / new-zip filename with `pipeline_name or pipeline_code` so the zip layout
      stays stable. In the wrapper-to-wrapper calls (`create_cwl_workflow_from_zip` ->
      `create_cwl_project_pipeline`; `..._from_zip`/`..._nf_core_zip` ->
      `create_nextflow_project_pipeline`) forward `pipeline_name`/`pipeline_version`/`pipeline_code`
      through unchanged. Update each function's `:param:` docs and the `.. code-block::` example
      (lines ~1989/2158/2256/2361/2526) to call with `pipeline_name=`/`pipeline_version=` and
      print `Name`/`Version` instead of `Code` (MUST-FIX 2 — after this, the only remaining
      pipeline-`.code` references are the deprecated shims).
      Files: src/wrapica/project_pipelines/functions/project_pipelines_functions.py
      Verify: `PYTHONPATH=src/ python3 -c "import wrapica.project_pipelines"` imports cleanly; `tests/test_project_pipelines.py::TestCreateCwlWorkflowFromZip` currently passes `pipeline_code=` — it will be updated in item 7, so after item 6 run that class and expect it to need item 7 (do not treat its failure as a regression beyond the kwarg rename; confirm the module imports and the four other create functions are unaffected).

- [ ] 7. `bundle_functions.list_pipelines_in_bundle` docstring (item 12 / Finding 1).
      Update the example print `Code: {pipeline.pipeline.code}` (~line 762) to print
      `Name`/`Version`. No functional change. Leave `region.code` (~line 345) untouched.
      Files: src/wrapica/bundle/functions/bundle_functions.py
      Verify: `make build_docs_quick` builds cleanly.

- [ ] 8. Export the new names from both package `__init__.py` files.
      In `pipelines/__init__.py` add to both the `.functions.pipelines_functions` import block
      and `__all__`: `get_pipeline_obj_from_pipeline_name`,
      `coerce_pipeline_id_or_name_to_pipeline_obj`, `coerce_pipeline_id_or_name_to_pipeline_id`
      (keep the `*_code` names). In `project_pipelines/__init__.py` add:
      `get_project_pipeline_obj_from_pipeline_name`, `get_project_pipeline_id_from_pipeline_name`,
      `coerce_pipeline_id_or_name_to_project_pipeline_obj` (keep the `*_code` names).
      Files: src/wrapica/pipelines/__init__.py, src/wrapica/project_pipelines/__init__.py
      Verify: `PYTHONPATH=src/ python3 -c "from wrapica.pipelines import get_pipeline_obj_from_pipeline_name, coerce_pipeline_id_or_name_to_pipeline_obj, coerce_pipeline_id_or_name_to_pipeline_id; from wrapica.project_pipelines import get_project_pipeline_obj_from_pipeline_name, get_project_pipeline_id_from_pipeline_name, coerce_pipeline_id_or_name_to_project_pipeline_obj; print('ok')"` prints `ok`.

- [ ] 9. Add the new names to the docs `:members:` lists (keep the deprecated ones).
      `docs/wrapica/pipelines.rst`: add `get_pipeline_obj_from_pipeline_name`,
      `coerce_pipeline_id_or_name_to_pipeline_obj`, `coerce_pipeline_id_or_name_to_pipeline_id`.
      `docs/wrapica/project_pipelines.rst`: add `get_project_pipeline_obj_from_pipeline_name`,
      `get_project_pipeline_id_from_pipeline_name`,
      `coerce_pipeline_id_or_name_to_project_pipeline_obj`.
      Files: docs/wrapica/pipelines.rst, docs/wrapica/project_pipelines.rst
      Verify: `make build_docs_quick` builds without "unknown document"/autodoc-import errors; new names appear in `docs/_build/`.

- [ ] 10. Tenant-scoped tests (`tests/test_pipelines.py`, `tests/test_error_handling.py`).
      Add `TestGetPipelineObjFromPipelineName`: mock `list_all_pipelines` (v3 objs with `.id`)
      and `get_pipeline_obj_from_pipeline_id` (v4 objs with `.name`/`.version`/
      `.latest_version_in_group`/`.pipeline_group.id`); assert empty-name -> ValueError with NO
      API call; exact version; single-match returned when flag is False/None; latest-when-omitted
      (exactly one flagged); not-found ValueError; ambiguous (zero flagged, and >1 flagged across
      two groups with group ids in the message); assert it fetches all ids (no short-circuit).
      Add name-based coerce tests asserting UUID delegates to id and non-UUID delegates to the
      **name** resolver (patch both, assert which was called), for both
      `coerce_pipeline_id_or_name_to_pipeline_obj` and `coerce_pipeline_id_or_name_to_pipeline_id`.
      Wrap the existing `TestCoercePipelineIdOrCodeToPipelineObj` non-UUID case in
      `pytest.warns(DeprecationWarning)` and change it to assert delegation to
      `coerce_pipeline_id_or_name_to_pipeline_obj` (patch the `_or_name` delegate). Add the same
      `pytest.warns` + delegation assertion for `coerce_pipeline_id_or_code_to_pipeline_id`.
      In `test_error_handling.py`, wrap `TestGetPipelineObjFromPipelineCodeStopIteration` in
      `pytest.warns(DeprecationWarning)` (still expects `StopIteration`).
      Files: tests/test_pipelines.py, tests/test_error_handling.py
      Verify: `PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/test_pipelines.py tests/test_error_handling.py -q -p pytest_mock` all pass.

- [ ] 11. Project-scoped + create-path tests (`tests/test_project_pipelines.py`).
      Add `TestGetProjectPipelineObjFromPipelineName` (empty-name ValueError no-API-call; exact
      version; single-match-not-flagged; latest default; not-found; ambiguous zero-flagged;
      ambiguous multi-flagged across two groups with group ids asserted in message; a
      `pipeline_group is None` match that does not raise — Finding R2-3) and
      `TestCoercePipelineIdOrNameToProjectPipelineObj` (UUID vs name delegation). Keep
      `TestGetProjectPipelineObjFromPipelineCode` but wrap calls in `pytest.warns(DeprecationWarning)`.
      Extend the project coerce-alias path to assert delegation to
      `coerce_pipeline_id_or_name_to_project_pipeline_obj` under `pytest.warns` (MUST-FIX /
      R2-2). Update `TestCreateCwlWorkflowFromZip` to the `pipeline_name=` form and assert
      `call_kwargs["pipeline_name"]`; retain one `pipeline_code=` case under
      `pytest.warns(DeprecationWarning)`. Add create-path tests for the five functions (mock the
      libica `create_cwl_pipeline`/`create_nextflow_pipeline`): `pipeline_name` passes
      `name=`/`version=`; `pipeline_code` passes `code=` and warns; neither -> ValueError;
      BOTH supplied -> ValueError with NO API call (Finding R2-1). Add `.pipeline.name`/
      `.pipeline.version`/`.pipeline.latest_version_in_group` to the relevant MagicMocks.
      Files: tests/test_project_pipelines.py
      Verify: `PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/test_project_pipelines.py -q -p pytest_mock` all pass.

- [ ] 12. Full-suite + docs regression gate.
      Run the whole test suite and the docs build; confirm no regressions against the recorded
      green baseline (177 passed before this change; count rises with the new tests). Grep to
      confirm the only remaining pipeline-identifier `.code` references in `src/wrapica` are the
      deprecated `*_code` shims (not a verification by itself — the tests above are — but a
      scope check).
      Files: (none — gate only)
      Verify: `PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/ -q -p pytest_mock` is all green, and `make build_docs_quick` succeeds.

---

## Verification checklist (final)
- [ ] `import wrapica`, `import wrapica.pipelines`, `import wrapica.project_pipelines` all succeed.
- [ ] New names importable from both package roots (item 8 one-liner prints `ok`).
- [ ] Full suite green via `PYTHONPATH=src/ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests/ -q -p pytest_mock` (or `make test` where the stray plugin is absent).
- [ ] `make build_docs_quick` builds without errors; new `*_from_pipeline_name` /
      `coerce_pipeline_id_or_name_*` members render and deprecated `*_code` members still render.
- [ ] Deprecation window intact: every `*_code` function and `pipeline_code` kwarg still works,
      emits `DeprecationWarning` (stacklevel=2), and the coerce aliases delegate to their
      `_or_name` resolver (asserted by tests, not warning-only).
- [ ] MUST-FIX 1: `coerce_pipeline_id_or_code_to_project_pipeline_obj` return hint stays
      `PipelineType`; `get_project_pipeline_obj_from_pipeline_code` stays `-> ProjectPipeline`;
      no existing hint narrowed. New `coerce_pipeline_id_or_name_to_project_pipeline_obj` is
      `-> ProjectPipelineV4`.
- [ ] MUST-FIX 2: all five create-function docstring examples call `pipeline_name=`/
      `pipeline_version=` and print `Name`/`Version`; after the change the only pipeline-`.code`
      sites left in `src/wrapica` are the deprecated shims.
- [ ] MUST-FIX 3: explicit non-empty-name `ValueError` in BOTH resolvers, raised before any API call.
- [ ] MUST-FIX 4: tenant resolver docstring prominently prefers the project-scoped helpers and
      notes the per-call ApiClient O(N) cost.
- [ ] Launch/analysis classes unchanged (Decision 4); out-of-scope `.code` sites untouched.

## Notes / assumptions
- Verification uses `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 ... -p pytest_mock` because an unrelated
  globally-installed plugin (`orcabus_pipeline_test_utils`) breaks pytest collection on this
  machine. This is an environment quirk, not a project issue; where it is absent, `make test`
  works directly. Only `pytest_mock` is needed by the suite (fixtures use `mocker`).
- Item ordering: tenant module (1-3) and project module (4-6) are independent of each other and
  can be done in either order; both must precede their test items (10, 11) and the exports/docs
  (8, 9). Item 12 is the final gate. Each item leaves the tree importable; the two test items
  are where the `*_code` tests get their `pytest.warns` wrappers, so run items 10/11 right after
  their corresponding source items to keep the suite green.
