# Pipeline `code` -> `name`/`version` migration

This change moves wrapica's pipeline identification off the single `code` string onto
`name` + optional `version`, matching ICAv2 v4's grouped pipeline model. It adds six new
name/version resolver and coerce functions across the tenant-scoped (`pipelines`) and
project-scoped (`project_pipelines`) modules, redefines the coerce path so a non-UUID string
resolves by **name**, deprecates every old `*_code` function and the `pipeline_code` create
kwarg as functional shims (no removals), and wires `name=`/`version=` into the five pipeline
create functions. Lookup/coerce stay upstream of the launch classes, so analysis launch is
untouched. The resolution algorithm (empty-name fail-fast, explicit-version exact match,
single match wins regardless of flag, multi-match requires exactly one `latest_version_in_group
is True`) is implemented identically in both modules.

Watch for: the create functions turn previously-required positional params into
`= None`-defaulted ones to seat `pipeline_name` first (confirmed, non-blocking, matches
design); the nf-core zip path does file I/O before the both-supplied `ValueError` fires but
makes no API call (confirmed, non-blocking). No blocking issues found; verification evidence
is present and consistent with the diff.

**Verdict**: APPROVED

## High-level view

The resolution algorithm is the load-bearing piece and is implemented identically in the
tenant and project resolvers: empty name is rejected before any API call, an explicit version
is an exact-or-ValueError lookup, a single name match returns regardless of its latest flag,
and multiple matches require exactly one `latest_version_in_group is True` (else a ValueError
that lists versions, and for the multi-flagged case the distinct group ids). The `is True`
check and the `getattr(..., "pipeline_group", None)`-style guard correctly handle the
`None`/`False` reality of the optional v4 fields.

The tenant resolver pays an O(N) cost — there is no v4 tenant *list* endpoint, so it eagerly
fetches every pipeline as a v4 object (no short-circuit, because "latest in group" needs all
siblings) and its docstring steers callers to the project-scoped helper when a project context
exists. `list_all_pipelines` deliberately stays on the v3 Accept header; `list_project_pipelines`
is hardened to force the v4 header and its return hint corrected to `List[ProjectPipelineV4]`.

The deprecation surface is intact: every `*_code` lookup keeps its original contract
(`get_pipeline_obj_from_pipeline_code` still re-raises `StopIteration`, hints unchanged), and
the three coerce aliases now delegate to their `_or_name` resolver with a warning that names
the resolution change from code to name. Return hints on the deprecated aliases are left as-is
(`PipelineType` / `Union[UUID4, str]` / `ProjectPipelineV4`) per the design's deferral.

The five create functions share one `_resolve_pipeline_create_identifier_kwargs` helper that
enforces both-supplied -> ValueError (before any API call), code-only -> `code=` + warning,
name-only -> `name=`/`version=`, neither -> ValueError. A latent name collision between the new
`pipeline_version` parameter and a pre-existing local `pipeline_version` (the Nextflow language
version object) was correctly resolved by renaming the local to `nextflow_language_version`.

<details>
<summary>Issues (2)</summary>

1. **Create-function required params defaulted to None** — seating `pipeline_name` first forced `workflow_path`/`zip_path`/`main_nextflow_file`/etc. to `= None`; required-ness is only enforced at runtime (identifier validation + file open). Intended by the plan; no action required, noted for awareness.
2. **nf-core both-supplied validation is late** — `create_nextflow_pipeline_from_nf_core_zip` extracts and repacks the zip before the both-supplied `ValueError` fires in the terminal create call. No API call occurs first (design requirement met); only wasted file I/O. Optional: hoist the identifier check to the top of the wrapper.

</details>

<details>
<summary>Details</summary>

### Resolution algorithm parity across both modules

`get_pipeline_obj_from_pipeline_name` (tenant) and `get_project_pipeline_obj_from_pipeline_name`
(project) implement the same ordered algorithm from design Decision 1a. Both reject a falsy
`pipeline_name` with `ValueError("pipeline_name must be a non-empty string")` before touching
the API (confirmed: the project resolver's test asserts `list_project_pipelines` is never
called, and the tenant resolver's test asserts `list_all_pipelines` is never called). An
explicit `pipeline_version` is an exact match returning the single hit, with a defensive
`ValueError` on zero or >1 hits. With the version omitted: zero matches raise, a single match
returns unconditionally (so a legitimately `False`/`None` `latest_version_in_group` does not
block it), and multiple matches require exactly one `latest_version_in_group is True`. The
`is True` comparison is used deliberately so `None` counts as "not flagged".

The ambiguity diagnostics match the design: the `>1 flagged` branch lists the distinct
`pipeline_group.id` values (read via `getattr(..., "pipeline_group", None)` so a `None`
container renders as the string `"None"` rather than raising), and the `0 flagged` branch lists
the available versions. Both are tested, including a `pipeline_group is None` case in the
project module that asserts no `AttributeError`.

### Tenant resolver O(N) fetch and the v3/v4 Accept-header split

Because there is no v4 tenant *list* endpoint, `get_pipeline_obj_from_pipeline_name` builds
`[get_pipeline_obj_from_pipeline_id(p.id) for p in list_all_pipelines()]` — one `get_pipeline`
round-trip per tenant pipeline, eagerly and without short-circuit. The no-short-circuit choice
is correct: "latest in group" cannot be decided without seeing every sibling version. The
`test_fetches_all_ids_no_short_circuit` test pins this by asserting the per-id getter is called
once per pipeline even when the first match is found early. The docstring carries the O(N)
warning and points callers to the project-scoped helper.

`list_all_pipelines` is correctly left on the v3 Accept header (it supplies only `.id` to the
resolver loop), while `list_project_pipelines` gains the forced
`Accept: application/vnd.illumina.v4+json` header in the same ApiClient block pattern as
`get_project_pipeline_obj`, and its return hint is corrected from `List[ProjectPipeline]` to
`List[ProjectPipelineV4]`. This split is the design's deliberate coupling and is honoured
exactly.

### Deprecation shims preserve contracts

The old `*_code` lookups keep their original behaviour under a `DeprecationWarning`
(`stacklevel=2`): `get_pipeline_obj_from_pipeline_code` still filters `PipelineV3.code` and
re-raises `StopIteration` (its error-handling test now wraps the call in
`pytest.warns(DeprecationWarning)` while still expecting `StopIteration`);
`get_project_pipeline_obj_from_pipeline_code` keeps its `.pipeline.code` filter and
`-> ProjectPipeline` hint. The three coerce aliases
(`coerce_pipeline_id_or_code_to_pipeline_obj`, `_to_pipeline_id`,
`_to_project_pipeline_obj`) now delegate to their matching `_or_name` resolver with a warning
whose text states the non-UUID string is now resolved by NAME not code. Tests assert both the
warning and the delegation target (patching the `_or_name` delegate and asserting the call) for
all three — not warning-only. Return hints on the deprecated aliases are intentionally left
unchanged per design (`PipelineType` remains valid since `PipelineV4` is in that union).

### Create path: shared identifier resolver

`_resolve_pipeline_create_identifier_kwargs` centralises the four-way identifier rule for all
five create functions and is checked in the required order: both supplied raises
`ValueError("Provide either pipeline_name (preferred) or the deprecated pipeline_code, not
both")` first, so an ambiguous `code=`+`name=` body never reaches libica; code-only emits the
deprecation warning (`stacklevel=3`, correctly accounting for the extra helper frame) and
returns `{"code": ...}`; name-only returns `{"name": ...}` plus `{"version": ...}` when given;
neither raises. The terminal create functions splat `**identifier_kwargs` into
`create_cwl_pipeline` / `create_nextflow_pipeline`, replacing the old hard-coded
`code=pipeline_code`. Tests cover name->`name=`/`version=`, code->`code=`+warning,
both->ValueError-with-no-API-call, and neither->ValueError for both the CWL and Nextflow
terminal functions.

The subtle part is `create_nextflow_project_pipeline`, which already had a local variable named
`pipeline_version` holding the resolved Nextflow *language* version object. The new
`pipeline_version` parameter would have collided with and overwritten it; the implementer
renamed the local to `nextflow_language_version` throughout, including the
`pipeline_language_version_id=coerce_to_uuid4_obj(nextflow_language_version.id)` call. This is
the kind of collision that silently corrupts behaviour if missed, and it was handled correctly.

The wrapper create functions (`create_cwl_workflow_from_zip`, `create_nextflow_pipeline_from_zip`,
`create_nextflow_pipeline_from_nf_core_zip`) forward `pipeline_name`/`pipeline_version`/
`pipeline_code` through unchanged. The nf-core wrapper derives its stable zip layout name from
`pipeline_name or pipeline_code`, preserving the archive structure. One wrinkle: in the
both-supplied case the nf-core wrapper extracts and repacks the zip before the terminal create
call raises `ValueError`. No API call happens first, so the design's "no API call" requirement
is met; the only cost is wasted file I/O on an input error. Non-blocking.

### download_pipeline_to_zip temp-dir naming

The internal `pipeline_code = pipeline_obj.code` is replaced with
`pipeline_dir_name = pipeline_obj.name or str(pipeline_obj.id)`, suffixed with `__{version}`
when a version is present. The `id` fallback keeps this cosmetic path from raising when `name`
is `None`. The test mock now sets `.name`/`.version`/`.id` instead of `.code`.

### Docstrings, re-exports, and out-of-scope sites

New names are added to both `__init__.py` import blocks and `__all__`, and to the `.rst`
`:members:` lists, with the deprecated names retained. The `bundle.list_pipelines_in_bundle`
and `list_project_pipelines` docstring examples now print `Name`/`Version`. The
`list_all_pipelines` example still prints `code`, which is correct — that endpoint returns
`PipelineV3`, which has no name/version. Out-of-scope `.code` sites (`AnalysisInput/Output.code`,
`region.code`, `PipelineConfigurationParameter.code`) are untouched; a diff of the
`project_analysis` and `region` modules shows no changes, and the bundle diff touches only the
pipeline example line.

### Verification evidence

`verification.md` records the libica 3.3 model/signature confirmation, clean imports of both
packages and all six new names, a 177 -> 210 passed test delta (33 new tests), and a successful
docs build with only the pre-existing `_static` warning. The recorded grep matches an
independent re-grep run during this review: every surviving pipeline-identifier `.code`
reference is a deprecated shim, its re-export/docs entry, the create-path deprecated kwarg
wiring, or a deprecated docstring example.

</details>

<details>
<summary>File map</summary>

- `src/wrapica/pipelines/functions/pipelines_functions.py` — new tenant name resolver + two coerce funcs; `*_code` funcs deprecated as shims; `download_pipeline_to_zip` temp-dir naming; docstring/import updates.
- `src/wrapica/project_pipelines/functions/project_pipelines_functions.py` — new project name resolver + id getter + coerce; `*_code` deprecated as shims; `list_project_pipelines` v4 header + return hint; shared create-identifier helper; five create funcs migrated to name/version; Nextflow local-var rename.
- `src/wrapica/bundle/functions/bundle_functions.py` — docstring example prints Name/Version (no functional change).
- `src/wrapica/pipelines/__init__.py`, `src/wrapica/project_pipelines/__init__.py` — new names added to imports + `__all__`; deprecated names retained.
- `docs/wrapica/pipelines.rst`, `docs/wrapica/project_pipelines.rst` — new names added to `:members:`; deprecated names retained.
- `tests/test_pipelines.py`, `tests/test_error_handling.py`, `tests/test_project_pipelines.py` — resolver cases, coerce delegation + warns, create validation (name/code/both/neither), deprecated-path `pytest.warns`.

Full diff: `git -C /home/alexiswl/GitHub/UMCCR/wrapica/.worktrees/pipeline-name-version diff feature/move-to-name-version-pipeline...migrate/pipeline-name-version`

</details>
