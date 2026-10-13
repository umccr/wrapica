# Design: Migrate wrapica pipeline identification from `code` to `name`/`version`

> **Revision note (design review round 2).** This revises the round-1 design in response to
> `.agents/tasks/pipeline-name-version/design-review.json` (round-2 verdict
> `CHANGES_REQUESTED`: 0 HIGH, 2 MEDIUM, 4 NIT). Every round-2 finding is answered in
> "Responses to design review (round 2)" at the end of this document; the round-1 responses
> are retained below it for history. The round-1 scope decision (whether the "workflow
> generation" / pipeline-creation half is in scope) remains **signed off as Option A:
> pipeline creation is IN SCOPE now.** The decision and its libica-3.3 verification are
> recorded immediately below.
>
> Round-2 findings touched: Decision 5 now specifies the `pipeline_name` + `pipeline_code`
> both-supplied collision rule (Finding R2-1); Decision 3 now makes the resolution-change
> `DeprecationWarning` text and the "assert the name resolver was called" test requirement
> explicit for **all three** coerce aliases and pins their return hints (Finding R2-2);
> `pipeline_group` reads are now guarded for `None` (Finding R2-3); Decision 1a documents the
> name->single-group assumption behind the "latest" guard (Finding R2-4); the empty
> `pipeline_name` behaviour is now an explicit `ValueError` (Finding R2-5); and Decision 1b
> documents the `list_all_pipelines` v3-Accept-header coupling (Finding R2-6).

## Scope decision (Finding 2) — RESOLVED: Option A, pipeline creation is in scope

The verbatim task says: *"we start to use name/version and deprecate 'code' as a parameter
**for workflow generation** and then as the analysis entrypoint."* There are therefore two
halves:

1. **Analysis entrypoint / lookup** — selecting which pipeline to run and coercing a
   pipeline reference. This is the load-bearing, *breaking* half (code-based filtering is
   semantically wrong under v4) and is fully designed below.
2. **Workflow generation** — the pipeline **creation** functions that today take a
   `pipeline_code` kwarg and pass `code=` to libica. There are **five** of them, all in
   `src/wrapica/project_pipelines/functions/project_pipelines_functions.py`:
   - `create_cwl_project_pipeline`
   - `create_cwl_workflow_from_zip`
   - `create_nextflow_pipeline_from_zip`
   - `create_nextflow_pipeline_from_nf_core_zip`
   - `create_nextflow_project_pipeline`

   libica 3.3's `create_cwl_pipeline` / `create_nextflow_pipeline` still accept `code` and
   *also* accept `name`/`version`, so the create endpoint is **not** broken by the upgrade.

**Decision (signed off): Option A — bring the creation path into scope now**, because the
task explicitly names "workflow generation." All five create functions gain `pipeline_name`
+ `pipeline_version` parameters, the `pipeline_code` kwarg is deprecated with a
`DeprecationWarning` (kept functional for one release, consistent with the lookup/coerce
deprecation-shim strategy in Decision 3), and `name=`/`version=` are wired through to libica.
Fully specified in **Decision 5**.

**libica 3.3 verification (done against the installed package, not the brief).** Both
`ProjectPipelineApi.create_cwl_pipeline` and `ProjectPipelineApi.create_nextflow_pipeline`
accept `code`, `name`, AND `version`, and all three are **optional** (default `None`):

```
create_cwl_pipeline     : code=None, name=None, version=None   (all present, all optional)
create_nextflow_pipeline: code=None, name=None, version=None   (all present, all optional)
```

So Option A invents no fields: new callers get `name=`/`version=`, deprecated callers keep
`code=`, and both endpoints already support the full set. No create endpoint in 3.3 is
missing name/version, so there is no fallback constraint to document — but Decision 5 still
preserves the `code=` passthrough when the deprecated kwarg is supplied, for backward
compatibility.

## Overview

The libica upgrade (`libica >= 3.3, < 4`) moves the ICAv2 pipeline models toward a
`name` + `version` identity (grouped by a `pipeline_group`) and away from the single
`code` string. wrapica is already partly on the v4 models: `get_project_pipeline_obj`
forces the `Accept: application/vnd.illumina.v4+json` header and returns a
`ProjectPipelineV4`, and `list_project_pipelines` resolves to `ProjectPipelineListV4`
(items `ProjectPipelineV4`). The code that still filters on
`project_pipeline_iter.pipeline.code` works by accident today because the installed
`PipelineV4` happens to retain a `code` field (see "Discrepancy with the task brief"
below), but `code` is the identifier we are migrating off, and the project's intent is to
select pipelines by `name`/`version`.

This design introduces `name`/`version`-based lookup and coercion helpers in both the
`pipelines` (tenant-scoped) and `project_pipelines` (project-scoped) modules, redefines
the `coerce_*` helpers so a non-UUID string resolves by name, deprecates the old
`code`-based functions with a `DeprecationWarning` (keeping them for one release), confirms
the analysis-entrypoint path already funnels through a resolved `pipeline_id` so name/version
flows into it without changing the launch classes, and (Option A) migrates the five pipeline
*creation* functions to accept `name`/`version` while deprecating `pipeline_code`. It also
fixes the one internal pipeline-`.code` dereference (`download_pipeline_to_zip`), hardens
`list_project_pipelines` to force the v4 `Accept` header, and updates the stale docstring
examples (including the `bundle` and `list_project_pipelines` examples), docs `.rst` member
lists, and tests.

The technology stack is locked: Python >=3.12,<3.15; `libica >= 3.3, < 4`
(`libica.openapi.v3` models and APIs); `pydantic` `UUID4`; `verboselogs` logger via
`utils.logger`; existing `is_uuid_format` helper in `utils.miscell`; stdlib `warnings` for
deprecations. No new third-party dependencies are introduced.

## Discrepancy with the task brief (must read before implementing)

The task brief states `PipelineV4` DROPS `code` and only exposes `name`/`version`/
`pipeline_group`. The **actually installed** libica 3.3 differs, and the implementer must
code against the installed reality (independently re-confirmed by the design reviewer):

- `PipelineV4` **still has** a `code` field (required `str`) **in addition to** `name`
  (Optional str, min_len 1), `version` (Optional str), `pipeline_group`
  (Optional `PipelineGroupIdentifier`), `latest_version_in_group` (Optional bool), and
  `id` (required). So `PipelineV4.code` does not raise `AttributeError` — the existing code
  is not hard-broken at attribute-access level; it is semantically wrong because `code` is
  the identifier we are retiring.
- `name`, `version`, `pipeline_group`, and `latest_version_in_group` on `PipelineV4` are
  all **Optional** and may be `None`. Resolution logic must treat a missing `name`/`version`
  as "no match", never assume they are populated. `latest_version_in_group` being
  `Optional[bool]` means `False` **and** `None` are both real runtime states (drives
  Decision 1a's ordered algorithm).
- `PipelineGroupIdentifier` exposes **only** `id` (no name). Grouping "latest version"
  logic therefore keys on `pipeline_group.id` + `latest_version_in_group`, not a group name.
- `PipelineV3` (returned by the tenant-wide `get_pipelines` list endpoint) has **only**
  `code` — **no** `name`/`version` fields at all.
- The tenant-wide list endpoint `PipelineApi.get_pipelines()` returns `PipelineList` whose
  `items` are `PipelineV3`; there is **no** v4 variant of the *list* endpoint in this
  libica. Only the single-item `get_pipeline(pipeline_id)` returns `PipelineV4`.
- The project-scoped `ProjectPipelineApi.get_project_pipelines()` deserializes to
  `ProjectPipelineListV4` (items are `ProjectPipelineV4`, each `.pipeline` a `PipelineV4`),
  so `list_project_pipelines` already yields v4 objects at runtime even though its return
  type hint still says `List[ProjectPipeline]`. **Caveat (Finding 5):** unlike
  `get_project_pipeline_obj`, `list_project_pipelines` does *not* force the v4 `Accept`
  header; it relies on libica's negotiated default. See Decision 1a for the hardening fix.

Consequence for the design: **project-scoped** name/version resolution is clean because
`PipelineV4.name`/`.version` are available in the list. **Tenant-scoped** name/version
resolution is constrained — `get_pipelines` cannot return name/version. See decision 1b.

## In-scope pipeline `.code` call-site inventory

Re-grepped across the **whole** `src/` tree, plus `tests/` and `docs/`, for `pipeline_code`,
`_from_pipeline_code`, `_or_code`, `.pipeline.code`, and pipeline-object `.code`, and
reconciled every hit (Finding 1). Each entry below is a pipeline **identifier** usage and is
**in scope**. Out-of-scope non-identifier `.code` hits are enumerated separately at the end.

### `src/wrapica/pipelines/functions/pipelines_functions.py`
1. `get_pipeline_obj_from_pipeline_code(pipeline_code)` — filters
   `pipeline_iter.code == pipeline_code` over `list_all_pipelines()`.
2. `coerce_pipeline_id_or_code_to_pipeline_obj(pipeline_id_or_code)` — non-UUID branch calls
   `get_pipeline_obj_from_pipeline_code`.
3. `coerce_pipeline_id_or_code_to_pipeline_id(pipeline_id_or_code)` — non-UUID branch calls
   `get_pipeline_obj_from_pipeline_code(...).id`.
4. `download_pipeline_to_zip(...)` — `pipeline_code = pipeline_obj.code` used only to name a
   temp dir (`output_dir = Path(tmp_dir) / pipeline_code`).
5. Docstring examples printing `Code: {pipeline_obj.code}` on
   `get_pipeline_obj_from_pipeline_id`, `list_all_pipelines`, and the two coerce functions.

### `src/wrapica/project_pipelines/functions/project_pipelines_functions.py`
6. `get_project_pipeline_obj_from_pipeline_code(project_id, pipeline_code)` — filters
   `project_pipeline_iter.pipeline.code == pipeline_code` over `list_project_pipelines`.
   (Note: this function already raises `ValueError` on `StopIteration`, not `StopIteration`.)
7. `get_project_pipeline_id_from_pipeline_code(project_id, pipeline_code)` — delegates to (6)
   and returns `str(obj.pipeline.id)`.
8. `coerce_pipeline_id_or_code_to_project_pipeline_obj(pipeline_id_or_code)` — non-UUID
   branch calls (6).
9. `list_project_pipelines` docstring example prints
   `Code: {pipeline.pipeline.code}` (line ~1541) — a pipeline-identifier example; update to
   print `Name`/`Version`. Also harden this function's `Accept` header (Finding 5) and fix
   its stale return hint `List[ProjectPipeline]` -> `List[ProjectPipelineV4]`.
10. Docstring examples on (6)/(7)/(8) printing `Code: {project_pipeline_obj.pipeline.code}`.
11. **Creation path (Option A only):** `create_cwl_project_pipeline`,
    `create_cwl_workflow_from_zip`, `create_nextflow_pipeline_from_zip`,
    `create_nextflow_pipeline_from_nf_core_zip`, `create_nextflow_project_pipeline` — each
    takes `pipeline_code: str` and passes `code=pipeline_code` to libica. `nf_core_zip` also
    uses `pipeline_code` to build zip arcnames / temp paths. See Decision 5.

### `src/wrapica/bundle/functions/bundle_functions.py` (Finding 1 — previously missed)
12. `list_pipelines_in_bundle` docstring example prints
    `print(f"Pipeline ID: {pipeline.pipeline.id}, Code: {pipeline.pipeline.code}")`
    (line ~762). This dereferences a **pipeline** identifier `.code`. **Decision: in scope
    as a docstring-only fix** — update the example to print `Name`/`Version`. No functional
    change to `list_pipelines_in_bundle` itself (it returns `BundlePipeline` items as-is;
    this is purely the illustrative print line). The other `.code` in this file
    (`region.code`, line ~345) is a region identifier and stays out of scope.

### `src/wrapica/pipelines/__init__.py` and `src/wrapica/project_pipelines/__init__.py`
13. `__all__` / import re-exports of the four `*_code` function names above.

### `src/wrapica/docs`
14. `docs/wrapica/pipelines.rst` and `docs/wrapica/project_pipelines.rst` list the `*_code`
    function names under explicit `:members:` lists (not autosummary), so new names must be
    added by hand.

### `tests/`
15. `tests/test_pipelines.py` — `TestCoercePipelineIdOrCodeToPipelineObj`, mocks of
    `mock_pipeline.code`, patches of `get_pipeline_obj_from_pipeline_code`.
16. `tests/test_error_handling.py` — `TestGetPipelineObjFromPipelineCodeStopIteration`.
17. `tests/test_project_pipelines.py` — `TestGetProjectPipelineObjFromPipelineCode`,
    `mock_pipeline.pipeline.code` setups, and `create_cwl_workflow_from_zip` tests passing
    `pipeline_code=`.

### Explicitly OUT of scope (NOT pipeline identifiers — leave untouched)
- `AnalysisInput.code` / `AnalysisOutput.code` in `project_analysis_functions.py`
  (`analysis_input_iter.code`, `analysis_iter.code`).
- `region.code` in `bundle_functions.py` (line ~345).
- Pipeline **configuration parameter** `.code` (`PipelineConfigurationParameter.code`).

With the whole-`src/` re-grep reconciled, the pipeline-identifier `.code` sites remaining
after this change are only the deprecated `*_code` shims (intentionally kept for the
deprecation window).

## Decision 1 — New function names and the coerce redefinition

### 1a. project_pipelines (project-scoped) — primary path

Add name/version lookups alongside the deprecated code ones:

```python
def get_project_pipeline_obj_from_pipeline_name(
        project_id: Union[UUID4, str],
        pipeline_name: str,
        pipeline_version: Optional[str] = None,
) -> ProjectPipelineV4:
    ...

def get_project_pipeline_id_from_pipeline_name(
        project_id: Union[UUID4, str],
        pipeline_name: str,
        pipeline_version: Optional[str] = None,
) -> str:
    ...
```

`get_project_pipeline_obj_from_pipeline_name` iterates `list_project_pipelines(project_id)`
and resolves with this **explicit ordered algorithm** (Finding 3 — resolves the
single-match-not-flagged ambiguity):

```
# 0. Reject an empty identifier up front (Finding R2-5). PipelineV4.name has
#    MinLen(1), so an empty name can never match a stored pipeline; fail fast
#    with an explicit validation error rather than a misleading "not found".
if not pipeline_name:
    logger.error("pipeline_name must be a non-empty string")
    raise ValueError("pipeline_name must be a non-empty string")

matches = [
    pp for pp in list_project_pipelines(project_id)
    if pp.pipeline.name == pipeline_name
]

# 1. Explicit version requested
if pipeline_version is not None:
    versioned = [pp for pp in matches if pp.pipeline.version == pipeline_version]
    if len(versioned) == 0:
        logger.error(...); raise ValueError(...)      # name+version not found
    if len(versioned) > 1:
        logger.error(...); raise ValueError(...)      # should not happen; defensive
    return versioned[0]

# 2. Version omitted -> "latest in group"
if len(matches) == 0:
    logger.error(...); raise ValueError(...)          # name not found
if len(matches) == 1:
    return matches[0]                                 # single version: flag irrelevant
flagged = [pp for pp in matches if pp.pipeline.latest_version_in_group is True]
if len(flagged) == 1:
    return flagged[0]
# 0 flagged (flags False/None) OR >1 flagged -> ambiguous
logger.error("multiple versions for '<name>': <versions>; pass pipeline_version")
raise ValueError(...)
```

Key points this pins down:
- A **single** name match is returned regardless of its `latest_version_in_group` value
  (which may legitimately be `False` or `None`). The "latest" flag only disambiguates when
  there is more than one version.
- The success path for multiple versions is **exactly one** flagged `latest_version_in_group
  is True`. Zero flagged or more than one flagged is a `ValueError` that lists the available
  versions and instructs the caller to pass `pipeline_version`.
- `is True` is used deliberately (not truthiness) so `None` is treated as "not flagged".

**Name-to-group assumption behind the "latest" guard (Finding R2-4).** The "latest in group"
branch filters `matches` by `name` only and then expects **exactly one**
`latest_version_in_group is True`. This relies on the assumption that a given `pipeline_name`
maps to a **single** `pipeline_group` within one project — i.e. all same-name pipelines are
versions of one logical pipeline, so only one of them is its group's latest. The design does
**not** hard-depend on that assumption being true: if two distinct groups each contain a
pipeline with the same `name`, each may independently flag `latest_version_in_group is True`,
in which case the ">1 flagged" branch is the **defensive guard** that raises `ValueError`
rather than silently guessing. To make that failure debuggable, the ambiguity `ValueError`
for the ">1 flagged" case must include the distinct `pipeline_group.id` values of the flagged
matches (guarded for `None`, see below), e.g.:

> `Multiple 'latest' pipelines named '<name>' across groups <group-ids>; pass pipeline_version`

**`pipeline_group` may be `None` — guard the container (Finding R2-3).** `PipelineV4.pipeline_group`
is itself `Optional` (it may be `None`), even though `PipelineGroupIdentifier.id` is a
*required* field when the container is present. Any read of the group id in the resolver or in
the ambiguity error message must guard the container, never assume it is populated:

```python
group_id = getattr(pp.pipeline.pipeline_group, "id", None)
```

The grouping/latest algorithm itself keys only on `latest_version_in_group is True`; the
`pipeline_group.id` is used solely for the ambiguity diagnostic. A `None` group id simply
renders as `None` in that message and never raises.

Rationale for "latest when version omitted": matches ICAv2's grouping model
(`latest_version_in_group`) and gives a convenient default, while the ambiguity guard
prevents accidentally launching the wrong version when the flag is absent/ambiguous.

Redefine the coercion helper to resolve a non-UUID string by name:

```python
def coerce_pipeline_id_or_name_to_project_pipeline_obj(
        pipeline_id_or_name: str,
        pipeline_version: Optional[str] = None,
) -> ProjectPipelineV4:
    # UUID -> get_project_pipeline_obj(project_id, pipeline_id_or_name)
    # else  -> get_project_pipeline_obj_from_pipeline_name(project_id, name, version)
```

**`list_project_pipelines` hardening (Finding 5).** Add the forced v4 `Accept` header to
`list_project_pipelines`, mirroring `get_project_pipeline_obj`:

```python
with ApiClient(get_icav2_configuration()) as api_client:
    api_client.set_default_header(
        header_name="Accept",
        header_value="application/vnd.illumina.v4+json",
    )
    api_instance = ProjectPipelineApi(api_client)
```

This removes the latent dependency on libica's content-negotiation default: the entire
project-scoped name/version path depends on `.pipeline.name`/`.version` being present, so we
must not let a future libica default silently degrade the response to `PipelineV3`
(code-only). Also fix the return hint to `List[ProjectPipelineV4]`.

### 1b. pipelines (tenant-scoped) — constrained path

Because `get_pipelines()` returns only `PipelineV3` (code only, no name/version), the
tenant-wide list cannot be filtered by name/version directly. Two viable approaches:

- **Approach A:** keep filtering `PipelineV3.code` under the hood but expose a name
  parameter — rejected: `PipelineV3` has no `name`, so this is impossible without the code
  field, defeating the migration.
- **Approach B (chosen):** resolve against the per-pipeline v4 object.

**Approach B — pinned iteration contract (Finding 4).** `list_all_pipelines()` returns full
`PipelineV3` objects (not bare ids). The resolver reads `.id` off each `PipelineV3` and
fetches the v4 object:

```python
def get_pipeline_obj_from_pipeline_name(
        pipeline_name: str,
        pipeline_version: Optional[str] = None,
) -> PipelineV4:
    # EAGERLY fetch every tenant pipeline as v4, then apply the SAME ordered
    # algorithm as 1a. We do NOT short-circuit on first name match, because
    # "latest in group" (pipeline_version=None) cannot be decided without seeing
    # all sibling versions in the group.
    pipeline_v4_objs = [
        get_pipeline_obj_from_pipeline_id(pipeline_v3.id)  # returns PipelineV4
        for pipeline_v3 in list_all_pipelines()
    ]
    matches = [p for p in pipeline_v4_objs if p.name == pipeline_name]
    # then identical ordered algorithm as 1a (explicit version / single / flagged-latest)
```

The empty-identifier guard from 1a applies here too: `get_pipeline_obj_from_pipeline_name`
raises `ValueError("pipeline_name must be a non-empty string")` before issuing any API call
when `pipeline_name` is falsy (Finding R2-5).

**`list_all_pipelines` must stay on the v3 Accept header (Finding R2-6).** This tenant-scoped
resolver deliberately depends on `list_all_pipelines()` keeping its current
`Accept: application/vnd.illumina.v3+json` header and returning `PipelineV3` items (used only
for their `.id`). There is **no v4 variant of the tenant list endpoint** in libica 3.3, so
`list_all_pipelines` must **not** be "upgraded" to the v4 Accept header the way
`list_project_pipelines` is hardened in Decision 1a. The `name`/`version` data comes exclusively
from the per-id `get_pipeline_obj_from_pipeline_id` v4 fetch inside the loop, never from the
list call. A future "harden all Accept headers to v4" sweep must therefore explicitly exclude
`list_all_pipelines`; this coupling is recorded here so it is not broken inadvertently.

Pinned behaviour and its cost (Finding 4):
- The loop iterates `pipeline_v3.id` (the v3 object's `.id`), one `get_pipeline()` call per
  tenant pipeline — **O(N) network round-trips**. This is accepted as the cost of correctness
  because there is no v4 tenant *list* endpoint in libica 3.3.
- It **eagerly fetches all** v4 objects before filtering (no short-circuit). Short-circuiting
  on the first name match is explicitly rejected because it would break `pipeline_version=None`
  "latest" semantics (you cannot know a version is the latest in its group without seeing the
  group). Eager fetch keeps the tenant-scoped "latest" rule identical to the project-scoped
  one.
- Because of the O(N) cost, the docstring must **recommend the project-scoped
  `project_pipelines` helpers whenever a project context is available**, and document the
  tenant-scoped resolver as the heavier parity path. The tenant resolver still honours
  `pipeline_version=None` correctly (it is not "best-effort"); it is just slower.

Coercion redefinitions:

```python
def coerce_pipeline_id_or_name_to_pipeline_obj(
        pipeline_id_or_name: str,
        pipeline_version: Optional[str] = None,
) -> PipelineV4: ...

def coerce_pipeline_id_or_name_to_pipeline_id(
        pipeline_id_or_name: str,
        pipeline_version: Optional[str] = None,
) -> Union[UUID4, str]: ...
```

UUID branch returns the id / fetches by id as before; non-UUID branch resolves by name.
Error type for not-found changes from the old `StopIteration` to `ValueError` for
consistency with the project-scoped helpers; documented in each `:raises:`.

### 1c. `download_pipeline_to_zip` temp-dir naming

Replace `pipeline_code = pipeline_obj.code` with a name/version-derived directory name that
is robust when name is None:

```python
pipeline_dir_name = pipeline_obj.name or str(pipeline_obj.id)
if pipeline_obj.version is not None:
    pipeline_dir_name = f"{pipeline_dir_name}__{pipeline_obj.version}"
output_dir = Path(tmp_dir) / pipeline_dir_name
```

This is cosmetic (names a temp dir) so it must not raise; falling back to `id` keeps it
safe. The surrounding zip logic is unchanged.

## Decision 2 — Version expression: separate `pipeline_version` arg vs combined string

**Chosen: a separate optional `pipeline_version: Optional[str] = None` argument**, not a
combined `"name/version"` string.

Justification:
- ICAv2 pipeline names can contain characters that make a delimiter choice fragile; a
  combined string forces the library to pick and parse a delimiter, which is error-prone and
  undocumented at the API level. `PipelineV4` models `name` and `version` as distinct fields,
  so distinct arguments mirror the data model.
- A separate arg lets `None` mean "latest in group" cleanly, which a combined string cannot
  express without a sentinel.
- Keyword arguments with type hints are self-documenting and match the project's convention
  of explicit scalar parameters (e.g. `analysis_storage_size`).

## Decision 3 — Deprecation strategy for the old `code` functions

**Chosen: keep the old `*_code` functions for one release as thin shims that emit a
`DeprecationWarning` and delegate, rather than hard-removing them.**

Although wrapica is a pre-1.0-style wrapper, these four functions and the `pipeline_code`
kwargs are part of the public surface (re-exported in `__all__`, documented in `.rst`, and
exercised by downstream UMCCR automation). A one-release deprecation window costs little and
prevents breaking in-flight callers. The shims:

- `get_project_pipeline_obj_from_pipeline_code` /
  `get_project_pipeline_id_from_pipeline_code` — keep current `.pipeline.code` filtering
  (still functional against `PipelineV4.code`), wrapped in `warnings.warn(...,
  DeprecationWarning, stacklevel=2)` pointing to the `_from_pipeline_name` replacement.
- `get_pipeline_obj_from_pipeline_code` — same treatment; still filters `PipelineV3.code`
  and keeps re-raising `StopIteration` to preserve its existing contract.
- `coerce_pipeline_id_or_code_to_project_pipeline_obj`,
  `coerce_pipeline_id_or_code_to_pipeline_obj`, `coerce_pipeline_id_or_code_to_pipeline_id`
  — keep the old names as deprecated aliases that delegate to the new
  `coerce_pipeline_id_or_name_*` functions.

**Behavioural-change callout for the coerce aliases (Finding 7 / Finding R2-2).** Each of the
old `code` coerce aliases resolved a non-UUID string by **code**; its deprecated alias now
delegates to the matching **name** resolver, so the same string now resolves by **name**. This
is the intended migration, and the requirement below applies **uniformly to all three** coerce
aliases, not just `coerce_pipeline_id_or_code_to_pipeline_id` (Finding R2-2 — the round-1 design
specified it for only one of the three):

| Deprecated alias | New delegate (name resolver) |
| --- | --- |
| `coerce_pipeline_id_or_code_to_pipeline_id` (tenant) | `coerce_pipeline_id_or_name_to_pipeline_id` |
| `coerce_pipeline_id_or_code_to_pipeline_obj` (tenant) | `coerce_pipeline_id_or_name_to_pipeline_obj` |
| `coerce_pipeline_id_or_code_to_project_pipeline_obj` (project) | `coerce_pipeline_id_or_name_to_project_pipeline_obj` |

Each of the three deprecated aliases must:

1. Emit a `DeprecationWarning` (via `warnings.warn(..., DeprecationWarning, stacklevel=2)`)
   whose text explicitly states the resolution target changed from code to name, e.g. for the
   tenant id alias:

   > `coerce_pipeline_id_or_code_to_pipeline_id is deprecated and will be removed in a future
   > release. A non-UUID string is now resolved by pipeline NAME, not code. Pass a UUID, or
   > migrate to coerce_pipeline_id_or_name_to_pipeline_id(pipeline_id_or_name, pipeline_version).`

   The other two aliases carry the equivalent text naming their own `_or_name` replacement.

2. Have a unit test that asserts the **new resolution path** — i.e. that the alias delegated to
   its name resolver with the passed string (patch the `_or_name` delegate and assert it was
   called), **in addition** to asserting the warning fired. A warning-only test is insufficient
   (Finding 7 / R2-2).

**Return-hint pinning for the aliases vs the new resolvers (Finding R2-2).** The two tenant
aliases currently declare the broad `PipelineType` (`Union[PipelineV3, PipelineV4]`) return
hint, and the project alias declares `ProjectPipelineV4`. Decision: **keep each deprecated
alias's existing return hint unchanged** (`coerce_pipeline_id_or_code_to_pipeline_obj ->
PipelineType`, `coerce_pipeline_id_or_code_to_pipeline_id -> Union[UUID4, str]`,
`coerce_pipeline_id_or_code_to_project_pipeline_obj -> ProjectPipelineV4`) so the shims make no
typing-contract change during the deprecation window. The **new** resolvers declare the precise
v4 types they actually return: `coerce_pipeline_id_or_name_to_pipeline_obj -> PipelineV4`,
`coerce_pipeline_id_or_name_to_pipeline_id -> Union[UUID4, str]`,
`coerce_pipeline_id_or_name_to_project_pipeline_obj -> ProjectPipelineV4`. The tenant alias's
`PipelineType` hint remains technically correct because `PipelineV4` is a member of that union;
narrowing it is deferred to the removal release to avoid a redundant typing change now.

Use the stdlib `warnings` module (add `import warnings` grouped with the standard-library
imports in each functions module). Emit `DeprecationWarning` with `stacklevel=2` so the
warning points at the caller.

New names are added to each module `__init__.py` import block and `__all__`, and to the
`.rst` `:members:` lists; the deprecated names remain listed (annotated as deprecated in
docstrings) for the window.

## Decision 4 — Analysis entrypoint resolution

Confirmed by reading the launch path:
`ICAv2CWLPipelineAnalysis.create_analysis()` (and the Nextflow equivalent) builds
`CreateCwlWithJsonInputAnalysis(pipelineId=self.pipeline_id, ...)` /
`CreateNextflowWithCustomInputAnalysis(...)`. `self.pipeline_id` originates from the
constructor and is threaded unchanged through `ICAv2EngineParameters`. The launch classes
**only ever receive an already-resolved `pipeline_id` (a UUID)** — they never resolve a
`code` themselves. `set_launch_parameters` uses `pipeline_id` solely to look up the default
analysis storage (`get_default_analysis_storage_id_from_project_pipeline`).

Therefore the entrypoint migration requires **no change inside the launch/analysis classes**.
The correct integration point is the boundary where a caller holds a name/version and needs a
`pipeline_id`:

- Callers who currently do `pipeline_id = get_project_pipeline_id_from_pipeline_code(...)`
  (or coerce a code) switch to `get_project_pipeline_id_from_pipeline_name(project_id, name,
  version)` / `coerce_pipeline_id_or_name_to_pipeline_id(...)` and pass the resulting UUID
  into `create_cwl_input_json_analysis_obj` / `ICAv2CWLPipelineAnalysis` /
  `ICAv2NextflowPipelineAnalysis` exactly as before.

Flow: **name/version -> `get_project_pipeline_obj_from_pipeline_name` -> `.pipeline.id` ->
`pipeline_id` -> `CreateCwlWithJsonInputAnalysis(pipelineId=...)`**.

**Stringification at the boundary (Finding 6).** The resolved id is stringified upstream: the
`_id_from_name` helpers return `str(obj.pipeline.id)`, so the value handed to the launch
classes is already a `str`, satisfying `CreateCwlWithJsonInputAnalysis.pipeline_id` /
`CreateNextflowWithCustomInputAnalysis.pipeline_id` (both required strict `str`). Note that
`ICAv2CWLPipelineAnalysis.create_analysis()` passes `pipelineId=self.pipeline_id` **without**
a `str()` wrap (whereas the Nextflow class uses `str(self.pipeline_id)`). This asymmetry is
**pre-existing and out of scope** for this migration — our resolvers already return strings,
so the name/version path is safe. It is noted only so the implementer does not mistake it for
something this change introduced; if desired it can be filed as a separate tidy-up, but it is
not required here.

The design keeps `pipeline_id` (a UUID string) as the launch-class contract and places
name/version resolution strictly upstream, keeping the launch classes simple and avoiding an
implicit extra API round-trip at launch time.

## Decision 5 — Pipeline creation (workflow generation) by name/version — Option A (signed off, in scope)

All five create functions in `project_pipelines_functions.py` currently take a required
`pipeline_code: str` and pass `code=pipeline_code` to libica
(`ProjectPipelineApi.create_cwl_pipeline` / `create_nextflow_pipeline`, which in libica 3.3
accept `code`, `name`, and `version` — all optional, verified above).

**New signature shape (applied to all five):**

```python
def create_cwl_project_pipeline(
        project_id: Union[UUID4, str],
        pipeline_name: str,                     # NEW primary identifier
        workflow_path: Path,
        tool_paths: Optional[List[Path]] = None,
        pipeline_version: Optional[str] = None, # NEW
        analysis_storage: Optional[AnalysisStorageType] = None,
        ...,
        pipeline_code: Optional[str] = None,    # DEPRECATED kwarg, now optional
) -> ...:
```

Rules enforced inside each create function, in this **exact order** (Finding R2-1 adds the
first branch):
- **Both supplied -> error (checked first).** libica's `create_cwl_pipeline` /
  `create_nextflow_pipeline` accept `code`, `name`, AND `version` simultaneously, so passing
  both a `pipeline_name` and a `pipeline_code` would send an ambiguous (`code=` + `name=`)
  body to the server. Reject it before any other branch:

  ```python
  if pipeline_code is not None and pipeline_name is not None:
      raise ValueError(
          "Provide either pipeline_name (preferred) or the deprecated pipeline_code, not both"
      )
  ```

- **Only `pipeline_code` supplied.** Emit a `DeprecationWarning` ("`pipeline_code` is
  deprecated; pass `pipeline_name` (and optional `pipeline_version`)") and, for backward
  compatibility, pass `code=pipeline_code` to libica. Do **not** send `name=`/`version=`.
- **Only `pipeline_name` supplied.** Pass `name=pipeline_name` and, when `pipeline_version` is
  given, `version=pipeline_version` to libica. Do **not** also send `code=` in that case.
- **Neither supplied.** `raise ValueError("Must provide pipeline_name (preferred) or the
  deprecated pipeline_code")`.

Because `pipeline_name` is the leading positional and `pipeline_code` a trailing optional
keyword, the "both supplied" case is reachable by a caller that passes a positional name and
also sets `pipeline_code=`; the first branch above is what makes that explicit rather than
silently ambiguous.
- `pipeline_name` is placed before the other required positional params so new callers can
  pass it positionally where they used to pass `pipeline_code`; the deprecated `pipeline_code`
  moves to a trailing optional keyword. This preserves positional-call compatibility for the
  common `(project_id, <identifier>, workflow_path)` shape while keeping the deprecated name
  reachable by keyword.
- `create_nextflow_pipeline_from_nf_core_zip` additionally uses the identifier to build zip
  arcnames / temp paths. Replace that internal use with `pipeline_name` (falling back to
  `pipeline_code` when only the deprecated kwarg is passed), so the generated zip layout is
  stable.
- The wrapper-to-wrapper calls (`create_cwl_workflow_from_zip` ->
  `create_cwl_project_pipeline`, `create_nextflow_pipeline_from_zip`/`..._nf_core_zip` ->
  `create_nextflow_project_pipeline`) forward `pipeline_name`/`pipeline_version` (or the
  deprecated `pipeline_code`) through unchanged.

**Open question for the operator (does not block the entrypoint half):** ICAv2 pipeline
*creation* with a `name`/`version` has group-assignment and version-bump semantics
(whether creating `name=X version=2` attaches to an existing group for `X` or starts a new
group). This design does not attempt to manage groups: it passes `name`/`version` straight
through to libica and lets the server apply its grouping rules, matching how the current
code passes `code` straight through. If the operator wants wrapica to manage group identity
(e.g. look up an existing `pipeline_group.id` and attach), that is a larger follow-up and is
explicitly **not** included here; it is tracked in "Backlogged / follow-up".

## New / changed public function signatures (summary)

`src/wrapica/project_pipelines/functions/project_pipelines_functions.py`:
- NEW `get_project_pipeline_obj_from_pipeline_name(project_id, pipeline_name, pipeline_version=None) -> ProjectPipelineV4`
- NEW `get_project_pipeline_id_from_pipeline_name(project_id, pipeline_name, pipeline_version=None) -> str`
- NEW `coerce_pipeline_id_or_name_to_project_pipeline_obj(pipeline_id_or_name, pipeline_version=None) -> ProjectPipelineV4`
- DEPRECATED (shim) `get_project_pipeline_obj_from_pipeline_code`,
  `get_project_pipeline_id_from_pipeline_code`,
  `coerce_pipeline_id_or_code_to_project_pipeline_obj` (keeps its existing
  `-> ProjectPipelineV4` return hint for the window; delegates to the `_or_name` resolver with
  a resolution-change `DeprecationWarning`, Finding R2-2)
- HARDEN `list_project_pipelines`: force `Accept: application/vnd.illumina.v4+json`; fix
  return hint `List[ProjectPipeline]` -> `List[ProjectPipelineV4]`; fix docstring example to
  print `Name`/`Version`.
- CHANGED (in scope, Decision 5) `create_cwl_project_pipeline`, `create_cwl_workflow_from_zip`,
  `create_nextflow_pipeline_from_zip`, `create_nextflow_pipeline_from_nf_core_zip`,
  `create_nextflow_project_pipeline`: add `pipeline_name` / `pipeline_version`, deprecate
  `pipeline_code` (now optional), per Decision 5.

`src/wrapica/pipelines/functions/pipelines_functions.py`:
- NEW `get_pipeline_obj_from_pipeline_name(pipeline_name, pipeline_version=None) -> PipelineV4`
- NEW `coerce_pipeline_id_or_name_to_pipeline_obj(pipeline_id_or_name, pipeline_version=None) -> PipelineV4`
- NEW `coerce_pipeline_id_or_name_to_pipeline_id(pipeline_id_or_name, pipeline_version=None) -> Union[UUID4, str]`
- DEPRECATED (shim) `get_pipeline_obj_from_pipeline_code`,
  `coerce_pipeline_id_or_code_to_pipeline_obj` (keeps `-> PipelineType`),
  `coerce_pipeline_id_or_code_to_pipeline_id` (keeps `-> Union[UUID4, str]`) — both tenant
  coerce aliases delegate to their `_or_name` resolver with a resolution-change
  `DeprecationWarning` (Finding R2-2).
- `download_pipeline_to_zip` internal temp-dir name derived from name/version with id
  fallback (Decision 1c).

`src/wrapica/bundle/functions/bundle_functions.py`:
- DOCSTRING only: `list_pipelines_in_bundle` example prints `Name`/`Version` instead of
  `Code` (Finding 1). No functional change.

`__init__.py` (both pipeline modules): import and `__all__` add the new names; keep deprecated
names for the window.

All new functions follow project conventions: snake_case names, type hints with
`Optional[str]`, grouped imports (stdlib -> libica -> local), and docstrings with `:param:`,
`:return:`, `:rtype:`, `:raises:`, and a `.. code-block:: python` example that prints
`Name`/`Version` rather than `Code`.

## Error handling (per operation)

- **Name not found (project-scoped)** in `get_project_pipeline_obj_from_pipeline_name`: no
  element matches `name` (and `version` if given). Fatal to the call. Log
  `logger.error(f"Could not find pipeline name '{pipeline_name}' (version '{pipeline_version}') in project {project_id}")`
  and `raise ValueError`. Matches the existing code-function convention of raising
  `ValueError` on project lookups.
- **Version omitted but ambiguous** (multiple name matches, zero or >1 flagged
  `latest_version_in_group is True`): fatal. Log an error listing the available versions and
  `raise ValueError` instructing the caller to pass `pipeline_version`. (Decision 1a's
  ordered algorithm.)
- **Name not found (tenant-scoped)** in `get_pipeline_obj_from_pipeline_name`: fatal. Log
  `logger.error` and `raise ValueError`. This intentionally changes the old
  `get_pipeline_obj_from_pipeline_code` behaviour of re-raising `StopIteration`; the new
  function documents `:raises ValueError:`. The deprecated code shim keeps raising
  `StopIteration`.
- **`name`/`version` is `None` on an object**: treated as a non-match by the equality filter;
  never dereferenced in a way that raises.
- **API failure** inside `list_project_pipelines` / `list_all_pipelines` /
  `get_pipeline_obj_from_pipeline_id`: these already raise `ValueError`/`ApiException` and are
  logged at the point of the API call; the new resolvers let those propagate unchanged. For
  the tenant-scoped resolver, an `ApiException` on any one of the N `get_pipeline` calls
  propagates and aborts resolution (fatal) — documented in `:raises:`.
- **Create path, both identifiers supplied (Option A, Finding R2-1)**: if both `pipeline_name`
  and `pipeline_code` are not `None`, `raise ValueError("Provide either pipeline_name
  (preferred) or the deprecated pipeline_code, not both")`. Checked as the first create-path
  branch, before the single-identifier branches, so an ambiguous `code=`+`name=` body is never
  sent to libica. Fatal to the call.
- **Create path, no identifier (Option A)**: `raise ValueError` if neither `pipeline_name`
  nor `pipeline_code` is supplied.
- **Empty `pipeline_name` (lookup path, Finding R2-5)**: `get_project_pipeline_obj_from_pipeline_name`
  and `get_pipeline_obj_from_pipeline_name` raise `ValueError("pipeline_name must be a
  non-empty string")` before any API call when `pipeline_name` is falsy. Fatal; logged via
  `logger.error`.
- **UUID coercion branch**: `is_uuid_format` decides the branch; a malformed non-UUID string
  falls into the name branch and surfaces as a name-not-found `ValueError`.
- **`DeprecationWarning` emission**: non-fatal; emitted via `warnings.warn(...,
  DeprecationWarning, stacklevel=2)`. Not routed through the `verboselogs` logger (the
  warnings channel is the idiomatic place for deprecations).

## Input validation (per external input)

- `project_id`: required for project-scoped functions; `Union[UUID4, str]`, stringified via
  `str(project_id)` before the API call, as existing functions do. No new validation.
- `pipeline_id_or_name`: required `str`. Routed by `is_uuid_format`: UUID -> id branch;
  otherwise -> name branch. No length/charset limits imposed by wrapica (the API owns those).
- `pipeline_name`: required `str` for the `_from_pipeline_name` functions. Not UUID-checked
  (it is explicitly a name). **Empty / falsy `pipeline_name` is rejected** with
  `ValueError("pipeline_name must be a non-empty string")` at the top of each
  `_from_pipeline_name` function (Finding R2-5). Rationale: `PipelineV4.name` carries
  `MinLen(1)`, so a stored name can never be empty and an empty input could only ever surface
  as a misleading "not found"; failing fast with a validation error is clearer and is the
  design's chosen behaviour (not the round-1 "allowed through, simply won't match").
- `pipeline_version`: optional `str`, default `None` meaning "latest in group". When provided
  it must equal `pipeline.version` exactly (string compare; ICAv2 versions are opaque strings,
  no semver parsing).
- `pipeline_code` (Option A create path): optional `str`; presence triggers a
  `DeprecationWarning` and the legacy `code=` passthrough. **Mutually exclusive with
  `pipeline_name`**: supplying both is a `ValueError` (Finding R2-1), checked as the first
  create-path branch.

## Invariant ownership

- **"A non-UUID pipeline reference resolves by name, not code"** — owned by the
  `coerce_pipeline_id_or_name_*` functions (single branch point via `is_uuid_format`).
  Centralising it there means launch classes and other callers never re-implement the rule.
- **"The launch body always receives a UUID `pipeline_id` string"** — owned by the caller
  boundary (the coerce / `_id_from_name` helpers that call `str(...)`), not the launch
  classes. The launch classes assume `self.pipeline_id` is a (stringifiable) UUID and this
  design preserves that assumption, which is why they need no change (Finding 6).
- **"`PipelineV4.name`/`.version`/`latest_version_in_group`/`pipeline_group` may be None"** —
  owned by the resolver filters, which compare/skip, use `is True` rather than assume presence,
  and read the group id via `getattr(..., "pipeline_group", ...)`-style guards (Findings R2-3).
- **"A pipeline identifier must be non-empty / unambiguous"** — owned by the
  `_from_pipeline_name` resolvers (empty-name `ValueError`, Finding R2-5) and the create
  functions (mutually-exclusive `pipeline_name`/`pipeline_code`, Finding R2-1). Enforced at the
  function boundary before any API call, so invalid identifiers never reach libica.
- **"The project-scoped list is always v4"** — now owned explicitly by `list_project_pipelines`
  via the forced `Accept` header (Finding 5), rather than left to libica negotiation defaults.

## Testability

Unit-testable (mock-based, matching the existing `tests/` style with `mocker`/`MagicMock`):
- `get_project_pipeline_obj_from_pipeline_name`: mock `list_project_pipelines` to return
  `MagicMock` pipelines with `.pipeline.name` / `.pipeline.version` /
  `.pipeline.latest_version_in_group` / `.pipeline.pipeline_group.id`; assert each branch of
  the ordered algorithm — empty `pipeline_name` -> `ValueError` with no API call (Finding R2-5);
  exact version match; single match returned even when `latest_version_in_group` is
  `False`/`None`; latest-when-omitted with exactly one flagged; no-match `ValueError`;
  ambiguous `ValueError` (zero flagged, and >1 flagged — the >1-flagged case covering two
  distinct `pipeline_group.id` values, asserting the group ids appear in the message, Finding
  R2-4); and a `pipeline_group is None` match to confirm the `getattr` guard does not raise
  (Finding R2-3).
- `get_project_pipeline_id_from_pipeline_name`: mock the obj function; assert it returns
  `str(obj.pipeline.id)`.
- `coerce_pipeline_id_or_name_to_*`: assert UUID input delegates to the id getter and non-UUID
  input delegates to the **name** getter (patch both and check which was called), mirroring
  the existing `TestCoercePipelineIdOrCodeToPipelineObj` structure.
- Deprecated coerce aliases — **all three** (Finding R2-2): for each of
  `coerce_pipeline_id_or_code_to_pipeline_id`, `coerce_pipeline_id_or_code_to_pipeline_obj`,
  and `coerce_pipeline_id_or_code_to_project_pipeline_obj`, assert under
  `pytest.warns(DeprecationWarning)` that the alias delegated to its **`_or_name` resolver**
  with the passed string (patch the delegate and assert the call) — not merely that a warning
  fired.
- `get_pipeline_obj_from_pipeline_name`: mock `list_all_pipelines` to return v3 objects with
  `.id`, and `get_pipeline_obj_from_pipeline_id` to return v4 objects; assert it fetches **all**
  (one call per v3 id, no short-circuit) and matches name/version; assert `ValueError` on no
  match.
- Deprecation shims: use `pytest.warns(DeprecationWarning)` AND assert the delegation target
  — e.g. patch the name resolver and assert the deprecated coerce alias called it with the
  passed string (Finding 7), not merely that a warning fired.
- `download_pipeline_to_zip`: assert the temp-dir name falls back to `id` when `name`/`version`
  are None (mock `download_pipeline_to_directory` and inspect the path passed).
- Create path (Option A): assert `pipeline_name` passes `name=`/`version=` to the libica create
  call (mock the API); assert `pipeline_code` passes `code=` and emits `DeprecationWarning`;
  assert `ValueError` when neither is supplied; and assert `ValueError` when **both**
  `pipeline_name` and `pipeline_code` are supplied, with no API call made (Finding R2-1).

Integration-tested (requires live ICAv2 creds; out of CI's reach, exercised manually):
- Real name/version resolution against a project with multiple versions in a group, to confirm
  `latest_version_in_group` behaves as assumed and that a launched analysis receives the
  resolved `pipeline_id`. The tenant-scoped resolver's O(N) fetch is also only meaningfully
  exercised live.

The design is unit-test-friendly because all name/version logic lives in pure functions driven
by list/getter calls already mocked in the suite; no launch-class changes means the existing
analysis tests stay valid.

## Test updates required

- `tests/test_project_pipelines.py`: add `TestGetProjectPipelineObjFromPipelineName`
  (exact version; single-match-not-flagged; latest default; not-found; ambiguous zero-flagged;
  ambiguous multi-flagged) and `TestCoercePipelineIdOrNameToProjectPipelineObj`. Keep
  `TestGetProjectPipelineObjFromPipelineCode` but wrap calls in `pytest.warns(DeprecationWarning)`.
  Add `.pipeline.name` / `.pipeline.version` / `.pipeline.latest_version_in_group` to the
  relevant `MagicMock`s. Add create-path tests (all five functions) asserting `name=`/`version=`
  passthrough to the mocked libica create call, and that supplying `pipeline_code` still works
  but emits `DeprecationWarning`; update the existing `create_cwl_workflow_from_zip` tests to
  the `pipeline_name=` form (retain one `pipeline_code=` case under `pytest.warns`); add a
  both-supplied `ValueError` case asserting no API call (Finding R2-1); and extend the project
  coerce-alias test to assert delegation to `coerce_pipeline_id_or_name_to_project_pipeline_obj`
  under `pytest.warns` (Finding R2-2).
- `tests/test_pipelines.py`: add name-based coerce tests (assert delegation target, Finding 7);
  wrap the existing code-based coerce tests in `pytest.warns` and, for **both** tenant aliases
  (`coerce_pipeline_id_or_code_to_pipeline_obj` and `coerce_pipeline_id_or_code_to_pipeline_id`),
  assert delegation to the matching `_or_name` resolver (Finding R2-2); add an empty-name
  `ValueError` test for `get_pipeline_obj_from_pipeline_name` (Finding R2-5).
- `tests/test_error_handling.py`: keep `TestGetPipelineObjFromPipelineCodeStopIteration` (now
  under `pytest.warns`) and add a `ValueError` case for `get_pipeline_obj_from_pipeline_name`.
- Run `pip install .[test]` then `pytest` to verify, per the project's commands.

## Docs updates required

- `docs/wrapica/pipelines.rst` and `docs/wrapica/project_pipelines.rst`: add the new
  `*_from_pipeline_name` / `coerce_pipeline_id_or_name_*` names to the `:members:` lists; keep
  the deprecated `*_code` names for the window.
- Docstring examples in both pipeline functions modules **and** in
  `bundle_functions.list_pipelines_in_bundle` and
  `project_pipelines_functions.list_project_pipelines`: update the illustrative
  `print(...Code: {...code})` lines on name/version-returning functions to print `Name` /
  `Version`, and reference `name`/`version` arguments in the new functions'
  `.. code-block:: python` examples. Do **not** alter examples for out-of-scope `.code`
  (region, analysis input/output, config parameters).
- `make build_docs_quick` to confirm the docs build.

## Backlogged / follow-up (out of this change)

- **Create-path group management**: Option A (in scope) passes `name`/`version` straight to
  libica and does not manage `pipeline_group` identity. Managing groups (attach-to-existing
  vs new-group, version-bump policy) is a separate follow-up (see Decision 5's open question).
- **CWL launch `str()` asymmetry**: `ICAv2CWLPipelineAnalysis.create_analysis()` passes
  `pipelineId=self.pipeline_id` without `str()` (Nextflow uses `str()`). Pre-existing, not
  introduced here (Finding 6). Optional tidy-up.
- **Removal of the deprecated `*_code` functions and the `pipeline_code` kwargs**: schedule for
  the release after the deprecation window.

## Responses to design review (round 2)

Round-2 verdict: `CHANGES_REQUESTED` (0 HIGH, 2 MEDIUM, 4 NIT). The review was positive
("a strong, well-grounded design"); every finding is an under-specified corner rather than a
correctness error. All six are addressed in-document; none are backlogged or ignored.

- **Finding R2-1 (MEDIUM — create path: `pipeline_name` + `pipeline_code` both supplied is
  unspecified): ADDRESSED.** Decision 5 now opens its create-path rule list with an explicit
  first branch: `if pipeline_code is not None and pipeline_name is not None: raise
  ValueError("Provide either pipeline_name (preferred) or the deprecated pipeline_code, not
  both")`, checked before the single-identifier branches so an ambiguous `code=`+`name=` body
  can never reach libica. The rule is also recorded in Error handling and Input validation, and
  a both-supplied `ValueError` (no-API-call) test is added to the create-path test list.
- **Finding R2-2 (MEDIUM — deprecation-warning spec and resolution-change callout covered only
  one of the coerce aliases): ADDRESSED.** Decision 3's behavioural-change callout is rewritten
  to apply **uniformly to all three** coerce aliases (tenant id, tenant obj, project obj), with
  a mapping table of each alias to its `_or_name` delegate. Each alias must emit a
  `DeprecationWarning` stating the non-UUID string is now resolved by NAME not code (naming its
  own replacement), and each must have a test asserting the name resolver was called (not
  warning-only). Return-hint pinning is now explicit: the three deprecated aliases keep their
  existing hints (`PipelineType` / `Union[UUID4, str]` / `ProjectPipelineV4`) for the window
  while the new resolvers declare precise v4 types (`PipelineV4` / `Union[UUID4, str]` /
  `ProjectPipelineV4`). Testability and Test-updates sections updated for all three.
- **Finding R2-3 (NIT — `pipeline_group` may be `None`): ADDRESSED.** Decision 1a now reads the
  group id via `getattr(pp.pipeline.pipeline_group, "id", None)`, explicitly noting that
  `PipelineV4.pipeline_group` is itself `Optional` (its `.id` is required only when the
  container is present), so the resolver never assumes the group is populated. A
  `pipeline_group is None` test case is added.
- **Finding R2-4 (NIT — "latest in group" does not scope matches to a single group): ADDRESSED.**
  Decision 1a now states the name->single-group assumption, that the ">1 flagged -> ValueError"
  branch is the defensive guard when that assumption does not hold, and requires the ambiguity
  message to include the distinct `pipeline_group.id` values. A >1-flagged-across-two-groups
  test is added.
- **Finding R2-5 (NIT — empty `pipeline_name` silently becomes "not found"): ADDRESSED.**
  Chosen behaviour: an explicit `if not pipeline_name: raise ValueError("pipeline_name must be
  a non-empty string")` at the top of both `get_project_pipeline_obj_from_pipeline_name` and
  `get_pipeline_obj_from_pipeline_name`, before any API call. Recorded in Decision 1a/1b, Error
  handling, and Input validation; empty-name tests added for both resolvers.
- **Finding R2-6 (NIT — tenant `list_all_pipelines` v3 Accept header coupling undocumented):
  ADDRESSED.** Decision 1b now notes that `list_all_pipelines` must remain on the v3 Accept
  header (no v4 tenant list endpoint exists), that name/version comes only from the per-id v4
  fetch, and that a future "harden all Accept headers to v4" sweep must exclude it.

## Responses to design review (round 1)

- **Finding 1 (HIGH — inventory missed `bundle_functions.list_pipelines_in_bundle`): ADDRESSED.**
  Re-grepped the whole `src/` tree. Added the bundle docstring example as explicit in-scope
  item 12 (docstring-only `Name`/`Version` fix) and confirmed `region.code` stays out of scope.
  Also found and added two more previously-unlisted in-scope pipeline-identifier sites in the
  same pass: the `list_project_pipelines` docstring example (item 9) and its missing v4 `Accept`
  header. The inventory now reconciles every pipeline-`.code` hit in `src/`.
- **Finding 2 (MEDIUM — "workflow generation" scope split): ADDRESSED and RESOLVED.**
  Surfaced the decision enumerating all **five** create functions; the operator signed off
  **Option A** (pipeline creation in scope now). Verified against installed libica 3.3 that
  `create_cwl_pipeline` and `create_nextflow_pipeline` both accept optional `code`/`name`/
  `version`, so Option A invents no fields. Fully specified in Decision 5 (add
  `pipeline_name`/`pipeline_version`, deprecate `pipeline_code` with a `DeprecationWarning`,
  keep `code=` passthrough for the deprecation window).
- **Finding 3 (MEDIUM — latest-when-omitted under-specified): ADDRESSED.** Decision 1a now
  states an explicit ordered algorithm: 0 matches -> `ValueError`; exactly 1 match -> return it
  regardless of flag; >1 -> exactly one `latest_version_in_group is True` returns, else
  `ValueError` listing versions. `is True` is used so `None`/`False` are both "not flagged".
- **Finding 4 (MEDIUM — tenant resolver iteration contract/N+1): ADDRESSED.** Decision 1b now
  states the loop reads `pipeline_v3.id` from the full `PipelineV3` objects, **eagerly fetches
  all** v4 objects (no short-circuit) so `pipeline_version=None` "latest" stays correct, accepts
  the O(N) round-trip cost explicitly, and documents preferring the project-scoped helpers.
- **Finding 5 (NIT — `list_project_pipelines` relies on implicit v4 default): ADDRESSED.**
  Decision 1a hardens `list_project_pipelines` to force `Accept: application/vnd.illumina.v4+json`
  and fixes the stale return hint.
- **Finding 6 (NIT — CWL launch missing `str()`): ADDRESSED (noted, not changed).** Decision 4
  confirms resolved ids are stringified at the boundary by the `_id_from_name` helpers and flags
  the CWL class's missing `str()` as a pre-existing, out-of-scope concern (backlogged).
- **Finding 7 (NIT — coerce shim changes resolution target): ADDRESSED.** Decision 3 specifies
  explicit `DeprecationWarning` text ("string now resolved by name, not code; pass a UUID or
  migrate to `coerce_pipeline_id_or_name_to_pipeline_id`") and requires tests to assert the new
  resolution path, not just that a warning fired.
