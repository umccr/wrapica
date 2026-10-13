# Design Review (round 3): pipeline `code` -> `name`/`version` migration

Reviewer: design-review subagent (fresh read, no prior-round context carried in).
Design under review: `.agents/tasks/pipeline-name-version/design.md` (self-labelled "revision
round 2", answering a round-2 `CHANGES_REQUESTED` verdict).
Source tree verified: `.worktrees/pipeline-name-version/src/wrapica/`, plus `docs/` and `tests/`,
against installed `libica` 3.3 models/APIs.

## Summary

This is a mature design. Its load-bearing technical claims were independently re-verified against
the installed libica 3.3 package and the actual source, and they hold: the model shapes
(`PipelineV4` retains `code` while exposing optional `name`/`version`/`pipeline_group`/
`latest_version_in_group`; `PipelineV3` is code-only; `PipelineGroupIdentifier` is id-only), the
list-endpoint return types, the launch-class `pipeline_id` wiring, and the create-API signatures
all match what the design asserts. The four decision points (naming/signatures, version
expression, deprecation, entrypoint resolution) are resolved concretely with justification, and
the in-scope/out-of-scope `.code` inventory correctly separates pipeline identifiers from
`AnalysisInput/Output.code`, `region.code`, and configuration-parameter `.code`.

Two blocking issues remain, both factual-accuracy defects in the design's own text that would
mislead an implementer who follows the document literally:

- A **MEDIUM** return-hint error: the design repeatedly states the project-scoped coerce alias
  currently declares `-> ProjectPipelineV4` and should "keep its existing" hint. The actual
  current hint is `PipelineType` (= `Union[ProjectPipeline, ProjectPipelineV4]`). Following the
  design literally produces a wrong "no change needed" conclusion.
- A **MEDIUM** inventory gap: the five create functions' own docstring `.. code-block::` examples
  print `Code: {pipeline.pipeline.code}` (lines ~1989/2158/2256/2361/2526), but these specific
  example lines are not enumerated in the inventory, and the Docs section only commits to updating
  examples on "name/version-returning functions" and *new* functions — leaving it ambiguous
  whether these five get updated.

Verdict: **CHANGES_REQUESTED** (2 MEDIUM, 0 HIGH, 2 NIT).

---

## Findings

### 1. MEDIUM — Wrong claim about the project coerce alias's current return hint

**Where:** Decision 3 ("Return-hint pinning for the aliases vs the new resolvers") and the
"New / changed public function signatures (summary)" section.

**Problem:** The design states the project alias "declares `ProjectPipelineV4`" and instructs the
implementer to "keep each deprecated alias's existing return hint unchanged
(`... coerce_pipeline_id_or_code_to_project_pipeline_obj -> ProjectPipelineV4`)". The actual source
is:

```python
# project_pipelines_functions.py:350
def coerce_pipeline_id_or_code_to_project_pipeline_obj(pipeline_id_or_code: str) -> PipelineType:
```

and in that module `PipelineType = Union[ProjectPipeline, ProjectPipelineV4]` (line 101) — a module
alias distinct from the tenant module's `Union[PipelineV3, PipelineV4]`. So the current hint is
`Union[ProjectPipeline, ProjectPipelineV4]`, **not** `ProjectPipelineV4`. An implementer told to
"keep the existing `ProjectPipelineV4` hint" will either (a) believe no change is needed when the
hint is actually the broader `PipelineType`, or (b) silently narrow the deprecated alias's hint
during the deprecation window — the exact typing-contract change the design says it wants to defer.

**Fix:** Correct the design text to reflect reality and state the intent explicitly, e.g.:

> `coerce_pipeline_id_or_code_to_project_pipeline_obj` currently returns the module alias
> `PipelineType` (= `Union[ProjectPipeline, ProjectPipelineV4]`). Keep this hint unchanged for the
> deprecation window. The **new** `coerce_pipeline_id_or_name_to_project_pipeline_obj` declares the
> precise `-> ProjectPipelineV4`.

While here, also note (for the implementer) that `get_project_pipeline_obj_from_pipeline_code`
currently returns `-> ProjectPipeline` (line 163), and the new `_from_pipeline_name` resolver is
specified to return `-> ProjectPipelineV4`; that is a deliberate, acceptable change for the new
function, but the deprecated shim's `-> ProjectPipeline` hint should likewise be left untouched.

### 2. MEDIUM — Create-function docstring examples printing `Code:` are not in the inventory and the docs plan does not clearly cover them

**Where:** Inventory item 11, Decision 5, and "Docs updates required".

**Problem:** All five create functions return `ProjectPipelineV4` and their docstring
`.. code-block:: python` examples end with
`print(f"Pipeline ID: {pipeline.pipeline.id}, Code: {pipeline.pipeline.code}")` at:

- `create_cwl_project_pipeline` — line ~1989
- `create_cwl_workflow_from_zip` — line ~2158
- `create_nextflow_pipeline_from_zip` — line ~2256
- `create_nextflow_pipeline_from_nf_core_zip` — line ~2361
- `create_nextflow_project_pipeline` — line ~2526

These are pipeline-identifier `.code` dereferences inside the exact functions being migrated, yet
the inventory (item 11) lists only the *signature/parameter* change for these functions and does
not enumerate their example lines. The Docs section commits to updating examples on "name/version
-returning functions" and the new functions, which *could* be read to include these — but it is
ambiguous, and the inventory's "the only pipeline-`.code` sites remaining after this change are the
deprecated `*_code` shims" closing statement is then factually wrong, because these five example
lines would still print `.code` if not updated.

Additionally, these example docstrings still show `pipeline_code=` as the call argument; after
Decision 5 the preferred call is `pipeline_name=` (+ optional `pipeline_version=`), so the examples
also need their call arguments migrated, not just the print line.

**Fix:** Add an explicit inventory sub-item under item 11 (or a new item) listing the five create
functions' docstring examples, and state in the Docs section that each of these examples must (a)
switch the call to `pipeline_name=`/`pipeline_version=` and (b) print `Name`/`Version` instead of
`Code`. Then the "only `*_code` shims remain" closing statement becomes true.

### 3. NIT — `PipelineV4.name` MinLen(1) is real but nested; make the justification precise

**Where:** Decision 1a / Input validation (Finding R2-5 rationale), which says `PipelineV4.name`
"carries `MinLen(1)`".

**Problem:** `PipelineV4.model_fields['name'].metadata` is empty (`[]`); the `MinLen(1)` lives on the
*inner* `Annotated[str, ...]` inside `Optional[Annotated[str, Field(min_length=1, strict=True,
max_length=255)]]`. The claim is correct (verified via the field annotation and the model source),
but a reader doing a shallow `.metadata` check would wrongly conclude the constraint is absent and
distrust the design. The empty-name `ValueError` is a sound choice regardless of the constraint.

**Fix:** Reword to "the inner constraint on `PipelineV4.name` is `min_length=1` (nested inside its
`Optional[Annotated[str, ...]]`), so a stored name is never empty" — or simply drop the libica-
constraint justification and keep the fail-fast rationale, which stands on its own.

### 4. NIT — Tenant-scoped O(N) resolver cost claim should name the concrete call

**Where:** Decision 1b (Approach B), "O(N) network round-trips".

**Problem:** The design is correct that there is no v4 tenant *list* endpoint (verified:
`PipelineApi.get_pipelines()` returns `PipelineList` with `PipelineV3` items; only
`get_pipeline(id)` returns `PipelineV4`), and the eager-fetch-all rationale for preserving
"latest" semantics is sound. The cost note is slightly understated: each loop iteration calls
`get_pipeline_obj_from_pipeline_id`, which itself opens a fresh `ApiClient`/`PipelineApi` and sets
the v4 Accept header per call. For large tenants this is N client constructions plus N HTTP
round-trips.

**Fix:** No functional change required. Optionally note in Decision 1b that the per-id helper
constructs a client per call, so the "prefer the project-scoped helpers" guidance in the docstring
is worth stating prominently; or (follow-up, out of scope) consider reusing one `ApiClient` across
the loop.

---

## Verified Assumptions (checked against source / libica 3.3)

- **`PipelineV4` field shape.** `id` required; `code` required `str`; `name`, `version`,
  `pipeline_group`, `latest_version_in_group` all **Optional** (default `None`).
  `latest_version_in_group` is `Optional[Annotated[bool, Strict]]`, so `None` and `False` are both
  real states — the design's `is True` guard is justified. VERIFIED.
- **`PipelineV3` is code-only** (no `name`/`version`). VERIFIED.
- **`PipelineGroupIdentifier` exposes only `id`** (required). VERIFIED. The design's `getattr`
  guard on the Optional *container* `pipeline_group` (Finding R2-3) is appropriate.
- **`PipelineV4.name` has `min_length=1`** (nested in the inner Annotated). VERIFIED (see NIT 3).
  libica's own source documents `code` as "deprecated, use the name & version attributes instead",
  corroborating the migration intent.
- **Tenant list endpoint.** `PipelineApi.get_pipelines() -> PipelineList`, `items: List[PipelineV3]`.
  No v4 list variant. VERIFIED — supports Decision 1b and the "keep v3 Accept header" coupling
  (Finding R2-6).
- **Project list endpoint.** `ProjectPipelineApi.get_project_pipelines() -> ProjectPipelineListV4`
  (items `ProjectPipelineV4`, each `.pipeline` a `PipelineV4`). VERIFIED — the runtime-v4 claim for
  `list_project_pipelines` holds despite the stale `List[ProjectPipeline]` hint.
- **`list_project_pipelines` does NOT force a v4 Accept header.** VERIFIED (source constructs
  `ProjectPipelineApi(api_client)` with no `set_default_header`), and its return hint is
  `List[ProjectPipeline]`. The Finding 5 hardening is warranted and consistent with
  `get_project_pipeline_obj` / `get_pipeline_obj_from_pipeline_id`, which DO set the v4 header.
- **`list_all_pipelines` forces the v3 Accept header.** VERIFIED — Finding R2-6's "do not upgrade
  this one" coupling is grounded.
- **Create API signatures.** `ProjectPipelineApi.create_cwl_pipeline` and
  `create_nextflow_pipeline` both accept `code=None`, `name=None`, `version=None` (all optional).
  VERIFIED — Decision 5 / Option A invents no fields.
- **Create-function current signatures.** All five take `pipeline_code: str` as the second
  positional param and pass `code=pipeline_code` to libica. VERIFIED.
  `create_nextflow_pipeline_from_nf_core_zip` additionally uses `pipeline_code` for zip arcnames,
  the new-zip filename, and forwards it to `create_nextflow_pipeline_from_zip`. VERIFIED (Decision 5
  accurately describes this).
- **Launch path.** `ICAv2CWLPipelineAnalysis.create_analysis()` passes
  `pipelineId=self.pipeline_id` (no `str()`); the Nextflow class passes
  `pipelineId=str(self.pipeline_id)`. VERIFIED — the asymmetry (Finding 6) is real and pre-existing.
- **`CreateCwlWithJsonInputAnalysis.pipeline_id` / `CreateNextflowWithCustomInputAnalysis.pipeline_id`**
  are both required plain `str`. VERIFIED — the `_id_from_name` helpers returning `str(...)` means
  name/version flows into `pipeline_id` safely, confirming Decision 4 needs no launch-class change.
- **Inventory call sites.** `get_project_pipeline_obj_from_pipeline_code` filters
  `project_pipeline_iter.pipeline.code` and raises `ValueError` on `StopIteration` (line ~193–200);
  `get_pipeline_obj_from_pipeline_code` filters `pipeline_iter.code` and re-raises `StopIteration`;
  `download_pipeline_to_zip` uses `pipeline_code = pipeline_obj.code` only for the temp-dir name
  (line ~431); `coerce_pipeline_id_or_code_to_project_pipeline_obj` resolves the project via
  `get_project_id()`. ALL VERIFIED.
- **Out-of-scope `.code` sites.** `AnalysisInput.code`/`AnalysisOutput.code` in
  `project_analysis_functions.py`; `region.code` in `bundle_functions.py` (line ~345); config/input
  `param.code` (lines ~842/894). VERIFIED as non-pipeline-identifier and correctly excluded.
- **Bundle docstring (item 12).** `list_pipelines_in_bundle` example prints
  `Code: {pipeline.pipeline.code}` at line ~762. VERIFIED as a pipeline-identifier docstring-only
  fix; the region.code at line ~345 is correctly left alone.
- **`__init__.py` re-exports.** Both pipeline modules re-export the `*_code` names in the import
  block and `__all__`. VERIFIED.
- **`.rst` member lists.** `docs/wrapica/pipelines.rst` and `project_pipelines.rst` use explicit
  `:members:` lists (not autosummary), so new names must be added by hand. VERIFIED (item 14).
- **Test anchors.** `TestCoercePipelineIdOrCodeToPipelineObj`,
  `TestGetProjectPipelineObjFromPipelineCode`, `TestGetPipelineObjFromPipelineCodeStopIteration`,
  and `create_cwl_workflow_from_zip` tests passing `pipeline_code=` all exist in the suite.
  VERIFIED — the test-update plan is grounded.
- **`is_uuid_format`** exists in `utils.miscell` and is the branch discriminator. VERIFIED.

## Unverified / Wrong Assumptions

- **WRONG:** "`coerce_pipeline_id_or_code_to_project_pipeline_obj` ... declares `ProjectPipelineV4`"
  / "keep its existing `-> ProjectPipelineV4` return hint." The actual current hint is `PipelineType`
  (= `Union[ProjectPipeline, ProjectPipelineV4]`). See Finding 1 (MEDIUM).
- **IMPRECISE:** "`PipelineV4.name` ... carries `MinLen(1)`" — true but the constraint is on the
  inner `Annotated`, not on `model_fields['name'].metadata` (which is empty). See NIT 3.
- **UNVERIFIED (runtime/live only, acknowledged by the design):** that omitting the forced v4
  Accept header on `list_project_pipelines` can actually degrade the response to `PipelineV3` under
  some libica content-negotiation default. The static types confirm the endpoint *maps* to
  `ProjectPipelineListV4`; whether a header-less call ever returns a v3-shaped body is a live-server
  behaviour not checkable here. The hardening is defensively correct regardless, so this does not
  block.
- **UNVERIFIED (live only, acknowledged):** ICAv2 server-side group-assignment/version-bump
  semantics when creating `name=X, version=N`. The design explicitly passes these through and
  backlogs group management; no static check is possible or required.
