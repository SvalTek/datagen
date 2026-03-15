# Branching Workflows (DAG Deep Dive)

This guide explains Datagen branching workflows in practical terms:

- how stage dependencies work
- how data flows through branches
- what conditional execution does
- what branching supports today (and what it does not)

If you are new to workflows in general, read
[Workflow Patterns](./workflow-patterns.md) first. Use
[Workflow Reference](./workflow-reference.md) for full schema details.

## Why Branching Exists

Linear pipelines are great until one stage needs to feed multiple downstream
paths.

Branching lets you:

- fan out from one seed stage into multiple enrichment stages
- run optional stages conditionally
- merge multiple branch outputs into a later stage

Think of this as a DAG (directed acyclic graph), not a strict list.

---

## Core Concepts

### `id`

Stable stage key used by dependencies and reports.

```yaml
- id: seed
  instructions: Generate base records.
```

### `dependsOn`

Explicit edges in the stage graph.

```yaml
- id: enrich_a
  dependsOn: [seed]
  instructions: Add feature group A.
```

### `when`

Simple conditional gate for stage execution.

```yaml
when:
  path: outputsByStage.seed.isEligible
  equals: true
```

Supported operators:

- `equals`
- `notEquals`
- `any`
- `notAny`

Exactly one must be set.

> **Important:** `when` does not invent values.\
> Your path must point to data that already exists in `initialContext` or a
> prior stage output.

### Stage Status

Each stage ends up as one of:

- `executed`
- `skipped` (condition false)
- `blocked` (a dependency was skipped/blocked)

---

## Execution Model

Datagen runs branching workflows in this order:

1. Build dependency graph from stage ids + `dependsOn`
2. Validate graph (unknown dependency keys, self-dependency, cycles)
3. Topologically order stages
4. For each stage:
   - if dependency skipped/blocked -> mark as `blocked`
   - else evaluate `when` (if present)
   - run stage if eligible

Important:

- Stage-level execution is deterministic.
- Stage-level execution is not currently parallel.
- Item-level parallelism is available inside `iter`/`record_transform` via
  `parallelism`.

---

## Dataflow Semantics

### What `when.path` can read

`when` is evaluated against:

- `initialContext`
- `outputsByStage`

Think of `when.path` as reading from this object:

```json
{
  "initialContext": { "...": "..." },
  "outputsByStage": {
    "seed": { "...": "..." }
  }
}
```

Example:

```yaml
when:
  path: outputsByStage.seed.runRewrite
  equals: true
```

If the path is missing, the condition evaluates false.

### Where condition fields come from (critical)

Condition fields like `outputsByStage.seed.shouldRunAudit` come from an upstream
stage output.

That means your upstream stage must produce that field explicitly.

Example:

```yaml
stages:
  - id: seed
    instructions: >
      Return one JSON object with fields:
      - shouldRunAudit (boolean)
      - records (array)
      Output valid JSON only.
    constrain:
      type: object
      shape:
        shouldRunAudit:
          type: boolean
        records:
          type: array
          items:
            type: object
            shape:
              id:
                type: string

  - id: audit
    dependsOn: [seed]
    when:
      path: outputsByStage.seed.shouldRunAudit
      equals: true
    instructions: Run expensive audit expansion.
```

Why this matters:

- without `shouldRunAudit` in seed output, path lookup fails
- failed lookup means condition evaluates false
- the stage will be skipped

## What downstream stages can see

Stages receive prior stage outputs as context. In branching workflows, this
allows merge stages to reference multiple upstream outputs.

## Important mode-specific detail

For `iter` and `record_transform` stages using `previous_stage` semantics:

- when a stage has multiple dependencies, Datagen currently uses the **last
  dependency key** as the direct array source for that mode.

Practical implication:

- for fan-in with `iter`/`record_transform`, create an explicit merge/prepare
  batch stage first, then iterate/transform from that merged output.

---

## Canonical Patterns

## 1) Fan-Out -> Fan-In

```yaml
stages:
  - id: seed
    instructions: Generate base object.

  - id: enrich_metadata
    dependsOn: [seed]
    instructions: Add metadata signals.

  - id: enrich_scoring
    dependsOn: [seed]
    instructions: Add scoring signals.

  - id: merge
    dependsOn: [enrich_metadata, enrich_scoring]
    instructions: Merge metadata and scoring into one output.
```

When to use:

- two or more independent enrichments from the same base stage.

## 2) Optional Expensive Branch

```yaml
stages:
  - id: seed
    instructions: >
      Return one JSON object with:
      - shouldRunAudit (boolean)
      - records (array)
      Output valid JSON only.
    constrain:
      type: object
      shape:
        shouldRunAudit:
          type: boolean
        records:
          type: array
          items:
            type: object
            shape:
              id:
                type: string

  - id: audit
    dependsOn: [seed]
    when:
      path: outputsByStage.seed.shouldRunAudit
      equals: true
    instructions: Run expensive audit expansion.

  - id: finalize
    dependsOn: [seed]
    instructions: Finalize core output.
```

When to use:

- branch should only run for specific scenarios.

Implementation note:

- `shouldRunAudit` is produced by `seed` and then consumed by `audit.when`.

## 3) Branch + Merge + Iter

```yaml
stages:
  - id: seed
    instructions: Generate raw records array.

  - id: annotate_a
    dependsOn: [seed]
    instructions: Add annotation A.

  - id: annotate_b
    dependsOn: [seed]
    instructions: Add annotation B.

  - id: prepare_for_iter
    dependsOn: [annotate_a, annotate_b]
    instructions: Build one merged records array.

  - id: normalize_item
    mode: iter
    dependsOn: [prepare_for_iter]
    instructions: Normalize one record item.
```

When to use:

- you need fan-in but later stages are item-wise.

## 4) Delegate a Branch to a Child Workflow

```yaml
stages:
  - id: seed
    instructions: Return candidate object.

  - id: judge_with_child
    mode: workflow_delegate
    dependsOn: [seed]
    instructions: Delegate evaluation to child workflow.
    delegate:
      workflowPath: ./workflows/judge.pipeline.yaml
      inputFromPath: outputsByStage.seed.candidate
      inputAs: initial_context
      outputFrom: final_stage_output
      onFailure: fail
      inheritParentCli: none

  - id: finalize
    dependsOn: [judge_with_child]
    when:
      path: outputsByStage.judge_with_child.decision.route
      equals: accept
    instructions: Finalize only accepted candidates.
```

When to use:

- second-opinion / arbitration branches
- different model/provider/backend for just one branch
- reusable complex subflows without duplicating stage blocks

Notes:

- child workflow config is respected by default (`inheritParentCli: none`)
- `onFailure: warn` emits `delegated_workflow_failed` and returns `null` output
- delegation supports nesting up to depth `3` and blocks cycles
- delegated child runs currently use eager execution path (no child
  streaming/resume in v1)

## 5) Compute Branch Conditions with Lua

```yaml
stages:
  - id: seed
    instructions: Return one JSON object with records.

  - id: compute_flags
    mode: lua
    dependsOn: [seed]
    instructions: Compute deterministic routing flags.
    lua:
      source: inline
      code: |
        local ctx = ...
        local records = (ctx.stageInput and ctx.stageInput.records) or {}
        return {
          shouldRunAudit = #records >= 100
        }

  - id: audit
    dependsOn: [compute_flags]
    when:
      path: outputsByStage.compute_flags.shouldRunAudit
      equals: true
    instructions: Run expensive audit branch.
```

When to use:

- branch gating is deterministic and policy-driven
- you want route logic versioned in workflow code, not prompt prose

---

## What Branching Supports Today

- Explicit DAG dependencies via `dependsOn`
- Conditional stage execution via `when` (`equals` / `notEquals`)
- Stage status reporting (`executed`, `skipped`, `blocked`)
- Fan-out/fan-in structures
- Mixed mode graphs (`batch`, `iter`, `record_transform`)
- Delegated branches via `workflow_delegate`

## What Branching Does Not Support Today

- Arbitrary boolean expressions in `when` (`and`/`or` trees, ranges, regex)
- Explicit `else` branches
- Stage-level parallel DAG execution
- Automatic join semantics for multi-dependency array inputs in
  `iter`/`record_transform`
- Cross-stage rollback/transactions

---

## Authoring Checklist for Branching Workflows

1. Give every branchable stage a stable `id`.
2. Define dependencies explicitly with `dependsOn`.
3. Keep branch outputs shape-compatible where you intend to merge.
4. If you need item-wise fan-in, add an explicit merge/prepare stage first.
5. Keep `when` conditions simple and path-based.
6. Add validation at merge points to catch schema drift.

---

## Debugging Branching Runs

When behavior looks wrong:

1. Run with `--console warnings`.
2. Open run report JSON.
3. Check:
   - `dependencyGraph`
   - `stageStatuses`
   - `failedStage`
   - stage traces and validation issues
4. Confirm dependency keys match stage ids exactly.
5. Confirm conditional paths under `outputsByStage.*` are valid.
