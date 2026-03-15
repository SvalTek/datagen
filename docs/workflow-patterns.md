# Workflow Patterns (Getting Started)

This guide is for people who already understand datasets/training, but are new
to **Datagen workflows**.

The goal is to help you read and write workflows confidently, and understand why
each block exists.

For full schema detail, see [Workflow Reference](./workflow-reference.md). For
CLI flags, see [CLI Reference](./cli-reference.md). For advanced graph behavior,
see [Branching Workflows](./branching_workflows.md).

## How To Read A Datagen Workflow

When you open a workflow file, read it in this order:

1. Runtime block (`model`, `provider`, `endpoint`, `structuredOutputMode`, `repeat`)
2. Input block (`input`) if present
3. Stage list (`stages`) and each stage `mode`
4. Quality guards (`constrain`, `validate`, `retry`)
5. Execution controls (`dependsOn`, `when`, `parallelism`)

> **Why this order?** It mirrors Datagen execution order.

---

## Pattern 1: Minimal Synthetic Workflow

Use this when you want to generate data from scratch.

```yaml
version: 1
name: product-catalog-seed
model: your-model-name
provider: openai
endpoint: https://your-endpoint.example/v1/
outputDir: ./output

stages:
  - id: seed
    instructions: >
      Generate 20 product records as a JSON array.
      Each record should include sku, category, and price.
      Return valid JSON only.
```

What this teaches:

- A workflow can be one stage.
- `batch` mode is implied when `mode` is omitted.
- Datagen expects valid JSON stage output.

> **Callout: `id` vs `name`** `id` is the stable stage key for
> dependencies/reporting. You can keep `name` for readability if you want, but
> `id` is the reliable reference key.

---

## Pattern 2: Seed Then Normalize (`batch` -> `iter`)

Use this when stage 1 creates an array, and stage 2 should process each item
individually.

```yaml
stages:
  - id: seed
    instructions: >
      Generate 50 support ticket objects as a JSON array.
      Include ticketId, priority, and summary.
      Return valid JSON only.

  - id: normalize_ticket
    mode: iter
    parallelism: 4
    instructions: >
      Normalize the current item.
      Return one JSON object only.
```

What this teaches:

- `iter` consumes array output from upstream stage.
- Each iter call should return **one object**.
- `parallelism` speeds up per-item stages.

> **Callout: `parallelism`** Start small (`2-4`) and increase carefully. Higher
> values can hit backend/model throughput limits.

## `iter` Mode Explained Properly

### What it is

`iter` is a loop stage. Datagen runs one model call per item from an input
array.

### What it receives

For each item, Datagen gives the stage:

- the current item (`currentIterItem`)
- initial context (if provided)
- relevant prior stage outputs

This means your instructions should be phrased for **one item at a time**, not
the whole dataset.

### What it must return

Each iter call must return:

- exactly **one JSON object**

Not allowed per iter call:

- array output
- primitive output (`"text"`, `123`, `true`)

### Minimal iter example

```yaml
stages:
  - id: seed
    instructions: >
      Generate an array of customer objects with id and note.
      Output valid JSON only.

  - id: clean_note
    mode: iter
    instructions: >
      Clean the current item's note text.
      Return one JSON object with the same id and cleaned note.
```

### How output flows to the next stage

- Datagen keeps item order deterministic in final output.
- The stage output becomes a JSON array of per-item results.
- That array is available to downstream stages.

### When to use it

Use `iter` when:

- each record can be processed independently
- failures should be isolated to one item
- you want per-item retries/validation behavior

Avoid `iter` when:

- the stage needs global reasoning across all items at once
- output is naturally one aggregated object

In those cases, use `batch`.

### Common mistakes

1. Writing instructions for the whole dataset instead of current item.
2. Returning arrays from the iter stage.
3. Forgetting schema guards when downstream stages expect stable fields.

### Reliability template

```yaml
- id: normalize_record
  mode: iter
  retry:
    enabled: true
    maxAttempts: 2
  validate:
    rules:
      - path: id
        kind: contains
        value: "cust-"
      - path: note
        kind: min_length
        value: 10
  instructions: >
    Normalize the current record.
    Return one JSON object only.
```

Use this as a default starting point for important iter stages.

---

## Pattern 3: Add Structural Guarantees (`constrain`)

Use this when downstream steps require strict shape/type guarantees.

```yaml
stages:
  - id: generate
    instructions: >
      Return one object containing exactly 10 records.
      Output valid JSON only.
    constrain:
      type: object
      shape:
        records:
          type: array
          minItems: 10
          maxItems: 10
          items:
            type: object
            shape:
              id:
                type: string
              score:
                type: number
                min: 0
                max: 1
              label:
                type: enum
                values: ["pass", "review", "fail"]
```

What this teaches:

- `constrain` is for structure/types/ranges.
- It is stronger than prompt wording alone.
- top-level `structuredOutputMode` decides how Datagen obtains constrained
  output from the backend:
  - `object`: provider/schema-based structured generation
  - `json`: backend JSON-object mode + local validation
  - `json-array`: backend JSON-object mode + strip one outer array wrapper + local validation
  - `off`: prompt/text generation + local validation

> **Callout: Use `constrain` early** If a stage output has a known schema, add
> `constrain` sooner rather than later.

---

## Pattern 4: Add Semantic Quality Rules (`validate` + `retry`)

Use this when valid structure is not enough.

```yaml
stages:
  - id: rewrite
    mode: iter
    instructions: >
      Rewrite the current item summary in a more concise style.
      Return one JSON object only.
    validate:
      rules:
        - path: summary
          kind: min_length
          value: 20
          hint: Keep enough detail to be useful.
        - path: summary
          kind: must_change_from_path
          otherPath: originalSummary
          hint: Avoid returning the original wording unchanged.
    retry:
      enabled: true
      maxAttempts: 2
```

What this teaches:

- `validate` checks meaning/quality constraints.
- `retry` gives the model another chance with feedback.

> **Callout: `constrain` and `validate` are different** `constrain` = shape/type
> correctness. `validate` = semantic/content correctness.

---

## Pattern 5: Conversation Rewrite (`record_transform`)

Use this when each input record contains a conversation and you want turn-level
rewriting.

```yaml
input:
  path: ./data/chat-records.jsonl
  format: jsonl

stages:
  - id: rewrite_assistant_turns
    mode: record_transform
    input:
      source: pipeline_input
    instructions: >
      Rewrite only assistant turns to be shorter and clearer.
      Return only the rewritten target turn text.
      Do not return JSON.
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: role
      contentField: text
      targetRoles:
        - assistant
```

What this teaches:

- `record_transform` works record-by-record.
- `conversation_rewrite` targets selected roles in a turn array.
- Rewrite output contract is raw text (not JSON wrapper).

> **Callout: Most common failure in rewrite workflows** Prompt asks for JSON
> while runtime expects raw rewritten turn text.

## `record_transform` Mode Explained Properly

### What it is

`record_transform` is a record-by-record transform stage. Datagen takes one
input record, transforms it, and emits one output record.

### What it receives

Per record, Datagen passes:

- the current input record
- stage + transform configuration
- model context needed for that record

Unlike `iter`, this mode is designed for structured record rewrites (especially
conversations).

### What it must return

- exactly one output record per input record
- output record order is deterministic

For `conversation_rewrite` specifically:

- Datagen rewrites target turns inside the record’s conversation array
- rewrite model output contract is raw rewritten turn text (not JSON wrapper)

### How output flows to the next stage

- Datagen assembles transformed records into one output array.
- That array is available to downstream stages.
- In large streaming runs, records can be written incrementally to output JSONL.

### Core transform fields (conversation rewrite)

```yaml
transform:
  kind: conversation_rewrite
  conversationsPath: conversations
  roleField: role
  contentField: text
  targetRoles:
    - assistant
  includeOriginalTargetTurn: true
  turnPreprocess:
    source: inline
    code: |
      local ctx = ...
      local turn = Datagen.clone(ctx.turn)
      local text = Datagen.get(turn, "text", "")
      turn.length_class = (#text <= 500) and "short" or "long"
      return turn
  turnWhen:
    path: length_class
    equals: short
```

What each one does:

- `conversationsPath`: where turn array lives in each record
- `roleField`: field that identifies speaker role
- `contentField`: field containing turn text
- `targetRoles`: only these turns are rewritten
- `includeOriginalTargetTurn`: include current original target turn in prompt
  context
- `turnPreprocess`: optional Lua hook that can annotate/mutate each target turn
  before rewrite
- `turnWhen`: optional gate evaluated against the preprocessed target turn so
  only matching turns are rewritten

`turnWhen` supports:

- `equals`
- `notEquals`
- `any`
- `notAny`

Example use:

- classify each assistant turn as `short` or `long`
- add that field directly onto the turn object
- rewrite only the `short` ones while preserving the full conversation record

### Input source behavior

For `record_transform`:

- first stage default input source is `pipeline_input`
- later stages default input source is `previous_stage`

You can set it explicitly with:

```yaml
input:
  source: pipeline_input
```

### Validation behavior in `record_transform`

There are two useful validation layers:

1. Turn-level checks during rewrite attempts (for `content` style rules)
2. Record-level checks on final transformed record

Important runtime behavior:

- turn-level rewrite failures can preserve original turn and emit warnings
- stage-level fail behavior still depends on `validate.onFailure`

### When to use it

Use `record_transform` when:

- each input is a structured record
- you need targeted field/turn rewriting inside each record
- record-level output shape must be preserved across the run

Avoid `record_transform` when:

- you are generating records from scratch
- you need single aggregated output instead of per-record transforms

In those cases, use `batch` or `iter`.

### Common mistakes

1. Wrong `conversationsPath` or wrong role/content field names.
2. Asking for JSON output in rewrite instructions.
3. Using transform mode on records that were never remapped into turn objects.
4. Forgetting `input.source` in multi-stage transform pipelines and accidentally
   reading wrong source.

### Reliability template

```yaml
stages:
  - id: rewrite_assistant_turns
    mode: record_transform
    input:
      source: pipeline_input
    parallelism: 4
    instructions: >
      Rewrite assistant turns for clarity.
      Return only the full rewritten target turn text.
      Do not return JSON.
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: role
      contentField: text
      targetRoles:
        - assistant
    validate:
      rules:
        - path: content
          kind: min_length
          value: 20
          hint: Keep enough detail in rewritten turn.
        - path: content
          kind: must_change_from_ref
          ref: original_target_content
          hint: Avoid no-op copy-through rewrites.
    retry:
      enabled: true
      maxAttempts: 2
```

### Performance notes

For large JSONL datasets:

- set `input.readMode: stream`
- run with `--checkpoint-every <n>`
- resume with `--resume <checkpoint-path>`

This combination is the safest baseline for long-running rewrite pipelines.

---

## Pattern 6: Input Remapping Before Transform

Use remap when the source format does not already match turn objects.

### Example A: Prefixed string arrays

```yaml
input:
  path: ./data/source.json
  format: json
  remap:
    kind: prefixed_string_array
    sourcePath: conversation
    prefixes:
      user: "user:"
      assistant: "assistant:"
```

### Example B: Alpaca-style fields

```yaml
input:
  path: ./data/source.jsonl
  format: jsonl
  remap:
    kind: alpaca
```

What this teaches:

- remap runs before stages.
- remap preserves original fields and adds normalized conversation fields.

---

## Pattern 7: Branching (DAG) With Conditions

Use this when stages are not purely linear.

```yaml
stages:
  - id: seed
    instructions: Generate a summary object.

  - id: enrich_tags
    dependsOn: [seed]
    instructions: Add tag-level enrichments.

  - id: enrich_scores
    dependsOn: [seed]
    instructions: Add scoring enrichments.

  - id: merge
    dependsOn: [enrich_tags, enrich_scores]
    when:
      path: outputsByStage.seed.isEligible
      equals: true
    instructions: Merge enrichments into final object.
```

What this teaches:

- `dependsOn` defines stage graph edges.
- `when` conditionally skips a stage.
- Downstream dependency behavior is visible in run traces/report.

> **Where does `when.path` data come from?** From `initialContext` or previous
> stage outputs (`outputsByStage`). If you reference
> `outputsByStage.seed.isEligible`, your `seed` stage must produce `isEligible`.

Example of producing a condition flag explicitly:

```yaml
stages:
  - id: seed
    instructions: >
      Return one JSON object with:
      - isEligible (boolean)
      - records (array)
      Output valid JSON only.
    constrain:
      type: object
      shape:
        isEligible:
          type: boolean
        records:
          type: array
          items:
            type: object
            shape:
              id:
                type: string

  - id: optional_enrich
    dependsOn: [seed]
    when:
      path: outputsByStage.seed.isEligible
      equals: true
    instructions: Run optional enrichments.
```

---

## Pattern 8: Large JSONL Runs (Streaming + Resume)

Use this for long-running `record_transform` jobs.

```yaml
input:
  path: ./data/large.jsonl
  format: jsonl
  readMode: stream
```

Run with checkpoints:

```bash
deno run -A main.ts workflow.yaml --checkpoint-every 500
```

Resume later:

```bash
deno run -A main.ts workflow.yaml --resume ./output/workflow.checkpoint.json
```

What this teaches:

- `readMode: stream` avoids eager full-file loading for JSONL.
- checkpoint/resume helps recover from interrupted long runs.

---

## Pattern 9: Deterministic Lua Stage

Use `mode: lua` when you need deterministic scripted logic between model stages.

```yaml
stages:
  - id: seed
    instructions: Return one JSON object with records.

  - id: compute_flags
    mode: lua
    dependsOn: [seed]
    instructions: Compute deterministic flags from seed output.
    lua:
      source: inline
      code: |
        local ctx = ...
        local seed = ctx.stageInput or {}
        local records = seed.records or {}
        return {
          recordCount = #records,
          shouldRunAudit = #records >= 100
        }
```

What this teaches:

- Lua stages run once and return one stage output.
- Lua stages can consume `previous_stage` outputs via `ctx.stageInput`.
- Lua output still supports `constrain` and `validate`.

Use Lua for deterministic transforms and branch gating, not generation.

---

## Practical Build Flow

Use this progression when authoring a new workflow:

1. Build one-stage minimal workflow.
2. Confirm output shape manually.
3. Add more stages/modes (`iter` or `record_transform`).
4. Add `constrain` where schema is known.
5. Add `validate` + `retry` for semantic quality.
6. Add `parallelism` for speed.
7. Add `dependsOn`/`when` only when branching is truly needed.
8. For big JSONL jobs, switch to `readMode: stream` and checkpointing.

---

## Debugging Checklist

If a run behaves unexpectedly:

1. Run with `--console warnings`.
2. Open the generated report (`*.report.json`).
3. Check stage id/mode, stage status, and validation issues.
4. Fix one thing at a time (prompt contract, then validator rule, then retry
   behavior).
