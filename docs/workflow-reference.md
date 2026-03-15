# Workflow Reference

This document describes the YAML workflow format Datagen currently supports.

Use it as the schema-level reference. For end-to-end examples, see
[Workflow Patterns](./workflow-patterns.md). For advanced DAG behavior, see
[Branching Workflows](./branching_workflows.md). For CLI flags and terminal
behavior, see [CLI Reference](./cli-reference.md).

## Full Shape

```yaml
version: 1
name: example-workflow
description: Optional description

model: qwen2.5:14b
endpoint: http://localhost:11434/
provider: openai
structuredOutputMode: off
reasoningMode: off
apiKeyEnv: OPENROUTER_API_KEY
httpReferer: https://example.com/datagen
xTitle: Datagen
maxTokens: 4000
temperature: 1.0
outputDir: ./output
repeat: 3

input:
  path: ./data/source.jsonl
  format: jsonl
  readMode: eager
  offset: 0
  limit: 100
  remap:
    kind: alpaca

stages:
  - name: rewrite_assistant_turns
    id: rewrite_assistant_turns
    description: Optional description
    dependsOn:
      - seed_stage
    when:
      path: outputsByStage.seed_stage.enabled
      equals: true
    parallelism: 4
    instructions: Rewrite the current target.
    system: Optional system prompt
    rules:
      include:
        - Output valid JSON
      exclude:
        - Commentary
    history: Optional extra context
    examples:
      - input: hello
        output: world
    reasoning: false
    mode: record_transform
    input:
      source: pipeline_input
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - assistant
      includeOriginalTargetTurn: true
    constrain:
      type: object
      shape:
        content:
          type: string
          minLength: 1
    validate:
      onFailure: fail
      rules:
        - path: content
          kind: contains
          value: "<think>"
          hint: Start with a short think block.
    retry:
      enabled: true
      maxAttempts: 2
    metadata:
      tag: demo
```

## Top-Level Fields

### `version`

Optional string or number.

Used as a document/schema version marker only. Datagen does not currently branch
runtime behavior by version.

### `name`

Optional string.

Used for:

- default dataset output naming
- default report naming
- human-readable run summaries

If omitted, Datagen falls back to the workflow filename.

### `description`

Optional free-text description.

Purely descriptive.

### `model`

Optional default model identifier for the run.

Can be overridden by CLI `--model`.

### `endpoint`

Optional OpenAI-compatible base URL.

Examples:

- `http://localhost:11434/`
- `https://openrouter.ai/api/`

Can be overridden by CLI `--endpoint`.

### `provider`

Optional provider selector.

Supported values:

- `openai`
- `ollama`

Default when omitted:

- `openai`

Resolution order:

1. CLI `--provider`
2. workflow `provider`
3. `DATAGEN_PROVIDER`
4. default `openai`

### `structuredOutputMode`

Optional structured-output strategy for constrained stages.

Supported values:

- `object`
- `json`
- `json-array`
- `off`

Default when omitted:

- `off`

This field is only relevant when a stage has `constrain`.

- `object`
  - use provider/schema-based structured generation
  - fail if the backend cannot satisfy structured object generation cleanly
- `json`
  - request backend JSON-object mode
  - then parse locally and validate against the constrain schema
- `json-array`
  - request backend JSON-object mode
  - strip exactly one top-level array wrapper from the raw JSON output
  - then validate the resulting JSON text against the constrain schema
- `off`
  - do not request backend structured/JSON mode
  - generate text, then parse locally and validate against the constrain schema

### `reasoningMode`

Optional transport-level reasoning protocol selector.

Supported values:

- `off`
- `think`
- `openai`

Default when omitted:

- `off`

With AI SDK providers, reasoning behavior is provider-native. Datagen preserves
the field for compatibility, but does not enforce legacy transport-level payload
shaping on provider-backed requests.

### `apiKeyEnv`

Optional environment-variable name containing the API key for the selected
endpoint.

Auth resolution order:

1. CLI `--api-key`
2. workflow `apiKeyEnv`
3. `DATAGEN_OPENAI_API_KEY`
4. `OPENAI_API_KEY`
5. `OPENROUTER_API_KEY`

### `httpReferer`

Optional `HTTP-Referer` provider-attribution header.

### `xTitle`

Optional `X-Title` provider-attribution header.

Header resolution order:

1. CLI flag
2. workflow value
3. `DATAGEN_HTTP_REFERER` / `DATAGEN_X_TITLE`

### `maxTokens`

Optional default max token budget for model completions in this workflow.

Can be overridden by CLI `--max-tokens`.

### `temperature`

Optional default completion temperature.

Can be overridden by CLI `--temperature`.

### `outputDir`

Optional output directory for the final JSONL dataset and default report file.

Defaults to `./output`.

### `repeat`

Optional positive integer.

Default when omitted:

- `1`

Runs the full non-streaming workflow this many times and appends each final
stage output to the output JSONL.

Notes:

- repeated runs are independent
- repeated runs are currently only supported for non-streaming workflows
- repeated JSONL output preserves the normal line-oriented shape of each repeat's
  final stage output
- when `repeat > 1`, the final report stores per-repeat outputs as arrays under
  `result.outputsByStage`
- the top-level run report also includes `repeatCount` and `completedRepeats`

### `luaRuntime`

Optional workflow-level default Lua runtime options applied to all `mode: lua`
stages.

```yaml
luaRuntime:
  functionTimeoutMs: 2000
  openStandardLibs: true
  injectObjects: true
  enableProxy: true
  traceAllocations: false
```

Supported fields:

- `functionTimeoutMs` non-negative integer
- `openStandardLibs` boolean
- `injectObjects` boolean
- `enableProxy` boolean
- `traceAllocations` boolean

Precedence:

- built-in Lua defaults
- workflow `luaRuntime` defaults
- per-stage `lua.runtime` override (highest precedence)

### `input`

Optional source dataset configuration.

Used by transform workflows. If omitted, the workflow behaves like synthetic
generation or stage-driven generation.

### `stages`

Required array of one or more stage objects.

Stage names, if provided, must be unique.

Stage ids, if provided, must be unique.

## `input`

Use `input` when the workflow starts from an existing dataset instead of
generating records from scratch.

```yaml
input:
  path: ./data/source.jsonl
  format: jsonl
  offset: 100
  limit: 50
```

### `input.path`

Required path to a local dataset file.

### `input.format`

Optional. Supported values:

- `json`
- `jsonl`

If omitted, Datagen infers the format from the file extension.

Current behavior:

- `json` expects a top-level array
- `jsonl` expects one JSON object per non-empty line

### `input.readMode`

Optional dataset read mode.

Supported values:

- `eager` default
- `stream`

Current scope:

- `stream` is only valid for `jsonl` input.
- Streaming execution is currently enabled only when all are true:
  - pipeline has exactly one stage
  - stage `mode` is `record_transform`
  - stage `transform.kind` is `conversation_rewrite`
  - stage input source resolves to `pipeline_input`
- Streaming path can be enabled by either:
  - `input.readMode: stream`
  - CLI `--resume`
  - CLI `--checkpoint-every`
- Outside that shape, Datagen falls back to eager loading/execution.

### `input.offset`

Optional non-negative integer.

Skips records before the workflow sees them.

### `input.limit`

Optional non-negative integer.

Caps the number of records loaded after `offset` is applied.

### `input.remap`

Optional ingestion-time normalization.

Runs:

1. after file load
2. after `offset` / `limit`
3. before any stages execute

Current supported remap kinds:

- `prefixed_string_array`
- `alpaca`

Remap is additive:

- original fields are preserved
- normalized conversation fields are added alongside them

## `input.remap`

### `kind: prefixed_string_array`

Use this when turns are stored as raw strings with embedded role prefixes.

Example source record:

```json
{
  "conversation": [
    "user: hello",
    "assistant: hi"
  ]
}
```

Example config:

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
    outputPath: conversations
    roleField: from
    contentField: value
```

Supported fields:

- `sourcePath` required
- `outputPath` optional, default `conversations`
- `roleField` optional, default `from`
- `contentField` optional, default `value`
- `prefixes.user` required
- `prefixes.assistant` required
- `prefixes.system` optional
- `trimContent` optional, default `true`

Behavior:

- `sourcePath` must resolve to an array
- every array item must be a string
- each string is matched against the configured prefixes after
  leading-whitespace trim
- the first matching prefix wins
- the prefix is stripped
- the remainder becomes the normalized turn content

Output example:

```json
{
  "conversation": [
    "user: Hello",
    "assistant: Hi there"
  ],
  "conversations": [
    { "from": "user", "value": "Hello" },
    { "from": "assistant", "value": "Hi there" }
  ]
}
```

### `kind: alpaca`

Use this when records are shaped like:

```json
{
  "instruction": "...",
  "input": "...",
  "output": "..."
}
```

Example config:

```yaml
input:
  path: ./data/alpaca.jsonl
  format: jsonl
  remap:
    kind: alpaca
```

Supported fields:

- `instructionField` optional, default `instruction`
- `inputField` optional, default `input`
- `outputField` optional, default `output`
- `outputPath` optional, default `conversations`
- `roleField` optional, default `from`
- `contentField` optional, default `value`

Behavior:

- if `input` exists and is non-empty after trim:
  - `instruction` becomes `system`
  - `input` becomes `user`
  - `output` becomes `assistant`
- if `input` is missing or empty after trim:
  - `instruction` becomes `user`
  - `output` becomes `assistant`

## Stage Object

Each stage describes one unit of model work.

```yaml
stages:
  - name: normalize_item
    instructions: Normalize the current record.
    mode: iter
```

### `name`

Optional string.

Used for:

- trace identifiers
- warning messages
- progress display

Must be unique if present.

### `id`

Optional stable stage key.

If present, this is used for dependency edges and output/report stage keys. If
omitted, Datagen falls back to `name`, then positional `stage-N`.

### `description`

Optional free-text description.

### `instructions`

Required string.

This is the main task instruction for the stage.

### `system`

Optional system prompt string.

### `rules`

Optional prompt-only guidance block.

```yaml
rules:
  include:
    - Return valid JSON
  exclude:
    - Commentary
```

This is not a runtime validator. It only affects prompting.

### `history`

Optional extra context string appended to the prompt.

### `examples`

Optional example list:

```yaml
examples:
  - input: hello
    output: world
```

Useful for few-shot shaping.

### `reasoning`

Optional boolean.

Requests reasoning mode for that stage.

With provider-backed execution, reasoning behavior is provider-native.
`reasoningMode` is retained as a compatibility setting, but Datagen does not
guarantee legacy transport-level payload shaping in provider mode.

### `mode`

Supported values:

- `batch`
- `iter`
- `record_transform`
- `workflow_delegate`
- `lua`

Default: `batch`

Execution meaning:

- `batch`
  - one model call for the stage
  - stage works against the whole current context
- `iter`
  - one model call per input item
  - stage expects an input array from previous stage output
- `record_transform`
  - one output record per input record
  - currently specialized for conversation rewriting
- `workflow_delegate`
  - executes a child workflow file from this stage
  - writes selected child output back into this stage output
- `lua`
  - executes deterministic Lua logic once for the stage
  - returns one JSON-serializable stage output value

### `dependsOn`

Optional explicit dependency list by stage key (`id`/`name`/fallback key).

When omitted, Datagen preserves legacy sequential behavior by depending on the
previous stage.

### `when`

Optional conditional execution gate:

```yaml
when:
  path: outputsByStage.seed.enabled
  equals: true
```

Supported operators:

- `equals`
- `notEquals`
- `any`
- `notAny`

Exactly one must be set.

`when.path` is evaluated against this object:

- `initialContext` (from `--context` / `--context-file`)
- `outputsByStage` (executed stage outputs keyed by stage key)

Example valid paths:

- `initialContext.flags.runAudit`
- `outputsByStage.seed.shouldRunAudit`

If a stage is skipped by `when`, downstream stages that depend on it are marked
as blocked.

### `parallelism`

Optional per-stage worker count for:

- `iter`
- `record_transform`

Default: `1`.

Must be an integer `>= 1`.

### `input`

Optional alternate stage input selection:

```yaml
input:
  source: pipeline_input
```

Supported values:

- `pipeline_input`
- `previous_stage`

Current scope:

- `input.source` is currently used by `record_transform` and `lua` stage modes.
- `iter` always reads from dependency output (last dependency key).
- `batch` and `workflow_delegate` do not use `input.source`.

Default behavior for `record_transform`:

- first stage defaults to `pipeline_input`
- later stages default to `previous_stage`

Default behavior for `lua`:

- when dependencies exist, defaults to `previous_stage`
- otherwise defaults to `pipeline_input` when pipeline input exists

### `transform`

Only valid for `record_transform`.

Current supported transform kind:

- `conversation_rewrite`

Example:

```yaml
transform:
  kind: conversation_rewrite
  conversationsPath: conversations
  roleField: from
  contentField: value
  targetRoles:
    - assistant
  includeOriginalTargetTurn: true
  turnPreprocess:
    source: inline
    code: |
      local ctx = ...
      local turn = Datagen.clone(ctx.turn)
      local text = Datagen.get(turn, "value", "")
      turn.length_class = (#text <= 500) and "short" or "long"
      return turn
  turnWhen:
    path: length_class
    equals: short
```

Field meanings:

- `conversationsPath`
  - dot-path to the conversation turns array inside each record
- `roleField`
  - field containing the speaker role
- `contentField`
  - field containing turn text
- `targetRoles`
  - only these roles are rewritten
  - enforced against the current target turn after `turnPreprocess`, if present
- `includeOriginalTargetTurn`
  - whether the current original target turn is included in the prompt
  - default `true`
- `turnPreprocess`
  - optional Lua hook that runs once for each target turn before rewrite
  - must return one JSON object representing the target turn
  - useful for annotating turns with derived fields such as `length_class`
  - Lua context includes `ctx.turn`, `ctx.record`, `ctx.turnIndex`,
    `ctx.priorTurns`, and `ctx.transform`
- `turnWhen`
  - optional per-target-turn gate evaluated after `turnPreprocess`
  - path is resolved against the current target turn object, not
    `{ initialContext, outputsByStage }`
  - supported operators: `equals`, `notEquals`, `any`, `notAny`
  - when false, the target turn is preserved without calling the rewrite model

### `delegate`

Only valid for `workflow_delegate`.

Example:

```yaml
delegate:
  workflowPath: ./workflows/child-judge.pipeline.yaml
  inputFromPath: outputsByStage.seed.candidate
  inputAs: initial_context
  outputFrom: final_stage_output
  outputSelectPath: decision
  onFailure: fail
  inheritParentCli: none
```

Fields:

- `workflowPath` required
  - child workflow YAML path
  - relative paths resolve from the current workflow file directory
- `inputFromPath` required
  - read path from parent `{ initialContext, outputsByStage }`
- `inputAs` optional, default `initial_context`
  - `initial_context`
  - `pipeline_input` (mapped value must be an array)
- `outputFrom` optional, default `final_stage_output`
  - `final_stage_output`
  - `stage_key`
- `outputStageKey` required when `outputFrom: stage_key`
- `outputSelectPath` optional
  - dot-path read inside selected child output before returning it
- `onFailure` optional, default `fail`
  - `fail`
  - `warn` (emits warning and returns `null` stage output)
- `inheritParentCli` optional, default `none`
  - `none`: child workflow config/env resolution only
  - `completion`: inherits parent completion overrides (`max_tokens`,
    `temperature`, `parallelism`)
  - `all`: inherits parent CLI runtime overrides

Delegation safety:

- nested delegation is supported with depth limit `3`
- cycles in delegated workflow path ancestry fail fast

### `lua`

Only valid for `mode: lua`.

Example:

```yaml
lua:
  source: inline
  code: |
    local ctx = ...
    return { ok = true, stage = ctx.stageIdentifier }
  runtime:
    functionTimeoutMs: 1000
    openStandardLibs: false
```

Fields:

- `source` required
  - `inline`
  - `file`
- `code` required when `source: inline`
- `filePath` required when `source: file`
  - relative paths resolve from the current workflow file directory
- `runtime` optional
  - `functionTimeoutMs` non-negative integer
  - `openStandardLibs` boolean
  - `injectObjects` boolean
  - `enableProxy` boolean
  - `traceAllocations` boolean

Lua execution context:

- Lua receives one vararg argument: `local ctx = ...`
- `ctx` includes:
  - `initialContext`
  - `outputsByStage`
  - `stageInput`
  - `stageIdentifier`
  - `stageIndex`

Lua output contract:

- script returns one value
- value must be JSON-serializable
- returned value is written to `outputsByStage[stage-key]`
- `constrain` and `validate` are applied to returned output

Lua bindings:

- Datagen injects `Datagen.emitWarning(kind, message)` for non-fatal warnings
- Datagen also injects utility bindings including `get/has/set/clone`,
  JSON/string helpers, and `textTemplate(...)`
- Datagen injects `LLM.generate(...)` and `LLM.generateObject(...)` bound to the
  current workflow run's model session (also available as `Datagen.LLM`)
- See [Lua Stage Reference](./lua-stage-reference.md) for the full binding
  contract and runtime-global guarantees

### `constrain`

Optional typed schema block.

This is enforced at runtime through Zod-backed conversion.

When `constrain` is present, Datagen switches into constrained-output handling.
The exact strategy depends on top-level `structuredOutputMode`.

Use `constrain` for:

- expected object shape
- primitive field types
- array/object structure
- bounds and refinements such as `min`/`max`, `minLength`/`maxLength`, and regex
  `pattern`

Basic pattern:

```yaml
constrain:
  type: object
  shape:
    records:
      type: array
      minItems: 1
      items:
        type: object
        shape:
          id:
            type: string
            pattern: "^item-[0-9]+$"
          score:
            type: number
            min: 0
            max: 100
          active:
            type: boolean
```

Use `validate` for semantic/content checks.

### `validate`

Optional semantic/content validation block.

```yaml
validate:
  onFailure: fail
  rules:
    - name: think_prefix
      path: content
      kind: contains
      value: "<think>"
      hint: Start with a short think block.
```

`onFailure`:

- `fail` default
- `warn`

Notes:

- `batch` and `iter` validate the stage output
- `record_transform` validates rewritten turns before patching and validates the
  final transformed record after patching
- turn-level failures in `record_transform` usually preserve the original turn
  and emit warnings, but fatal model-call failures can still fail the stage
  (for example auth failures and streaming rewrite path failures)

### `retry`

Optional correction retry block:

```yaml
retry:
  enabled: true
  maxAttempts: 2
```

Current scope:

- `iter`
- `record_transform`

Behavior:

- retries only on prompt-correctable failures such as invalid JSON, constrain
  mismatch, validator mismatch, or empty rewrite output
- `maxAttempts` counts the initial attempt
- when `enabled: true` and `maxAttempts` is omitted, the default is `2`
- retry feedback includes the failure reason and any validator hints

### `metadata`

Optional free-form object.

Copied through as stage metadata. Datagen does not interpret it.

## Validator Reference

Datagen currently supports these validator kinds:

- `contains`
- `not_contains`
- `regex`
- `not_regex`
- `min_length`
- `max_length`
- `equals`
- `not_equals`
- `must_change_from_path`
- `must_change_from_ref`
- `not_equal_to_path`
- `min_similarity_to_path`
- `max_similarity_to_path`
- `not_equal_to_ref`
- `min_similarity_to_ref`
- `max_similarity_to_ref`
- `array_min_length`
- `array_max_length`

Common rule fields:

- `name` optional
- `path` optional
- `hint` optional
- `when` optional
- `scope` optional on comparison-style validators
- `similarity` optional on similarity validators

### `name`

Optional workflow-defined label for a validator rule.

Use this when you want warnings and retry traces to identify a validator more
clearly than its rule kind alone.

Example:

```yaml
- name: semantic_anchor
  path: content
  kind: min_similarity_to_ref
  ref: original_target_content
  threshold: 0.5
  similarity:
    mode: detailed
```

When present, warning output can surface both:

- the validator kind, such as `validator_mismatch.min_similarity_to_ref`
- the validator name, such as `validator=semantic_anchor`

### `path`

Optional dot-path to the value being validated.

If omitted, the rule targets the whole current validation value.

### `hint`

Optional user-provided correction hint.

Hints are included in retry feedback, which makes them useful for stylistic or
format-sensitive rules.

### `when`

Minimal conditional gating:

```yaml
when:
  path: from
  equals: assistant
```

Supported operators:

- `equals`
- `notEquals`

Exactly one must be set.

### `scope`

Available only on comparison-style validators:

```yaml
scope:
  includePattern:
    pattern: "<final>[\\s\\S]*?</final>"
  excludePatterns:
    - pattern: "<think>[\\s\\S]*?</think>"
```

Behavior:

- `includePattern` extracts matching spans
- `excludePatterns` remove matching spans
- scoping runs before normalized equality or similarity comparison

Use this to avoid intentional wrappers, such as `<think>...</think>`, inflating
similarity scores.

### `similarity`

Available only on similarity validators:

```yaml
similarity:
  mode: fast
```

Supported modes:

- `fast`
- `detailed`

Behavior:

- `fast`
  - current low-cost lexical overlap behavior
  - based on token Jaccard and token-sequence overlap
- `detailed`
  - more paraphrase-tolerant, still CPU-friendly
  - adds word-bigram, character-trigram, and content-token recall signals

Default:

- `fast`

Practical guidance:

- use `fast` for anti-copy / near-duplicate checks
- use `detailed` for meaning-preservation checks such as `min_similarity_*`

### Comparison References in `record_transform`

During turn-level `record_transform` validation, these refs are available:

- `original_target_turn`
- `original_target_content`
- `previous_turn`
- `previous_same_role_turn`

Examples:

- `original_target_content`
- `previous_same_role_turn.value`
- `original_target_turn.from`

## Rewrite Output Contract

For `conversation_rewrite`, the intended model contract is:

```text
rewritten turn text
```

The model should return only the rewritten turn text itself:

- no JSON wrapper
- no markdown/code fences
- no explanatory text before or after the rewritten turn

Most reliable wording so far for local reasoning-oriented models:

`Return only the full rewritten target turn text. Do not return JSON. Do not include any text before or after the rewritten turn.`

## Output Files

Successful runs normally produce:

- final dataset: `<resolved-output-dir>/<workflow-name>.jsonl`
- run report: `<resolved-output-dir>/<workflow-name>.report.json`

Run report notes:

- `repeatCount` records the configured top-level workflow repeat count
- `completedRepeats` records how many repeats finished successfully before the
  run ended
- `result.outputsByStage` stores per-stage outputs
  - when `repeat > 1`, each stage key maps to an array of per-repeat outputs
- `result.stageMeta` stores aggregate per-stage execution stats:
  - counts reflect final execution outcomes, not intermediate retry attempts
  - `sampleCount`
  - `successCount`
  - `failureCount`
  - `warningCount`
  - `successRatePct`
  - `failureRatePct`
  - `warningRatePct`

## CLI Overrides

Workflow settings can be overridden by CLI flags such as:

- `--model`
- `--endpoint`
- `--api-key`
- `--output-dir`
- `--max-tokens`
- `--temperature`
- `--parallelism`

See [CLI Reference](./cli-reference.md) for the full flag list.
