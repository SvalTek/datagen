# Lua Stage Reference

This document defines Datagen `mode: lua` stage behavior.

Use this with:

- [Workflow Reference](./workflow-reference.md)
- [Lua Stage Patterns](./lua-stage-patterns.md)

## Stage Shape

```yaml
stages:
  - id: compute_flags
    mode: lua
    instructions: Run lua logic
    input:
      source: previous_stage
    lua:
      source: inline
      code: |
        local ctx = ...
        return { ok = true, stage = ctx.stageIdentifier }
      runtime:
        functionTimeoutMs: 1000
        openStandardLibs: false
        injectObjects: true
        enableProxy: true
        traceAllocations: false
```

## `lua` Block

### `lua.source`

Supported values:

- `inline`
- `file`

### `lua.code`

Required when `lua.source: inline`.

### `lua.filePath`

Required when `lua.source: file`. Path resolves relative to the workflow file
location unless already absolute.

### `lua.runtime`

Optional runtime overrides:

- `functionTimeoutMs` non-negative integer
- `openStandardLibs` boolean
- `injectObjects` boolean
- `enableProxy` boolean
- `traceAllocations` boolean

## Defaults (Safe-by-Default)

When runtime values are omitted, Datagen uses:

- `functionTimeoutMs: 1000`
- `openStandardLibs: false`
- `injectObjects: true`
- `enableProxy: true`
- `traceAllocations: false`

## Workflow-Level Lua Runtime Defaults

You can define workflow-wide Lua runtime defaults once at the top level:

```yaml
luaRuntime:
  functionTimeoutMs: 2000
  openStandardLibs: true
  injectObjects: true
  enableProxy: true
  traceAllocations: false
```

Resolution order for each Lua stage runtime field:

1. built-in defaults
2. workflow `luaRuntime` defaults
3. stage `lua.runtime` overrides

## Execution Context (`ctx`)

Lua receives one vararg argument:

```lua
local ctx = ...
```

`ctx` fields:

- `initialContext`
- `outputsByStage`
- `stageInput`
- `stageIdentifier`
- `stageIndex`

Context field details:

- `initialContext`
  - value from CLI `--context` / `--context-file`
  - may be `nil`
- `outputsByStage`
  - object keyed by stage key (`id` or fallback key)
  - contains already executed stage outputs only
- `stageInput`
  - resolved by `input.source` / stage defaults
  - may be `nil`
- `stageIdentifier`
  - current stage key string
- `stageIndex`
  - zero-based stage index in execution order

## Available Lua Bindings

Datagen injects:

- global table `Datagen`
- global table `LLM`

Current guaranteed bindings:

- `Datagen.emitWarning(kind, message)`
  - `kind`: non-empty string
  - `message`: any value, converted to string
  - effect: emits a non-fatal stage warning into run report + warning console
    mode
  - return value: none
- `Datagen.emitMetric(name, value)`
  - records a numeric metric on the current Lua stage trace
  - `value` must be a finite number
- `Datagen.emitNote(kind, value)`
  - records a JSON-serializable note payload on the current Lua stage trace
- `Datagen.emitDebug(label, value)`
  - records a JSON-serializable debug payload on the current Lua stage trace
- `Datagen.stageInput()`, `Datagen.initialContext()`, `Datagen.output(stageId)`,
  `Datagen.outputs()`, `Datagen.stageInfo()`
  - convenience accessors over the current runtime context
- `Datagen.assert(condition, message)`, `Datagen.fail(message)`,
  `Datagen.require(value, message?)`, `Datagen.requirePath(root, path, message?)`,
  `Datagen.requireType(value, expectedType, message?)`
  - assertion helpers for failing fast with explicit Lua-stage errors
- `Datagen.get(root, path, defaultValue?)`
  - safe dot-path lookup (`a.b.c`)
  - returns `defaultValue` when path is missing
- `Datagen.getOrThrow(root, path, message?)`
  - path lookup that throws when the path is missing
- `Datagen.has(root, path)`
  - returns `true` when path exists, otherwise `false`
- `Datagen.set(root, path, value)`
  - immutable update (returns cloned object with updated path)
  - path must already exist
- `Datagen.setOrCreate(root, path, value)`
  - immutable update that creates missing object-path segments
- `Datagen.delete(root, path)`
  - immutable delete helper for object paths
- `Datagen.merge(a, b)`, `Datagen.pick(root, paths)`, `Datagen.omit(root, paths)`
  - object shaping helpers
- `Datagen.clone(value)`
  - deep clone utility
- `Datagen.map`, `Datagen.filter`, `Datagen.reduce`, `Datagen.find`,
  `Datagen.flatMap`, `Datagen.groupBy`, `Datagen.indexBy`, `Datagen.pluck`,
  `Datagen.unique`, `Datagen.compact`, `Datagen.countBy`
  - collection helpers for array-like Lua tables
  - `groupBy` returns `{ [key]: array }`
  - `indexBy` returns `{ [key]: item }` and duplicate keys are last-write-wins
- `Datagen.toJson(value)`
  - JSON stringify utility
- `Datagen.prettyJson(value)`
  - pretty-printed JSON stringify utility
- `Datagen.toJsonl(list)`, `Datagen.fromJsonl(text)`
  - JSONL encode/decode helpers
- `Datagen.fromJson(text, defaultValue?)`
  - JSON parse utility with fallback when parse fails
- `Datagen.trim(value)`, `Datagen.lower(value)`, `Datagen.upper(value)`,
  `Datagen.slug(value)`, `Datagen.isBlank(value)`,
  `Datagen.normalizeWhitespace(value)`, `Datagen.split(value, sep)`,
  `Datagen.join(list, sep)`, `Datagen.startsWith(value, prefix)`,
  `Datagen.endsWith(value, suffix)`, `Datagen.contains(value, needle)`,
  `Datagen.truncate(value, maxLen, suffix?)`
  - common string normalization helpers
- `Datagen.textTemplate(template, args?, ...positional)`
  - templating helper with placeholders:
    - named placeholders: `{name}`
    - spread positional placeholder: `${...}`
    - indexed positional placeholders: `$1`, `$2`, ...
  - unknown named placeholders remain unchanged
- `Datagen.bullets(list)`, `Datagen.numbered(list)`,
  `Datagen.codeFence(text, lang?)`, `Datagen.prompt(parts)`
  - prompt and formatting helpers
  - `Datagen.prompt(parts)` joins non-empty parts with blank lines
- `LLM.generate(prompt, options?)`
  - calls the current workflow stage model session and returns raw text
  - Lua receives the resolved string directly (no manual promise handling)
  - `options` supports:
    - `max_tokens` number
    - `temperature` number
    - `think` boolean
    - `reasoning_mode` one of `off | think | openai`
- `LLM.generateObject(prompt, options?)`
  - same model call path as `LLM.generate`
  - Lua receives the resolved object directly (no manual promise handling)
  - parses model output as JSON
  - requires parsed JSON to be an object (not array/primitive)
  - throws when output is not valid JSON object
- `LLM.generateJson(prompt, options?)`
  - parses any valid JSON value and may return object, array, primitive, or
    `null`
- `LLM.generateMany(prompts, options?)`
  - sequentially executes an array of prompt strings and returns an array of
    text outputs in matching order
- `LLM.withRetry(prompt, options?, retry?)`
  - retries helper-local model call failures using
    `{ maxAttempts, backoffMs }`
- `LLM.generateObjectWithRetry(prompt, options?, retry?)`
  - retries `generateObject` failures while preserving the object-only JSON
    contract

Compatibility alias:

- `Datagen.LLM` points to the same table as global `LLM`

Example:

```lua
Datagen.emitWarning("lua.low_confidence", "Fallback branch used")
Datagen.emitMetric("selected_count", 3)
local greeting = Datagen.textTemplate("Hello, {name}!", { name = "Ada" })
local updated = Datagen.set({ item = { score = 1 } }, "item.score", 2)
local prompt = Datagen.prompt({
  "Write one line summary.",
  Datagen.codeFence(Datagen.prettyJson(updated), "json")
})
local text = LLM.withRetry(prompt, { max_tokens = 64 }, { maxAttempts = 2 })
local obj = LLM.generateObjectWithRetry(
  "Return {\"route\":\"ok\"} JSON only",
  { max_tokens = 64 },
  { maxAttempts = 2 }
)
```

## Runtime Globals: What Exists and What Does Not

Guaranteed by Datagen:

- `Datagen` global table
- `LLM` global table
- script vararg context via `local ctx = ...`

Not guaranteed unless your runtime settings allow it:

- Lua standard library globals (for example `table`, `string`, `math`, `_G`)
  when `openStandardLibs: false`

Practical rule:

- if you need stdlib helpers, enable them explicitly in
  `lua.runtime.openStandardLibs`
- keep scripts defensive about optional globals for portability

## Input Source Behavior

Lua stages follow `input.source` rules:

- `pipeline_input` -> `ctx.stageInput` is pipeline input records
- `previous_stage` -> `ctx.stageInput` is output of last dependency key

Default selection:

- if stage has dependencies: `previous_stage`
- else if pipeline input exists: `pipeline_input`
- else `undefined`

If `previous_stage` is selected but no dependency exists, stage fails.

## Output Contract

Lua script must return exactly one value.

That value:

- must be JSON-serializable
- becomes `outputsByStage[<stage-key>]`
- is then checked by stage `constrain` and `validate` if configured

## Trace Telemetry from Lua

Datagen stage traces can include:

```lua
Datagen.emitWarning(kind, message)
Datagen.emitMetric(name, value)
Datagen.emitNote(kind, value)
Datagen.emitDebug(label, value)
```

- warnings are forwarded to warning reporting and run report output
- metrics, notes, and debug entries are attached to the Lua stage trace/report
- `emitMetric` rejects non-finite numeric values
- `emitNote` and `emitDebug` require JSON-serializable payloads

## Error Kinds

Lua stages may fail with:

- `lua_script_load_failed` file read/resolve failure
- `lua_execution_failed` script runtime or syntax failure
- `lua_invalid_output` non-serializable return value

Constrain/validate failures still use existing kinds:

- `constrain_mismatch`
- `validator_mismatch`

## Troubleshooting

1. `lua_script_load_failed`: verify `lua.filePath` and workflow-relative
   resolution.
2. `lua_execution_failed`: test script body with minimal `return` and add logic
   incrementally.
3. `lua_invalid_output`: remove functions/symbol-like values and ensure finite
   numbers only.
4. Missing data in `ctx.stageInput`: check `input.source`, dependencies, and
   upstream output keys.
