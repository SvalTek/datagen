# Lua Stage Patterns

This guide shows practical `mode: lua` usage patterns.

## Binding Quick Reference

Available runtime bindings used in these examples:

- `local ctx = ...`
  - stage execution context object from Datagen
- `Datagen.emitWarning(kind, message)`
  - emit non-fatal warnings into run report/console warnings mode
- `Datagen.emitMetric / emitNote / emitDebug`
  - attach trace telemetry to the current Lua stage
- `Datagen.get / getOrThrow / has / set / setOrCreate / delete / merge`
  - path and object manipulation helpers
- `Datagen.map / filter / reduce / groupBy / indexBy / countBy`
  - collection helpers for array-like tables
- `Datagen.toJson / prettyJson / toJsonl / fromJson / fromJsonl`
  - JSON and JSONL conversion helpers
- `Datagen.trim / lower / upper / slug / normalizeWhitespace / truncate`
  - string utility helpers
- `Datagen.textTemplate / bullets / numbered / codeFence / prompt`
  - deterministic formatting and prompt-building helpers
- `LLM.generate / generateObject / generateJson / generateMany`
  - model helpers for text and JSON
- `LLM.withRetry / generateObjectWithRetry`
  - helper-local retry wrappers

## Pattern 1: Compute Branch Flags

Use Lua to derive deterministic flags for `when` conditions.

```yaml
stages:
  - id: seed
    instructions: Return one object with records.

  - id: compute_flags
    mode: lua
    dependsOn: [seed]
    instructions: Compute branch gates
    lua:
      source: inline
      code: |
        local ctx = ...
        local seed = ctx.stageInput or {}
        local count = 0
        if seed.records and #seed.records then
          count = #seed.records
        end
        return {
          recordCount = count,
          runAudit = count >= 100
        }

  - id: audit
    dependsOn: [compute_flags]
    when:
      path: outputsByStage.compute_flags.runAudit
      equals: true
    instructions: Run expensive audit branch.
```

## Pattern 2: Normalize Upstream Data

Use Lua to perform deterministic shape cleanup before LLM stages.

```yaml
stages:
  - id: seed
    instructions: Return one object with records.

  - id: normalize
    mode: lua
    dependsOn: [seed]
    instructions: Normalize records
    lua:
      source: file
      filePath: ./lua/normalize.lua
    constrain:
      type: object
      shape:
        records:
          type: array
          items:
            type: object
            shape:
              id:
                type: string
              text:
                type: string
```

## Pattern 3: Emit Soft Warnings

Use `Datagen.emitWarning` for non-fatal quality signals.

```lua
local ctx = ...
if not ctx.stageInput or not ctx.stageInput.records then
  Datagen.emitWarning("lua.missing_records", "records array missing; returning empty output")
  return { records = {} }
end
return ctx.stageInput
```

## Pattern 4: Lua + Validate for Policy Rules

Use Lua for deterministic transforms, then `validate` for policy checks.

```yaml
validate:
  rules:
    - path: records
      kind: array_min_length
      value: 1
    - path: flags.route
      kind: not_equals
      value: "unknown"
```

## Pattern 5: Hybrid Lua + LLM Calls

Use this when Lua should orchestrate deterministic logic around a targeted model
call.

```lua
local summary = LLM.generate(
  "Summarize in 1 sentence: " .. Datagen.toJson(ctx.stageInput),
  { max_tokens = 80, temperature = 0.2 }
)

local route = LLM.generateObject(
  "Return JSON object only: {\"route\":\"review|approve\"}",
  { max_tokens = 40, temperature = 0 }
)

return {
  summary = Datagen.trim(summary),
  route = route.route
}
```

## Pattern 6: Replace Manual Loops with Collection Helpers

Use collection helpers to keep Lua stages concise and deterministic.

```lua
local tickets = Datagen.requirePath(Datagen.stageInput(), "tickets")
local grouped = Datagen.groupBy(tickets, "product")
local ids = Datagen.pluck(tickets, "id")
local highPriority = Datagen.filter(tickets, function(item)
  return Datagen.get(item, "priority") == "high"
end)

Datagen.emitMetric("ticket_count", #tickets)
Datagen.emitNote("lua.products", Datagen.countBy(tickets, "product"))

return {
  ids = ids,
  grouped = grouped,
  highPriorityCount = #highPriority
}
```

## Pattern 7: Build Cleaner Prompts

Use `prompt`, `bullets`, `numbered`, and `codeFence` instead of string
concatenation.

```lua
local payload = {
  title = "Reset password fails",
  body = "Customer reports token is always expired."
}

local prompt = Datagen.prompt({
  "Summarize this support ticket in one sentence.",
  Datagen.bullets({
    "Focus on customer-visible impact",
    "Do not invent root cause"
  }),
  Datagen.codeFence(Datagen.prettyJson(payload), "json")
})

local summary = LLM.withRetry(prompt, { max_tokens = 80 }, { maxAttempts = 2 })
return { summary = Datagen.normalizeWhitespace(summary) }
```

## Anti-Patterns

1. Using Lua for model-style generation tasks.
2. Returning non-serializable values.
3. Building hidden side effects or stateful lifecycle logic.
4. Encoding fragile business logic without `constrain`/`validate`.

## Migration Advice

When moving from prompt-only transforms:

1. Keep semantic generation in model stages.
2. Move deterministic transformations into Lua stages.
3. Add `constrain` immediately after Lua to lock shape.
4. Add `validate` for policy checks and explainability.
