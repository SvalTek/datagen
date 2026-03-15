# Datagen

Datagen is a Deno CLI for building dataset workflows with LLMs.

Instead of one giant prompt, you define a YAML pipeline of stages and run it as
a reproducible dataflow:

- generate structured records
- transform existing datasets
- branch conditionally
- delegate complex branches to child workflows
- validate and retry outputs

Datagen writes both:

- dataset output (`.jsonl`)
- full run report (`.report.json`) with traces, warnings, stage statuses, and
  dependency graph

## What It Can Do

- Strict YAML workflow schema validation
- Stage DAG execution with `dependsOn` and conditional `when`
- Stage modes:
  - `batch`
  - `iter`
  - `record_transform` (`conversation_rewrite`)
  - `workflow_delegate` (run child workflow from a stage)
  - `lua` (deterministic scripted transforms and branch logic)
- Parallel per-item/per-record execution for `iter` and `record_transform`
- Input dataset loading from `json` / `jsonl`
- Input remapping (`prefixed_string_array`, `alpaca`)
- Typed structured output with `constrain` and configurable `structuredOutputMode`
- Top-level workflow `repeat` for multi-sample synthetic runs
- Semantic/content validators with retry feedback
- Streaming + resume/checkpoint support for long JSONL rewrite runs
  (streaming-compatible shape)
- Run reports and terminal summaries with per-stage success/fail/warn percentages
- Provider support: `openai`, `ollama`

## Requirements

- Deno 2.x
- Access to an LLM backend (local Ollama or hosted OpenAI-compatible endpoint)

## Install

### Option 1: Run from repo (no install)

```bash
deno task start -- ./examples/example.pipeline.yaml
```

### Option 2: Install global CLI from local checkout

From repo root:

```bash
deno install -g -n datagen -A main.ts
```

Then use it anywhere:

```bash
datagen your-workflow.pipeline.yaml
```

### Option 3: Install global CLI directly from GitHub

```bash
deno install -g -n datagen -A https://raw.githubusercontent.com/SvalTek/datagen/main/main.ts
```

Then run:

```bash
datagen your-workflow.pipeline.yaml
```

## Quick Start

Run a sample workflow:

```bash
deno task start -- ./examples/example.pipeline.yaml
```

Run with warning-focused console output:

```bash
deno task start -- ./examples/sharegpt-rewrite.pipeline.yaml --console warnings
```

The dataset-backed examples under `examples/` include small sample inputs in
`examples/data/`, so the example commands run from a clean checkout.

Run with full report in terminal:

```bash
deno task start -- ./examples/07_lua_bindings_triage.pipeline.yaml --console full
```

## Path Behavior

- Workflow/data paths are resolved from your current working directory when you
  run the command.
- In delegated stages (`workflow_delegate`), child `delegate.workflowPath` is
  resolved relative to the parent workflow file location.
- In Lua stages (`mode: lua`, `lua.source: file`), `lua.filePath` is resolved
  relative to the workflow file location.

## Tasks

Defined in [`deno.jsonc`](./deno.jsonc):

- `deno task start -- <workflow.pipeline.yaml> [flags]`
- `deno task test` (runs full test suite under `tests/`)
- `deno task dev` (watch mode for `main.ts`)

## Documentation

- [Workflow Patterns](./docs/workflow-patterns.md)\
  Tutorial-style crash course for building workflows
- [Branching Workflows](./docs/branching_workflows.md)\
  DAG/branching patterns, `dependsOn`, `when`, and delegated branch usage
- [Workflow Reference](./docs/workflow-reference.md)\
  Full workflow schema and field-level behavior
- [CLI Reference](./docs/cli-reference.md)\
  Flags, precedence rules, progress/report behavior
- [Lua Stage Reference](./docs/lua-stage-reference.md)\
  Full `mode: lua` schema, runtime defaults, context contract, and error
  behavior
- [Lua Stage Patterns](./docs/lua-stage-patterns.md)\
  Practical Lua stage authoring patterns and anti-patterns

## Sample Workflows

- [examples/example.pipeline.yaml](./examples/example.pipeline.yaml)
- [examples/example-constrain.pipeline.yaml](./examples/example-constrain.pipeline.yaml)
- [examples/rp.pipeline.yaml](./examples/rp.pipeline.yaml)
- [examples/sharegpt-rewrite.pipeline.yaml](./examples/sharegpt-rewrite.pipeline.yaml)
- [examples/07_lua_bindings_triage.pipeline.yaml](./examples/07_lua_bindings_triage.pipeline.yaml)
- [examples/](./examples/README.md) (feature-tour workflows with detailed
  comments)
