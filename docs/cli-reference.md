# CLI Reference

Datagen runs through:

```bash
deno run -A main.ts <workflow.yml> [options]
```

## Required Argument

- `<workflow.yml>`: path to the workflow file

## Core Command

```bash
deno run -A main.ts ./examples/sharegpt-rewrite.pipeline.yaml
```

This will:

- load the workflow
- run the configured stages
- write the final dataset JSONL
- write a full report JSON file

## Options

- `--help` prints usage and exits.

### Model and Endpoint

- `--model <name>`
- `--endpoint <url>`
- `--provider <name>`

These override workflow defaults.

### Auth and Provider Headers

- `--api-key <token>`
- `--http-referer <url>`
- `--x-title <name>`

Use these for hosted OpenAI-compatible providers such as OpenRouter.

### Completion Settings

- `--max-tokens <number>`
- `--temperature <number>`
- `--parallelism <number>`

These override workflow-level `maxTokens` and `temperature`. `--parallelism`
sets a global worker cap for `iter` and `record_transform`. `--parallelism` must
be an integer `>= 1`.

### Output and Files

- `--out <path>`
- `--output-dir <path>`
- `--resume <path>`
- `--checkpoint-every <n>`

Behavior:

- final dataset output still goes to the workflow-selected output location under
  the resolved output directory
- `--out` controls the report JSON path
- if `--out` is omitted, Datagen writes
  `<resolved-output-dir>/<workflow-name>.report.json`
- `--checkpoint-every` must be an integer `>= 1`
- `--checkpoint-every` writes checkpoint metadata during streaming runs
- checkpoint path defaults to `<output-dir>/<workflow-name>.checkpoint.json`
- if `--resume` is provided, that path is loaded as checkpoint input
- `--resume` also reuses the same path for checkpoint writes when checkpoint
  writing is enabled

Streaming note:

- resume/checkpoint behavior is currently designed for streaming-compatible
  `record_transform` runs
- streaming-compatible means: single-stage `record_transform` +
  `conversation_rewrite` + input source `pipeline_input`
- if that shape is not met, Datagen runs the normal eager path
- delegated child workflows (`workflow_delegate`) always run eager in v1

### Context Injection

- `--context <json>`
- `--context-file <path>`

These inject initial JSON context. You can pass only one of `--context` or
`--context-file`.

They are useful for generation workflows that need starting metadata or input
context outside a dataset file.

### Console Output

- `--console summary`
- `--console warnings`
- `--console quiet`
- `--console full`

#### `summary`

Default mode.

Shows:

- short run summary
- live progress
- final summary
- per-stage success/fail/warn percentages in the final summary when available

Does not print the full JSON report to the terminal.

#### `warnings`

Like `summary`, plus compact warning lines.

This is useful for long runs where you want to see validator/runtime warnings
without the full report dump.

The end-of-run summary still includes per-stage percentage lines such as:

```text
rewrite: 96.00% success, 4.00% fail, 4.00% warn, samples=100
```

#### `quiet`

Suppresses normal terminal output.

Useful for scripting.

#### `full`

Prints the full JSON report to stdout or stderr.

This is the closest behavior to the old verbose mode.

### Progress

- `--progress`
- `--no-progress`

Default behavior:

- enabled in `summary` and `warnings`
- disabled in `quiet` and `full`

Progress behavior:

- `iter` shows per-item progress
- `record_transform` shows per-record progress
- `batch` does not show bar-style iterative progress

### Thoughts / Reasoning Output

- `--show-thoughts`

By default, model reasoning/thought output is suppressed in terminal output.

Use this flag if you explicitly want it printed during the run.

This flag only affects terminal display. It does not request reasoning mode from
the model. Request-side reasoning transport is controlled by workflow
`reasoning` plus top-level `reasoningMode`.

## Model Requirement

Datagen requires a model from one of:

1. `--model`
2. workflow `model`
3. `DATAGEN_MODEL`
4. `OLLAMA_MODEL`

If none are set, the run fails with a usage/config error and writes a failure
report.

## Output Files

Successful runs normally produce:

- final dataset: `<resolved-output-dir>/<workflow-name>.jsonl`
- run report: `<resolved-output-dir>/<workflow-name>.report.json`

The report contains:

- run metadata
- repeat metadata (`repeatCount`, `completedRepeats`) when relevant
- per-stage traces
- warnings
- per-stage outputs
- per-stage aggregate stats under `result.stageMeta` based on final execution
  outcomes rather than intermediate retry attempts

## Resolution Order

### Auth Resolution

Datagen resolves API keys in this order:

1. `--api-key`
2. workflow `apiKeyEnv`
3. `DATAGEN_OPENAI_API_KEY`
4. `OPENAI_API_KEY`
5. `OPENROUTER_API_KEY`

### Provider Resolution

Datagen resolves provider in this order:

1. `--provider`
2. workflow `provider`
3. `DATAGEN_PROVIDER`
4. default `openai`

### Header Resolution

For `HTTP-Referer` and `X-Title`:

1. CLI flag
2. workflow value
3. `DATAGEN_HTTP_REFERER` / `DATAGEN_X_TITLE`

### Endpoint Resolution

1. `--endpoint`
2. workflow `endpoint`
3. `DATAGEN_OPENAI_ENDPOINT`
4. default `http://localhost:11434/`

## Common Commands

### Run a local workflow

```bash
deno run -A main.ts ./examples/example.pipeline.yaml
```

### Run with warning output

```bash
deno run -A main.ts ./examples/sharegpt-rewrite.pipeline.yaml --console warnings
```

### Run with full JSON report in terminal

```bash
deno run -A main.ts ./examples/07_lua_bindings_triage.pipeline.yaml --console full
```

### Disable progress

```bash
deno run -A main.ts ./examples/07_lua_bindings_triage.pipeline.yaml --no-progress
```

### Override temperature

```bash
deno run -A main.ts ./examples/sharegpt-rewrite.pipeline.yaml --temperature 3.0
```

### Use a hosted provider

```powershell
$env:OPENROUTER_API_KEY="your-key"
deno run -A main.ts ./examples/example.pipeline.yaml --endpoint https://openrouter.ai/api/ --model openai/gpt-4o-mini
```

## Notes

- CLI values override workflow defaults where both exist.
- delegated stages can opt into parent override inheritance via
  `delegate.inheritParentCli`.
- with `delegate.inheritParentCli: none` (default), child workflow runtime
  config is respected.
- `mode: lua` does not introduce new CLI flags in v1; Lua behavior is configured
  in workflow YAML (`stage.lua`).
- The report file is now the canonical place for full detail; normal terminal
  output is intentionally concise.
- For rewrite-heavy local workflows, `--console warnings` is usually the most
  useful interactive mode.
