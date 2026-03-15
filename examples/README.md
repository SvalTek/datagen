# Datagen Example Workflows

These examples are designed to be copyable and educational.

- Most examples use `model: qwen2.5-coder:14b`.
- `07_lua_bindings_triage.pipeline.yaml` uses `model: llama3.2:latest`.
- Local Ollama endpoint: `http://localhost:11434/`.
- Dataset-backed examples ship small sample inputs under `examples/data/` so the
  documented commands work from a clean checkout.

## Files

- `01_synthetic_dag_delegate.pipeline.yaml`
  - DAG branching with `dependsOn`
  - conditional execution with `when`
  - `iter` + `parallelism`
  - `constrain`, `validate`, `retry`
  - delegated child workflow via `workflow_delegate`
- `01_child_judge.pipeline.yaml`
  - child workflow used by delegated stage
- `02_input_remap_prefixed_iter.pipeline.yaml`
  - `input.remap.kind: prefixed_string_array`
  - dataset slicing (`offset`/`limit`)
  - iterative normalization
- `03_record_transform_stream_resume.pipeline.yaml`
  - `record_transform` conversation rewrite
  - stream-friendly JSONL input (`readMode: stream`)
  - designed for `--checkpoint-every` / `--resume`
- `04_input_remap_alpaca_rewrite.pipeline.yaml`
  - `input.remap.kind: alpaca`
  - conversation rewrite from Alpaca-style records
- `sharegpt-rewrite.pipeline.yaml`
  - ShareGPT-style `conversation_rewrite`
  - rewrite contract uses raw turn text, not JSON wrapper output
  - bundled sample ShareGPT-like input under `examples/data/sample-sharegpt.jsonl`
- `05_lua_stage_flags.pipeline.yaml`
  - `mode: lua` inline script
  - computes deterministic branch flags for `when` conditions
- `06_lua_file_transform.pipeline.yaml`
  - `mode: lua` file-backed script (`examples/lua/normalize.lua`)
  - deterministic record normalization before downstream stages
- `07_lua_bindings_triage.pipeline.yaml`
  - realistic support-ticket triage pipeline
  - file-backed Lua preprocessing + inline Lua model calls
  - `LLM.generate(...)` + `LLM.generateObject(...)`
  - conditional escalation branch with `when`
  - Lua-to-`iter` handoff for parallel reply drafting
  - final Lua packaging to markdown + JSONL outputs
- `08_story_object_bridge.pipeline.yaml`
  - prose -> constrained object -> prose round-trip
  - Lua stage selects one structured action and synthesizes a bridge user turn
  - final Lua stage collapses generation mechanics into a clean conversation record

## Run

From repo root:

```bash
deno task start -- ./examples/01_synthetic_dag_delegate.pipeline.yaml
```

Dataset-backed example with bundled sample input:

```bash
deno task start -- ./examples/sharegpt-rewrite.pipeline.yaml --console warnings
```

For streaming/resume example:

```bash
deno task start -- ./examples/03_record_transform_stream_resume.pipeline.yaml --checkpoint-every 50
# later:
# deno task start -- ./examples/03_record_transform_stream_resume.pipeline.yaml --resume ./examples/outputs/03-record-transform-stream.checkpoint.json
```
