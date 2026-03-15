# Datagen: Current Project Status

## What this project is

Datagen is a CLI tool for generating structured synthetic datasets with LLMs by running a YAML-defined pipeline.

Instead of sending one long prompt and hoping the output is usable, the project breaks generation into ordered stages. Each stage can generate, clean up, enrich, or finalize data, and the output from one stage is passed into the next.

In plain terms: this is already more than "a dataset generator". It is a small pipeline runtime for staged dataset creation and dataset transformation.

## What it currently does

Right now, the project can:

- Load a pipeline from a YAML file
- Validate that pipeline against a strict schema
- Run stages in sequence against configured providers/runtimes (`openai` / `ollama`)
- Resolve stage DAG execution order with explicit dependencies
- Load source datasets from `json` or `jsonl` for transformation pipelines
- Normalize common source dataset shapes during ingestion with `input.remap`
- Pass prior stage outputs forward as structured JSON context
- Support two execution modes:
  - `batch`: one model call for the whole stage
  - `iter`: one model call per item from the previous stage's array output
- Support `record_transform` stages for record-by-record conversation rewriting
- Support `workflow_delegate` stages for delegated child-workflow execution
- Support branching and conditional execution with `dependsOn` and `when`
- Require JSON-only responses for non-constrained stages and parse them automatically
- Use typed `constrain` schemas and structured generation for constrained stages
- Compile `constrain` schemas into Zod validation
- Apply semantic validators such as `contains`, `regex`, and length checks
- Apply comparison-aware validators such as cross-field inequality and repetition checks against prior rewrite context
- Scope similarity and equality comparisons to exclude intentional boilerplate such as reasoning wrappers
- Detect whether rewritten content actually changed from the original source turn without relying on similarity thresholds
- Retry `iter` items and rewritten transform turns with explicit failure feedback
- Stop cleanly with structured errors when parsing, validation, or model calls fail
- Keep original turn content and emit warnings when one conversation-turn rewrite fails
- Write the final stage output as a `.jsonl` dataset file
- Emit a full JSON run report with traces, timing, and failure details
- Write a default run report file for every CLI execution
- Show live per-item/per-record progress in the CLI for long-running `iter` and `record_transform` stages
- Support concise terminal logging modes instead of always dumping the full report JSON
- Suppress model-thought terminal output by default unless explicitly enabled
- Support nested delegated workflow execution (depth-limited) with cycle detection

Current integration note:

- The runtime supports configuring model, provider, and endpoint
- Provider resolution is:
  - CLI `--provider`
  - pipeline `provider`
  - `DATAGEN_PROVIDER`
  - default `openai`
- It now supports bearer-token auth through:
  - CLI `--api-key`
  - pipeline `apiKeyEnv`
  - environment fallbacks such as `DATAGEN_OPENAI_API_KEY`, `OPENAI_API_KEY`, and `OPENROUTER_API_KEY`
- In practice, the current setup works for local Ollama and hosted OpenAI-compatible providers

## What the workflow looks like

The current runtime flow is:

1. Read a pipeline YAML file
2. Validate its structure
3. Create prompts for each stage
4. Call the configured provider/runtime
5. For unconstrained stages: parse text response as JSON
6. For constrained stages: generate structured output and validate against typed schema
7. Feed the result into later stages
8. Write the final output to `output/<pipeline-name>.jsonl`

## What is already implemented and working

This is not just scaffolding. The repo already contains:

- A working CLI entrypoint in `main.ts`
- Pipeline loading and validation
- Multi-stage execution with stage chaining
- Provider-aware model backend wiring (`openai` and `ollama`)
- Per-stage reasoning flag support
- Iterative per-record transformation stages
- Source-dataset transformation from `json` and `jsonl`
- Input remapping for prefixed raw-string conversation arrays and Alpaca-style records
- Conversation-aware replay rewriting for selected turns inside a copied output record
- Semantic validation for stage outputs and rewritten turn payloads
- Repetition-aware comparison validation against original and prior-turn text during conversation rewrites
- Scoped repetition control so wrapper blocks do not dominate similarity checks
- Explicit change-detection validators for rewrite tasks
- Narrow correction-style retries for iter and record_transform stages
- AI SDK-backed chat layer with compatibility transport retained for tests/backward wiring
- Bearer auth support for hosted providers such as OpenRouter
- Local endpoint-oriented example pipelines for Ollama-style usage
- Example pipelines for:
  - fintech account risk dataset generation
  - constrain DSL coverage/smoke (`example-constrain.pipeline.yaml`)
  - cyber-arcane roleplay dataset generation
- Example generated outputs in the `output/` folder

## Current quality level

The project is in a functional prototype / early tool stage.

What is solid:

- Core pipeline execution works end to end
- Error handling is structured instead of ad hoc
- The YAML schema is strict, which reduces config drift
- There is meaningful automated test coverage around loading, execution, parsing, constraints, and CLI behavior
- The local ShareGPT rewrite sample now runs cleanly with a standard Ollama chat model on Ollama's OpenAI-compatible endpoint
- The CLI now has usable run ergonomics for longer jobs instead of looking frozen and dumping massive JSON blobs by default

What is still early:

- Retry support is still narrow and only applies to iter/record_transform correction paths
- Conversation validation is improving but still mostly lexical rather than deeply semantic
- No persistence layer beyond generated files and run reports
- Constraint DSL is now typed and useful, but still intentionally narrower than full raw Zod expressiveness
- Live model behavior still depends heavily on prompt quality and model reliability
- Some Ollama reasoning models can still be unreliable for strict JSON-only tasks on certain prompts

## Evidence of current state

The repository includes an automated test suite covering core runtime behavior,
including:

- pipeline parsing
- stage chaining
- iter-mode execution
- source dataset loading
- record_transform execution
- JSON enforcement
- constraint validation
- typed constrain schema parsing/validation
- CLI end-to-end execution


## Current bottom line

The project has moved past the idea stage. It already works as a reusable,
test-covered CLI for staged synthetic dataset generation and
conversation-dataset transformation, with sample pipelines and sample outputs.
The next step is less about proving the concept and more about expanding the
runtime: deeper validation quality, broader retry control, and stronger
production ergonomics.
