import {
  assertEquals,
  assertRejects,
  assertThrows,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import {
  PipelineDocumentSchema,
  StageSchema,
} from "../structures/TaskSchema.ts";
import {
  loadPipelineFromFile,
  parsePipelineYaml,
  PipelineParseError,
  PipelineValidationError,
} from "../lib/PipelineLoader.ts";

Deno.test("StageSchema accepts required instructions with optional fields", () => {
  const out = StageSchema.parse({
    instructions: "Generate records",
    system: "You are precise",
    mode: "iter",
    rules: { include: ["valid JSON"], exclude: ["hallucinations"] },
    history: "Prior context",
    examples: [{ input: "A", output: "B" }],
    reasoning: true,
    constrain: {
      type: "object",
      shape: {
        content: { type: "string" },
      },
    },
    validate: {
      rules: [
        { path: "content", kind: "contains", value: "ok" },
      ],
    },
  });

  assertEquals(out.instructions, "Generate records");
  assertEquals(out.mode, "iter");
  assertEquals(out.reasoning, true);
  assertEquals(out.constrain?.type, "object");
  assertEquals(out.validate?.rules.length, 1);
});

Deno.test("StageSchema rejects missing instructions", () => {
  assertThrows(() => StageSchema.parse({ system: "No instructions" }), Error);
});

Deno.test("StageSchema parses minimal direct stage contract", () => {
  const out = StageSchema.parse({ instructions: "Generate records" });
  assertEquals(out.instructions, "Generate records");
});

Deno.test("StageSchema rejects unknown keys under strict policy", () => {
  assertThrows(
    () =>
      StageSchema.parse({ instructions: "Generate records", unexpected: true }),
    Error,
  );
});

Deno.test("PipelineDocumentSchema enforces stage ordering shape and duplicate name rejection", () => {
  const ok = PipelineDocumentSchema.parse({
    version: 1,
    name: "dataset-pipeline",
    reasoningMode: "openai",
    stages: [
      { name: "seed", instructions: "Seed examples" },
      { name: "refine", instructions: "Refine examples" },
    ],
  });

  assertEquals(ok.stages.length, 2);
  assertEquals(ok.stages[0].name, "seed");
  assertEquals(ok.reasoningMode, "openai");

  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        stages: [
          { name: "same", instructions: "A" },
          { name: "same", instructions: "B" },
        ],
      }),
    Error,
    "Duplicate stage name",
  );
});

Deno.test("PipelineDocumentSchema rejects unknown keys under strict policy", () => {
  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        stages: [{ instructions: "ok" }],
        unexpectedTopLevel: "nope",
      }),
    Error,
  );
});

Deno.test("StageSchema rejects invalid validate blocks", () => {
  assertThrows(
    () =>
      StageSchema.parse({
        instructions: "Generate records",
        validate: {
          rules: [{ kind: "unknown", value: "x" }],
        },
      }),
    Error,
  );

  assertThrows(
    () =>
      StageSchema.parse({
        instructions: "Generate records",
        validate: {
          rules: [{ kind: "min_length", value: -1 }],
        },
      }),
    Error,
  );

  assertThrows(
    () =>
      StageSchema.parse({
        instructions: "Generate records",
        validate: {
          rules: [{
            path: "content",
            kind: "max_similarity_to_ref",
            ref: "previous_same_role_turn.value",
            threshold: 1.5,
          }],
        },
      }),
    Error,
  );
});

Deno.test("StageSchema accepts comparison-aware validate rules", () => {
  const out = StageSchema.parse({
    instructions: "Generate records",
    validate: {
      rules: [
        {
          path: "rewritten",
          kind: "must_change_from_path",
          otherPath: "original",
          hint: "Make a real edit instead of returning the original text.",
        },
        {
          path: "rewritten",
          kind: "not_equal_to_path",
          otherPath: "original",
        },
        {
          path: "content",
          kind: "must_change_from_ref",
          ref: "previous_same_role_turn.value",
          scope: {
            excludePatterns: [{
              pattern: "<reasoning>[\\s\\S]*?</reasoning>",
            }],
          },
        },
        {
          path: "content",
          kind: "not_regex",
          pattern: "^<think>[\\s\\S]*?</think>\\s*\\{",
        },
        {
          name: "meaning_anchor",
          path: "content",
          kind: "min_similarity_to_ref",
          ref: "original_target_content",
          threshold: 0.55,
          similarity: {
            mode: "detailed",
          },
        },
      ],
    },
  });

  assertEquals(out.validate?.rules.length, 5);
  assertEquals(
    (out.validate?.rules[0] as { hint?: string }).hint,
    "Make a real edit instead of returning the original text.",
  );
  assertEquals(
    (out.validate?.rules[4] as { name?: string }).name,
    "meaning_anchor",
  );
});

Deno.test("StageSchema rejects invalid similarity modes on similarity validators", () => {
  assertThrows(
    () =>
      StageSchema.parse({
        instructions: "Generate records",
        validate: {
          rules: [{
            path: "content",
            kind: "min_similarity_to_ref",
            ref: "original_target_content",
            threshold: 0.55,
            similarity: {
              mode: "semantic",
            },
          }],
        },
      }),
    Error,
  );
});

Deno.test("StageSchema rejects empty scope blocks on comparison validators", () => {
  assertThrows(
    () =>
      StageSchema.parse({
        instructions: "Generate records",
        validate: {
          rules: [{
            path: "content",
            kind: "max_similarity_to_ref",
            ref: "previous_same_role_turn.value",
            threshold: 0.82,
            scope: {},
          }],
        },
      }),
    Error,
  );
});

Deno.test("StageSchema rejects missing comparison references for must_change validators", () => {
  assertThrows(
    () =>
      StageSchema.parse({
        instructions: "Generate records",
        validate: {
          rules: [{
            path: "content",
            kind: "must_change_from_ref",
          }],
        },
      }),
    Error,
  );

  assertThrows(
    () =>
      StageSchema.parse({
        instructions: "Generate records",
        validate: {
          rules: [{
            path: "content",
            kind: "must_change_from_path",
          }],
        },
      }),
    Error,
  );
});

Deno.test("StageSchema accepts id/dependsOn/when/parallelism additions", () => {
  const out = StageSchema.parse({
    id: "rewrite_stage",
    instructions: "Rewrite records",
    mode: "iter",
    dependsOn: ["seed_stage"],
    when: {
      path: "outputsByStage.seed_stage.enabled",
      equals: true,
    },
    parallelism: 4,
  });

  assertEquals(out.id, "rewrite_stage");
  assertEquals(out.dependsOn, ["seed_stage"]);
  assertEquals(out.when?.path, "outputsByStage.seed_stage.enabled");
  assertEquals(out.parallelism, 4);
});

Deno.test("StageSchema accepts workflow_delegate config and enforces delegate constraints", () => {
  const valid = StageSchema.parse({
    id: "delegate_judge",
    instructions: "Delegate to child workflow",
    mode: "workflow_delegate",
    delegate: {
      workflowPath: "./child.pipeline.yaml",
      inputFromPath: "outputsByStage.seed.candidates",
      inputAs: "pipeline_input",
      outputFrom: "stage_key",
      outputStageKey: "judge",
      outputSelectPath: "decision",
      onFailure: "warn",
      inheritParentCli: "completion",
    },
  });
  assertEquals(valid.mode, "workflow_delegate");
  assertEquals(valid.delegate?.outputStageKey, "judge");

  assertThrows(
    () =>
      StageSchema.parse({
        id: "delegate_bad",
        instructions: "Delegate to child workflow",
        mode: "workflow_delegate",
        delegate: {
          workflowPath: "./child.pipeline.yaml",
          inputFromPath: "outputsByStage.seed.candidates",
          outputFrom: "stage_key",
        },
      }),
    Error,
    "outputStageKey",
  );

  assertThrows(
    () =>
      StageSchema.parse({
        id: "batch_bad",
        instructions: "Not delegated",
        mode: "batch",
        delegate: {
          workflowPath: "./child.pipeline.yaml",
          inputFromPath: "outputsByStage.seed",
        },
      }),
    Error,
    "delegate is only supported",
  );
});

Deno.test("StageSchema accepts lua config and enforces lua constraints", () => {
  const inlineLua = StageSchema.parse({
    id: "lua_inline",
    instructions: "Compute output",
    mode: "lua",
    lua: {
      source: "inline",
      code: "local ctx = ...; return { ok = true, input = ctx.stageInput }",
      runtime: {
        functionTimeoutMs: 750,
        openStandardLibs: false,
      },
    },
  });
  assertEquals(inlineLua.mode, "lua");
  assertEquals(inlineLua.lua?.source, "inline");
  assertEquals(inlineLua.lua?.runtime?.functionTimeoutMs, 750);

  const fileLua = StageSchema.parse({
    id: "lua_file",
    instructions: "Compute output",
    mode: "lua",
    lua: {
      source: "file",
      filePath: "./scripts/compute.lua",
    },
  });
  assertEquals(fileLua.lua?.source, "file");
  assertEquals(fileLua.lua?.filePath, "./scripts/compute.lua");

  assertThrows(
    () =>
      StageSchema.parse({
        id: "lua_missing_block",
        instructions: "Missing block",
        mode: "lua",
      }),
    Error,
    "lua stages require a lua block",
  );

  assertThrows(
    () =>
      StageSchema.parse({
        id: "lua_wrong_mode",
        instructions: "Wrong mode",
        mode: "batch",
        lua: {
          source: "inline",
          code: "return {}",
        },
      }),
    Error,
    "lua is only supported",
  );

  assertThrows(
    () =>
      StageSchema.parse({
        id: "lua_inline_bad",
        instructions: "Invalid inline",
        mode: "lua",
        lua: {
          source: "inline",
          filePath: "./bad.lua",
        },
      }),
    Error,
    "lua.code is required",
  );

  assertThrows(
    () =>
      StageSchema.parse({
        id: "lua_file_bad",
        instructions: "Invalid file",
        mode: "lua",
        lua: {
          source: "file",
          code: "return {}",
        },
      }),
    Error,
    "lua.filePath is required",
  );

  assertThrows(
    () =>
      StageSchema.parse({
        id: "lua_runtime_bad",
        instructions: "Invalid runtime",
        mode: "lua",
        lua: {
          source: "inline",
          code: "return {}",
          runtime: {
            functionTimeoutMs: -1,
          },
        },
      }),
    Error,
  );
});

Deno.test("PipelineDocumentSchema accepts workflow-level luaRuntime defaults", () => {
  const parsed = PipelineDocumentSchema.parse({
    model: "mock-model",
    luaRuntime: {
      functionTimeoutMs: 1500,
      openStandardLibs: true,
      injectObjects: true,
      enableProxy: true,
      traceAllocations: false,
    },
    stages: [{
      id: "lua_stage",
      mode: "lua",
      instructions: "Run lua",
      lua: {
        source: "inline",
        code: "return { ok = true }",
      },
    }],
  });

  assertEquals(parsed.luaRuntime?.functionTimeoutMs, 1500);
  assertEquals(parsed.luaRuntime?.openStandardLibs, true);
});

Deno.test("PipelineDocumentSchema validates dependency keys and input readMode", () => {
  const out = PipelineDocumentSchema.parse({
    input: {
      path: "./data/source.jsonl",
      format: "jsonl",
      readMode: "stream",
    },
    stages: [
      { id: "seed", instructions: "Seed" },
      { id: "rewrite", instructions: "Rewrite", dependsOn: ["seed"] },
    ],
  });

  assertEquals(out.input?.readMode, "stream");
  assertEquals(out.stages[1].dependsOn, ["seed"]);

  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        stages: [
          { id: "seed", instructions: "Seed" },
          { id: "rewrite", instructions: "Rewrite", dependsOn: ["missing"] },
        ],
      }),
    Error,
    "Unknown stage dependency",
  );

  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        stages: [{
          id: "lua_stage",
          mode: "lua",
          instructions: "Run lua",
          input: { source: "pipeline_input" },
          lua: {
            source: "inline",
            code: "return {}",
          },
        }],
      }),
    Error,
    "lua stages using pipeline_input require pipeline.input to be configured",
  );

  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        stages: [{
          id: "lua_stage",
          mode: "lua",
          instructions: "Run lua",
          input: { source: "previous_stage" },
          lua: {
            source: "inline",
            code: "return {}",
          },
        }],
      }),
    Error,
    "first lua stage cannot use previous_stage without a dependency",
  );
});

Deno.test("parsePipelineYaml parses and validates valid YAML", () => {
  const pipeline = parsePipelineYaml(`
version: 1
name: synthetic-dataset
description: simple pipeline
model: llama3.2:latest
endpoint: http://localhost:11434/
reasoningMode: think
apiKeyEnv: OPENROUTER_API_KEY
httpReferer: https://example.com/datagen
xTitle: Datagen
maxTokens: 2048
temperature: 1.25
outputDir: ./tests/outputs
stages:
  - name: seed
    instructions: generate seed records
  - name: refine
    instructions: refine records
    system: be strict
    reasoning: true
    rules:
      include:
        - valid json
    constrain:
      type: object
      shape:
        item:
          type: string
`);

  assertEquals(pipeline.name, "synthetic-dataset");
  assertEquals(pipeline.model, "llama3.2:latest");
  assertEquals(pipeline.endpoint, "http://localhost:11434/");
  assertEquals(pipeline.reasoningMode, "think");
  assertEquals(pipeline.apiKeyEnv, "OPENROUTER_API_KEY");
  assertEquals(pipeline.httpReferer, "https://example.com/datagen");
  assertEquals(pipeline.xTitle, "Datagen");
  assertEquals(pipeline.maxTokens, 2048);
  assertEquals(pipeline.temperature, 1.25);
  assertEquals(pipeline.outputDir, "./tests/outputs");
  assertEquals(pipeline.stages[1].reasoning, true);
  assertEquals(pipeline.stages.length, 2);
  assertEquals(pipeline.stages[1].constrain?.type, "object");
});

Deno.test("PipelineDocumentSchema accepts supported reasoning modes and omits by default", () => {
  const off = PipelineDocumentSchema.parse({
    reasoningMode: "off",
    stages: [{ instructions: "seed" }],
  });
  const think = PipelineDocumentSchema.parse({
    reasoningMode: "think",
    stages: [{ instructions: "seed" }],
  });
  const openai = PipelineDocumentSchema.parse({
    reasoningMode: "openai",
    stages: [{ instructions: "seed" }],
  });
  const omitted = PipelineDocumentSchema.parse({
    stages: [{ instructions: "seed" }],
  });

  assertEquals(off.reasoningMode, "off");
  assertEquals(think.reasoningMode, "think");
  assertEquals(openai.reasoningMode, "openai");
  assertEquals(omitted.reasoningMode, undefined);
});

Deno.test("PipelineDocumentSchema rejects invalid reasoningMode values", () => {
  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        reasoningMode: "auto",
        stages: [{ instructions: "seed" }],
      }),
    Error,
  );
});

Deno.test("PipelineDocumentSchema accepts supported structuredOutputMode values and omits by default", () => {
  const object = PipelineDocumentSchema.parse({
    structuredOutputMode: "object",
    stages: [{ instructions: "seed" }],
  });
  const json = PipelineDocumentSchema.parse({
    structuredOutputMode: "json",
    stages: [{ instructions: "seed" }],
  });
  const jsonArray = PipelineDocumentSchema.parse({
    structuredOutputMode: "json-array",
    stages: [{ instructions: "seed" }],
  });
  const off = PipelineDocumentSchema.parse({
    structuredOutputMode: "off",
    stages: [{ instructions: "seed" }],
  });
  const omitted = PipelineDocumentSchema.parse({
    stages: [{ instructions: "seed" }],
  });

  assertEquals(object.structuredOutputMode, "object");
  assertEquals(json.structuredOutputMode, "json");
  assertEquals(jsonArray.structuredOutputMode, "json-array");
  assertEquals(off.structuredOutputMode, "off");
  assertEquals(omitted.structuredOutputMode, undefined);
});

Deno.test("PipelineDocumentSchema rejects invalid structuredOutputMode values", () => {
  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        structuredOutputMode: "auto",
        stages: [{ instructions: "seed" }],
      }),
    Error,
  );
});

Deno.test("PipelineDocumentSchema accepts repeat and rejects invalid values", () => {
  const repeated = PipelineDocumentSchema.parse({
    repeat: 3,
    stages: [{ instructions: "seed" }],
  });
  assertEquals(repeated.repeat, 3);

  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        repeat: 0,
        stages: [{ instructions: "seed" }],
      }),
    Error,
  );
});

Deno.test("PipelineDocumentSchema accepts supported providers and omits by default", () => {
  const openai = PipelineDocumentSchema.parse({
    provider: "openai",
    stages: [{ instructions: "seed" }],
  });
  const ollama = PipelineDocumentSchema.parse({
    provider: "ollama",
    stages: [{ instructions: "seed" }],
  });
  const omitted = PipelineDocumentSchema.parse({
    stages: [{ instructions: "seed" }],
  });

  assertEquals(openai.provider, "openai");
  assertEquals(ollama.provider, "ollama");
  assertEquals(omitted.provider, undefined);
});

Deno.test("PipelineDocumentSchema rejects invalid provider values", () => {
  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        provider: "anthropic",
        stages: [{ instructions: "seed" }],
      }),
    Error,
  );
});

Deno.test("parsePipelineYaml accepts record_transform pipelines with pipeline input", () => {
  const pipeline = parsePipelineYaml(`
version: 1
name: transform-dataset
input:
  path: ./data/source.jsonl
  format: jsonl
  offset: 10
  limit: 25
  remap:
    kind: prefixed_string_array
    sourcePath: conversation
    prefixes:
      user: "user:"
      assistant: "assistant:"
stages:
  - name: rewrite
    mode: record_transform
    instructions: rewrite turns
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
    validate:
      rules:
        - path: conversations
          kind: array_min_length
          value: 2
`);

  assertEquals(pipeline.input?.path, "./data/source.jsonl");
  assertEquals(pipeline.input?.format, "jsonl");
  assertEquals(pipeline.input?.offset, 10);
  assertEquals(pipeline.input?.limit, 25);
  assertEquals(pipeline.input?.remap?.kind, "prefixed_string_array");
  assertEquals(pipeline.stages[0].mode, "record_transform");
  assertEquals(pipeline.stages[0].transform?.kind, "conversation_rewrite");
  assertEquals(pipeline.stages[0].validate?.rules.length, 1);
});

Deno.test("PipelineDocumentSchema accepts supported input remap configs", () => {
  const prefixed = PipelineDocumentSchema.parse({
    input: {
      path: "./data/source.json",
      remap: {
        kind: "prefixed_string_array",
        sourcePath: "conversation",
        prefixes: {
          user: "user:",
          assistant: "assistant:",
        },
      },
    },
    stages: [{ instructions: "seed" }],
  });

  const alpaca = PipelineDocumentSchema.parse({
    input: {
      path: "./data/source.jsonl",
      remap: {
        kind: "alpaca",
        instructionField: "prompt",
        inputField: "context",
        outputField: "answer",
      },
    },
    stages: [{ instructions: "seed" }],
  });

  assertEquals(prefixed.input?.remap?.kind, "prefixed_string_array");
  assertEquals(alpaca.input?.remap?.kind, "alpaca");
});

Deno.test("PipelineDocumentSchema rejects invalid input remap configs", () => {
  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        input: {
          path: "./data/source.json",
          remap: {
            kind: "prefixed_string_array",
            prefixes: {
              user: "user:",
              assistant: "assistant:",
            },
          },
        },
        stages: [{ instructions: "seed" }],
      }),
    Error,
  );

  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        input: {
          path: "./data/source.json",
          remap: {
            kind: "prefixed_string_array",
            sourcePath: "conversation",
            prefixes: {
              user: "",
              assistant: "assistant:",
            },
          },
        },
        stages: [{ instructions: "seed" }],
      }),
    Error,
  );

  assertThrows(
    () =>
      PipelineDocumentSchema.parse({
        input: {
          path: "./data/source.json",
          remap: {
            kind: "unknown",
          },
        },
        stages: [{ instructions: "seed" }],
      }),
    Error,
  );
});

Deno.test("parsePipelineYaml rejects record_transform pipelines without pipeline input", () => {
  assertThrows(
    () =>
      parsePipelineYaml(`
stages:
  - name: rewrite
    mode: record_transform
    instructions: rewrite turns
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`),
    PipelineValidationError,
  );
});

Deno.test("parsePipelineYaml throws PipelineParseError for malformed YAML", () => {
  assertThrows(
    () =>
      parsePipelineYaml("stages:\n  - name: a\n    instructions: hi\n  - ["),
    PipelineParseError,
    "Failed to parse pipeline YAML",
  );
});

Deno.test("parsePipelineYaml throws PipelineValidationError for schema mismatch", () => {
  assertThrows(
    () => parsePipelineYaml("stages:\n  - name: stage-only"),
    PipelineValidationError,
    "Pipeline YAML failed schema validation",
  );
});

Deno.test({
  name: "loadPipelineFromFile reads YAML file and validates",
  permissions: { read: true, write: true },
  async fn() {
    const filePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });

    try {
      await Deno.writeTextFile(
        filePath,
        `
name: file-based-pipeline
apiKeyEnv: DATAGEN_OPENAI_API_KEY
httpReferer: https://example.com/datagen
xTitle: Datagen Local Test
stages:
  - instructions: stage one
`,
      );

      const pipeline = await loadPipelineFromFile(filePath);
      assertEquals(pipeline.name, "file-based-pipeline");
      assertEquals(pipeline.apiKeyEnv, "DATAGEN_OPENAI_API_KEY");
      assertEquals(pipeline.httpReferer, "https://example.com/datagen");
      assertEquals(pipeline.xTitle, "Datagen Local Test");
      assertEquals(pipeline.stages[0].instructions, "stage one");

      await assertRejects(
        async () => {
          await Deno.writeTextFile(
            filePath,
            "stages:\n  - name: missing-instructions",
          );
          await loadPipelineFromFile(filePath);
        },
        PipelineValidationError,
      );
    } finally {
      await Deno.remove(filePath).catch(() => {});
    }
  },
});

Deno.test({
  name: "checked-in sample pipeline files parse successfully",
  permissions: { read: true },
  async fn() {
    const samplePaths = [
      "./examples/example.pipeline.yaml",
      "./examples/rp.pipeline.yaml",
      "./examples/sharegpt-rewrite.pipeline.yaml",
      "./examples/05_lua_stage_flags.pipeline.yaml",
      "./examples/06_lua_file_transform.pipeline.yaml",
      "./examples/08_story_object_bridge.pipeline.yaml",
      "./tests/workflows/delegate-sample.pipeline.yaml",
    ];

    for (const samplePath of samplePaths) {
      const pipeline = await loadPipelineFromFile(samplePath);
      assertEquals(typeof pipeline.stages?.length, "number");
    }
  },
});
