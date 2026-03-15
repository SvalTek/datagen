import {
  assertEquals,
  assertExists,
  assertStringIncludes,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import {
  ChatSession,
  type ChatTransport,
  type ChatTransportPayload,
} from "../lib/ChatSession.ts";
import {
  StageExecutionEngine,
  type StageExecutionResult,
} from "../lib/StageExecutionEngine.ts";
import type { StageInput } from "../structures/TaskSchema.ts";

function makeChatSessionFromResponses(
  responses: Array<string | Error>,
  seenPayloads: ChatTransportPayload[],
): ChatSession {
  let callIndex = 0;

  const transport: ChatTransport = {
    endpoint: "mock://stage-execution",
    async request(payload) {
      seenPayloads.push(payload);
      const response = responses[callIndex++];

      if (response instanceof Error) {
        throw response;
      }

      return {
        choices: [{ message: { content: response ?? "{}" } }],
      };
    },
  };

  return new ChatSession("mock-model", {}, transport);
}

function getUserPrompt(payload: ChatTransportPayload): string {
  const userMessage = payload.messages[payload.messages.length - 1];
  return userMessage?.content ?? "";
}

Deno.test("StageExecutionEngine executes stages sequentially and chains prior JSON outputs", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [
      JSON.stringify({ seed: [1, 2] }),
      JSON.stringify({ refined: true }),
    ],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
    completionOptions: { temperature: 0, max_tokens: 128 },
  });

  const stages: StageInput[] = [
    {
      name: "seed",
      instructions: "Generate seed data",
      rules: { include: ["valid json"] },
    },
    {
      name: "refine",
      instructions: "Refine using previous stage output",
      history: "Prior stage output should be visible",
    },
  ];

  const result = await engine.executeStages(stages, { dataset: "customers" });

  assertEquals(result.ok, true);
  assertEquals(result.traces.length, 2);
  assertEquals(result.outputsByStage.seed, { seed: [1, 2] });
  assertEquals(result.outputsByStage.refine, { refined: true });

  const firstPrompt = getUserPrompt(seenPayloads[0]);
  assertStringIncludes(firstPrompt, "Instructions:\nGenerate seed data");
  assertStringIncludes(firstPrompt, "Initial Context (JSON):");
  assertStringIncludes(firstPrompt, '"dataset": "customers"');
  assertStringIncludes(firstPrompt, "Output Contract:");
  assertStringIncludes(firstPrompt, "Respond with JSON only");

  const secondPrompt = getUserPrompt(seenPayloads[1]);
  assertStringIncludes(
    secondPrompt,
    "Instructions:\nRefine using previous stage output",
  );
  assertStringIncludes(
    secondPrompt,
    "Prior Stage Outputs For Chaining (JSON):",
  );
  assertStringIncludes(secondPrompt, '"stageIdentifier": "seed"');
  assertStringIncludes(secondPrompt, '"seed": [');
});

Deno.test("StageExecutionEngine returns structured invalid_json failure when model emits non-JSON", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    ["not-json-output"],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const stages: StageInput[] = [{ name: "seed", instructions: "Emit object" }];
  const result: StageExecutionResult = await engine.executeStages(stages);

  assertEquals(result.ok, false);
  assertEquals(result.traces.length, 1);
  assertEquals(result.outputsByStage, {});
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.stageIdentifier, "seed");
  assertEquals(result.failedStage!.error.stageIdentifier, "seed");
  assertEquals(result.failedStage!.error.stageIndex, 0);
  assertEquals(result.failedStage!.error.kind, "invalid_json");
  assertEquals(result.failedStage!.error.retryable, true);
  assertEquals(result.failedStage!.error.rawModelOutput, "not-json-output");
  assertEquals(result.traces[0].rawModelOutput, "not-json-output");
  assertEquals(result.traces[0].parsedJsonOutput, undefined);
});

Deno.test("StageExecutionEngine returns structured model_call_failed failure when chat call throws", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [new Error("upstream unavailable")],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages([{
    name: "seed",
    instructions: "Emit object",
  }]);

  assertEquals(result.ok, false);
  assertEquals(result.traces.length, 1);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.error.kind, "model_call_failed");
  assertEquals(result.failedStage!.error.stageIdentifier, "seed");
  assertEquals(result.failedStage!.error.stageIndex, 0);
  assertEquals(result.failedStage!.error.retryable, true);
  assertStringIncludes(
    result.failedStage!.error.message,
    "upstream unavailable",
  );
});

Deno.test("StageExecutionEngine validates constrain schema successfully and continues chaining", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [
      JSON.stringify({ user: { name: "Ada", active: true, score: 42 } }),
      JSON.stringify({ done: true }),
    ],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const stages: StageInput[] = [
    {
      name: "validate",
      instructions: "Emit validated object",
      constrain: {
        type: "object",
        shape: {
          user: {
            type: "object",
            shape: {
              name: { type: "string" },
              active: { type: "boolean" },
              score: { type: "number" },
            },
          },
        },
      },
    },
    {
      name: "follow-up",
      instructions: "Use prior output",
    },
  ];

  const result = await engine.executeStages(stages);

  assertEquals(result.ok, true);
  assertEquals(result.traces.length, 2);
  assertEquals(result.outputsByStage.validate, {
    user: { name: "Ada", active: true, score: 42 },
  });
  assertEquals(result.outputsByStage["follow-up"], { done: true });

  const secondPrompt = getUserPrompt(seenPayloads[1]);
  assertStringIncludes(secondPrompt, '"stageIdentifier": "validate"');
});

Deno.test("StageExecutionEngine returns constrain_mismatch failure when parsed JSON violates constrain schema", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [JSON.stringify({ count: "not-a-number" })],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages([
    {
      name: "typed",
      instructions: "Emit typed object",
      constrain: {
        type: "object",
        shape: {
          count: { type: "number" },
        },
      },
    },
  ]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.stageIdentifier, "typed");
  assertEquals(result.failedStage!.error.kind, "constrain_mismatch");
  assertEquals(result.failedStage!.error.stageIdentifier, "typed");
  assertEquals(result.failedStage!.error.stageIndex, 0);
  assertEquals(result.failedStage!.error.retryable, true);
  assertEquals(
    result.failedStage!.error.rawModelOutput,
    JSON.stringify({ count: "not-a-number" }),
  );
  assertEquals(result.traces.length, 1);
  assertEquals(result.traces[0].parsedJsonOutput, undefined);
  assertEquals(result.outputsByStage, {});
});

Deno.test("StageExecutionEngine stops chaining when constrain validation fails", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [
      JSON.stringify({ items: ["x", "y"] }),
      JSON.stringify({ shouldNotRun: true }),
    ],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages([
    {
      name: "first",
      instructions: "Emit object",
      constrain: {
        type: "object",
        shape: {
          items: {
            type: "array",
            items: { type: "number" },
          },
        },
      },
    },
    {
      name: "second",
      instructions: "This stage must not execute",
    },
  ]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.stageIdentifier, "first");
  assertEquals(result.failedStage!.error.kind, "constrain_mismatch");
  assertEquals(result.traces.length, 1);
  assertEquals(seenPayloads.length, 1);
  assertEquals(result.outputsByStage, {});
});

Deno.test("StageExecutionEngine returns constrain_mismatch failure for nested constrain declaration violations", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [JSON.stringify({ user: { name: "Ada", profile: { age: "42" } } })],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages([
    {
      name: "nested",
      instructions: "Emit nested object",
      constrain: {
        type: "object",
        shape: {
          user: {
            type: "object",
            shape: {
              name: { type: "string" },
              profile: {
                type: "object",
                shape: {
                  age: { type: "number" },
                },
              },
            },
          },
        },
      },
    },
  ]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.error.kind, "constrain_mismatch");
  assertStringIncludes(result.failedStage!.error.message, "expected number");
  assertEquals(result.failedStage!.error.stageIdentifier, "nested");
  assertEquals(result.failedStage!.error.stageIndex, 0);
});

Deno.test("StageExecutionEngine supports multi-stage chain with constrain on stage 2", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [
      JSON.stringify({ seed: [1, 2, 3] }),
      JSON.stringify({ summary: { count: 3 } }),
    ],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });
  const result = await engine.executeStages([
    { name: "stage-1", instructions: "Generate seed data" },
    {
      name: "stage-2",
      instructions: "Summarize seed",
      constrain: {
        type: "object",
        shape: {
          summary: {
            type: "object",
            shape: {
              count: { type: "number" },
            },
          },
        },
      },
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals(result.outputsByStage["stage-2"], { summary: { count: 3 } });
  assertEquals(seenPayloads.length, 2);
  const secondPrompt = getUserPrompt(seenPayloads[1]);
  assertStringIncludes(secondPrompt, '"stageIdentifier": "stage-1"');
});

Deno.test("StageExecutionEngine normalizes invalid constrain declaration failures", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [JSON.stringify({ values: [1, 2, 3] })],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });
  const result = await engine.executeStages([
    {
      name: "invalid-constrain",
      instructions: "Emit array values",
      constrain: {
        type: "object",
        shape: {
          values: {
            type: "array",
            // invalid on purpose: missing `items`
          } as unknown as any,
        },
      },
    },
  ]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.error.kind, "constrain_mismatch");
  assertStringIncludes(
    result.failedStage!.error.message,
    "Invalid stage constrain declaration:",
  );
  assertStringIncludes(
    result.failedStage!.error.message,
    "Invalid constrain declaration",
  );
  assertEquals(result.failedStage!.error.stageIdentifier, "invalid-constrain");
  assertEquals(result.failedStage!.error.stageIndex, 0);
});

Deno.test("StageExecutionEngine clears ChatSession history between executeStages runs", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [
      JSON.stringify({ run: 1 }),
      JSON.stringify({ run: 2 }),
    ],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  await engine.executeStages([{ name: "first", instructions: "Run one" }]);
  await engine.executeStages([{ name: "second", instructions: "Run two" }]);

  assertEquals(seenPayloads.length, 2);
  assertEquals(seenPayloads[0].messages.length, 1);
  assertEquals(seenPayloads[1].messages.length, 1);
  assertStringIncludes(
    getUserPrompt(seenPayloads[1]),
    "Instructions:\nRun two",
  );
});

Deno.test("StageExecutionEngine applies per-stage reasoning to transport think flag", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const transport: ChatTransport = {
    endpoint: "mock://stage-execution",
    async request(payload) {
      seenPayloads.push(payload);
      return {
        choices: [{ message: { content: JSON.stringify({ ok: true }) } }],
      };
    },
  };
  const session = new ChatSession(
    "mock-model",
    { reasoning_mode: "think" },
    transport,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });
  const result = await engine.executeStages([
    { name: "stage-1", instructions: "No reasoning" },
    { name: "stage-2", instructions: "With reasoning", reasoning: true },
  ]);

  assertEquals(result.ok, true);
  assertEquals(seenPayloads[0].think, false);
  assertEquals(seenPayloads[1].think, true);
  assertEquals(seenPayloads[0].extra_body, undefined);
  assertEquals(seenPayloads[1].extra_body, undefined);
});

Deno.test("StageExecutionEngine reasoning false overrides inherited true defaults", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const transport: ChatTransport = {
    endpoint: "mock://stage-execution",
    async request(payload) {
      seenPayloads.push(payload);
      return {
        choices: [{ message: { content: JSON.stringify({ ok: true }) } }],
      };
    },
  };
  const session = new ChatSession(
    "mock-model",
    { think: true, reasoning_mode: "think" },
    transport,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });
  const result = await engine.executeStages([
    { name: "stage-1", instructions: "Force off", reasoning: false },
    { name: "stage-2", instructions: "Force on", reasoning: true },
  ]);

  assertEquals(result.ok, true);
  assertEquals(seenPayloads[0].think, false);
  assertEquals(seenPayloads[1].think, true);
});

Deno.test("StageExecutionEngine accepts JSON wrapped in markdown code fences", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    ['```json\n{"ok":true}\n```'],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });
  const result = await engine.executeStages([
    { name: "fenced-json", instructions: "Return fenced JSON" },
  ]);

  assertEquals(result.ok, true);
  assertEquals(result.outputsByStage["fenced-json"], { ok: true });
});

Deno.test("StageExecutionEngine accepts JSON when response starts with opening json fence only", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    ['```json\n[{"characterId":"char-011"}]'],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });
  const result = await engine.executeStages([
    { name: "fence-open-only", instructions: "Return JSON array" },
  ]);

  assertEquals(result.ok, true);
  assertEquals(result.outputsByStage["fence-open-only"], [{
    characterId: "char-011",
  }]);
});

Deno.test("StageExecutionEngine accepts JSON from embedded fenced block with surrounding text", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    ['Here is the data:\n```json\n{"ok":true}\n```\nDone.'],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });
  const result = await engine.executeStages([
    { name: "embedded-fence", instructions: "Return JSON object" },
  ]);

  assertEquals(result.ok, true);
  assertEquals(result.outputsByStage["embedded-fence"], { ok: true });
});

Deno.test("StageExecutionEngine repairs js-style concatenated string literals in json output", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [`{
      "content": "<reasoning>Plan</reasoning> Hello" +
        " there"
    }`],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });
  const result = await engine.executeStages([{
    name: "concat-json",
    instructions: "Return JSON object",
  }]);

  assertEquals(result.ok, true);
  assertEquals(result.outputsByStage["concat-json"], {
    content: "<reasoning>Plan</reasoning> Hello there",
  });
});

Deno.test("StageExecutionEngine iter mode transforms each previous-array item as single-object input", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [
      JSON.stringify([{ id: 1, value: "a" }, { id: 2, value: "b" }]),
      JSON.stringify({ id: 1, value: "a", normalized: true }),
      JSON.stringify({ id: 2, value: "b", normalized: true }),
    ],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    { name: "normalize", mode: "iter", instructions: "Normalize one record" },
  ]);

  assertEquals(result.ok, true);
  assertEquals(result.outputsByStage.seed, [{ id: 1, value: "a" }, {
    id: 2,
    value: "b",
  }]);
  assertEquals(result.outputsByStage.normalize, [
    { id: 1, value: "a", normalized: true },
    { id: 2, value: "b", normalized: true },
  ]);
  assertEquals(seenPayloads.length, 3);

  const iterPrompt1 = getUserPrompt(seenPayloads[1]);
  const iterPrompt2 = getUserPrompt(seenPayloads[2]);
  assertStringIncludes(iterPrompt1, "Current Iter Item (JSON):");
  assertStringIncludes(iterPrompt1, '"id": 1');
  assertStringIncludes(iterPrompt2, '"id": 2');
});

Deno.test("StageExecutionEngine iter mode fails when previous stage output is not an array", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [JSON.stringify({ not: "array" })],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit object" },
    { name: "normalize", mode: "iter", instructions: "Normalize one record" },
  ]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.error.kind, "invalid_iter_input");
  assertStringIncludes(
    result.failedStage!.error.message,
    "requires previous stage 'seed' output to be a JSON array",
  );
});

Deno.test("StageExecutionEngine iter mode requires each item output to be a single object", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [
      JSON.stringify([{ id: 1 }]),
      JSON.stringify([{ id: 1, normalized: true }]),
    ],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    { name: "normalize", mode: "iter", instructions: "Normalize one record" },
  ]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.error.kind, "invalid_json");
  assertStringIncludes(
    result.failedStage!.error.message,
    "Iter item 0 failed:",
  );
  assertStringIncludes(
    result.failedStage!.error.message,
    "must be a single JSON object",
  );
});

Deno.test("StageExecutionEngine record_transform rewrites pipeline input records and accumulates warnings", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [
      "<reasoning>A</reasoning>\nHello",
      "   ",
    ],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages(
    [
      {
        name: "rewrite",
        mode: "record_transform",
        instructions: "Rewrite assistant turns.",
        transform: {
          kind: "conversation_rewrite",
          conversationsPath: "conversations",
          roleField: "from",
          contentField: "value",
          targetRoles: ["gpt"],
        },
      },
    ],
    undefined,
    [
      {
        conversations: [
          { from: "human", value: "Hi" },
          { from: "gpt", value: "Hello" },
        ],
      },
      {
        conversations: [
          { from: "human", value: "Hi again" },
          { from: "gpt", value: "Original" },
        ],
      },
    ],
  );

  assertEquals(result.ok, true);
  assertEquals((result.outputsByStage.rewrite as any[])[0].conversations[1].value, "<reasoning>A</reasoning>\nHello");
  assertEquals((result.outputsByStage.rewrite as any[])[1].conversations[1].value, "Original");
  assertEquals(result.warnings.length, 1);
  assertEquals(result.warnings[0].recordIndex, 1);
  assertEquals(result.warnings[0].kind, "empty_output");
});

Deno.test("StageExecutionEngine record_transform can consume previous stage arrays", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [
      JSON.stringify([
        {
          conversations: [
            { from: "human", value: "Hi" },
            { from: "gpt", value: "Hello" },
          ],
        },
      ]),
      "<reasoning>X</reasoning>\nHello",
    ],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    {
      name: "rewrite",
      mode: "record_transform",
      instructions: "Rewrite assistant turns.",
      transform: {
        kind: "conversation_rewrite",
        conversationsPath: "conversations",
        roleField: "from",
        contentField: "value",
        targetRoles: ["gpt"],
      },
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals((result.outputsByStage.rewrite as any[])[0].conversations[1].value, "<reasoning>X</reasoning>\nHello");
});

Deno.test("StageExecutionEngine record_transform fails when required input array is missing", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses([], []),
  });

  const result = await engine.executeStages([
    {
      name: "rewrite",
      mode: "record_transform",
      instructions: "Rewrite assistant turns.",
      transform: {
        kind: "conversation_rewrite",
        conversationsPath: "conversations",
        roleField: "from",
        contentField: "value",
        targetRoles: ["gpt"],
      },
    },
  ]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.error.kind, "invalid_record_transform_input");
});

Deno.test("StageExecutionEngine batch validator failure hard-fails stage by default", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [JSON.stringify({ content: "hello" })],
      [],
    ),
  });

  const result = await engine.executeStages([{
    name: "validate",
    instructions: "Emit object",
    validate: {
      rules: [{ path: "content", kind: "contains", value: "<reasoning>" }],
    },
  }]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.error.kind, "validator_mismatch");
});

Deno.test("StageExecutionEngine batch validator warning mode accumulates warnings and continues", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [JSON.stringify({ content: "hello" }), JSON.stringify({ done: true })],
      [],
    ),
  });

  const result = await engine.executeStages([
    {
      name: "validate",
      instructions: "Emit object",
      validate: {
        onFailure: "warn",
        rules: [{ path: "content", kind: "contains", value: "<reasoning>" }],
      },
    },
    {
      name: "follow-up",
      instructions: "Continue",
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals(result.warnings.length, 1);
  assertEquals(result.warnings[0].kind, "validator_mismatch.contains");
  assertEquals(result.outputsByStage["follow-up"], { done: true });
});

Deno.test("StageExecutionEngine record_transform validator failures keep original turn and capture subtraces", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [JSON.stringify({ content: "Hello" })],
      seenPayloads,
    ),
  });

  const result = await engine.executeStages(
    [{
      name: "rewrite",
      mode: "record_transform",
      instructions: "Rewrite assistant turns.",
      validate: {
        rules: [{ path: "content", kind: "contains", value: "<reasoning>" }],
      },
      transform: {
        kind: "conversation_rewrite",
        conversationsPath: "conversations",
        roleField: "from",
        contentField: "value",
        targetRoles: ["gpt"],
      },
    }],
    undefined,
    [{
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello" },
      ],
    }],
  );

  assertEquals(result.ok, true);
  assertEquals((result.outputsByStage.rewrite as any[])[0].conversations[1].value, "Hello");
  assertEquals(result.warnings.length, 1);
  assertEquals(result.warnings[0].kind, "validator_mismatch.contains");
  assertEquals(result.traces[0].subtraces?.length, 1);
  assertEquals((result.traces[0].subtraces?.[0] as any).validationIssues[0].kind, "contains");
});

Deno.test("StageExecutionEngine record_transform fails fast on authorization errors", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [new Error("HTTP 401: Unauthorized")],
      [],
    ),
  });

  const result = await engine.executeStages(
    [{
      name: "rewrite",
      mode: "record_transform",
      instructions: "Rewrite assistant turns.",
      transform: {
        kind: "conversation_rewrite",
        conversationsPath: "conversations",
        roleField: "from",
        contentField: "value",
        targetRoles: ["gpt"],
      },
    }],
    undefined,
    [{
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello" },
      ],
    }],
  );

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.error.kind, "model_call_failed");
  assertEquals(result.failedStage!.error.retryable, false);
  assertStringIncludes(result.failedStage!.error.message, "401");
  assertEquals(result.traces.length, 2);
  assertEquals(result.traces[1].success, false);
});

Deno.test("StageExecutionEngine iter retries after invalid json and succeeds", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [
        JSON.stringify([{ id: 1 }]),
        "not-json",
        JSON.stringify({ id: 1, normalized: true }),
      ],
      seenPayloads,
    ),
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    {
      name: "normalize",
      mode: "iter",
      instructions: "Normalize one record",
      retry: {
        enabled: true,
        maxAttempts: 2,
      },
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals((result.outputsByStage.normalize as any[])[0].normalized, true);
  assertEquals(result.traces.length, 3);
  assertEquals(result.traces[1].attempt, 1);
  assertEquals(result.traces[2].attempt, 2);
  assertStringIncludes(
    getUserPrompt(seenPayloads[2]),
    "Previous Attempt Failed:",
  );
});

Deno.test("StageExecutionEngine iter retries after validator mismatch and succeeds", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [
        JSON.stringify([{ id: 1 }]),
        JSON.stringify({ normalized: false }),
        JSON.stringify({ normalized: true }),
      ],
      seenPayloads,
    ),
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    {
      name: "normalize",
      mode: "iter",
      instructions: "Normalize one record",
      retry: {
        enabled: true,
        maxAttempts: 2,
      },
      validate: {
        rules: [{ path: "normalized", kind: "equals", value: true }],
      },
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals((result.outputsByStage.normalize as any[])[0].normalized, true);
  assertStringIncludes(
    getUserPrompt(seenPayloads[2]),
    "Failure Kind: validator_mismatch",
  );
  assertStringIncludes(
    getUserPrompt(seenPayloads[2]),
    "Next Attempt: 2 of 2",
  );
});

Deno.test("StageExecutionEngine iter still fails after exhausted retries", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [
        JSON.stringify([{ id: 1 }]),
        "not-json",
        "still-not-json",
      ],
      [],
    ),
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    {
      name: "normalize",
      mode: "iter",
      instructions: "Normalize one record",
      retry: {
        enabled: true,
        maxAttempts: 2,
      },
    },
  ]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.traces.length, 3);
  assertEquals(result.failedStage!.error.kind, "invalid_json");
});

Deno.test("StageExecutionEngine iter supports not_equal_to_path comparison validators", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [
        JSON.stringify([{ original: "Hello world" }]),
        JSON.stringify({ original: "Hello world", rewritten: "Hello there" }),
      ],
      [],
    ),
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    {
      name: "rewrite",
      mode: "iter",
      instructions: "Rewrite one record",
      validate: {
        rules: [{
          path: "rewritten",
          kind: "not_equal_to_path",
          otherPath: "original",
        }],
      },
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals((result.outputsByStage.rewrite as any[])[0].rewritten, "Hello there");
});

Deno.test("StageExecutionEngine iter reports ref_missing when ref validators are used without refs", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [
        JSON.stringify([{ content: "Hello there" }]),
        JSON.stringify({ content: "Hello there" }),
      ],
      [],
    ),
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    {
      name: "rewrite",
      mode: "iter",
      instructions: "Rewrite one record",
      validate: {
        rules: [{
          path: "content",
          kind: "max_similarity_to_ref",
          ref: "previous_same_role_turn.value",
          threshold: 0.82,
        }],
      },
    },
  ]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertEquals(result.failedStage!.error.kind, "validator_mismatch");
  assertStringIncludes(result.failedStage!.error.message, "Reference 'previous_same_role_turn'");
});

Deno.test("StageExecutionEngine iter supports scoped comparison validators", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [
        JSON.stringify([{
          original: "<reasoning>chain</reasoning> Final answer A",
        }]),
        JSON.stringify({
          original: "<reasoning>chain</reasoning> Final answer A",
          rewritten: "<reasoning>chain</reasoning> Final answer B",
        }),
      ],
      [],
    ),
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    {
      name: "rewrite",
      mode: "iter",
      instructions: "Rewrite one record",
      validate: {
        rules: [{
          path: "rewritten",
          kind: "max_similarity_to_path",
          otherPath: "original",
          threshold: 0.82,
          scope: {
            excludePatterns: [{
              pattern: "<reasoning>[\\s\\S]*?</reasoning>",
            }],
          },
        }],
      },
    },
  ]);

  assertEquals(result.ok, true);
});

Deno.test("StageExecutionEngine iter supports must_change_from_path and retries no-op outputs", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [
        JSON.stringify([{ original: "Hello world" }]),
        JSON.stringify({ original: "Hello world", rewritten: "Hello world" }),
        JSON.stringify({ original: "Hello world", rewritten: "Hello there" }),
      ],
      seenPayloads,
    ),
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    {
      name: "rewrite",
      mode: "iter",
      instructions: "Rewrite one record",
      retry: {
        enabled: true,
        maxAttempts: 2,
      },
      validate: {
        rules: [{
          path: "rewritten",
          kind: "must_change_from_path",
          otherPath: "original",
        }],
      },
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals((result.outputsByStage.rewrite as any[])[0].rewritten, "Hello there");
  assertStringIncludes(
    getUserPrompt(seenPayloads[2]),
    "Value must differ from value at path 'original'",
  );
});

Deno.test("StageExecutionEngine emits one iter progress event per item", async () => {
  const progressEvents: Array<{
    stageIdentifier: string;
    current: number;
    total?: number;
    warningsSoFar: number;
  }> = [];
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [
        JSON.stringify([{ id: 1 }, { id: 2 }]),
        JSON.stringify({ id: 1, normalized: true }),
        JSON.stringify({ id: 2, normalized: true }),
      ],
      [],
    ),
    progress: {
      onProgress: (event) =>
        progressEvents.push({
          stageIdentifier: event.stageIdentifier,
          current: event.current,
          total: event.total,
          warningsSoFar: event.warningsSoFar,
        }),
    },
  });

  const result = await engine.executeStages([
    { name: "seed", instructions: "Emit array" },
    { name: "normalize", mode: "iter", instructions: "Normalize one item" },
  ]);

  assertEquals(result.ok, true);
  assertEquals(progressEvents, [
    { stageIdentifier: "normalize", current: 1, total: 2, warningsSoFar: 0 },
    { stageIdentifier: "normalize", current: 2, total: 2, warningsSoFar: 0 },
  ]);
});

Deno.test("StageExecutionEngine emits record_transform progress events with warning counts", async () => {
  const progressEvents: Array<{
    current: number;
    total?: number;
    warningsSoFar: number;
  }> = [];
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [
        "<reasoning>One</reasoning>\nHello",
        "   ",
      ],
      [],
    ),
    progress: {
      onProgress: (event) =>
        progressEvents.push({
          current: event.current,
          total: event.total,
          warningsSoFar: event.warningsSoFar,
        }),
    },
  });

  const result = await engine.executeStages(
    [{
      name: "rewrite",
      mode: "record_transform",
      instructions: "Rewrite assistant turns",
      transform: {
        kind: "conversation_rewrite",
        conversationsPath: "conversations",
        roleField: "from",
        contentField: "value",
        targetRoles: ["gpt"],
      },
    }],
    undefined,
    [
      {
        conversations: [
          { from: "human", value: "Hi" },
          { from: "gpt", value: "Hello" },
        ],
      },
      {
        conversations: [
          { from: "human", value: "Hi again" },
          { from: "gpt", value: "Original" },
        ],
      },
    ],
  );

  assertEquals(result.ok, true);
  assertEquals(progressEvents, [
    { current: 1, total: 2, warningsSoFar: 0 },
    { current: 2, total: 2, warningsSoFar: 1 },
  ]);
});

Deno.test("StageExecutionEngine emits warnings as they are generated", async () => {
  const warnings: Array<{
    stageIdentifier: string;
    recordIndex?: number;
    kind: string;
  }> = [];
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses(
      [
        "<reasoning>One</reasoning>\nHello",
        "   ",
      ],
      [],
    ),
    progress: {
      onWarning: (warning) =>
        warnings.push({
          stageIdentifier: warning.stageIdentifier,
          recordIndex: warning.recordIndex,
          kind: warning.kind,
        }),
    },
  });

  const result = await engine.executeStages(
    [{
      name: "rewrite",
      mode: "record_transform",
      instructions: "Rewrite assistant turns",
      transform: {
        kind: "conversation_rewrite",
        conversationsPath: "conversations",
        roleField: "from",
        contentField: "value",
        targetRoles: ["gpt"],
      },
    }],
    undefined,
    [
      {
        conversations: [
          { from: "human", value: "Hi" },
          { from: "gpt", value: "Hello" },
        ],
      },
      {
        conversations: [
          { from: "human", value: "Hi again" },
          { from: "gpt", value: "Original" },
        ],
      },
    ],
  );

  assertEquals(result.ok, true);
  assertEquals(warnings, [
    { stageIdentifier: "rewrite", recordIndex: 1, kind: "empty_output" },
  ]);
  assertEquals(result.warnings.length, 1);
});

Deno.test("StageExecutionEngine supports DAG dependencies with fan-out and fan-in", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const session = makeChatSessionFromResponses(
    [
      JSON.stringify({ seed: true }),
      JSON.stringify({ a: 1 }),
      JSON.stringify({ b: 2 }),
      JSON.stringify({ merged: true }),
    ],
    seenPayloads,
  );

  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages([
    { id: "seed", instructions: "Seed data" },
    { id: "a", instructions: "Branch A", dependsOn: ["seed"] },
    { id: "b", instructions: "Branch B", dependsOn: ["seed"] },
    { id: "merge", instructions: "Merge outputs", dependsOn: ["a", "b"] },
  ]);

  assertEquals(result.ok, true);
  assertEquals(Object.keys(result.outputsByStage), ["seed", "a", "b", "merge"]);
  assertEquals(result.stageStatuses, {
    seed: "executed",
    a: "executed",
    b: "executed",
    merge: "executed",
  });
  assertEquals(result.dependencyGraph, {
    seed: [],
    a: ["seed"],
    b: ["seed"],
    merge: ["a", "b"],
  });

  const mergePrompt = getUserPrompt(seenPayloads[3]);
  assertStringIncludes(mergePrompt, '"stageIdentifier": "a"');
  assertStringIncludes(mergePrompt, '"stageIdentifier": "b"');
});

Deno.test("StageExecutionEngine skips conditional stages and blocks dependents", async () => {
  const session = makeChatSessionFromResponses(
    [JSON.stringify({ enabled: false })],
    [],
  );
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
  });

  const result = await engine.executeStages([
    { id: "seed", instructions: "Seed data" },
    {
      id: "conditional",
      instructions: "Run only when enabled",
      dependsOn: ["seed"],
      when: {
        path: "outputsByStage.seed.enabled",
        equals: true,
      },
    },
    {
      id: "downstream",
      instructions: "Depends on conditional",
      dependsOn: ["conditional"],
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals(result.stageStatuses, {
    seed: "executed",
    conditional: "skipped",
    downstream: "blocked",
  });
  assertEquals(result.outputsByStage.seed, { enabled: false });
  assertEquals(result.outputsByStage.conditional, undefined);
});

Deno.test("StageExecutionEngine reports dependency cycle failures", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses([], []),
  });

  const result = await engine.executeStages([
    { id: "a", instructions: "A", dependsOn: ["b"] },
    { id: "b", instructions: "B", dependsOn: ["a"] },
  ]);

  assertEquals(result.ok, false);
  assertExists(result.failedStage);
  assertStringIncludes(result.failedStage!.error.message, "Cycle detected");
});

Deno.test("StageExecutionEngine workflow_delegate maps input and stores delegated output", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses([JSON.stringify({ shouldRun: true })], []),
    runDelegatedWorkflow: async (request) => {
      assertEquals(request.delegate.workflowPath, "./child.pipeline.yaml");
      assertEquals(request.mappedInput, true);
      return {
        ok: true,
        workflowPath: request.delegate.workflowPath,
        resolvedWorkflowPath: "E:/tmp/child.pipeline.yaml",
        durationMs: 12,
        model: "child-model",
        provider: "openai",
        endpoint: "http://child.local/",
        finalStageKey: "judge",
        result: {
          ok: true,
          traces: [],
          outputsByStage: {
            judge: {
              route: "audit",
              score: 0.82,
            },
          },
          warnings: [],
          stageStatuses: { judge: "executed" },
          dependencyGraph: { judge: [] },
        },
      };
    },
  });

  const result = await engine.executeStages([
    {
      id: "seed",
      instructions: "Seed run flag",
    },
    {
      id: "delegate_judge",
      mode: "workflow_delegate",
      dependsOn: ["seed"],
      instructions: "Delegate to child workflow",
      delegate: {
        workflowPath: "./child.pipeline.yaml",
        inputFromPath: "outputsByStage.seed.shouldRun",
        outputFrom: "final_stage_output",
      },
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals(result.outputsByStage.delegate_judge, {
    route: "audit",
    score: 0.82,
  });
  assertEquals(result.stageStatuses.delegate_judge, "executed");
  assertExists(result.traces.find((trace) =>
    trace.stageIdentifier === "delegate_judge" && !!trace.delegatedRun
  ));
});

Deno.test("StageExecutionEngine workflow_delegate onFailure=warn emits warning and returns null output", async () => {
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: makeChatSessionFromResponses([JSON.stringify({ enabled: true })], []),
    runDelegatedWorkflow: async () => ({
      ok: false,
      workflowPath: "./child.pipeline.yaml",
      resolvedWorkflowPath: "E:/tmp/child.pipeline.yaml",
      durationMs: 10,
      finalStageKey: "judge",
      warningMessage: "Child workflow failed",
      result: {
        ok: false,
        traces: [],
        outputsByStage: {},
        warnings: [],
        stageStatuses: { judge: "blocked" },
        dependencyGraph: { judge: [] },
        failedStage: {
          stageIdentifier: "judge",
          stageIndex: 0,
          error: {
            kind: "model_call_failed",
            stageIdentifier: "judge",
            stageIndex: 0,
            message: "Child workflow failed",
            retryable: false,
          },
        },
      },
    }),
  });

  const result = await engine.executeStages([
    { id: "seed", instructions: "Seed" },
    {
      id: "delegate_judge",
      mode: "workflow_delegate",
      dependsOn: ["seed"],
      instructions: "Delegate to child workflow",
      delegate: {
        workflowPath: "./child.pipeline.yaml",
        inputFromPath: "outputsByStage.seed.enabled",
        onFailure: "warn",
      },
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals(result.outputsByStage.delegate_judge, null);
  assertEquals(result.warnings.some((warning) => warning.kind === "delegated_workflow_failed"), true);
});

Deno.test("StageExecutionEngine preserves iter output order under parallelism", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const transport: ChatTransport = {
    endpoint: "mock://parallel-order",
    async request(payload) {
      seenPayloads.push(payload);
      const userPrompt = getUserPrompt(payload);
      if (userPrompt.includes("Emit array")) {
        return {
          choices: [{ message: { content: JSON.stringify([{ id: 1 }, { id: 2 }, { id: 3 }]) } }],
        };
      }

      const idMatch = userPrompt.match(/"id":\s*(\d+)/);
      const id = idMatch ? Number(idMatch[1]) : 0;
      const delayMs = id === 1 ? 40 : id === 2 ? 10 : 1;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      return {
        choices: [{ message: { content: JSON.stringify({ id, normalized: true }) } }],
      };
    },
  };

  const session = new ChatSession("mock-model", {}, transport);
  const engine = new StageExecutionEngine({
    model: "mock-model",
    chatSession: session,
    globalParallelism: 3,
  });

  const result = await engine.executeStages([
    { id: "seed", instructions: "Emit array" },
    {
      id: "normalize",
      mode: "iter",
      instructions: "Normalize item",
      parallelism: 3,
    },
  ]);

  assertEquals(result.ok, true);
  assertEquals(result.outputsByStage.normalize, [
    { id: 1, normalized: true },
    { id: 2, normalized: true },
    { id: 3, normalized: true },
  ]);
});
