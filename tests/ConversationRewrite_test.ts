import {
  assertEquals,
  assertStringIncludes,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import {
  ChatSession,
  type ChatTransport,
  type ChatTransportPayload,
} from "../lib/ChatSession.ts";
import {
  buildConversationRewritePrompt,
  rewriteConversationRecord,
} from "../lib/ConversationRewrite.ts";
import type { StageInput } from "../structures/TaskSchema.ts";

function makeSessionFactory(
  responses: Array<string | Error>,
  seenPayloads: ChatTransportPayload[],
  defaultOptions: ConstructorParameters<typeof ChatSession>[1] = {},
): () => ChatSession {
  let callIndex = 0;
  const transport: ChatTransport = {
    endpoint: "mock://rewrite",
    async request(payload) {
      seenPayloads.push(payload);
      const response = responses[callIndex++];
      if (response instanceof Error) throw response;
      return {
        choices: [{ message: { content: response ?? "{}" } }],
      };
    },
  };

  const baseSession = new ChatSession("mock-model", defaultOptions, transport);
  return () => baseSession.fork();
}

Deno.test("buildConversationRewritePrompt includes prior turns and current target turn", () => {
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn",
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
    mode: "record_transform",
  };

  const prompt = buildConversationRewritePrompt(
    stage,
    [{ from: "human", value: "Hi" }],
    { from: "gpt", value: "Hello" },
  );

  assertStringIncludes(prompt, "Rewrite Task:");
  assertStringIncludes(prompt, "Prior Conversation Turns:");
  assertStringIncludes(prompt, '"from": "human"');
  assertStringIncludes(prompt, "Current Target Turn:");
  assertStringIncludes(prompt, "Rewrite Output Rules:");
  assertStringIncludes(prompt, "Return only the full rewritten target turn text.");
  assertStringIncludes(prompt, "Do not return JSON.");
  assertStringIncludes(prompt, "Do not include any text before or after the rewritten turn.");
  assertEquals(
    prompt.indexOf("Prior Conversation Turns:") < prompt.indexOf("Current Target Turn:"),
    true,
  );
  assertEquals(
    prompt.indexOf("Current Target Turn:") < prompt.indexOf("Rewrite Task:"),
    true,
  );
});

Deno.test("buildConversationRewritePrompt places retry feedback before the final rewrite output rules block", () => {
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn",
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
    mode: "record_transform",
  };

  const prompt = buildConversationRewritePrompt(
    stage,
    [{ from: "human", value: "Hi" }],
    { from: "gpt", value: "Hello" },
    "Previous Attempt Failed:\n- Attempt: 1\n- Failure Kind: invalid_json",
  );

  const targetIndex = prompt.indexOf("Current Target Turn:");
  const taskIndex = prompt.indexOf("Rewrite Task:");
  const retryIndex = prompt.indexOf("Previous Attempt Failed:");
  const rulesIndex = prompt.indexOf("Rewrite Output Rules:");
  assertEquals(targetIndex >= 0, true);
  assertEquals(taskIndex > targetIndex, true);
  assertEquals(retryIndex >= 0, true);
  assertEquals(rulesIndex > retryIndex, true);
  assertEquals(prompt.trimEnd().endsWith("- Preserve the target turn content while applying the requested rewrite."), true);
});

Deno.test("rewriteConversationRecord rewrites only target turns and uses prior rewritten turns", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn with a reasoning prefix.",
    mode: "record_transform",
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      id: 1,
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
        { from: "human", value: "Tell me more" },
        { from: "gpt", value: "More details" },
      ],
    },
    stage,
    makeSessionFactory(
      [
        "<reasoning>First</reasoning>\nHello there",
        "<reasoning>Second</reasoning>\nMore details",
      ],
      seenPayloads,
    ),
  );

  assertEquals(result.warnings, []);
  assertEquals((result.record as any).conversations[0].value, "Hi");
  assertEquals(
    (result.record as any).conversations[1].value,
    "<reasoning>First</reasoning>\nHello there",
  );
  assertEquals(
    (result.record as any).conversations[3].value,
    "<reasoning>Second</reasoning>\nMore details",
  );

  const secondPrompt = seenPayloads[1].messages[seenPayloads[1].messages.length - 1]
    .content;
  assertStringIncludes(
    secondPrompt,
    "<reasoning>First</reasoning>\\nHello there",
  );
});

Deno.test("rewriteConversationRecord keeps original turn and emits warning on empty output", async () => {
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    mode: "record_transform",
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
      ],
    },
    stage,
    makeSessionFactory(["   "], []),
  );

  assertEquals((result.record as any).conversations[1].value, "Hello there");
  assertEquals(result.warnings.length, 1);
  assertEquals(result.warnings[0].kind, "empty_output");
});

Deno.test("rewriteConversationRecord reasoning false overrides inherited think default", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    reasoning: false,
    mode: "record_transform",
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
      ],
    },
    stage,
    makeSessionFactory(
      ["Hello there, rewritten"],
      seenPayloads,
      { think: true, reasoning_mode: "think" },
    ),
  );

  assertEquals(result.warnings.length, 0);
  assertEquals(seenPayloads[0].think, false);
  assertEquals(seenPayloads[0].extra_body, undefined);
});

Deno.test("rewriteConversationRecord uses openai reasoning payload when configured", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    reasoning: true,
    mode: "record_transform",
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
      ],
    },
    stage,
    makeSessionFactory(
      ["Hello there, rewritten"],
      seenPayloads,
      { reasoning_mode: "openai" },
    ),
  );

  assertEquals(result.warnings.length, 0);
  assertEquals(seenPayloads[0].think, undefined);
  assertEquals(seenPayloads[0].extra_body, {
    reasoning: { enabled: true },
  });
});

Deno.test("rewriteConversationRecord keeps original turn when turn-level validators fail", async () => {
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    mode: "record_transform",
    validate: {
      rules: [{
        name: "reasoning_prefix",
        path: "content",
        kind: "contains",
        value: "<reasoning>",
      }],
    },
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
      ],
    },
    stage,
    makeSessionFactory(["Hello there"], []),
  );

  assertEquals((result.record as any).conversations[1].value, "Hello there");
  assertEquals(result.warnings.length, 1);
  assertEquals(result.warnings[0].kind, "validator_mismatch.contains");
  assertEquals(result.warnings[0].validatorName, "reasoning_prefix");
  assertEquals(result.traces.length, 1);
  assertEquals(result.traces[0].success, false);
  assertEquals(result.traces[0].validationIssues?.[0].kind, "contains");
});

Deno.test("rewriteConversationRecord retries failed turn rewrite and succeeds on second attempt", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    mode: "record_transform",
    retry: {
      enabled: true,
      maxAttempts: 2,
    },
    validate: {
      rules: [{
        path: "content",
        kind: "contains",
        value: "<reasoning>",
        hint: "Start with a <reasoning> block before the final answer.",
      }],
    },
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
      ],
    },
    stage,
    makeSessionFactory([
      "Hello there",
      "<reasoning>ok</reasoning>\nHello there",
    ], seenPayloads),
  );

  assertEquals(result.warnings.length, 0);
  assertEquals(result.traces.length, 2);
  assertEquals(result.traces[0].attempt, 1);
  assertEquals(result.traces[1].attempt, 2);
  assertEquals(result.traces[1].success, true);
  assertEquals(
    (result.record as any).conversations[1].value,
    "<reasoning>ok</reasoning>\nHello there",
  );
  const secondPrompt = seenPayloads[1].messages[seenPayloads[1].messages.length - 1]
    .content;
  assertStringIncludes(secondPrompt, "Previous Attempt Failed:");
  assertStringIncludes(secondPrompt, "Failure Kind: validator_mismatch");
  assertStringIncludes(secondPrompt, "Retry Status:");
  assertStringIncludes(secondPrompt, "Next Attempt: 2 of 2");
  assertStringIncludes(
    secondPrompt,
    "This is the final retry. Prioritize satisfying the correction requirements exactly.",
  );
  assertStringIncludes(secondPrompt, "Hints:");
  assertStringIncludes(
    secondPrompt,
    "Start with a <reasoning> block before the final answer.",
  );
});

Deno.test("rewriteConversationRecord strips one surrounding markdown fence block", async () => {
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    mode: "record_transform",
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
      ],
    },
    stage,
    makeSessionFactory([
      "```text\n<reasoning>Plan</reasoning> Hello there\n```",
    ], []),
  );

  assertEquals(result.warnings, []);
  assertEquals(
    (result.record as any).conversations[1].value,
    "<reasoning>Plan</reasoning> Hello there",
  );
});

Deno.test("rewriteConversationRecord keeps original turn after exhausted retries", async () => {
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    mode: "record_transform",
    retry: {
      enabled: true,
      maxAttempts: 2,
    },
    validate: {
      rules: [{
        path: "content",
        kind: "contains",
        value: "<reasoning>",
      }],
    },
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
      ],
    },
    stage,
    makeSessionFactory([
      "Hello there",
      "Still wrong",
    ], []),
  );

  assertEquals(result.warnings.length, 1);
  assertEquals(result.warnings[0].attempt, 2);
  assertEquals(result.warnings[0].maxAttempts, 2);
  assertEquals((result.record as any).conversations[1].value, "Hello there");
});

Deno.test("rewriteConversationRecord retries repeated assistant content against previous same-role turn", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    mode: "record_transform",
    retry: {
      enabled: true,
      maxAttempts: 2,
    },
    validate: {
      rules: [{
        path: "content",
        kind: "max_similarity_to_ref",
        ref: "previous_same_role_turn.value",
        threshold: 0.82,
      }],
    },
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Explain a hash map simply." },
        { from: "gpt", value: "A hash map stores values by key for fast lookup." },
        { from: "human", value: "When would I use one?" },
        { from: "gpt", value: "Use one when you need fast lookups by key." },
      ],
    },
    stage,
    makeSessionFactory([
      "A hash map stores values by key for fast lookup.",
      "A hash map stores values by key for fast lookup.",
      "Use one when you need quick lookups, inserts, or updates by key, such as storing users by ID.",
    ], seenPayloads),
  );

  assertEquals(result.warnings.length, 0);
  assertEquals(result.traces.length, 3);
  assertEquals(result.traces[1].failureKind, "validator_mismatch");
  assertEquals(result.traces[2].success, true);
  assertStringIncludes(
    seenPayloads[2].messages[seenPayloads[2].messages.length - 1].content,
    "previous_same_role_turn.value",
  );
  assertEquals(
    (result.record as any).conversations[3].value,
    "Use one when you need quick lookups, inserts, or updates by key, such as storing users by ID.",
  );
});

Deno.test("rewriteConversationRecord can reject copy-through against original target content", async () => {
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    mode: "record_transform",
    validate: {
      rules: [{
        name: "avoid_copy_through",
        path: "content",
        kind: "max_similarity_to_ref",
        ref: "original_target_content",
        threshold: 0.98,
      }],
    },
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
      ],
    },
    stage,
    makeSessionFactory(["hello there"], []),
  );

  assertEquals(result.warnings.length, 1);
  assertEquals(result.warnings[0].kind, "validator_mismatch.max_similarity_to_ref");
  assertEquals(result.warnings[0].validatorName, "avoid_copy_through");
  assertEquals((result.record as any).conversations[1].value, "Hello there");
});

Deno.test("rewriteConversationRecord accepts reasoning-prefix rewrites with must_change_from_ref and retries exact copies", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    mode: "record_transform",
    retry: {
      enabled: true,
      maxAttempts: 2,
    },
    validate: {
      rules: [{
        path: "content",
        kind: "must_change_from_ref",
        ref: "original_target_content",
      }],
    },
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const success = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
      ],
    },
    stage,
    makeSessionFactory([
      "<reasoning>Plan</reasoning> Hello there",
    ], []),
  );
  assertEquals(success.warnings.length, 0);
  assertEquals((success.record as any).conversations[1].value, "<reasoning>Plan</reasoning> Hello there");

  const retry = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Hi" },
        { from: "gpt", value: "Hello there" },
      ],
    },
    stage,
    makeSessionFactory([
      "Hello there",
      "<reasoning>Plan</reasoning> Hello there",
    ], seenPayloads),
  );
  assertEquals(retry.warnings.length, 0);
  assertEquals(retry.traces[0].failureKind, "validator_mismatch");
  assertStringIncludes(retry.traces[0].validationIssues?.[0].message ?? "", "original_target_content");
  assertStringIncludes(
    seenPayloads[1].messages[seenPayloads[1].messages.length - 1].content,
    "Value must differ from ref 'original_target_content'",
  );
});

Deno.test("rewriteConversationRecord scoped similarity ignores repeated reasoning but still rejects repeated answer bodies", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    mode: "record_transform",
    retry: {
      enabled: true,
      maxAttempts: 2,
    },
    validate: {
      rules: [{
        path: "content",
        kind: "max_similarity_to_ref",
        ref: "previous_same_role_turn.value",
        threshold: 0.82,
        scope: {
          excludePatterns: [{
            pattern: "<reasoning>[\\s\\S]*?</reasoning>",
          }],
        },
      }],
    },
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["gpt"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "human", value: "Explain a hash map simply." },
        { from: "gpt", value: "A hash map stores values by key for fast lookup." },
        { from: "human", value: "When would I use one?" },
        { from: "gpt", value: "Use one when you need fast lookups by key." },
      ],
    },
    stage,
    makeSessionFactory([
      "<reasoning>Same chain</reasoning> A hash map stores values by key for fast lookup.",
      "<reasoning>Same chain</reasoning> A hash map stores values by key for fast lookup.",
      "<reasoning>Same chain</reasoning> Use one when you need quick lookups, inserts, or updates by key, such as storing users by ID.",
    ], seenPayloads),
  );

  assertEquals(result.warnings.length, 0);
  assertEquals(result.traces.length, 3);
  assertEquals(result.traces[1].failureKind, "validator_mismatch");
  assertStringIncludes(
    result.traces[1].validationIssues?.[0].message ?? "",
    "previous_same_role_turn.value",
  );
  assertStringIncludes(
    seenPayloads[2].messages[seenPayloads[2].messages.length - 1].content,
    "previous_same_role_turn.value",
  );
  assertEquals(
    (result.record as any).conversations[3].value,
    "<reasoning>Same chain</reasoning> Use one when you need quick lookups, inserts, or updates by key, such as storing users by ID.",
  );
});

Deno.test("rewriteConversationRecord rejects wrong-topic rewrites that drift away from the original target", async () => {
  const seenPayloads: ChatTransportPayload[] = [];
  const stage: StageInput = {
    instructions: "Rewrite the assistant turn.",
    mode: "record_transform",
    retry: {
      enabled: true,
      maxAttempts: 2,
    },
    validate: {
      rules: [
        {
          path: "content",
          kind: "contains",
          value: "<think>",
        },
        {
          path: "content",
          kind: "min_similarity_to_ref",
          ref: "original_target_content",
          threshold: 0.45,
          scope: {
            excludePatterns: [{
              pattern: "<think>[\\s\\S]*?</think>",
            }],
          },
        },
      ],
    },
    transform: {
      kind: "conversation_rewrite",
      conversationsPath: "conversations",
      roleField: "from",
      contentField: "value",
      targetRoles: ["assistant"],
    },
  };

  const result = await rewriteConversationRecord(
    {
      conversations: [
        { from: "user", value: "Is it morally right to try to have a certain percentage of females on managerial positions?" },
        { from: "assistant", value: "It is a complex ethical question with arguments about fairness and merit." },
        { from: "user", value: "OK, does pineapple belong on a pizza? Relax and give me fun answer." },
        { from: "assistant", value: "Pineapple on pizza is the chaos goblin of toppings, and whether it belongs depends on whether you like sweet crashing the cheese party." },
      ],
    },
    stage,
    makeSessionFactory([
      "<think>Weigh both ethical sides carefully.</think> It is a complex ethical question with arguments about fairness and merit.",
      "<think>Stay balanced.</think> It is a complex ethical question with arguments about fairness and merit.",
      "<think>Keep it playful.</think> Pineapple on pizza is the chaos goblin of toppings, and whether it belongs depends on whether you like sweet crashing the cheese party.",
    ], seenPayloads),
  );

  assertEquals(result.warnings.length, 0);
  assertEquals(result.traces.length, 3);
  assertEquals(result.traces[1].failureKind, "validator_mismatch");
  assertStringIncludes(
    result.traces[1].validationIssues?.[0].message ?? "",
    "original_target_content",
  );
  assertStringIncludes(
    seenPayloads[2].messages[seenPayloads[2].messages.length - 1].content,
    "original_target_content",
  );
  assertEquals(
    (result.record as any).conversations[3].value,
    "<think>Keep it playful.</think> Pineapple on pizza is the chaos goblin of toppings, and whether it belongs depends on whether you like sweet crashing the cheese party.",
  );
});
