import {
  assertEquals,
  assertThrows,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import {
  InputDatasetRemapError,
  remapInputDataset,
} from "../lib/InputDatasetRemapper.ts";

Deno.test("remapInputDataset converts prefixed string arrays into normalized turns", () => {
  const result = remapInputDataset(
    [{
      instruction: "You are helpful.",
      conversation: [
        " user: Hello ",
        "assistant: Hi there",
        "system: Be concise",
      ],
    }],
    {
      kind: "prefixed_string_array",
      sourcePath: "conversation",
      prefixes: {
        user: "user:",
        assistant: "assistant:",
        system: "system:",
      },
    },
  );

  assertEquals(result, [{
    instruction: "You are helpful.",
    conversation: [
      " user: Hello ",
      "assistant: Hi there",
      "system: Be concise",
    ],
    conversations: [
      { from: "user", value: "Hello" },
      { from: "assistant", value: "Hi there" },
      { from: "system", value: "Be concise" },
    ],
  }]);
});

Deno.test("remapInputDataset respects prefixed string custom output fields", () => {
  const result = remapInputDataset(
    [{ nested: { convo: ["user: hi", "assistant: ok"] } }],
    {
      kind: "prefixed_string_array",
      sourcePath: "nested.convo",
      outputPath: "normalized.turns",
      roleField: "role",
      contentField: "content",
      prefixes: {
        user: "user:",
        assistant: "assistant:",
      },
    },
  );

  assertEquals(result, [{
    nested: { convo: ["user: hi", "assistant: ok"] },
    normalized: {
      turns: [
        { role: "user", content: "hi" },
        { role: "assistant", content: "ok" },
      ],
    },
  }]);
});

Deno.test("remapInputDataset fails fast on invalid prefixed string records", () => {
  assertThrows(
    () =>
      remapInputDataset(
        [{ conversation: ["user: hi", 42] }],
        {
          kind: "prefixed_string_array",
          sourcePath: "conversation",
          prefixes: {
            user: "user:",
            assistant: "assistant:",
          },
        },
      ),
    InputDatasetRemapError,
    "conversation item 1 is not a string",
  );

  assertThrows(
    () =>
      remapInputDataset(
        [{ conversation: ["moderator: hi"] }],
        {
          kind: "prefixed_string_array",
          sourcePath: "conversation",
          prefixes: {
            user: "user:",
            assistant: "assistant:",
          },
        },
      ),
    InputDatasetRemapError,
    "does not match any configured prefix",
  );
});

Deno.test("remapInputDataset maps Alpaca records with and without input", () => {
  const result = remapInputDataset(
    [
      {
        instruction: "Answer as a pirate.",
        input: "Explain recursion.",
        output: "Arrr...",
      },
      {
        instruction: "Explain recursion simply.",
        input: "   ",
        output: "Recursion is...",
      },
    ],
    {
      kind: "alpaca",
    },
  );

  assertEquals(result, [
    {
      instruction: "Answer as a pirate.",
      input: "Explain recursion.",
      output: "Arrr...",
      conversations: [
        { from: "system", value: "Answer as a pirate." },
        { from: "user", value: "Explain recursion." },
        { from: "assistant", value: "Arrr..." },
      ],
    },
    {
      instruction: "Explain recursion simply.",
      input: "   ",
      output: "Recursion is...",
      conversations: [
        { from: "user", value: "Explain recursion simply." },
        { from: "assistant", value: "Recursion is..." },
      ],
    },
  ]);
});

Deno.test("remapInputDataset respects Alpaca custom field names and output path", () => {
  const result = remapInputDataset(
    [{
      prompt: "Answer tersely.",
      context: "Name a prime number.",
      answer: "2",
    }],
    {
      kind: "alpaca",
      instructionField: "prompt",
      inputField: "context",
      outputField: "answer",
      outputPath: "normalized.conversations",
      roleField: "role",
      contentField: "text",
    },
  );

  assertEquals(result, [{
    prompt: "Answer tersely.",
    context: "Name a prime number.",
    answer: "2",
    normalized: {
      conversations: [
        { role: "system", text: "Answer tersely." },
        { role: "user", text: "Name a prime number." },
        { role: "assistant", text: "2" },
      ],
    },
  }]);
});

Deno.test("remapInputDataset fails fast on invalid Alpaca records", () => {
  assertThrows(
    () =>
      remapInputDataset(
        [{ input: "Explain recursion.", output: "Recursion is..." }],
        { kind: "alpaca" },
      ),
    InputDatasetRemapError,
    "missing required Alpaca field 'instruction'",
  );

  assertThrows(
    () =>
      remapInputDataset(
        [{ instruction: "Explain recursion.", input: ["bad"], output: "Recursion is..." }],
        { kind: "alpaca" },
      ),
    InputDatasetRemapError,
    "field 'input' must be a string",
  );
});
