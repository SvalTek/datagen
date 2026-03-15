import {
  assertEquals,
  assertRejects,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import {
  inferInputFormat,
  InputDatasetParseError,
  InputDatasetRemapError,
  InputDatasetValidationError,
  loadInputDataset,
} from "../lib/InputDatasetLoader.ts";

Deno.test("inferInputFormat resolves json and jsonl from file extension", () => {
  assertEquals(inferInputFormat("records.json"), "json");
  assertEquals(inferInputFormat("records.jsonl"), "jsonl");
});

Deno.test({
  name: "loadInputDataset reads JSON arrays",
  permissions: { read: true, write: true },
  async fn() {
    const filePath = await Deno.makeTempFile({ suffix: ".json" });

    try {
      await Deno.writeTextFile(filePath, JSON.stringify([{ id: 1 }, { id: 2 }]));
      const result = await loadInputDataset({ path: filePath });
      assertEquals(result.format, "json");
      assertEquals(result.records, [{ id: 1 }, { id: 2 }]);
    } finally {
      await Deno.remove(filePath).catch(() => {});
    }
  },
});

Deno.test({
  name: "loadInputDataset slices JSON arrays with offset and limit",
  permissions: { read: true, write: true },
  async fn() {
    const filePath = await Deno.makeTempFile({ suffix: ".json" });

    try {
      await Deno.writeTextFile(
        filePath,
        JSON.stringify([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]),
      );
      const result = await loadInputDataset({
        path: filePath,
        offset: 1,
        limit: 2,
      });
      assertEquals(result.format, "json");
      assertEquals(result.records, [{ id: 2 }, { id: 3 }]);
    } finally {
      await Deno.remove(filePath).catch(() => {});
    }
  },
});

Deno.test({
  name: "loadInputDataset reads JSONL and ignores blank lines",
  permissions: { read: true, write: true },
  async fn() {
    const filePath = await Deno.makeTempFile({ suffix: ".jsonl" });

    try {
      await Deno.writeTextFile(filePath, '{"id":1}\n\n{"id":2}\n');
      const result = await loadInputDataset({ path: filePath });
      assertEquals(result.format, "jsonl");
      assertEquals(result.records, [{ id: 1 }, { id: 2 }]);
    } finally {
      await Deno.remove(filePath).catch(() => {});
    }
  },
});

Deno.test({
  name: "loadInputDataset slices JSONL with offset and limit",
  permissions: { read: true, write: true },
  async fn() {
    const filePath = await Deno.makeTempFile({ suffix: ".jsonl" });

    try {
      await Deno.writeTextFile(
        filePath,
        '{"id":1}\n{"id":2}\n{"id":3}\n{"id":4}\n',
      );
      const result = await loadInputDataset({
        path: filePath,
        offset: 2,
        limit: 1,
      });
      assertEquals(result.format, "jsonl");
      assertEquals(result.records, [{ id: 3 }]);
    } finally {
      await Deno.remove(filePath).catch(() => {});
    }
  },
});

Deno.test({
  name: "loadInputDataset remaps prefixed string array records after slicing",
  permissions: { read: true, write: true },
  async fn() {
    const filePath = await Deno.makeTempFile({ suffix: ".json" });

    try {
      await Deno.writeTextFile(
        filePath,
        JSON.stringify([
          { id: 1, conversation: ["user: skip", "assistant: skip"] },
          { id: 2, conversation: ["user: keep", "assistant: ok"] },
        ]),
      );
      const result = await loadInputDataset({
        path: filePath,
        offset: 1,
        limit: 1,
        remap: {
          kind: "prefixed_string_array",
          sourcePath: "conversation",
          prefixes: {
            user: "user:",
            assistant: "assistant:",
          },
        },
      });
      assertEquals(result.format, "json");
      assertEquals(result.records, [{
        id: 2,
        conversation: ["user: keep", "assistant: ok"],
        conversations: [
          { from: "user", value: "keep" },
          { from: "assistant", value: "ok" },
        ],
      }]);
    } finally {
      await Deno.remove(filePath).catch(() => {});
    }
  },
});

Deno.test({
  name: "loadInputDataset remaps Alpaca JSONL records",
  permissions: { read: true, write: true },
  async fn() {
    const filePath = await Deno.makeTempFile({ suffix: ".jsonl" });

    try {
      await Deno.writeTextFile(
        filePath,
        [
          JSON.stringify({
            instruction: "Answer as a pirate.",
            input: "Explain recursion.",
            output: "Arrr...",
          }),
          "",
        ].join("\n"),
      );
      const result = await loadInputDataset({
        path: filePath,
        remap: { kind: "alpaca" },
      });
      assertEquals(result.format, "jsonl");
      assertEquals(result.records, [{
        instruction: "Answer as a pirate.",
        input: "Explain recursion.",
        output: "Arrr...",
        conversations: [
          { from: "system", value: "Answer as a pirate." },
          { from: "user", value: "Explain recursion." },
          { from: "assistant", value: "Arrr..." },
        ],
      }]);
    } finally {
      await Deno.remove(filePath).catch(() => {});
    }
  },
});

Deno.test({
  name: "loadInputDataset rejects JSON input that is not an array",
  permissions: { read: true, write: true },
  async fn() {
    const filePath = await Deno.makeTempFile({ suffix: ".json" });

    try {
      await Deno.writeTextFile(filePath, JSON.stringify({ id: 1 }));
      await assertRejects(
        () => loadInputDataset({ path: filePath }),
        InputDatasetValidationError,
      );
    } finally {
      await Deno.remove(filePath).catch(() => {});
    }
  },
});

Deno.test({
  name: "loadInputDataset includes JSONL line number on parse failure",
  permissions: { read: true, write: true },
  async fn() {
    const filePath = await Deno.makeTempFile({ suffix: ".jsonl" });

    try {
      await Deno.writeTextFile(filePath, '{"id":1}\nnot-json\n');
      await assertRejects(
        () => loadInputDataset({ path: filePath }),
        InputDatasetParseError,
        "line 2",
      );
    } finally {
      await Deno.remove(filePath).catch(() => {});
    }
  },
});

Deno.test({
  name: "loadInputDataset throws explicit remap errors for malformed records",
  permissions: { read: true, write: true },
  async fn() {
    const filePath = await Deno.makeTempFile({ suffix: ".json" });

    try {
      await Deno.writeTextFile(
        filePath,
        JSON.stringify([{ conversation: ["moderator: nope"] }]),
      );
      await assertRejects(
        () =>
          loadInputDataset({
            path: filePath,
            remap: {
              kind: "prefixed_string_array",
              sourcePath: "conversation",
              prefixes: {
                user: "user:",
                assistant: "assistant:",
              },
            },
          }),
        InputDatasetRemapError,
      );
    } finally {
      await Deno.remove(filePath).catch(() => {});
    }
  },
});
