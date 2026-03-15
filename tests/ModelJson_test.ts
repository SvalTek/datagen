import {
  assertEquals,
  assertThrows,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import { extractJsonCandidate, parseModelJson } from "../lib/ModelJson.ts";

Deno.test("extractJsonCandidate unwraps embedded fenced json", () => {
  assertEquals(
    extractJsonCandidate('Here:\n```json\n{"ok":true}\n```\nDone.'),
    '{"ok":true}',
  );
});

Deno.test("parseModelJson extracts balanced json from surrounding prose", () => {
  assertEquals(
    parseModelJson('Response follows: {"ok":true,"items":[1,2]} Thanks.'),
    { ok: true, items: [1, 2] },
  );
});

Deno.test("parseModelJson merges js-style concatenated string literals inside json", () => {
  const raw = `{
    "content": "<reasoning>Plan</reasoning> A hash map stores values by key." +
      " Use one when you need quick lookups."
  }`;

  assertEquals(parseModelJson(raw), {
    content:
      "<reasoning>Plan</reasoning> A hash map stores values by key. Use one when you need quick lookups.",
  });
});

Deno.test("parseModelJson falls back to jsonrepair for common non-json syntax", () => {
  const raw = `{
    content: '<reasoning>Plan</reasoning> Hello there',
  }`;

  assertEquals(parseModelJson(raw), {
    content: "<reasoning>Plan</reasoning> Hello there",
  });
});

Deno.test("parseModelJson repairs multiline content strings in otherwise obvious object outputs", () => {
  const raw = `{
  "content": "<think>Plan the explanation.</think>
Sure, here's an example:

\`\`\`
function greet(name) {
  console.log(\\"Hello, \\" + name);
}
\`\`\`"
}`;

  assertEquals(parseModelJson(raw), {
    content:
      "<think>Plan the explanation.</think>\nSure, here's an example:\n\n```\nfunction greet(name) {\n  console.log(\"Hello, \" + name);\n}\n```",
  });
});

Deno.test("parseModelJson repairs multiline value strings in otherwise obvious object outputs", () => {
  const raw = `{
  "from": "assistant",
  "value": "<think>Plan the explanation.</think>
Line one.
Line two."
}`;

  assertEquals(parseModelJson(raw), {
    from: "assistant",
    value: "<think>Plan the explanation.</think>\nLine one.\nLine two.",
  });
});

Deno.test("parseModelJson still throws on genuinely invalid json", () => {
  assertThrows(() => parseModelJson('{"content": }'));
});
