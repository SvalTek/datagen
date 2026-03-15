import { assertEquals } from "https://deno.land/std@0.203.0/assert/mod.ts";
import { applyValidationScope } from "../lib/ValidationScope.ts";

Deno.test("ValidationScope includePattern extracts and concatenates matches", () => {
  const result = applyValidationScope(
    "<final>One</final> ignored <final>Two</final>",
    {
      includePattern: {
        pattern: "<final>[\\s\\S]*?</final>",
      },
    },
  );

  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value, "<final>One</final> <final>Two</final>");
  }
});

Deno.test("ValidationScope returns scope_no_match when includePattern matches nothing", () => {
  const result = applyValidationScope("plain text", {
    includePattern: {
      pattern: "<final>[\\s\\S]*?</final>",
    },
  });

  assertEquals(result.ok, false);
  if (!result.ok) {
    assertEquals(result.kind, "scope_no_match");
  }
});

Deno.test("ValidationScope excludePatterns remove matched spans and can empty the result", () => {
  const ok = applyValidationScope(
    "<reasoning>scratch</reasoning> final answer",
    {
      excludePatterns: [{
        pattern: "<reasoning>[\\s\\S]*?</reasoning>",
      }],
    },
  );
  assertEquals(ok.ok, true);
  if (ok.ok) {
    assertEquals(ok.value, "final answer");
  }

  const empty = applyValidationScope(
    "<reasoning>scratch</reasoning>",
    {
      excludePatterns: [{
        pattern: "<reasoning>[\\s\\S]*?</reasoning>",
      }],
    },
  );
  assertEquals(empty.ok, false);
  if (!empty.ok) {
    assertEquals(empty.kind, "scope_empty");
  }
});

Deno.test("ValidationScope reports invalid scope regex patterns", () => {
  const result = applyValidationScope("hello", {
    excludePatterns: [{ pattern: "(" }],
  });

  assertEquals(result.ok, false);
  if (!result.ok) {
    assertEquals(result.kind, "invalid_scope_pattern");
  }
});

Deno.test("ValidationScope applies include before exclude", () => {
  const result = applyValidationScope(
    "<assistant_output><reasoning>scratch</reasoning> final</assistant_output> trailing",
    {
      includePattern: {
        pattern: "<assistant_output>[\\s\\S]*?</assistant_output>",
      },
      excludePatterns: [{
        pattern: "<reasoning>[\\s\\S]*?</reasoning>",
      }],
    },
  );

  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value, "<assistant_output> final</assistant_output>");
  }
});
