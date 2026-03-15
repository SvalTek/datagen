import {
  assertEquals,
  assertStringIncludes,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import { validateStageValue } from "../lib/StageValidator.ts";

Deno.test("StageValidator supports comparison validators against paths", () => {
  const success = validateStageValue({
    original: "Hello world",
    rewritten: "Hello there",
  }, {
    rules: [
      { path: "rewritten", kind: "not_equal_to_path", otherPath: "original" },
      {
        path: "rewritten",
        kind: "max_similarity_to_path",
        otherPath: "original",
        threshold: 0.99,
      },
    ],
  });

  assertEquals(success.success, true);

  const failure = validateStageValue({
    original: "Hello world",
    rewritten: "  hello, world  ",
  }, {
    rules: [{ path: "rewritten", kind: "not_equal_to_path", otherPath: "original" }],
  });
  assertEquals(failure.success, false);
  assertEquals(failure.issues[0].kind, "not_equal_to_path");
});

Deno.test("StageValidator supports must_change validators against paths and refs", () => {
  const pathSuccess = validateStageValue({
    original: "Hello world",
    rewritten: "Hello there",
  }, {
    rules: [{
      path: "rewritten",
      kind: "must_change_from_path",
      otherPath: "original",
    }],
  });
  assertEquals(pathSuccess.success, true);

  const pathFailure = validateStageValue({
    original: "Hello, world!",
    rewritten: " hello world ",
  }, {
    rules: [{
      path: "rewritten",
      kind: "must_change_from_path",
      otherPath: "original",
    }],
  });
  assertEquals(pathFailure.success, false);
  assertEquals(pathFailure.issues[0].kind, "must_change_from_path");

  const refSuccess = validateStageValue(
    { content: "<reasoning>Plan</reasoning> Hello world" },
    {
      rules: [{
        path: "content",
        kind: "must_change_from_ref",
        ref: "original_target_content",
      }],
    },
    {
      refs: {
        original_target_content: "Hello world",
      },
    },
  );
  assertEquals(refSuccess.success, true);

  const refFailure = validateStageValue(
    { content: "Hello world" },
    {
      rules: [{
        path: "content",
        kind: "must_change_from_ref",
        ref: "original_target_content",
      }],
    },
    {
      refs: {
        original_target_content: "Hello world",
      },
    },
  );
  assertEquals(refFailure.success, false);
  assertEquals(refFailure.issues[0].kind, "must_change_from_ref");
});

Deno.test("StageValidator supports scoped must_change validators", () => {
  const scopedFailure = validateStageValue(
    { content: "<reasoning>a</reasoning><final>Hello world</final>" },
    {
      rules: [{
        path: "content",
        kind: "must_change_from_ref",
        ref: "original_target_content",
        scope: {
          includePattern: {
            pattern: "<final>[\\s\\S]*?</final>",
          },
        },
      }],
    },
    {
      refs: {
        original_target_content: "<final>Hello world</final>",
      },
    },
  );
  assertEquals(scopedFailure.success, false);
  assertEquals(scopedFailure.issues[0].kind, "must_change_from_ref");
});

Deno.test("StageValidator supports comparison validators against refs", () => {
  const success = validateStageValue(
    { content: "A concise different answer" },
    {
      rules: [
        {
          path: "content",
          kind: "max_similarity_to_ref",
          ref: "previous_same_role_turn.value",
          threshold: 0.9,
        },
      ],
    },
    {
      refs: {
        previous_same_role_turn: {
          value: "A different earlier answer",
        },
      },
    },
  );

  assertEquals(success.success, true);

  const failure = validateStageValue(
    { content: "Repeat me" },
    {
      rules: [
        {
          path: "content",
          kind: "not_equal_to_ref",
          ref: "original_target_content",
        },
      ],
    },
    {
      refs: {
        original_target_content: "repeat me",
      },
    },
  );

  assertEquals(failure.success, false);
  assertEquals(failure.issues[0].kind, "not_equal_to_ref");
});

Deno.test("StageValidator supports minimum similarity validators against paths and refs", () => {
  const pathSuccess = validateStageValue({
    original: "Pineapple on pizza is a sweet-and-savory debate.",
    rewritten: "Pineapple on pizza is a sweet and savory debate with loyal fans.",
  }, {
    rules: [{
      path: "rewritten",
      kind: "min_similarity_to_path",
      otherPath: "original",
      threshold: 0.5,
    }],
  });
  assertEquals(pathSuccess.success, true);

  const pathFailure = validateStageValue({
    original: "Pineapple on pizza is a sweet-and-savory debate.",
    rewritten: "Gender quotas in management raise fairness concerns.",
  }, {
    rules: [{
      path: "rewritten",
      kind: "min_similarity_to_path",
      otherPath: "original",
      threshold: 0.5,
    }],
  });
  assertEquals(pathFailure.success, false);
  assertEquals(pathFailure.issues[0].kind, "min_similarity_to_path");

  const refSuccess = validateStageValue(
    { content: "<think>Keep it playful.</think> Pineapple on pizza is chaotic but fun." },
    {
      rules: [{
        path: "content",
        kind: "min_similarity_to_ref",
        ref: "original_target_content",
        threshold: 0.45,
        scope: {
          excludePatterns: [{
            pattern: "<think>[\\s\\S]*?</think>",
          }],
        },
      }],
    },
    {
      refs: {
        original_target_content: "Pineapple on pizza is chaotic but fun.",
      },
    },
  );
  assertEquals(refSuccess.success, true);

  const refFailure = validateStageValue(
    { content: "<think>Be balanced.</think> Gender quotas in management are ethically complex." },
    {
      rules: [{
        path: "content",
        kind: "min_similarity_to_ref",
        ref: "original_target_content",
        threshold: 0.45,
        scope: {
          excludePatterns: [{
            pattern: "<think>[\\s\\S]*?</think>",
          }],
        },
      }],
    },
    {
      refs: {
        original_target_content: "Pineapple on pizza is chaotic but fun.",
      },
    },
  );
  assertEquals(refFailure.success, false);
  assertEquals(refFailure.issues[0].kind, "min_similarity_to_ref");
  assertEquals(refFailure.issues[0].similarityThreshold, 0.45);
  assertEquals(typeof refFailure.issues[0].similarityScore, "number");
  assertEquals(typeof refFailure.issues[0].similarityDistance, "number");
  assertEquals(refFailure.issues[0].similarityMode, "fast");
});

Deno.test("StageValidator similarity validators default to fast mode", () => {
  const fastDefault = validateStageValue({
    original: "The link expires after fifteen minutes.",
    rewritten: "You have fifteen minutes before the link expires.",
  }, {
    rules: [{
      path: "rewritten",
      kind: "min_similarity_to_path",
      otherPath: "original",
      threshold: 0.75,
    }],
  });

  assertEquals(fastDefault.success, false);
  assertEquals(fastDefault.issues[0].kind, "min_similarity_to_path");
});

Deno.test("StageValidator detailed similarity can pass paraphrases that fast rejects", () => {
  const fast = validateStageValue({
    original: "The link expires after fifteen minutes.",
    rewritten: "You have fifteen minutes before the link expires.",
  }, {
    rules: [{
      path: "rewritten",
      kind: "min_similarity_to_path",
      otherPath: "original",
      threshold: 0.75,
    }],
  });
  const detailed = validateStageValue({
    original: "The link expires after fifteen minutes.",
    rewritten: "You have fifteen minutes before the link expires.",
  }, {
    rules: [{
      path: "rewritten",
      kind: "min_similarity_to_path",
      otherPath: "original",
      threshold: 0.55,
      similarity: {
        mode: "detailed",
      },
    }],
  });

  assertEquals(fast.success, false);
  assertEquals(detailed.success, true);
});

Deno.test("StageValidator detailed similarity still rejects unrelated content", () => {
  const detailed = validateStageValue({
    original: "The link expires after fifteen minutes.",
    rewritten: "Hash maps store values by key for quick lookup.",
  }, {
    rules: [{
      path: "rewritten",
      kind: "min_similarity_to_path",
      otherPath: "original",
      threshold: 0.4,
      similarity: {
        mode: "detailed",
      },
    }],
  });

  assertEquals(detailed.success, false);
  assertEquals(detailed.issues[0].kind, "min_similarity_to_path");
});

Deno.test("StageValidator scoped detailed similarity applies scope before comparison", () => {
  const detailed = validateStageValue(
    { content: "<think>private</think> You have fifteen minutes before the link expires." },
    {
      rules: [{
        path: "content",
        kind: "min_similarity_to_ref",
        ref: "original_target_content",
        threshold: 0.55,
        similarity: {
          mode: "detailed",
        },
        scope: {
          excludePatterns: [{
            pattern: "<think>[\\s\\S]*?</think>",
          }],
        },
      }],
    },
    {
      refs: {
        original_target_content: "The link expires after fifteen minutes.",
      },
    },
  );

  assertEquals(detailed.success, true);
});

Deno.test("StageValidator scoped comparison can ignore repeated reasoning blocks", () => {
  const unscoped = validateStageValue(
    { content: "<reasoning>Same chain</reasoning> Final answer A" },
    {
      rules: [{
        path: "content",
        kind: "max_similarity_to_ref",
        ref: "previous_same_role_turn.value",
        threshold: 0.82,
      }],
    },
    {
      refs: {
        previous_same_role_turn: {
          value: "<reasoning>Same chain</reasoning> Final answer B",
        },
      },
    },
  );
  assertEquals(unscoped.success, false);

  const scoped = validateStageValue(
    { content: "<reasoning>Same chain</reasoning> Final answer A" },
    {
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
    {
      refs: {
        previous_same_role_turn: {
          value: "<reasoning>Same chain</reasoning> Final answer B",
        },
      },
    },
  );
  assertEquals(scoped.success, true);
});

Deno.test("StageValidator scoped comparison reports scope failures explicitly", () => {
  const invalidPattern = validateStageValue(
    { content: "Hello there" },
    {
      rules: [{
        path: "content",
        kind: "max_similarity_to_ref",
        ref: "original_target_content",
        threshold: 0.9,
        scope: {
          excludePatterns: [{ pattern: "(" }],
        },
      }],
    },
    {
      refs: {
        original_target_content: "Hello there again",
      },
    },
  );
  assertEquals(invalidPattern.success, false);
  assertEquals(invalidPattern.issues[0].kind, "invalid_scope_pattern");

  const noMatch = validateStageValue(
    { content: "Hello there" },
    {
      rules: [{
        path: "content",
        kind: "max_similarity_to_ref",
        ref: "original_target_content",
        threshold: 0.9,
        scope: {
          includePattern: {
            pattern: "<final>[\\s\\S]*?</final>",
          },
        },
      }],
    },
    {
      refs: {
        original_target_content: "Hello there again",
      },
    },
  );
  assertEquals(noMatch.success, false);
  assertEquals(noMatch.issues[0].kind, "scope_no_match");

  const empty = validateStageValue(
    { content: "<reasoning>x</reasoning>" },
    {
      rules: [{
        path: "content",
        kind: "max_similarity_to_ref",
        ref: "original_target_content",
        threshold: 0.9,
        scope: {
          excludePatterns: [{
            pattern: "<reasoning>[\\s\\S]*?</reasoning>",
          }],
        },
      }],
    },
    {
      refs: {
        original_target_content: "<reasoning>y</reasoning>",
      },
    },
  );
  assertEquals(empty.success, false);
  assertEquals(empty.issues[0].kind, "scope_empty");
});

Deno.test("StageValidator supports contains and not_contains", () => {
  assertEquals(
    validateStageValue("hello world", {
      rules: [{ kind: "contains", value: "world" }],
    }).success,
    true,
  );
  assertEquals(
    validateStageValue("hello world", {
      rules: [{ kind: "not_contains", value: "world" }],
    }).success,
    false,
  );
});

Deno.test("StageValidator carries rule hints into validation issues", () => {
  const result = validateStageValue(
    { content: "Hello" },
    {
      rules: [{
        name: "reasoning_prefix",
        path: "content",
        kind: "contains",
        value: "<reasoning>",
        hint: "Start with a <reasoning> block before the final answer.",
      }],
    },
  );

  assertEquals(result.success, false);
  assertEquals(
    result.issues[0].hint,
    "Start with a <reasoning> block before the final answer.",
  );
  assertEquals(result.issues[0].ruleName, "reasoning_prefix");
});

Deno.test("StageValidator supports regex and invalid regex normalization", () => {
  const ok = validateStageValue({ content: "<reasoning>x</reasoning>" }, {
    rules: [{
      path: "content",
      kind: "regex",
      pattern: "<reasoning>[\\s\\S]*?</reasoning>",
    }],
  });
  assertEquals(ok.success, true);

  const invalid = validateStageValue("x", {
    rules: [{ kind: "regex", pattern: "(" }],
  });
  assertEquals(invalid.success, false);
  assertEquals(invalid.issues[0].kind, "invalid_regex_pattern");

  const forbidden = validateStageValue(
    { content: "<think>scratch</think>{\"content\":\"answer\"}" },
    {
      rules: [{
        path: "content",
        kind: "not_regex",
        pattern: "^<think>[\\s\\S]*?</think>\\s*\\{",
      }],
    },
  );
  assertEquals(forbidden.success, false);
  assertEquals(forbidden.issues[0].kind, "not_regex");

  const allowed = validateStageValue(
    { content: "{\"content\":\"<think>scratch</think> answer\"}" },
    {
      rules: [{
        path: "content",
        kind: "not_regex",
        pattern: "^<think>[\\s\\S]*?</think>\\s*\\{",
      }],
    },
  );
  assertEquals(allowed.success, true);
});

Deno.test("StageValidator supports string and array length validators", () => {
  assertEquals(
    validateStageValue({ content: "hello", items: [1, 2] }, {
      rules: [
        { path: "content", kind: "min_length", value: 3 },
        { path: "content", kind: "max_length", value: 10 },
        { path: "items", kind: "array_min_length", value: 2 },
        { path: "items", kind: "array_max_length", value: 3 },
      ],
    }).success,
    true,
  );
});

Deno.test("StageValidator supports equals, not_equals, nested paths, and when gating", () => {
  const result = validateStageValue({
    turn: {
      from: "gpt",
      value: "<reasoning>ok</reasoning>",
    },
  }, {
    rules: [
      { path: "turn.from", kind: "equals", value: "gpt" },
      {
        path: "turn.value",
        when: { path: "turn.from", equals: "gpt" },
        kind: "contains",
        value: "<reasoning>",
      },
      { path: "turn.from", kind: "not_equals", value: "human" },
    ],
  });

  assertEquals(result.success, true);
});

Deno.test("StageValidator reports missing path failures unless explicitly skipped", () => {
  const strict = validateStageValue({ content: "x" }, {
    rules: [{ path: "missing", kind: "contains", value: "x" }],
  });
  assertEquals(strict.success, false);
  assertEquals(strict.issues[0].kind, "path_missing");

  const skipped = validateStageValue(
    { content: "x" },
    { rules: [{ path: "missing", kind: "contains", value: "x" }] },
    { skipInapplicablePaths: true },
  );
  assertEquals(skipped.success, true);
});

Deno.test("StageValidator reports invalid target types clearly", () => {
  const result = validateStageValue({ items: [1, 2] }, {
    rules: [{ path: "items", kind: "contains", value: "x" }],
  });
  assertEquals(result.success, false);
  assertStringIncludes(result.issues[0].message, "requires a string target");
});

Deno.test("StageValidator reports missing refs and invalid comparison target types clearly", () => {
  const missingRef = validateStageValue(
    { content: "Hello" },
    {
      rules: [{
        path: "content",
        kind: "max_similarity_to_ref",
        ref: "previous_same_role_turn.value",
        threshold: 0.82,
      }],
    },
  );

  assertEquals(missingRef.success, false);
  assertEquals(missingRef.issues[0].kind, "ref_missing");

  const invalidType = validateStageValue(
    { content: "Hello", other: 42 },
    {
      rules: [{
        path: "content",
        kind: "max_similarity_to_path",
        otherPath: "other",
        threshold: 0.5,
      }],
    },
  );

  assertEquals(invalidType.success, false);
  assertEquals(invalidType.issues[0].kind, "invalid_target_type");

  const mustChangeInvalidType = validateStageValue(
    { content: "Hello", other: 42 },
    {
      rules: [{
        path: "content",
        kind: "must_change_from_path",
        otherPath: "other",
      }],
    },
  );
  assertEquals(mustChangeInvalidType.success, false);
  assertEquals(mustChangeInvalidType.issues[0].kind, "invalid_target_type");
});
