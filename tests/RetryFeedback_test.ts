import {
  assertEquals,
  assertStringIncludes,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import { buildRetryFeedbackAppendix } from "../lib/RetryFeedback.ts";

Deno.test("RetryFeedback formats invalid json failures", () => {
  const text = buildRetryFeedbackAppendix({
    attempt: 1,
    maxAttempts: 3,
    failureKind: "invalid_json",
    reason: "Model output is not valid JSON",
  });

  assertStringIncludes(text, "Previous Attempt Failed:");
  assertStringIncludes(text, "Failure Kind: invalid_json");
  assertStringIncludes(text, "Retry Status:");
  assertStringIncludes(text, "Next Attempt: 2 of 3");
  assertStringIncludes(
    text,
    "Previous response was not valid JSON parseable by JSON.parse.",
  );
});

Deno.test("RetryFeedback formats validator failures with issue list", () => {
  const text = buildRetryFeedbackAppendix({
    attempt: 1,
    maxAttempts: 2,
    failureKind: "validator_mismatch",
    reason: "Model output failed stage validators",
    validationIssues: [{
      kind: "min_similarity_to_ref",
      path: "content",
      message: "Similarity 0.410 is below minimum 0.450 when compared to ref 'original_target_content'",
      hint: "Keep the final answer body semantically anchored to the original target turn.",
      similarityScore: 0.41,
      similarityThreshold: 0.45,
      similarityDistance: 0.04,
      similarityStatus: "close",
      similarityMode: "detailed",
    }],
  });

  assertStringIncludes(text, "Failure Kind: validator_mismatch");
  assertStringIncludes(text, "Next Attempt: 2 of 2");
  assertStringIncludes(text, "This is the final retry.");
  assertStringIncludes(
    text,
    "Similarity 0.410 is below minimum 0.450 when compared to ref 'original_target_content'",
  );
  assertStringIncludes(text, "Hints:");
  assertStringIncludes(
    text,
    "Keep the final answer body semantically anchored to the original target turn.",
  );
  assertStringIncludes(text, "similarity_score: 0.410");
  assertStringIncludes(text, "threshold: 0.450");
  assertStringIncludes(text, "distance: 0.040");
  assertStringIncludes(text, "status: close");
  assertStringIncludes(text, "similarity_mode: detailed");
  assertStringIncludes(text, "Fix Instructions:");
});

Deno.test("RetryFeedback formats invalid content payload failures", () => {
  const text = buildRetryFeedbackAppendix({
    attempt: 1,
    failureKind: "invalid_content_output",
    reason: "Turn rewrite output must contain content",
  });

  assertStringIncludes(
    text,
    "The response must be exactly one JSON object containing string field 'content'.",
  );
  assertEquals(text.includes("Validation Issues:"), true);
});

Deno.test("RetryFeedback formats empty text output failures for rewrite retries", () => {
  const text = buildRetryFeedbackAppendix({
    attempt: 1,
    maxAttempts: 2,
    failureKind: "empty_output",
    reason: "Turn rewrite output was empty",
    responseFormat: "text",
  });

  assertStringIncludes(
    text,
    "Previous response was empty or did not contain a usable rewritten turn.",
  );
  assertStringIncludes(text, "Return only the rewritten target turn text.");
  assertStringIncludes(
    text,
    "Do not include explanations, markdown, JSON, or extra wrapper text.",
  );
});
