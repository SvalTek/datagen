import type { ValidationIssue } from "./StageValidator.ts";

export type RetryFailureKind =
  | "invalid_json"
  | "invalid_content_output"
  | "empty_output"
  | "constrain_mismatch"
  | "validator_mismatch";

export interface RetryFeedbackInput {
  attempt: number;
  maxAttempts?: number;
  failureKind: RetryFailureKind;
  reason: string;
  validationIssues?: ValidationIssue[];
  responseFormat?: "json" | "text";
}

export function buildRetryFeedbackAppendix(
  input: RetryFeedbackInput,
): string {
  const uniqueHints = [...new Set(
    (input.validationIssues ?? [])
      .map((issue) => issue.hint?.trim())
      .filter((hint): hint is string => Boolean(hint)),
  )];
  const responseFormat = input.responseFormat ?? "json";
  const nextAttempt = input.maxAttempts !== undefined
    ? Math.min(input.attempt + 1, input.maxAttempts)
    : input.attempt + 1;
  const isFinalRetry = input.maxAttempts !== undefined &&
    nextAttempt >= input.maxAttempts;
  const lines = [
    "Previous Attempt Failed:",
    `- Attempt: ${input.attempt}`,
    `- Failure Kind: ${input.failureKind}`,
    `- Reason: ${input.reason}`,
    "",
    "Retry Status:",
    `- Next Attempt: ${nextAttempt}${
      input.maxAttempts !== undefined ? ` of ${input.maxAttempts}` : ""
    }`,
  ];

  if (isFinalRetry) {
    lines.push(
      "- This is the final retry. Prioritize satisfying the correction requirements exactly.",
    );
  }

  lines.push("");

  if (input.failureKind === "invalid_json") {
    lines.push("Validation Issues:");
    lines.push("- Previous response was not valid JSON parseable by JSON.parse.");
    lines.push("");
  } else if (input.failureKind === "invalid_content_output") {
    lines.push("Validation Issues:");
    lines.push(
      "- The response must be exactly one JSON object containing string field 'content'.",
    );
    lines.push("");
  } else if (input.failureKind === "empty_output") {
    lines.push("Validation Issues:");
    lines.push(
      "- Previous response was empty or did not contain a usable rewritten turn.",
    );
    lines.push("");
  } else if (input.validationIssues?.length) {
    lines.push("Validation Issues:");
    for (const issue of input.validationIssues) {
      lines.push(
        `- ${
          issue.ruleName?.trim() ? `[${issue.ruleName.trim()}] ` : ""
        }${issue.message}`,
      );
      if (
        issue.similarityScore !== undefined &&
        issue.similarityThreshold !== undefined &&
        issue.similarityDistance !== undefined &&
        issue.similarityStatus
      ) {
        lines.push(`  similarity_score: ${issue.similarityScore.toFixed(3)}`);
        lines.push(`  threshold: ${issue.similarityThreshold.toFixed(3)}`);
        lines.push(`  distance: ${issue.similarityDistance.toFixed(3)}`);
        lines.push(`  status: ${issue.similarityStatus}`);
        if (issue.similarityMode) {
          lines.push(`  similarity_mode: ${issue.similarityMode}`);
        }
      }
    }
    lines.push("");
  }

  if (uniqueHints.length > 0) {
    lines.push("Hints:");
    for (const hint of uniqueHints) {
      lines.push(`- ${hint}`);
    }
    lines.push("");
  }

  lines.push("Fix Instructions:");
  lines.push("- Correct the issue described above.");
  lines.push("- Keep all original task requirements.");
  if (responseFormat === "text") {
    lines.push("- Return only the rewritten target turn text.");
    lines.push(
      "- Do not include explanations, markdown, JSON, or extra wrapper text.",
    );
  } else {
    lines.push("- Return only the required JSON shape.");
    lines.push("- Do not include explanations, markdown, or extra text.");
  }

  return lines.join("\n");
}
