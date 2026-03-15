import { jsonrepair } from "npm:jsonrepair";

function stripMarkdownFences(rawModelOutput: string): string {
  const trimmed = rawModelOutput.trim();
  const embeddedFencedMatches = trimmed.matchAll(/```(?:json)?\s*([\s\S]*?)\s*```/gi);
  for (const match of embeddedFencedMatches) {
    const candidate = match[1]?.trim();
    if (candidate?.startsWith("{") || candidate?.startsWith("[")) {
      return candidate;
    }
  }

  const withoutLeadingFence = trimmed.replace(/^```(?:json)?\s*/i, "");
  const withoutTrailingFence = withoutLeadingFence.replace(/\s*```$/, "");
  return withoutTrailingFence.trim();
}

function findBalancedJsonSubstring(input: string): string | undefined {
  let startIndex = -1;
  let openingChar = "";
  let closingChar = "";

  for (let index = 0; index < input.length; index++) {
    const char = input[index];
    if (char === "{") {
      startIndex = index;
      openingChar = "{";
      closingChar = "}";
      break;
    }

    if (char === "[") {
      startIndex = index;
      openingChar = "[";
      closingChar = "]";
      break;
    }
  }

  if (startIndex === -1) {
    return undefined;
  }

  let depth = 0;
  let inString = false;
  let escaping = false;

  for (let index = startIndex; index < input.length; index++) {
    const char = input[index];

    if (inString) {
      if (escaping) {
        escaping = false;
        continue;
      }

      if (char === "\\") {
        escaping = true;
        continue;
      }

      if (char === "\"") {
        inString = false;
      }

      continue;
    }

    if (char === "\"") {
      inString = true;
      continue;
    }

    if (char === openingChar) {
      depth++;
      continue;
    }

    if (char === closingChar) {
      depth--;
      if (depth === 0) {
        return input.slice(startIndex, index + 1).trim();
      }
    }
  }

  return undefined;
}

function mergeConcatenatedJsonStrings(input: string): string {
  const concatenatedStringPattern = /"((?:\\.|[^"\\])*)"\s*\+\s*"((?:\\.|[^"\\])*)"/g;
  let current = input;

  while (true) {
    let changed = false;
    const next = current.replace(
      concatenatedStringPattern,
      (_match, leftEscaped: string, rightEscaped: string) => {
        changed = true;
        const left = JSON.parse(`"${leftEscaped}"`) as string;
        const right = JSON.parse(`"${rightEscaped}"`) as string;
        return JSON.stringify(left + right);
      },
    );

    if (!changed) {
      return current;
    }

    current = next;
  }
}

function findLastUnescapedQuote(input: string, beforeIndex: number): number {
  for (let index = beforeIndex; index >= 0; index--) {
    if (input[index] !== "\"") continue;

    let backslashCount = 0;
    for (let scan = index - 1; scan >= 0 && input[scan] === "\\"; scan--) {
      backslashCount++;
    }

    if (backslashCount % 2 === 0) {
      return index;
    }
  }

  return -1;
}

function escapeRawControlCharsForJsonString(input: string): string {
  return input
    .replace(/\r/g, "\\r")
    .replace(/\n/g, "\\n")
    .replace(/\t/g, "\\t");
}

function repairTrailingMultilineStringField(
  candidate: string,
  fieldName: string,
): string | undefined {
  const trimmed = candidate.trim();
  if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
    return undefined;
  }

  const fieldPattern = new RegExp(`"${fieldName}"\\s*:\\s*"`);
  const fieldMatch = fieldPattern.exec(trimmed);
  if (!fieldMatch) {
    return undefined;
  }

  const valueStart = fieldMatch.index + fieldMatch[0].length;
  const objectEnd = trimmed.lastIndexOf("}");
  const valueEnd = findLastUnescapedQuote(trimmed, objectEnd - 1);

  if (valueEnd <= valueStart) {
    return undefined;
  }

  const suffix = trimmed.slice(valueEnd + 1);
  if (!/^\s*}$/.test(suffix)) {
    return undefined;
  }

  try {
    const decodedValue = JSON.parse(
      `"${escapeRawControlCharsForJsonString(trimmed.slice(valueStart, valueEnd))}"`,
    ) as string;
    return trimmed.slice(0, valueStart) + JSON.stringify(decodedValue) +
      trimmed.slice(valueEnd + 1);
  } catch {
    return undefined;
  }
}

function collectMultilineStringRepairs(candidate: string): string[] {
  const repairs = new Set<string>();

  for (const fieldName of ["content", "value"]) {
    const repaired = repairTrailingMultilineStringField(candidate, fieldName);
    if (repaired) {
      repairs.add(repaired);
    }
  }

  return [...repairs];
}

function tryParseSimpleRewriteObject(candidate: string): Record<string, unknown> | undefined {
  const trimmed = candidate.trim();
  const match =
    /^\{\s*(?:"from"\s*:\s*"((?:\\.|[^"\\])*)"\s*,\s*)?"(content|value)"\s*:\s*"([\s\S]*)"\s*\}$/s
      .exec(trimmed);

  if (!match) {
    return undefined;
  }

  const [, fromEscaped, fieldName, rawValue] = match;

  try {
    const parsedValue = JSON.parse(
      `"${escapeRawControlCharsForJsonString(rawValue)}"`,
    ) as string;
    const result: Record<string, unknown> = {
      [fieldName]: parsedValue,
    };

    if (typeof fromEscaped === "string") {
      result.from = JSON.parse(`"${fromEscaped}"`) as string;
    }

    return result;
  } catch {
    return undefined;
  }
}

function collectCandidates(rawModelOutput: string): string[] {
  const stripped = stripMarkdownFences(rawModelOutput);
  const candidates = new Set<string>();

  if (stripped) {
    candidates.add(stripped);
  }

  const balanced = findBalancedJsonSubstring(stripped);
  if (balanced) {
    candidates.add(balanced);
  }

  const repairedCandidates = [...candidates].map((candidate) =>
    mergeConcatenatedJsonStrings(candidate)
  ).filter((candidate) => candidate.length > 0);

  for (const candidate of repairedCandidates) {
    candidates.add(candidate);
    const balancedCandidate = findBalancedJsonSubstring(candidate);
    if (balancedCandidate) {
      candidates.add(balancedCandidate);
    }
  }

  for (const candidate of [...candidates]) {
    for (const repaired of collectMultilineStringRepairs(candidate)) {
      candidates.add(repaired);
    }
  }

  return [...candidates];
}

function shouldAttemptJsonRepair(candidate: string): boolean {
  if (!/[{\[]/.test(candidate)) {
    return false;
  }

  return /"((?:\\.|[^"\\])*)"\s*\+\s*"/.test(candidate) ||
    /:\s*'/.test(candidate) ||
    /[{,]\s*[A-Za-z_$][\w$]*\s*:/.test(candidate) ||
    /,\s*[}\]]/.test(candidate) ||
    /\/[/*]/.test(candidate);
}

export function extractJsonCandidate(rawModelOutput: string): string {
  const [firstCandidate] = collectCandidates(rawModelOutput);
  return firstCandidate ?? rawModelOutput.trim();
}

export function parseModelJson(rawModelOutput: string): unknown {
  let lastError: unknown;
  const candidates = collectCandidates(rawModelOutput);
  for (const candidate of candidates) {
    try {
      return JSON.parse(candidate);
    } catch (error) {
      lastError = error;
    }
  }

  for (const candidate of candidates) {
    const parsedRewriteObject = tryParseSimpleRewriteObject(candidate);
    if (parsedRewriteObject) {
      return parsedRewriteObject;
    }
  }

  for (const candidate of candidates) {
    if (!shouldAttemptJsonRepair(candidate)) {
      continue;
    }

    try {
      return JSON.parse(jsonrepair(candidate));
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError ?? new SyntaxError("Model output is not valid JSON");
}
