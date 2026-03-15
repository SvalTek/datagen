function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

const STOPWORDS = new Set([
  "a",
  "an",
  "and",
  "are",
  "as",
  "at",
  "be",
  "before",
  "by",
  "for",
  "from",
  "has",
  "have",
  "in",
  "is",
  "it",
  "its",
  "of",
  "on",
  "or",
  "that",
  "the",
  "their",
  "there",
  "this",
  "to",
  "use",
  "when",
  "with",
  "you",
  "your",
]);

export function normalizeComparableText(value: string): string {
  return normalizeWhitespace(
    value.toLowerCase().replace(/[^\p{L}\p{N}\s]+/gu, " "),
  );
}

export function tokenizeComparableText(value: string): string[] {
  const normalized = normalizeComparableText(value);
  if (!normalized) return [];
  return normalized.split(" ").filter(Boolean);
}

function computeJaccardSimilarity(tokensA: string[], tokensB: string[]): number {
  if (tokensA.length === 0 && tokensB.length === 0) return 1;
  if (tokensA.length === 0 || tokensB.length === 0) return 0;

  const setA = new Set(tokensA);
  const setB = new Set(tokensB);
  let intersection = 0;

  for (const token of setA) {
    if (setB.has(token)) {
      intersection++;
    }
  }

  const union = new Set([...setA, ...setB]).size;
  return union === 0 ? 0 : intersection / union;
}

function buildNgrams(values: string[], size: number): string[] {
  if (values.length < size) return [];
  const out: string[] = [];
  for (let index = 0; index <= values.length - size; index++) {
    out.push(values.slice(index, index + size).join("|"));
  }
  return out;
}

function buildCharacterTrigrams(value: string): string[] {
  if (value.length < 3) return [];
  const out: string[] = [];
  for (let index = 0; index <= value.length - 3; index++) {
    out.push(value.slice(index, index + 3));
  }
  return out;
}

function computeDiceSimilarity(valuesA: string[], valuesB: string[]): number {
  if (valuesA.length === 0 && valuesB.length === 0) return 1;
  if (valuesA.length === 0 || valuesB.length === 0) return 0;

  const setA = new Set(valuesA);
  const setB = new Set(valuesB);
  let intersection = 0;

  for (const value of setA) {
    if (setB.has(value)) {
      intersection++;
    }
  }

  return (2 * intersection) / (setA.size + setB.size);
}

function longestCommonSubsequenceLength(tokensA: string[], tokensB: string[]): number {
  if (tokensA.length === 0 || tokensB.length === 0) return 0;

  const previous = new Array<number>(tokensB.length + 1).fill(0);
  const current = new Array<number>(tokensB.length + 1).fill(0);

  for (let i = 1; i <= tokensA.length; i++) {
    current.fill(0);
    for (let j = 1; j <= tokensB.length; j++) {
      if (tokensA[i - 1] === tokensB[j - 1]) {
        current[j] = previous[j - 1] + 1;
      } else {
        current[j] = Math.max(previous[j], current[j - 1]);
      }
    }

    for (let j = 0; j <= tokensB.length; j++) {
      previous[j] = current[j];
    }
  }

  return previous[tokensB.length];
}

function computeSequenceOverlapSimilarity(tokensA: string[], tokensB: string[]): number {
  if (tokensA.length === 0 && tokensB.length === 0) return 1;
  if (tokensA.length === 0 || tokensB.length === 0) return 0;

  const lcsLength = longestCommonSubsequenceLength(tokensA, tokensB);
  return lcsLength / Math.max(tokensA.length, tokensB.length);
}

export interface TextSimilarityResult {
  mode: "fast" | "detailed";
  similarity: number;
  jaccardSimilarity: number;
  sequenceOverlapSimilarity: number;
  normalizedA: string;
  normalizedB: string;
  wordBigramDice?: number;
  charTrigramDice?: number;
  contentRecallSimilarity?: number;
}

export type TextSimilarityMode = "fast" | "detailed";

export function calculateTextSimilarity(
  valueA: string,
  valueB: string,
  options: {
    mode?: TextSimilarityMode;
  } = {},
): TextSimilarityResult {
  const mode = options.mode ?? "fast";
  const normalizedA = normalizeComparableText(valueA);
  const normalizedB = normalizeComparableText(valueB);

  if (normalizedA === normalizedB) {
    return {
      mode,
      similarity: 1,
      jaccardSimilarity: 1,
      sequenceOverlapSimilarity: 1,
      normalizedA,
      normalizedB,
      ...(mode === "detailed"
        ? {
          wordBigramDice: 1,
          charTrigramDice: 1,
          contentRecallSimilarity: 1,
        }
        : {}),
    };
  }

  const tokensA = tokenizeComparableText(valueA);
  const tokensB = tokenizeComparableText(valueB);
  const jaccardSimilarity = computeJaccardSimilarity(tokensA, tokensB);
  const sequenceOverlapSimilarity = computeSequenceOverlapSimilarity(tokensA, tokensB);
  const fastSimilarity = Math.max(jaccardSimilarity, sequenceOverlapSimilarity);

  if (mode === "fast") {
    return {
      mode,
      similarity: fastSimilarity,
      jaccardSimilarity,
      sequenceOverlapSimilarity,
      normalizedA,
      normalizedB,
    };
  }

  if (fastSimilarity === 1 || (normalizedA.length === 0 && normalizedB.length === 0)) {
    return {
      mode,
      similarity: fastSimilarity,
      jaccardSimilarity,
      sequenceOverlapSimilarity,
      normalizedA,
      normalizedB,
      wordBigramDice: fastSimilarity,
      charTrigramDice: fastSimilarity,
      contentRecallSimilarity: fastSimilarity,
    };
  }

  const wordBigramDice = computeDiceSimilarity(
    buildNgrams(tokensA, 2),
    buildNgrams(tokensB, 2),
  );
  const charTrigramDice = computeDiceSimilarity(
    buildCharacterTrigrams(normalizedA),
    buildCharacterTrigrams(normalizedB),
  );

  const contentTokensA = [...new Set(tokensA.filter((token) => !STOPWORDS.has(token)))];
  const contentTokensB = [...new Set(tokensB.filter((token) => !STOPWORDS.has(token)))];
  let contentRecallSimilarity = fastSimilarity;
  if (contentTokensA.length > 0 || contentTokensB.length > 0) {
    if (contentTokensA.length === 0 || contentTokensB.length === 0) {
      contentRecallSimilarity = 0;
    } else {
      const setA = new Set(contentTokensA);
      const setB = new Set(contentTokensB);
      let intersection = 0;
      for (const token of setA) {
        if (setB.has(token)) {
          intersection++;
        }
      }
      const recallA = intersection / setA.size;
      const recallB = intersection / setB.size;
      contentRecallSimilarity = Math.min(recallA, recallB);
    }
  }

  const detailedSimilarity = Math.min(1, Math.max(0,
    0.35 * fastSimilarity +
    0.25 * wordBigramDice +
    0.20 * charTrigramDice +
    0.20 * contentRecallSimilarity,
  ));

  return {
    mode,
    similarity: detailedSimilarity,
    jaccardSimilarity,
    sequenceOverlapSimilarity,
    normalizedA,
    normalizedB,
    wordBigramDice,
    charTrigramDice,
    contentRecallSimilarity,
  };
}
