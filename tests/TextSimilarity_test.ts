import { assertEquals } from "https://deno.land/std@0.203.0/assert/mod.ts";
import { calculateTextSimilarity } from "../lib/TextSimilarity.ts";

Deno.test("TextSimilarity returns 1 for exact and normalized matches", () => {
  assertEquals(calculateTextSimilarity("Hello world", "Hello world").similarity, 1);
  assertEquals(
    calculateTextSimilarity("Hello,   WORLD!", " hello world ").similarity,
    1,
  );
  assertEquals(
    calculateTextSimilarity("Hello,   WORLD!", " hello world ", { mode: "detailed" })
      .similarity,
    1,
  );
});

Deno.test("TextSimilarity returns low similarity for clearly different strings", () => {
  const result = calculateTextSimilarity(
    "Password reset links expire in fifteen minutes",
    "Hash maps store values by key for quick lookup",
  );

  assertEquals(result.similarity < 0.4, true);
  assertEquals(
    calculateTextSimilarity(
      "Password reset links expire in fifteen minutes",
      "Hash maps store values by key for quick lookup",
      { mode: "detailed" },
    ).similarity < 0.5,
    true,
  );
});

Deno.test("TextSimilarity catches reordered token overlap and copied sequence overlap", () => {
  const reordered = calculateTextSimilarity(
    "warm friendly support reply",
    "support reply warm friendly",
  );
  assertEquals(reordered.jaccardSimilarity, 1);

  const copied = calculateTextSimilarity(
    "Your password reset link expires in 15 minutes. Please use it soon.",
    "Your password reset link expires in 15 minutes. Please use it as soon as possible.",
  );
  assertEquals(copied.sequenceOverlapSimilarity > 0.7, true);
  assertEquals(copied.similarity > 0.7, true);
});

Deno.test("TextSimilarity detailed mode scores paraphrases higher than fast mode", () => {
  const fast = calculateTextSimilarity(
    "The link expires after fifteen minutes.",
    "You have fifteen minutes before the link expires.",
  );
  const detailed = calculateTextSimilarity(
    "The link expires after fifteen minutes.",
    "You have fifteen minutes before the link expires.",
    { mode: "detailed" },
  );

  assertEquals(detailed.similarity > fast.similarity, true);
});

Deno.test("TextSimilarity detailed mode preserves content-word overlap despite stopword changes", () => {
  const result = calculateTextSimilarity(
    "The password reset link expires in fifteen minutes.",
    "Your reset link will expire in fifteen minutes.",
    { mode: "detailed" },
  );

  assertEquals((result.contentRecallSimilarity ?? 0) > 0.4, true);
  assertEquals(result.similarity > 0.4, true);
});

Deno.test("TextSimilarity detailed mode exposes component metrics", () => {
  const result = calculateTextSimilarity(
    "Color values are grouped by key.",
    "Colors are stored by key in a grouped mapping.",
    { mode: "detailed" },
  );

  assertEquals(result.mode, "detailed");
  assertEquals(typeof result.wordBigramDice, "number");
  assertEquals(typeof result.charTrigramDice, "number");
  assertEquals(typeof result.contentRecallSimilarity, "number");
});

Deno.test("TextSimilarity handles empty-string edge cases", () => {
  assertEquals(calculateTextSimilarity("", "").similarity, 1);
  assertEquals(calculateTextSimilarity("", "hello").similarity, 0);
  assertEquals(calculateTextSimilarity("", "", { mode: "detailed" }).similarity, 1);
});
