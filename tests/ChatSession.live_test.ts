import { assertEquals, assertStringIncludes } from "https://deno.land/std@0.203.0/assert/mod.ts";
import { RequestChatCompletion, RequestChatStream } from "../lib/ChatSession.ts";

const LIVE_MODEL = "llama3.2:latest";

Deno.test({
  name: "live: RequestChatCompletion returns text from Ollama",
  permissions: { net: true },
  async fn() {
    const text = await RequestChatCompletion(
      "Return exactly the word OK in uppercase.",
      LIVE_MODEL,
      { max_tokens: 16, temperature: 0 },
    );

    console.log("Received completion:", text);
    assertEquals(typeof text, "string");
    assertStringIncludes(text.toUpperCase(), "OK");
  },
});

Deno.test({
  name: "live: RequestChatStream emits tokens from Ollama",
  permissions: { net: true },
  async fn() {
    const chunks: string[] = [];
    await RequestChatStream(
      "Answer with exactly: STREAM_OK",
      LIVE_MODEL,
      { max_tokens: 24, temperature: 0 },
      (token) => chunks.push(token),
    );

    const out = chunks.join("");
    console.log("Received stream:", out);
    assertEquals(typeof out, "string");
    assertStringIncludes(out.toUpperCase(), "STREAM");
  },
});
