import {
  assertEquals,
  assertRejects,
  assertThrows,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import { TextTemplate } from "../lib/TextTemplate.ts";

Deno.test("TextTemplate renders basic named placeholders", async () => {
  const t = new TextTemplate("Hello, {name}!");
  assertEquals(await t({ name: "Alice" }), "Hello, Alice!");
});

Deno.test("TextTemplate normalizes leading and trailing single newline in constructor", async () => {
  const t = new TextTemplate("\nline-1\nline-2\n");
  assertEquals(await t({}), "line-1\nline-2");
});

Deno.test("TextTemplate leaves unknown placeholders unchanged", async () => {
  const t = new TextTemplate("A={a}, B={b}");
  assertEquals(await t({ a: 1 }), "A=1, B={b}");
});

Deno.test("TextTemplate resolves async values and async value functions", async () => {
  const t = new TextTemplate("{a}-{b}");
  const out = await t({
    a: Promise.resolve("X"),
    b: async () => "Y",
  });
  assertEquals(out, "X-Y");
});

Deno.test("TextTemplate passes filtered args and positional params to value functions", async () => {
  const t = new TextTemplate("{a}:{b}:$1:$2");
  let seen: Record<string, unknown> | null = null;
  const out = await t(
    {
      a: (args: Record<string, unknown>, ...pos: unknown[]) => {
        seen = args;
        return `A-${String(pos[0])}-${String(pos[1])}`;
      },
      b: "B",
    },
    "P1",
    "P2",
  );
  assertEquals(out, "A-P1-P2:B:P1:P2");
  assertEquals(seen, { b: "B" });
});

Deno.test("TextTemplate escapes regex-significant placeholder keys", async () => {
  const t = new TextTemplate("{a.b}|{x+y}|{p*q}|{d$}");
  const out = await t({
    "a.b": "AB",
    "x+y": "XY",
    "p*q": "PQ",
    "d$": "D",
  });
  assertEquals(out, "AB|XY|PQ|D");
});

Deno.test("TextTemplate applies global replacement for repeated named placeholders", async () => {
  const t = new TextTemplate("{x}-{x}-{x}");
  assertEquals(await t({ x: "v" }), "v-v-v");
});

Deno.test("TextTemplate supports spread and indexed positional placeholders", async () => {
  const t = new TextTemplate("all=${...}; one=$1; two=$2; missing=$9");
  const out = await t({}, "first", 2, true);
  assertEquals(out, "all=first 2 true; one=first; two=2; missing=$9");
});

Deno.test("TextTemplate handles replacement values that include $ without mangling", async () => {
  const t = new TextTemplate("v={k}");
  assertEquals(await t({ k: "$1 and $$ and $&" }), "v=$1 and $$ and $&");
});

Deno.test("TextTemplate toString throws and Symbol.toPrimitive returns template string", () => {
  const t = new TextTemplate("raw");
  assertThrows(() => t.toString(), Error, "Use await template()");
  assertEquals(String(t), "raw");
  assertEquals(`${t}`, "raw");
});

Deno.test("TextTemplate clone returns independent instance with same content", async () => {
  const t = new TextTemplate("a\nb");
  const c = t.clone();
  const indented = c.indent(2);

  assertEquals(await t({}), "a\nb");
  assertEquals(await c({}), "a\nb");
  assertEquals(await indented({}), "  a\n  b");
});

Deno.test("TextTemplate indent pads non-empty lines only", async () => {
  const t = new TextTemplate("a\n\n  \n b ");
  const out = await t.indent(2)({});
  assertEquals(out, "  a\n\n  \n   b ");
});

Deno.test("TextTemplate trimLines trims only line ends", async () => {
  const t = new TextTemplate("  a  \n b\t \n\tc");
  const out = await t.trimLines()({});
  assertEquals(out, "  a\n b\n\tc");
});

Deno.test("TextTemplate propagates errors from value functions", async () => {
  const t = new TextTemplate("{a}");
  await assertRejects(
    async () => {
      await t({
        a: () => {
          throw new Error("boom");
        },
      });
    },
    Error,
    "boom",
  );
});
