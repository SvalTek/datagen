import {
  assertEquals,
  assertThrows,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import {
  getValueAtPath,
  setValueAtPathClone,
} from "../lib/ObjectPath.ts";

Deno.test("getValueAtPath resolves top-level and nested paths", () => {
  const record = {
    conversations: [{ from: "human", value: "hi" }],
    meta: {
      info: {
        id: "abc",
      },
    },
  };

  assertEquals(getValueAtPath(record, "conversations"), record.conversations);
  assertEquals(getValueAtPath(record, "meta.info.id"), "abc");
});

Deno.test("setValueAtPathClone updates cloned object only", () => {
  const record = {
    meta: {
      info: {
        id: "abc",
      },
    },
  };

  const updated = setValueAtPathClone(record, "meta.info.id", "xyz");
  assertEquals(updated.meta.info.id, "xyz");
  assertEquals(record.meta.info.id, "abc");
});

Deno.test("object path helpers fail on missing or invalid paths", () => {
  assertThrows(() => getValueAtPath({ meta: {} }, "meta.info.id"), Error);
  assertThrows(
    () => getValueAtPath({ conversations: {} }, "conversations.0"),
    Error,
  );
});
