import {
  assertEquals,
  assertRejects,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import {
  ChatSession,
  OpenAICompatibleTransport,
  type ChatTransport,
  type ChatTransportPayload,
} from "../lib/ChatSession.ts";

function makeStreamResponse(chunks: string[]): Response {
  const enc = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(enc.encode(chunk));
      }
      controller.close();
    },
  });
  return new Response(body, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

function startLocalServer(
  handler: (req: Request) => Response | Promise<Response>,
): { baseUrl: string; close: () => Promise<void> } {
  const ac = new AbortController();
  const server = Deno.serve(
    { hostname: "127.0.0.1", port: 0, signal: ac.signal },
    handler,
  );
  const addr = server.addr as Deno.NetAddr;
  return {
    baseUrl: `http://${addr.hostname}:${addr.port}/`,
    close: async () => {
      ac.abort();
      await server.finished.catch(() => {});
    },
  };
}

Deno.test("ChatSession.send stores user+assistant history and preserves zero values", async () => {
  const requestPayloads: ChatTransportPayload[] = [];
  const transport: ChatTransport = {
    endpoint: "mock://chat",
    async request(payload) {
      requestPayloads.push(payload);
      return {
        choices: [{ message: { content: `assistant-${requestPayloads.length}` } }],
      };
    },
  };

  const chat = new ChatSession(
    "mock-model",
    { max_tokens: 111, temperature: 0.9 },
    transport,
  );

  await chat.send("hello", { max_tokens: 0, temperature: 0 });
  await chat.send("follow-up");

  assertEquals(requestPayloads[0].max_tokens, 0);
  assertEquals(requestPayloads[0].temperature, 0);
  assertEquals(
    requestPayloads[1].messages.map((m) => `${m.role}:${m.content}`),
    ["user:hello", "assistant:assistant-1", "user:follow-up"],
  );
});

Deno.test("ChatSession.stream stores streamed assistant reply and handles undefined options", async () => {
  const streamPayloads: ChatTransportPayload[] = [];
  const requestPayloads: ChatTransportPayload[] = [];
  const seenTokens: string[] = [];

  const transport: ChatTransport = {
    endpoint: "mock://chat",
    async request(payload) {
      requestPayloads.push(payload);
      return { choices: [{ message: { content: "done" } }] };
    },
    async stream(payload, onToken) {
      streamPayloads.push(payload);
      onToken("A");
      onToken("B");
    },
  };

  const chat = new ChatSession(
    "mock-model",
    { max_tokens: 77, temperature: 0 },
    transport,
  );

  await chat.stream("hi", undefined as any, (token) => seenTokens.push(token));
  await chat.send("after-stream");

  assertEquals(seenTokens.join(""), "AB");
  assertEquals(streamPayloads[0].max_tokens, 77);
  assertEquals(streamPayloads[0].temperature, 0);
  assertEquals(
    requestPayloads[0].messages.map((m) => `${m.role}:${m.content}`),
    ["user:hi", "assistant:AB", "user:after-stream"],
  );
});

Deno.test("ChatSession.send forwards model reasoning to debug hooks when present", async () => {
  const thoughts: string[] = [];
  const transport: ChatTransport = {
    endpoint: "mock://chat",
    async request() {
      return {
        choices: [{
          message: {
            content: "assistant-1",
            reasoning: "internal plan",
          },
        }],
      };
    },
  };

  const chat = new ChatSession(
    "mock-model",
    {},
    transport,
    {
      onThoughts: (value) => thoughts.push(value),
    },
  );

  await chat.send("hello");

  assertEquals(thoughts, ["internal plan"]);
});

Deno.test("ChatSession send defaults reasoning transport mode to off", async () => {
  const requestPayloads: ChatTransportPayload[] = [];
  const transport: ChatTransport = {
    endpoint: "mock://chat",
    async request(payload) {
      requestPayloads.push(payload);
      return { choices: [{ message: { content: "ok" } }] };
    },
  };

  const chat = new ChatSession("mock-model", { think: true }, transport);
  await chat.send("hello");

  assertEquals(requestPayloads[0].think, undefined);
  assertEquals(requestPayloads[0].extra_body, undefined);
});

Deno.test("ChatSession send emits think transport field in think mode", async () => {
  const requestPayloads: ChatTransportPayload[] = [];
  const transport: ChatTransport = {
    endpoint: "mock://chat",
    async request(payload) {
      requestPayloads.push(payload);
      return { choices: [{ message: { content: "ok" } }] };
    },
  };

  const chat = new ChatSession(
    "mock-model",
    { think: true, reasoning_mode: "think" },
    transport,
  );

  await chat.send("hello");
  await chat.send("follow-up", { think: false });

  assertEquals(requestPayloads[0].think, true);
  assertEquals(requestPayloads[0].extra_body, undefined);
  assertEquals(requestPayloads[1].think, false);
  assertEquals(requestPayloads[1].extra_body, undefined);
});

Deno.test("ChatSession send emits openai reasoning field in openai mode", async () => {
  const requestPayloads: ChatTransportPayload[] = [];
  const transport: ChatTransport = {
    endpoint: "mock://chat",
    async request(payload) {
      requestPayloads.push(payload);
      return { choices: [{ message: { content: "ok" } }] };
    },
  };

  const chat = new ChatSession(
    "mock-model",
    { think: true, reasoning_mode: "openai" },
    transport,
  );

  await chat.send("hello");
  await chat.send("follow-up", { think: false });

  assertEquals(requestPayloads[0].think, undefined);
  assertEquals(requestPayloads[0].extra_body, {
    reasoning: { enabled: true },
  });
  assertEquals(requestPayloads[1].think, undefined);
  assertEquals(requestPayloads[1].extra_body, {
    reasoning: { enabled: false },
  });
});

Deno.test("ChatSession fork preserves reasoning transport mode and stream uses same shaping", async () => {
  const streamPayloads: ChatTransportPayload[] = [];
  const transport: ChatTransport = {
    endpoint: "mock://chat",
    async request() {
      return { choices: [{ message: { content: "ok" } }] };
    },
    async stream(payload, onToken) {
      streamPayloads.push(payload);
      onToken("A");
    },
  };

  const chat = new ChatSession(
    "mock-model",
    { think: true, reasoning_mode: "openai" },
    transport,
  );
  const fork = chat.fork();

  await fork.stream("hello", undefined as any, () => {});
  await fork.stream("override", { think: false }, () => {});

  assertEquals(streamPayloads[0].think, undefined);
  assertEquals(streamPayloads[0].extra_body, {
    reasoning: { enabled: true },
  });
  assertEquals(streamPayloads[1].think, undefined);
  assertEquals(streamPayloads[1].extra_body, {
    reasoning: { enabled: false },
  });
});

Deno.test({
  name: "OpenAICompatibleTransport.request sends bearer auth header when api key is configured",
  permissions: { net: true },
  async fn() {
    let authorizationHeader = "";
    let httpRefererHeader = "";
    let xTitleHeader = "";
    const server = startLocalServer(async (req) => {
      authorizationHeader = req.headers.get("authorization") ?? "";
      httpRefererHeader = req.headers.get("http-referer") ?? "";
      xTitleHeader = req.headers.get("x-title") ?? "";
      return Response.json({
        choices: [{ message: { content: "ok" } }],
      });
    });

    try {
      const transport = new OpenAICompatibleTransport(server.baseUrl, {
        apiKey: "secret-token",
        httpReferer: "https://example.com/datagen",
        xTitle: "Datagen Test",
      });

      const response = await transport.request({
        model: "mock-model",
        messages: [{ role: "user", content: "ping" }],
      });

      assertEquals(response.choices[0].message.content, "ok");
      assertEquals(authorizationHeader, "Bearer secret-token");
      assertEquals(httpRefererHeader, "https://example.com/datagen");
      assertEquals(xTitleHeader, "Datagen Test");
    } finally {
      await server.close();
    }
  },
});

Deno.test({
  name: "OpenAICompatibleTransport.stream handles SSE JSON split across chunks",
  permissions: { net: true },
  async fn() {
    const server = startLocalServer((_req) =>
      makeStreamResponse([
        'data: {"choices":[{"delta":{"content":"Hel',
        'lo"}}]}\n\n',
        'data: {"choices":[{"delta":{"content":" world"}}]}\n\n',
        "data: [DONE]\n\n",
      ])
    );

    try {
      const transport = new OpenAICompatibleTransport(server.baseUrl);
      const tokens: string[] = [];

      await transport.stream(
        { model: "mock-model", messages: [{ role: "user", content: "ping" }], stream: true },
        (token) => tokens.push(token),
      );

      assertEquals(tokens.join(""), "Hello world");
    } finally {
      await server.close();
    }
  },
});

Deno.test({
  name: "OpenAICompatibleTransport.stream throws explicit error on HTTP failure",
  permissions: { net: true },
  async fn() {
    const server = startLocalServer((_req) =>
      new Response("upstream failed", { status: 500 })
    );

    try {
      const transport = new OpenAICompatibleTransport(server.baseUrl);
      await assertRejects(
        () =>
          transport.stream(
            { model: "mock-model", messages: [{ role: "user", content: "ping" }], stream: true },
            () => {},
          ),
        Error,
        "HTTP 500",
      );
    } finally {
      await server.close();
    }
  },
});
