import {
  assertEquals,
  assertStringIncludes,
} from "https://deno.land/std@0.203.0/assert/mod.ts";

function startLocalServer(
  handler: (req: Request) => Response | Promise<Response>,
): { baseUrl: string; close: () => Promise<void> } {
  const controller = new AbortController();
  const server = Deno.serve(
    { hostname: "127.0.0.1", port: 0, signal: controller.signal },
    handler,
  );
  const addr = server.addr as Deno.NetAddr;

  return {
    baseUrl: `http://${addr.hostname}:${addr.port}/`,
    close: async () => {
      controller.abort();
      await server.finished.catch(() => {});
    },
  };
}

Deno.test({
  name: "main.ts runs a pipeline end-to-end and writes output report",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let lastPrompt = "";
    let authHeader = "";
    let httpRefererHeader = "";
    let xTitleHeader = "";
    const server = startLocalServer(async (req) => {
      if (!req.url.endsWith("/v1/chat/completions")) {
        return new Response("not found", { status: 404 });
      }

      authHeader = req.headers.get("authorization") ?? "";
      httpRefererHeader = req.headers.get("http-referer") ?? "";
      xTitleHeader = req.headers.get("x-title") ?? "";
      const payload = await req.json();
      const messages = payload.messages ?? [];
      const userMessage = messages[messages.length - 1];
      lastPrompt = userMessage?.content ?? "";

      return Response.json({
        choices: [
          {
            message: {
              content: JSON.stringify({ records: [{ id: 1 }] }),
            },
          },
        ],
      });
    });

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const reportPath = await Deno.makeTempFile({ suffix: ".run.json" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-" });

    try {
      await Deno.writeTextFile(
        pipelinePath,
        `
name: integration-run
model: yaml-model
endpoint: ${server.baseUrl}
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one record
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
          "--context",
          '{"tenant":"acme"}',
          "--out",
          reportPath,
        ],
        env: {
          DATAGEN_MODEL: "",
        },
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);
      const writtenReport = JSON.parse(await Deno.readTextFile(reportPath));

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(report.model, "yaml-model");
      assertEquals(report.pipeline.name, "integration-run");
      assertEquals(report.result.outputsByStage.seed, { records: [{ id: 1 }] });
      assertEquals(writtenReport.ok, true);
      assertStringIncludes(report.outputJsonlPath, "integration-run.jsonl");
      const jsonlText = await Deno.readTextFile(
        `${outputDir}/integration-run.jsonl`,
      );
      assertEquals(jsonlText.trim(), JSON.stringify({ records: [{ id: 1 }] }));
      assertEquals(authHeader, "");
      assertEquals(httpRefererHeader, "");
      assertEquals(xTitleHeader, "");
      assertStringIncludes(lastPrompt, "Initial Context (JSON):");
      assertStringIncludes(lastPrompt, '"tenant": "acme"');
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(reportPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts applies pipeline-level completion settings when CLI does not override them",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let lastPayload: Record<string, unknown> | undefined;
    const server = startLocalServer(async (req) => {
      lastPayload = await req.json();
      return Response.json({
        choices: [
          {
            message: {
              content: JSON.stringify([{ id: 1 }]),
            },
          },
        ],
      });
    });

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-pipeline-completion-" });

    try {
      await Deno.writeTextFile(
        pipelinePath,
        `
name: pipeline-completion-run
model: yaml-model
endpoint: ${server.baseUrl}
maxTokens: 2048
temperature: 1.5
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one record
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(lastPayload?.max_tokens, 2048);
      assertEquals(lastPayload?.temperature, 1.5);
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts keeps reasoningMode config compatible without transport-specific payload shaping",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const seenPayloads: Array<Record<string, unknown>> = [];
    const server = startLocalServer(async (req) => {
      seenPayloads.push(await req.json());
      return Response.json({
        choices: [
          {
            message: {
              content: JSON.stringify({ ok: true }),
            },
          },
        ],
      });
    });

    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-reasoning-mode-" });
    const pipelinePaths = await Promise.all([
      Deno.makeTempFile({ suffix: ".pipeline.yaml" }),
      Deno.makeTempFile({ suffix: ".pipeline.yaml" }),
      Deno.makeTempFile({ suffix: ".pipeline.yaml" }),
      Deno.makeTempFile({ suffix: ".pipeline.yaml" }),
    ]);

    try {
      await Deno.writeTextFile(
        pipelinePaths[0],
        `
name: reasoning-think
model: yaml-model
endpoint: ${server.baseUrl}
reasoningMode: think
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one record
    reasoning: true
`,
      );
      await Deno.writeTextFile(
        pipelinePaths[1],
        `
name: reasoning-openai
model: yaml-model
endpoint: ${server.baseUrl}
reasoningMode: openai
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one record
    reasoning: true
`,
      );
      await Deno.writeTextFile(
        pipelinePaths[2],
        `
name: reasoning-implicit-off
model: yaml-model
endpoint: ${server.baseUrl}
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one record
    reasoning: true
`,
      );
      await Deno.writeTextFile(
        pipelinePaths[3],
        `
name: reasoning-explicit-off
model: yaml-model
endpoint: ${server.baseUrl}
reasoningMode: off
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one record
    reasoning: true
`,
      );

      for (const pipelinePath of pipelinePaths) {
        const command = new Deno.Command(Deno.execPath(), {
          args: [
            "run",
            "--allow-read",
            "--allow-write",
            "--allow-net",
            "--allow-env",
            "main.ts",
            "--console",
            "full",
            pipelinePath,
          ],
          stdout: "piped",
          stderr: "piped",
        });
        const output = await command.output();
        assertEquals(output.code, 0);
      }

      assertEquals(seenPayloads[0].think, undefined);
      assertEquals(seenPayloads[0].extra_body, undefined);
      assertEquals(seenPayloads[1].think, undefined);
      assertEquals(seenPayloads[1].extra_body, undefined);
      assertEquals(seenPayloads[2].think, undefined);
      assertEquals(seenPayloads[2].extra_body, undefined);
      assertEquals(seenPayloads[3].think, undefined);
      assertEquals(seenPayloads[3].extra_body, undefined);
    } finally {
      await server.close();
      for (const pipelinePath of pipelinePaths) {
        await Deno.remove(pipelinePath).catch(() => {});
      }
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts resolves provider precedence as CLI > pipeline > DATAGEN_PROVIDER > default",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const server = startLocalServer(async () =>
      Response.json({
        choices: [{ message: { content: JSON.stringify({ ok: true }) } }],
      })
    );

    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-provider-" });
    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });

    try {
      await Deno.writeTextFile(
        pipelinePath,
        `
name: provider-precedence
model: yaml-model
provider: ollama
endpoint: ${server.baseUrl}
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one record
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
          "--provider",
          "openai",
        ],
        env: {
          DATAGEN_PROVIDER: "ollama",
        },
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const report = JSON.parse(new TextDecoder().decode(output.stdout).trim());

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(report.provider, "openai");
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts resolves api key from pipeline apiKeyEnv and sends bearer auth header",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let authHeader = "";
    let httpRefererHeader = "";
    let xTitleHeader = "";
    const server = startLocalServer(async (req) => {
      authHeader = req.headers.get("authorization") ?? "";
      httpRefererHeader = req.headers.get("http-referer") ?? "";
      xTitleHeader = req.headers.get("x-title") ?? "";
      return Response.json({
        choices: [
          {
            message: {
              content: JSON.stringify([{ id: 1 }]),
            },
          },
        ],
      });
    });

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-auth-" });

    try {
      await Deno.writeTextFile(
        pipelinePath,
        `
name: auth-run
model: yaml-model
endpoint: ${server.baseUrl}
apiKeyEnv: OPENROUTER_API_KEY
httpReferer: https://example.com/datagen
xTitle: Datagen Auth Test
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one record
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        env: {
          OPENROUTER_API_KEY: "pipeline-secret",
        },
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(authHeader, "Bearer pipeline-secret");
      assertEquals(httpRefererHeader, "https://example.com/datagen");
      assertEquals(xTitleHeader, "Datagen Auth Test");
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts fails fast when pipeline apiKeyEnv is declared but missing",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-missing-auth-" });

    try {
      await Deno.writeTextFile(
        pipelinePath,
        `
name: missing-auth-run
model: yaml-model
endpoint: http://127.0.0.1:9/
apiKeyEnv: OPENROUTER_API_KEY
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one record
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        env: {
          OPENROUTER_API_KEY: "",
        },
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stderr = new TextDecoder().decode(output.stderr).trim();
      const reportPath = `${outputDir}/missing-auth-run.report.json`;
      const report = JSON.parse(await Deno.readTextFile(reportPath));

      assertEquals(output.code, 2);
      assertStringIncludes(stderr, "AuthenticationError");
      assertEquals(report.ok, false);
      assertEquals(report.errorType, "AuthenticationError");
      assertStringIncludes(report.errorHint, "token");
      assertStringIncludes(report.error.message, "OPENROUTER_API_KEY");
    } finally {
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts accepts provider attribution headers from CLI flags",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let httpRefererHeader = "";
    let xTitleHeader = "";
    const server = startLocalServer(async (req) => {
      httpRefererHeader = req.headers.get("http-referer") ?? "";
      xTitleHeader = req.headers.get("x-title") ?? "";
      return Response.json({
        choices: [
          {
            message: {
              content: JSON.stringify([{ id: 1 }]),
            },
          },
        ],
      });
    });

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-headers-" });

    try {
      await Deno.writeTextFile(
        pipelinePath,
        `
name: headers-run
model: yaml-model
endpoint: ${server.baseUrl}
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one record
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
          "--http-referer",
          "https://example.com/cli",
          "--x-title",
          "Datagen CLI Test",
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(httpRefererHeader, "https://example.com/cli");
      assertEquals(xTitleHeader, "Datagen CLI Test");
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts exits with config error when model is missing",
  permissions: { run: true, read: true, write: true },
  async fn() {
    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });

    try {
      await Deno.writeTextFile(
        pipelinePath,
        `
stages:
  - instructions: Generate one record
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: ["run", "--allow-read", "--allow-write", "--allow-env", "main.ts", "--console", "full", pipelinePath],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stderr = new TextDecoder().decode(output.stderr);

      assertEquals(output.code, 2);
      assertStringIncludes(stderr, "Model is required");
    } finally {
      await Deno.remove(pipelinePath).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts transforms JSONL input datasets and reports warnings",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let callIndex = 0;
    const server = startLocalServer(async () => {
      callIndex++;
      return Response.json({
        choices: [
          {
            message: {
              content: callIndex === 1
                ? "<reasoning>One</reasoning>\nHello"
                : "   ",
            },
          },
        ],
      });
    });

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-transform-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        [
          JSON.stringify({
            id: 1,
            conversations: [
              { from: "human", value: "Hi" },
              { from: "gpt", value: "Hello" },
            ],
          }),
          JSON.stringify({
            id: 2,
            conversations: [
              { from: "human", value: "Hi again" },
              { from: "gpt", value: "Original" },
            ],
          }),
        ].join("\n"),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);
      const outputText = await Deno.readTextFile(`${outputDir}/transform-run.jsonl`);
      const outputLines = outputText.trim().split(/\r?\n/).map((line) => JSON.parse(line));

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(report.inputFormat, "jsonl");
      assertEquals(report.inputRecordCount, 2);
      assertEquals(report.result.warnings.length, 1);
      assertEquals(outputLines[0].conversations[1].value, "<reasoning>One</reasoning>\nHello");
      assertEquals(outputLines[1].conversations[1].value, "Original");
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts record_transform fails instead of warning-spamming on authorization errors",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const server = startLocalServer(async () =>
      new Response("Unauthorized", { status: 401 })
    );

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-authfail-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        [
          JSON.stringify({
            conversations: [
              { from: "human", value: "Hi" },
              { from: "gpt", value: "Hello" },
            ],
          }),
          JSON.stringify({
            conversations: [
              { from: "human", value: "Hi again" },
              { from: "gpt", value: "Original" },
            ],
          }),
        ].join("\n"),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: authfail-transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);

      assertEquals(output.code, 3);
      assertEquals(report.ok, false);
      assertEquals(report.result.failedStage.error.kind, "model_call_failed");
      assertEquals(report.result.failedStage.error.retryable, false);
      assertStringIncludes(
        report.result.failedStage.error.message.toLowerCase(),
        "unauthorized",
      );
      assertEquals(report.result.warnings.length, 1);
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts reports validator warnings for record_transform turn validation failures",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const server = startLocalServer(async () =>
      Response.json({
        choices: [
          {
            message: {
              content: "Hello",
            },
          },
        ],
      })
    );

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-validator-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        JSON.stringify({
          id: 1,
          conversations: [
            { from: "human", value: "Hi" },
            { from: "gpt", value: "Hello" },
          ],
        }),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: validator-transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    validate:
      rules:
        - path: content
          kind: contains
          value: "<reasoning>"
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(report.result.warnings.length, 1);
      assertEquals(report.result.warnings[0].kind, "validator_mismatch.contains");
      assertEquals(report.result.traces[0].subtraces.length, 1);
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts retries record_transform turn rewrites and reports attempt metadata",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let callIndex = 0;
    const server = startLocalServer(async () => {
      callIndex++;
      return Response.json({
        choices: [
          {
            message: {
              content: callIndex === 1
                ? "Hello"
                : "<reasoning>ok</reasoning>\nHello",
            },
          },
        ],
      });
    });

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-retry-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        JSON.stringify({
          id: 1,
          conversations: [
            { from: "human", value: "Hi" },
            { from: "gpt", value: "Hello" },
          ],
        }),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: retry-transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    retry:
      enabled: true
      maxAttempts: 2
    validate:
      rules:
        - path: content
          kind: contains
          value: "<reasoning>"
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(report.result.traces[0].subtraces.length, 2);
      assertEquals(report.result.traces[0].subtraces[0].attempt, 1);
      assertEquals(report.result.traces[0].subtraces[1].attempt, 2);
      assertEquals(report.result.warnings.length, 0);
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts similarity validators can retry repeated assistant rewrites and preserve warning semantics",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let callIndex = 0;
    const server = startLocalServer(async () => {
      callIndex++;
      const content = callIndex === 1
        ? "A hash map stores values by key for fast lookup."
        : callIndex === 2
        ? "A hash map stores values by key for fast lookup."
        : "Use one when you need quick lookups, inserts, or updates by key, such as storing users by ID.";

      return Response.json({
        choices: [{ message: { content } }],
      });
    });

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-similarity-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        JSON.stringify({
          id: 1,
          conversations: [
            { from: "human", value: "Explain a hash map simply." },
            { from: "gpt", value: "A hash map stores values by key for fast lookup." },
            { from: "human", value: "When would I use one?" },
            { from: "gpt", value: "Use one when you need fast lookups by key." },
          ],
        }),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: similarity-transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    retry:
      enabled: true
      maxAttempts: 2
    validate:
      rules:
        - path: content
          kind: max_similarity_to_ref
          ref: previous_same_role_turn.value
          threshold: 0.82
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);
      const outputText = await Deno.readTextFile(
        `${outputDir}/similarity-transform-run.jsonl`,
      );
      const outputRecord = JSON.parse(outputText.trim());

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(report.result.warnings.length, 0);
      assertEquals(report.result.traces[0].subtraces.length, 3);
      assertEquals(report.result.traces[0].subtraces[1].failureKind, "validator_mismatch");
      assertStringIncludes(
        report.result.traces[0].subtraces[1].validationIssues[0].message,
        "previous_same_role_turn.value",
      );
      assertEquals(
        outputRecord.conversations[3].value,
        "Use one when you need quick lookups, inserts, or updates by key, such as storing users by ID.",
      );
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts scoped similarity validators ignore reasoning boilerplate while still enforcing answer diversity",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let callIndex = 0;
    const server = startLocalServer(async () => {
      callIndex++;
      const content = callIndex === 1
        ? "<reasoning>Same chain</reasoning> A hash map stores values by key for fast lookup."
        : callIndex === 2
        ? "<reasoning>Same chain</reasoning> A hash map stores values by key for fast lookup."
        : "<reasoning>Same chain</reasoning> Use one when you need quick lookups, inserts, or updates by key, such as storing users by ID.";

      return Response.json({
        choices: [{ message: { content } }],
      });
    });

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-scope-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        JSON.stringify({
          id: 1,
          conversations: [
            { from: "human", value: "Explain a hash map simply." },
            { from: "gpt", value: "A hash map stores values by key for fast lookup." },
            { from: "human", value: "When would I use one?" },
            { from: "gpt", value: "Use one when you need fast lookups by key." },
          ],
        }),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: scoped-similarity-transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    retry:
      enabled: true
      maxAttempts: 2
    validate:
      rules:
        - path: content
          kind: max_similarity_to_ref
          ref: previous_same_role_turn.value
          threshold: 0.82
          scope:
            excludePatterns:
              - pattern: "<reasoning>[\\\\s\\\\S]*?</reasoning>"
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);
      const outputText = await Deno.readTextFile(
        `${outputDir}/scoped-similarity-transform-run.jsonl`,
      );
      const outputRecord = JSON.parse(outputText.trim());

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(report.result.warnings.length, 0);
      assertEquals(report.result.traces[0].subtraces.length, 3);
      assertStringIncludes(
        report.result.traces[0].subtraces[1].validationIssues[0].message,
        "previous_same_role_turn.value",
      );
      assertEquals(
        outputRecord.conversations[3].value,
        "<reasoning>Same chain</reasoning> Use one when you need quick lookups, inserts, or updates by key, such as storing users by ID.",
      );
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts detailed similarity mode can accept paraphrases that fast mode rejects",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const paraphrase = "You have fifteen minutes before the link expires.";
    const server = startLocalServer(async () =>
      Response.json({
        choices: [{ message: { content: paraphrase } }],
      })
    );

    const fastPipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const detailedPipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-detailed-sim-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        JSON.stringify({
          id: 1,
          conversations: [
            { from: "human", value: "How long is the link valid?" },
            { from: "gpt", value: "The link expires after fifteen minutes." },
          ],
        }),
      );

      await Deno.writeTextFile(
        fastPipelinePath,
        `
name: fast-sim-transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    validate:
      rules:
        - path: content
          kind: min_similarity_to_ref
          ref: original_target_content
          threshold: 0.75
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      await Deno.writeTextFile(
        detailedPipelinePath,
        `
name: detailed-sim-transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    validate:
      rules:
        - path: content
          kind: min_similarity_to_ref
          ref: original_target_content
          threshold: 0.55
          similarity:
            mode: detailed
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      const fastCommand = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          fastPipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });
      const detailedCommand = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          detailedPipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const fastOutput = await fastCommand.output();
      const detailedOutput = await detailedCommand.output();
      const fastReport = JSON.parse(new TextDecoder().decode(fastOutput.stdout).trim());
      const detailedReport = JSON.parse(new TextDecoder().decode(detailedOutput.stdout).trim());

      assertEquals(fastOutput.code, 0);
      assertEquals(detailedOutput.code, 0);
      assertEquals(fastReport.result.warnings.length, 1);
      assertEquals(
        fastReport.result.warnings[0].kind,
        "validator_mismatch.min_similarity_to_ref",
      );
      assertEquals(detailedReport.result.warnings.length, 0);
    } finally {
      await server.close();
      await Deno.remove(fastPipelinePath).catch(() => {});
      await Deno.remove(detailedPipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts must_change_from_ref accepts reasoning-prefix rewrites and retries copy-through outputs",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let callIndex = 0;
    const server = startLocalServer(async () => {
      callIndex++;
      const content = callIndex === 1
        ? "Hello there"
        : "<reasoning>Plan</reasoning> Hello there";

      return Response.json({
        choices: [{ message: { content } }],
      });
    });

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-must-change-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        JSON.stringify({
          id: 1,
          conversations: [
            { from: "human", value: "Hi" },
            { from: "gpt", value: "Hello there" },
          ],
        }),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: must-change-transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    retry:
      enabled: true
      maxAttempts: 2
    validate:
      rules:
        - path: content
          kind: must_change_from_ref
          ref: original_target_content
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);
      const outputText = await Deno.readTextFile(`${outputDir}/must-change-transform-run.jsonl`);
      const outputRecord = JSON.parse(outputText.trim());

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(report.result.warnings.length, 0);
      assertEquals(report.result.traces[0].subtraces.length, 2);
      assertStringIncludes(
        report.result.traces[0].subtraces[0].validationIssues[0].message,
        "Value must differ from ref 'original_target_content'",
      );
      assertEquals(outputRecord.conversations[1].value, "<reasoning>Plan</reasoning> Hello there");
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts remaps prefixed string conversation input before record_transform",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const server = startLocalServer(async () =>
      Response.json({
        choices: [{
          message: {
            content: "<reasoning>Plan</reasoning> Hi there",
          },
        }],
      })
    );

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".json" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-remap-prefixed-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        JSON.stringify([{
          id: 1,
          conversation: [
            "user: Hello",
            "assistant: Original",
          ],
        }]),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: remap-prefixed-transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: json
  remap:
    kind: prefixed_string_array
    sourcePath: conversation
    prefixes:
      user: "user:"
      assistant: "assistant:"
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - assistant
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);
      const outputText = await Deno.readTextFile(
        `${outputDir}/remap-prefixed-transform-run.jsonl`,
      );
      const outputRecord = JSON.parse(outputText.trim());

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(report.inputFormat, "json");
      assertEquals(report.inputRecordCount, 1);
      assertEquals(outputRecord.conversation[0], "user: Hello");
      assertEquals(outputRecord.conversations[0], { from: "user", value: "Hello" });
      assertEquals(outputRecord.conversations[1].value, "<reasoning>Plan</reasoning> Hi there");
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts remaps Alpaca input before record_transform",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const server = startLocalServer(async () =>
      Response.json({
        choices: [{
          message: {
            content: "Pirate answer upgraded",
          },
        }],
      })
    );

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-remap-alpaca-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        JSON.stringify({
          instruction: "Answer as a pirate.",
          input: "Explain recursion.",
          output: "Original answer",
        }),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: remap-alpaca-transform-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
  remap:
    kind: alpaca
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - assistant
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout).trim();
      const report = JSON.parse(stdout);
      const outputText = await Deno.readTextFile(
        `${outputDir}/remap-alpaca-transform-run.jsonl`,
      );
      const outputRecord = JSON.parse(outputText.trim());

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(report.inputFormat, "jsonl");
      assertEquals(report.inputRecordCount, 1);
      assertEquals(outputRecord.instruction, "Answer as a pirate.");
      assertEquals(outputRecord.conversations[0], {
        from: "system",
        value: "Answer as a pirate.",
      });
      assertEquals(outputRecord.conversations[1], {
        from: "user",
        value: "Explain recursion.",
      });
      assertEquals(outputRecord.conversations[2], {
        from: "assistant",
        value: "Pirate answer upgraded",
      });
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts writes a default report file and prints summary output by default",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const server = startLocalServer(async () =>
      Response.json({
        choices: [{ message: { content: JSON.stringify({ ok: true }) } }],
      })
    );

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-summary-" });

    try {
      await Deno.writeTextFile(
        pipelinePath,
        `
name: summary-run
model: yaml-model
endpoint: ${server.baseUrl}
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one object
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--no-progress",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout);
      const reportPath = `${outputDir}/summary-run.report.json`;
      const report = JSON.parse(await Deno.readTextFile(reportPath));

      assertEquals(output.code, 0);
      assertStringIncludes(stdout, "Running summary-run");
      assertStringIncludes(stdout, "Done");
      assertEquals(report.ok, true);
      assertStringIncludes(
        report.outputJsonlPath.replace(/\\/g, "/"),
        `${outputDir.replace(/\\/g, "/")}/summary-run.jsonl`,
      );
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts warnings console mode prints compact warnings instead of full report",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const server = startLocalServer(async () =>
      Response.json({
        choices: [{ message: { content: "Hello" } }],
      })
    );

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-warnings-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        JSON.stringify({
          conversations: [
            { from: "human", value: "Hi" },
            { from: "gpt", value: "Hello" },
          ],
        }),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: warnings-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    validate:
      rules:
        - path: content
          kind: contains
          value: "<think>"
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "warnings",
          "--no-progress",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const stdout = new TextDecoder().decode(output.stdout);
      const warningCount = (stdout.match(/^WARN /gm) ?? []).length;

      assertEquals(output.code, 0);
      assertStringIncludes(stdout, "WARN stage=rewrite");
      assertStringIncludes(stdout, "validator_mismatch");
      assertStringIncludes(stdout, "Value does not contain required substring");
      assertEquals(warningCount, 1);
      assertEquals(stdout.includes('"traces"'), false);
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts only prints model thoughts when --show-thoughts is enabled",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    const server = startLocalServer(async () =>
      Response.json({
        choices: [{
          message: {
            content: JSON.stringify({ ok: true }),
            reasoning: "private chain",
          },
        }],
      })
    );

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-thoughts-" });

    try {
      await Deno.writeTextFile(
        pipelinePath,
        `
name: thoughts-run
model: yaml-model
endpoint: ${server.baseUrl}
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: seed
    instructions: Generate one object
`,
      );

      const baseArgs = [
        "run",
        "--allow-read",
        "--allow-write",
        "--allow-net",
        "--allow-env",
        "main.ts",
        "--no-progress",
        pipelinePath,
      ];

      const withoutThoughts = await new Deno.Command(Deno.execPath(), {
        args: baseArgs,
        stdout: "piped",
        stderr: "piped",
      }).output();
      const withThoughts = await new Deno.Command(Deno.execPath(), {
        args: [...baseArgs.slice(0, 7), "--show-thoughts", ...baseArgs.slice(7)],
        stdout: "piped",
        stderr: "piped",
      }).output();

      assertEquals(
        new TextDecoder().decode(withoutThoughts.stdout).includes("Model thoughts:"),
        false,
      );
      assertStringIncludes(new TextDecoder().decode(withThoughts.stdout), "Model thoughts:");
      assertStringIncludes(new TextDecoder().decode(withThoughts.stdout), "private chain");
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts workflow_delegate uses child workflow runtime config and propagates delegated output",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let parentCallCount = 0;
    const parentServer = startLocalServer(async () => {
      parentCallCount++;
      if (parentCallCount === 1) {
        return Response.json({
          choices: [{ message: { content: JSON.stringify({ candidate: { text: "hello" } }) } }],
        });
      }
      return Response.json({
        choices: [{ message: { content: JSON.stringify({ finalized: true }) } }],
      });
    });

    let childCallCount = 0;
    let childLastPrompt = "";
    const childServer = startLocalServer(async (req) => {
      childCallCount++;
      const payload = await req.json();
      const messages = payload.messages ?? [];
      childLastPrompt = messages[messages.length - 1]?.content ?? "";
      return Response.json({
        choices: [{ message: { content: JSON.stringify({ decision: { route: "accept" } }) } }],
      });
    });

    const parentPath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const childPath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-delegate-" });

    try {
      await Deno.writeTextFile(
        childPath,
        `
name: child-decision
model: child-model
endpoint: ${childServer.baseUrl}
stages:
  - id: judge
    instructions: Return decision JSON
`,
      );

      await Deno.writeTextFile(
        parentPath,
        `
name: delegated-parent
model: parent-model
endpoint: ${parentServer.baseUrl}
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - id: seed
    instructions: Return seed candidate
  - id: delegated
    mode: workflow_delegate
    dependsOn: [seed]
    instructions: Delegate to child
    delegate:
      workflowPath: ${childPath.replace(/\\/g, "/")}
      inputFromPath: outputsByStage.seed.candidate
      inputAs: initial_context
  - id: finalize
    dependsOn: [delegated]
    when:
      path: outputsByStage.delegated.decision.route
      equals: accept
    instructions: Return final output
`,
      );

      const command = new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          parentPath,
        ],
        stdout: "piped",
        stderr: "piped",
      });

      const output = await command.output();
      const report = JSON.parse(new TextDecoder().decode(output.stdout).trim());

      assertEquals(output.code, 0);
      assertEquals(report.ok, true);
      assertEquals(parentCallCount, 2);
      assertEquals(childCallCount, 1);
      assertEquals(report.result.stageStatuses.finalize, "executed");
      assertStringIncludes(childLastPrompt, "Initial Context (JSON):");
      assertStringIncludes(childLastPrompt, '"text": "hello"');
      assertEquals(report.result.outputsByStage.delegated, { decision: { route: "accept" } });
    } finally {
      await parentServer.close();
      await childServer.close();
      await Deno.remove(parentPath).catch(() => {});
      await Deno.remove(childPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});

Deno.test({
  name: "main.ts supports streaming record_transform with checkpoint and resume",
  permissions: { read: true, write: true, net: true, run: true, env: true },
  async fn() {
    let callIndex = 0;
    const server = startLocalServer(async () => {
      callIndex++;
      return Response.json({
        choices: [{
          message: {
            content: `<reasoning>${callIndex}</reasoning>\nrewritten-${callIndex}`,
          },
        }],
      });
    });

    const pipelinePath = await Deno.makeTempFile({ suffix: ".pipeline.yaml" });
    const inputPath = await Deno.makeTempFile({ suffix: ".jsonl" });
    const outputDir = await Deno.makeTempDir({ prefix: "datagen-output-stream-resume-" });

    try {
      await Deno.writeTextFile(
        inputPath,
        [
          JSON.stringify({
            id: 1,
            conversations: [
              { from: "human", value: "Hi 1" },
              { from: "gpt", value: "Original 1" },
            ],
          }),
          JSON.stringify({
            id: 2,
            conversations: [
              { from: "human", value: "Hi 2" },
              { from: "gpt", value: "Original 2" },
            ],
          }),
        ].join("\n"),
      );

      await Deno.writeTextFile(
        pipelinePath,
        `
name: stream-resume-run
model: yaml-model
endpoint: ${server.baseUrl}
input:
  path: ${inputPath.replace(/\\/g, "/")}
  format: jsonl
  readMode: stream
outputDir: ${outputDir.replace(/\\/g, "/")}
stages:
  - name: rewrite
    mode: record_transform
    instructions: Rewrite assistant turns
    transform:
      kind: conversation_rewrite
      conversationsPath: conversations
      roleField: from
      contentField: value
      targetRoles:
        - gpt
`,
      );

      const firstRun = await new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          "--checkpoint-every",
          "1",
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      }).output();
      const firstReport = JSON.parse(new TextDecoder().decode(firstRun.stdout).trim());

      const checkpointPath = `${outputDir}/stream-resume-run.checkpoint.json`;
      const checkpoint = JSON.parse(await Deno.readTextFile(checkpointPath));
      const outputPath = `${outputDir}/stream-resume-run.jsonl`;
      const firstOutputLines = (await Deno.readTextFile(outputPath)).trim().split("\n");

      assertEquals(firstRun.code, 0);
      assertEquals(firstReport.ok, true);
      assertEquals(firstReport.processedCount, 2);
      assertEquals(checkpoint.nextRecordOffset, 2);
      assertEquals(firstOutputLines.length, 2);

      const secondRun = await new Deno.Command(Deno.execPath(), {
        args: [
          "run",
          "--allow-read",
          "--allow-write",
          "--allow-net",
          "--allow-env",
          "main.ts",
          "--console",
          "full",
          "--resume",
          checkpointPath,
          pipelinePath,
        ],
        stdout: "piped",
        stderr: "piped",
      }).output();
      const secondReport = JSON.parse(new TextDecoder().decode(secondRun.stdout).trim());
      const secondOutputLines = (await Deno.readTextFile(outputPath)).trim().split("\n");

      assertEquals(secondRun.code, 0);
      assertEquals(secondReport.ok, true);
      assertEquals(secondReport.resumeFrom, 2);
      assertEquals(secondReport.processedCount, 0);
      assertEquals(secondOutputLines.length, 2);
    } finally {
      await server.close();
      await Deno.remove(pipelinePath).catch(() => {});
      await Deno.remove(inputPath).catch(() => {});
      await Deno.remove(outputDir, { recursive: true }).catch(() => {});
    }
  },
});
