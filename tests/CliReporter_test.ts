import {
  assertEquals,
  assertStringIncludes,
} from "https://deno.land/std@0.203.0/assert/mod.ts";
import { CliReporter } from "../lib/CliReporter.ts";

function makeReporter(
  options: ConstructorParameters<typeof CliReporter>[0],
): { reporter: CliReporter; stdout: string[]; stderr: string[]; rawStdout: string[]; rawStderr: string[] } {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const rawStdout: string[] = [];
  const rawStderr: string[] = [];
  const reporter = new CliReporter(options, {
    writeStdoutLine: (line) => stdout.push(line),
    writeStderrLine: (line) => stderr.push(line),
    writeStdoutRaw: (text) => rawStdout.push(text),
    writeStderrRaw: (text) => rawStderr.push(text),
  });
  return { reporter, stdout, stderr, rawStdout, rawStderr };
}

Deno.test("CliReporter summary mode prints start and finish summaries", () => {
  const { reporter, stdout } = makeReporter({
    consoleMode: "summary",
    progressEnabled: false,
    showThoughts: false,
  });

  reporter.startRun({
    pipelineName: "demo",
    pipelinePath: "demo.pipeline.yaml",
    model: "mock-model",
    endpoint: "http://localhost:11434/",
    inputPath: "./data/input.jsonl",
    inputRecordCount: 2,
    outputJsonlPath: "./output/demo.jsonl",
    stageCount: 1,
    firstStageMode: "record_transform",
  });
  reporter.finishSuccess({
    outputJsonlPath: "./output/demo.jsonl",
    reportPath: "./output/demo.report.json",
    warningsCount: 3,
    durationMs: 1234,
  });

  assertEquals(stdout[0], "Running demo");
  assertStringIncludes(stdout[1], "model=mock-model");
  assertStringIncludes(stdout[2], "output=./output/demo.jsonl");
  assertEquals(stdout[3], "Done");
  assertEquals(stdout[4], "output=./output/demo.jsonl");
  assertEquals(stdout[5], "report=./output/demo.report.json");
  assertEquals(stdout[6], "warnings=3");
});

Deno.test("CliReporter failure summary can print an auth hint", () => {
  const { reporter, stderr } = makeReporter({
    consoleMode: "summary",
    progressEnabled: false,
    showThoughts: false,
  });

  reporter.finishFailure({
    errorType: "AuthenticationError",
    hint: "Check whether your API token or apiKeyEnv variable is missing.",
    reportPath: "./output/demo.report.json",
    durationMs: 250,
  });

  assertEquals(stderr[0], "Failed");
  assertEquals(stderr[1], "errorType=AuthenticationError");
  assertEquals(
    stderr[2],
    "hint=Check whether your API token or apiKeyEnv variable is missing.",
  );
  assertEquals(stderr[3], "report=./output/demo.report.json");
});

Deno.test("CliReporter warnings mode prints compact warning lines", () => {
  const { reporter, stdout } = makeReporter({
    consoleMode: "warnings",
    progressEnabled: false,
    showThoughts: false,
  });

  reporter.printWarnings([{
    stageIdentifier: "rewrite",
    stageIndex: 0,
    recordIndex: 4,
    turnIndex: 1,
    attempt: 2,
    maxAttempts: 2,
    kind: "validator_mismatch.min_similarity_to_ref",
    validatorName: "semantic_anchor",
    message: "Value must contain <think>",
  }]);

  assertEquals(
    stdout[0],
    "WARN stage=rewrite record=4 turn=1 attempt=2 kind=validator_mismatch.min_similarity_to_ref validator=semantic_anchor",
  );
  assertEquals(stdout[1], "  Value must contain <think>");
});

Deno.test("CliReporter warning output clears an active progress line first", () => {
  const { reporter, stdout, rawStdout } = makeReporter({
    consoleMode: "warnings",
    progressEnabled: true,
    showThoughts: false,
  });

  reporter.onProgress({
    stageIdentifier: "rewrite",
    stageIndex: 0,
    mode: "record_transform",
    current: 1,
    total: 3,
    warningsSoFar: 0,
  });
  reporter.onWarning({
    stageIdentifier: "rewrite",
    stageIndex: 0,
    recordIndex: 4,
    kind: "invalid_json",
    message: "Model returned malformed JSON",
  });

  assertEquals(rawStdout.length, 2);
  assertStringIncludes(rawStdout[0], "\r[rewrite] 1/3");
  assertStringIncludes(rawStdout[1], "\r");
  assertStringIncludes(rawStdout[1], "\n");
  assertEquals(stdout[0], "WARN stage=rewrite record=4 kind=invalid_json");
  assertEquals(stdout[1], "  Model returned malformed JSON");
});

Deno.test("CliReporter full mode only emits full report output", () => {
  const { reporter, stdout, rawStdout } = makeReporter({
    consoleMode: "full",
    progressEnabled: false,
    showThoughts: false,
  });

  reporter.startRun({
    pipelineName: "demo",
    pipelinePath: "demo.pipeline.yaml",
    model: "mock-model",
    outputJsonlPath: "./output/demo.jsonl",
    stageCount: 1,
    firstStageMode: "batch",
  });
  reporter.finishSuccess({
    outputJsonlPath: "./output/demo.jsonl",
    reportPath: "./output/demo.report.json",
    warningsCount: 0,
    durationMs: 10,
  });
  reporter.printFullReport('{"ok":true}');

  assertEquals(stdout, []);
  assertEquals(rawStdout, ['{"ok":true}\n']);
});

Deno.test("CliReporter progress rendering updates a single line", () => {
  const { reporter, rawStdout } = makeReporter({
    consoleMode: "summary",
    progressEnabled: true,
    showThoughts: false,
  });

  reporter.onProgress({
    stageIdentifier: "rewrite",
    stageIndex: 0,
    mode: "record_transform",
    current: 1,
    total: 3,
    warningsSoFar: 0,
  });
  reporter.onProgress({
    stageIdentifier: "rewrite",
    stageIndex: 0,
    mode: "record_transform",
    current: 3,
    total: 3,
    warningsSoFar: 1,
  });

  assertEquals(rawStdout.length, 3);
  assertStringIncludes(rawStdout[0], "\r[rewrite] 1/3");
  assertStringIncludes(rawStdout[1], "\r[rewrite] 3/3");
  assertStringIncludes(rawStdout[2], "\r[rewrite] 3/3");
  assertStringIncludes(rawStdout[2], "complete");
});
