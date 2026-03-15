import {
  loadPipelineFromFile,
  PipelineParseError,
  PipelineValidationError,
} from "./lib/PipelineLoader.ts";
import {
  StageExecutionEngine,
  type StageExecutionResult,
} from "./lib/StageExecutionEngine.ts";
import {
  ChatSession,
  type ChatSessionBackendOptions,
  type ChatSessionDebugHooks,
  type CompletionSettings,
} from "./lib/ChatSession.ts";
import {
  InputDatasetParseError,
  InputDatasetRemapError,
  InputDatasetValidationError,
  loadInputDataset,
  streamInputDataset,
} from "./lib/InputDatasetLoader.ts";
import { CliReporter, type ConsoleMode } from "./lib/CliReporter.ts";
import type { PipelineProvider, StageInput } from "./structures/TaskSchema.ts";
import { rewriteConversationRecord } from "./lib/ConversationRewrite.ts";
import { validateStageValue } from "./lib/StageValidator.ts";
import { executeWorkflowFile } from "./lib/WorkflowRuntime.ts";

const EXIT_OK = 0;
const EXIT_USAGE_OR_CONFIG_ERROR = 2;
const EXIT_STAGE_EXECUTION_FAILED = 3;

interface CliOptions {
  pipelinePath: string;
  consoleMode: ConsoleMode;
  progressEnabled?: boolean;
  showThoughts: boolean;
  model?: string;
  apiKey?: string;
  httpReferer?: string;
  xTitle?: string;
  outputPath?: string;
  outputDir?: string;
  contextInline?: string;
  contextFilePath?: string;
  endpoint?: string;
  provider?: PipelineProvider;
  completionOptions?: CompletionSettings;
  parallelism?: number;
  resumePath?: string;
  checkpointEvery?: number;
}

type CliParseResult =
  | { ok: true; options: CliOptions }
  | { ok: false; message: string; showUsage: boolean };

function usage(): string {
  return [
    "Usage:",
    "  deno run -A main.ts <pipeline.yml> [options]",
    "",
    "Options:",
    "  --model <name>          Model identifier (or DATAGEN_MODEL / OLLAMA_MODEL env var)",
    "  --api-key <token>       API key for OpenAI-compatible auth header",
    "  --http-referer <url>    HTTP-Referer header value for provider attribution",
    "  --x-title <name>        X-Title header value for provider attribution",
    "  --out <path>            Write full run report JSON to file",
    "  --output-dir <path>     Directory for final JSONL output (overrides pipeline outputDir)",
    "  --context <json>        Initial context payload as inline JSON",
    "  --context-file <path>   Initial context payload from JSON file",
    "  --endpoint <url>        OpenAI-compatible base URL (or DATAGEN_OPENAI_ENDPOINT env var)",
    "  --provider <name>       Provider: openai or ollama (or DATAGEN_PROVIDER env var)",
    "  --max-tokens <number>   Completion max tokens",
    "  --temperature <number>  Completion temperature",
    "  --parallelism <number>  Max parallel workers for iter/record_transform stages",
    "  --resume <path>         Resume from checkpoint JSON path",
    "  --checkpoint-every <n>  Write checkpoint every N processed records (streaming runs)",
    "  --console <mode>        Console output mode: summary, warnings, quiet, full",
    "  --progress              Force live progress display on",
    "  --no-progress           Disable live progress display",
    "  --show-thoughts         Show model reasoning/thoughts in terminal output",
    "  --help                  Show this help",
  ].join("\n");
}

function parseNumberArg(raw: string, flagName: string): number | string {
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    return `Invalid value for ${flagName}: ${raw}`;
  }
  return parsed;
}

function parseCliArgs(args: string[]): CliParseResult {
  if (args.includes("--help")) {
    return { ok: false, message: usage(), showUsage: false };
  }

  let pipelinePath: string | undefined;
  const options: CliOptions = {
    pipelinePath: "",
    consoleMode: "summary",
    showThoughts: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    if (!arg.startsWith("--")) {
      if (pipelinePath) {
        return {
          ok: false,
          message: `Unexpected positional argument: ${arg}`,
          showUsage: true,
        };
      }
      pipelinePath = arg;
      continue;
    }

    const takeValue = (flagName: string): string | null => {
      const value = args[i + 1];
      if (!value || value.startsWith("--")) {
        return null;
      }
      i++;
      return value;
    };

    switch (arg) {
      case "--model": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --model",
            showUsage: true,
          };
        }
        options.model = value;
        break;
      }
      case "--console": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --console",
            showUsage: true,
          };
        }
        if (
          value !== "summary" && value !== "warnings" && value !== "quiet" &&
          value !== "full"
        ) {
          return {
            ok: false,
            message:
              "Invalid value for --console. Expected one of: summary, warnings, quiet, full",
            showUsage: true,
          };
        }
        options.consoleMode = value;
        break;
      }
      case "--progress":
        options.progressEnabled = true;
        break;
      case "--no-progress":
        options.progressEnabled = false;
        break;
      case "--show-thoughts":
        options.showThoughts = true;
        break;
      case "--api-key": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --api-key",
            showUsage: true,
          };
        }
        options.apiKey = value;
        break;
      }
      case "--http-referer": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --http-referer",
            showUsage: true,
          };
        }
        options.httpReferer = value;
        break;
      }
      case "--x-title": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --x-title",
            showUsage: true,
          };
        }
        options.xTitle = value;
        break;
      }
      case "--out": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --out",
            showUsage: true,
          };
        }
        options.outputPath = value;
        break;
      }
      case "--context": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --context",
            showUsage: true,
          };
        }
        options.contextInline = value;
        break;
      }
      case "--context-file": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --context-file",
            showUsage: true,
          };
        }
        options.contextFilePath = value;
        break;
      }
      case "--output-dir": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --output-dir",
            showUsage: true,
          };
        }
        options.outputDir = value;
        break;
      }
      case "--endpoint": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --endpoint",
            showUsage: true,
          };
        }
        options.endpoint = value;
        break;
      }
      case "--provider": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --provider",
            showUsage: true,
          };
        }
        if (value !== "openai" && value !== "ollama") {
          return {
            ok: false,
            message: "Invalid value for --provider. Expected one of: openai, ollama",
            showUsage: true,
          };
        }
        options.provider = value;
        break;
      }
      case "--max-tokens": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --max-tokens",
            showUsage: true,
          };
        }
        const parsed = parseNumberArg(value, "--max-tokens");
        if (typeof parsed === "string") {
          return { ok: false, message: parsed, showUsage: true };
        }
        options.completionOptions = {
          ...options.completionOptions,
          max_tokens: parsed,
        };
        break;
      }
      case "--temperature": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --temperature",
            showUsage: true,
          };
        }
        const parsed = parseNumberArg(value, "--temperature");
        if (typeof parsed === "string") {
          return { ok: false, message: parsed, showUsage: true };
        }
        options.completionOptions = {
          ...options.completionOptions,
          temperature: parsed,
        };
        break;
      }
      case "--parallelism": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --parallelism",
            showUsage: true,
          };
        }
        const parsed = parseNumberArg(value, "--parallelism");
        if (typeof parsed === "string" || !Number.isInteger(parsed) || parsed < 1) {
          return {
            ok: false,
            message: "Invalid value for --parallelism. Expected integer >= 1",
            showUsage: true,
          };
        }
        options.parallelism = parsed;
        break;
      }
      case "--resume": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --resume",
            showUsage: true,
          };
        }
        options.resumePath = value;
        break;
      }
      case "--checkpoint-every": {
        const value = takeValue(arg);
        if (!value) {
          return {
            ok: false,
            message: "Missing value for --checkpoint-every",
            showUsage: true,
          };
        }
        const parsed = parseNumberArg(value, "--checkpoint-every");
        if (typeof parsed === "string" || !Number.isInteger(parsed) || parsed < 1) {
          return {
            ok: false,
            message: "Invalid value for --checkpoint-every. Expected integer >= 1",
            showUsage: true,
          };
        }
        options.checkpointEvery = parsed;
        break;
      }
      default:
        return { ok: false, message: `Unknown flag: ${arg}`, showUsage: true };
    }
  }

  if (!pipelinePath) {
    return {
      ok: false,
      message: "Missing required pipeline YAML path",
      showUsage: true,
    };
  }

  options.pipelinePath = pipelinePath;
  return { ok: true, options };
}

function toErrorDetail(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
    };
  }
  return { message: String(error) };
}

function classifyTopLevelError(error: unknown): {
  errorType: string;
  hint?: string;
} {
  const detail = toErrorDetail(error);
  const message = String(detail.message ?? "").toLowerCase();

  if (
    message.includes("api key env") ||
    message.includes("http 401") ||
    message.includes("http 403") ||
    message.includes("unauthorized") ||
    message.includes("forbidden") ||
    message.includes("authentication")
  ) {
    return {
      errorType: "AuthenticationError",
      hint:
        "Check whether your API token is missing, expired, or not exported into the expected env var.",
    };
  }

  return { errorType: "RuntimeError" };
}

function stringifyJson(value: unknown): string {
  return JSON.stringify(
    value,
    (_key, currentValue) => {
      if (currentValue instanceof Error) {
        return toErrorDetail(currentValue);
      }
      return currentValue;
    },
    2,
  );
}

function resolveProgressEnabled(options: CliOptions): boolean {
  if (options.progressEnabled !== undefined) {
    return options.progressEnabled;
  }

  return options.consoleMode === "summary" || options.consoleMode === "warnings";
}

async function resolveInitialContext(
  contextInline?: string,
  contextFilePath?: string,
): Promise<unknown> {
  if (contextInline && contextFilePath) {
    throw new Error("Use either --context or --context-file, not both");
  }

  if (contextInline) {
    return JSON.parse(contextInline);
  }

  if (contextFilePath) {
    const fileText = await Deno.readTextFile(contextFilePath);
    return JSON.parse(fileText);
  }

  return undefined;
}

function canUseStreamingRecordTransform(
  stages: StageInput[],
): boolean {
  if (stages.length !== 1) return false;
  const stage = stages[0];
  if ((stage.mode ?? "batch") !== "record_transform") return false;
  if (!stage.transform || stage.transform.kind !== "conversation_rewrite") return false;
  return (stage.input?.source ?? "pipeline_input") === "pipeline_input";
}

async function runStreamingRecordTransform(
  input: {
    stage: StageInput;
    stageIdentifier: string;
    stageIndex: number;
    inputRecords: AsyncGenerator<unknown>;
    chatSession: ChatSession;
    completionOptions: CompletionSettings;
    reporter: CliReporter;
    outputJsonlPath: string;
    appendOutput: boolean;
    checkpointPath?: string;
    checkpointEvery?: number;
    checkpointBaseOffset: number;
    pipelinePath: string;
    inputPath?: string;
  },
): Promise<StageExecutionResult & { processedCount: number }> {
  const warnings: StageExecutionResult["warnings"] = [];
  const traces: StageExecutionResult["traces"] = [];
  const outputsByStage: StageExecutionResult["outputsByStage"] = {};
  const stageStatuses: StageExecutionResult["stageStatuses"] = {
    [input.stageIdentifier]: "executed",
  };
  const dependencyGraph: StageExecutionResult["dependencyGraph"] = {
    [input.stageIdentifier]: [],
  };

  if (!input.appendOutput) {
    await Deno.writeTextFile(input.outputJsonlPath, "");
  }

  let processedCount = 0;
  for await (const record of input.inputRecords) {
    const recordIndex = processedCount;
    const inputContextSnapshot = {
      currentIterItem: record,
      priorStageOutputs: [] as Array<{ stageIdentifier: string; output: unknown }>,
    };

    const rewriteResult = await rewriteConversationRecord(
      record,
      input.stage,
      () => input.chatSession.fork(),
      input.completionOptions,
    );

    const fatalRewriteWarning = rewriteResult.warnings.find((warning) =>
      warning.kind === "model_call_failed"
    );
    if (fatalRewriteWarning) {
      const executionError = {
        kind: "model_call_failed" as const,
        stageIdentifier: input.stageIdentifier,
        stageIndex: input.stageIndex,
        message: fatalRewriteWarning.message,
        retryable: false,
      };
      traces.push({
        stageIdentifier: `${input.stageIdentifier}[${recordIndex}]`,
        stageIndex: input.stageIndex,
        stageStatus: "executed",
        promptSnapshot: "",
        inputContextSnapshot,
        parsedJsonOutput: rewriteResult.record,
        subtraces: rewriteResult.traces,
        error: executionError,
        success: false,
      });
      return {
        ok: false,
        traces,
        outputsByStage,
        warnings,
        stageStatuses,
        dependencyGraph,
        failedStage: {
          stageIdentifier: input.stageIdentifier,
          stageIndex: input.stageIndex,
          error: executionError,
        },
        processedCount,
      };
    }

    const recordValidationResult = validateStageValue(
      rewriteResult.record,
      input.stage.validate,
      { skipInapplicablePaths: true },
    );
    if (!recordValidationResult.success && (input.stage.validate?.onFailure ?? "fail") === "fail") {
      const executionError = {
        kind: "validator_mismatch" as const,
        stageIdentifier: input.stageIdentifier,
        stageIndex: input.stageIndex,
        message: `Transformed record failed stage validators: ${
          recordValidationResult.issues.map((issue) => issue.message).join("; ")
        }`,
        retryable: false,
      };
      traces.push({
        stageIdentifier: `${input.stageIdentifier}[${recordIndex}]`,
        stageIndex: input.stageIndex,
        stageStatus: "executed",
        promptSnapshot: "",
        inputContextSnapshot,
        parsedJsonOutput: rewriteResult.record,
        validationIssues: recordValidationResult.issues,
        subtraces: rewriteResult.traces,
        error: executionError,
        success: false,
      });
      return {
        ok: false,
        traces,
        outputsByStage,
        warnings,
        stageStatuses,
        dependencyGraph,
        failedStage: {
          stageIdentifier: input.stageIdentifier,
          stageIndex: input.stageIndex,
          error: executionError,
        },
        processedCount,
      };
    }

    traces.push({
      stageIdentifier: `${input.stageIdentifier}[${recordIndex}]`,
      stageIndex: input.stageIndex,
      stageStatus: "executed",
      promptSnapshot: "",
      inputContextSnapshot,
      parsedJsonOutput: rewriteResult.record,
      validationIssues: recordValidationResult.success ? undefined : recordValidationResult.issues,
      subtraces: rewriteResult.traces,
      success: true,
    });

    warnings.push(
      ...rewriteResult.warnings.map((warning) => ({
        stageIdentifier: input.stageIdentifier,
        stageIndex: input.stageIndex,
        recordIndex,
        turnIndex: warning.turnIndex,
        attempt: warning.attempt,
        maxAttempts: warning.maxAttempts,
        kind: warning.kind,
        message: warning.message,
        validatorName: warning.validatorName,
      })),
    );
    if (!recordValidationResult.success) {
      warnings.push(
        ...recordValidationResult.issues.map((issue) => ({
          stageIdentifier: input.stageIdentifier,
          stageIndex: input.stageIndex,
          recordIndex,
          kind: `validator_mismatch.${issue.kind}`,
          message: issue.message,
          validatorName: issue.ruleName?.trim() || undefined,
        })),
      );
    }

    await Deno.writeTextFile(
      input.outputJsonlPath,
      `${JSON.stringify(rewriteResult.record)}\n`,
      { append: true },
    );
    processedCount++;

    input.reporter.onProgress({
      stageIdentifier: input.stageIdentifier,
      stageIndex: input.stageIndex,
      mode: "record_transform",
      current: processedCount,
      warningsSoFar: warnings.length,
    });

    if (input.checkpointPath && input.checkpointEvery && processedCount % input.checkpointEvery === 0) {
      await writeRunCheckpoint(input.checkpointPath, {
        pipelinePath: input.pipelinePath,
        inputPath: input.inputPath,
        stageIdentifier: input.stageIdentifier,
        nextRecordOffset: input.checkpointBaseOffset + processedCount,
        outputJsonlPath: input.outputJsonlPath,
        updatedAt: new Date().toISOString(),
      });
    }
  }

  outputsByStage[input.stageIdentifier] = {
    streamedOutputPath: input.outputJsonlPath,
    processedCount,
  };

  if (input.checkpointPath) {
    await writeRunCheckpoint(input.checkpointPath, {
      pipelinePath: input.pipelinePath,
      inputPath: input.inputPath,
      stageIdentifier: input.stageIdentifier,
      nextRecordOffset: input.checkpointBaseOffset + processedCount,
      outputJsonlPath: input.outputJsonlPath,
      updatedAt: new Date().toISOString(),
    });
  }

  return {
    ok: true,
    traces,
    outputsByStage,
    warnings,
    stageStatuses,
    dependencyGraph,
    processedCount,
  };
}

function buildRunReport(
  input: {
    pipelinePath: string;
    pipelineName?: string;
    model: string;
    provider: PipelineProvider;
    endpoint?: string;
    inputPath?: string;
    inputFormat?: string;
    inputRecordCount?: number;
    outputJsonlPath: string;
    resumeFrom?: number;
    processedCount?: number;
    remainingCount?: number;
    startedAt: Date;
    endedAt: Date;
    result: StageExecutionResult;
  },
): Record<string, unknown> {
  return {
    ok: input.result.ok,
    pipeline: {
      source: input.pipelinePath,
      name: input.pipelineName,
    },
    model: input.model,
    provider: input.provider,
    endpoint: input.endpoint,
    inputPath: input.inputPath,
    inputFormat: input.inputFormat,
    inputRecordCount: input.inputRecordCount,
    outputJsonlPath: input.outputJsonlPath,
    resumeFrom: input.resumeFrom,
    processedCount: input.processedCount,
    remainingCount: input.remainingCount,
    startedAt: input.startedAt.toISOString(),
    endedAt: input.endedAt.toISOString(),
    durationMs: input.endedAt.getTime() - input.startedAt.getTime(),
    result: input.result,
  };
}

function resolveApiKey(options: CliOptions, pipelineApiKeyEnv?: string): string | undefined {
  if (options.apiKey?.trim()) {
    return options.apiKey.trim();
  }

  if (pipelineApiKeyEnv?.trim()) {
    const envName = pipelineApiKeyEnv.trim();
    const value = Deno.env.get(envName)?.trim();
    if (value) return value;
    throw new Error(
      `Pipeline requires API key env '${envName}', but it is not set or is empty.`,
    );
  }

  const fallbackEnvVars = [
    "DATAGEN_OPENAI_API_KEY",
    "OPENAI_API_KEY",
    "OPENROUTER_API_KEY",
  ];

  for (const envName of fallbackEnvVars) {
    const value = Deno.env.get(envName)?.trim();
    if (value) return value;
  }

  return undefined;
}

function resolvePreferredValue(
  cliValue: string | undefined,
  pipelineValue: string | undefined,
  envNames: string[],
): string | undefined {
  if (cliValue?.trim()) {
    return cliValue.trim();
  }

  if (pipelineValue?.trim()) {
    return pipelineValue.trim();
  }

  for (const envName of envNames) {
    const value = Deno.env.get(envName)?.trim();
    if (value) return value;
  }

  return undefined;
}

function resolveProvider(
  cliValue: PipelineProvider | undefined,
  pipelineValue: PipelineProvider | undefined,
): PipelineProvider {
  if (cliValue) return cliValue;
  if (pipelineValue) return pipelineValue;

  const fromEnv = Deno.env.get("DATAGEN_PROVIDER")?.trim().toLowerCase();
  if (fromEnv === "openai" || fromEnv === "ollama") {
    return fromEnv;
  }

  return "openai";
}

function stageIdentifier(stage: StageInput, index: number): string {
  return stage.id?.trim() || stage.name?.trim() || `stage-${index + 1}`;
}

function toJsonl(output: unknown): string {
  if (Array.isArray(output)) {
    return output.map((item) => JSON.stringify(item)).join("\n");
  }
  return JSON.stringify(output);
}

function resolveTaskName(
  pipelineName: string | undefined,
  pipelinePath: string,
): string {
  if (pipelineName?.trim()) return pipelineName.trim();
  const pathParts = pipelinePath.split(/[/\\]/);
  const fileName = pathParts[pathParts.length - 1] || "task";
  return fileName.replace(/\.[^.]+$/, "") || "task";
}

function ensureParentDir(path: string): Promise<void> {
  const parent = path.replace(/[\\/][^\\/]+$/, "");
  return Deno.mkdir(parent, { recursive: true }).catch(() => {});
}

interface RunCheckpoint {
  pipelinePath: string;
  inputPath?: string;
  stageIdentifier: string;
  nextRecordOffset: number;
  outputJsonlPath?: string;
  updatedAt: string;
}

async function loadRunCheckpoint(path: string): Promise<RunCheckpoint> {
  const text = await Deno.readTextFile(path);
  const parsed = JSON.parse(text);
  if (!parsed || typeof parsed !== "object") {
    throw new Error("Checkpoint file must contain a JSON object");
  }
  const candidate = parsed as Record<string, unknown>;
  if (typeof candidate.pipelinePath !== "string" || candidate.pipelinePath.length === 0) {
    throw new Error("Checkpoint is missing pipelinePath");
  }
  if (typeof candidate.stageIdentifier !== "string" || candidate.stageIdentifier.length === 0) {
    throw new Error("Checkpoint is missing stageIdentifier");
  }
  if (!Number.isInteger(candidate.nextRecordOffset) || Number(candidate.nextRecordOffset) < 0) {
    throw new Error("Checkpoint is missing valid nextRecordOffset");
  }
  return {
    pipelinePath: candidate.pipelinePath,
    inputPath: typeof candidate.inputPath === "string" ? candidate.inputPath : undefined,
    stageIdentifier: candidate.stageIdentifier,
    nextRecordOffset: Number(candidate.nextRecordOffset),
    outputJsonlPath: typeof candidate.outputJsonlPath === "string"
      ? candidate.outputJsonlPath
      : undefined,
    updatedAt: typeof candidate.updatedAt === "string"
      ? candidate.updatedAt
      : new Date().toISOString(),
  };
}

async function writeRunCheckpoint(path: string, checkpoint: RunCheckpoint): Promise<void> {
  await ensureParentDir(path);
  await Deno.writeTextFile(path, JSON.stringify(checkpoint, null, 2));
}

export async function runMain(args: string[]): Promise<number> {
  const parsed = parseCliArgs(args);
  if (!parsed.ok) {
    console.error(parsed.message);
    if (parsed.showUsage) console.error(`\n${usage()}`);
    return parsed.message === usage() ? EXIT_OK : EXIT_USAGE_OR_CONFIG_ERROR;
  }

  const { options } = parsed;
  const reporter = new CliReporter({
    consoleMode: options.consoleMode,
    progressEnabled: resolveProgressEnabled(options),
    showThoughts: options.showThoughts,
  });
  const fallbackModel = options.model ?? Deno.env.get("DATAGEN_MODEL") ??
    Deno.env.get("OLLAMA_MODEL");

  let initialContext: unknown;
  try {
    initialContext = await resolveInitialContext(
      options.contextInline,
      options.contextFilePath,
    );
  } catch (error) {
    console.error(
      `Failed to load initial context: ${toErrorDetail(error).message}`,
    );
    return EXIT_USAGE_OR_CONFIG_ERROR;
  }

  const startedAt = new Date();

  let reportPath = options.outputPath;
  let loadedPipelineName: string | undefined;
  let loadedPipelineOutputDir: string | undefined;

  try {
    const loadedPipeline = await loadPipelineFromFile(options.pipelinePath);
    loadedPipelineName = loadedPipeline.name;
    loadedPipelineOutputDir = loadedPipeline.outputDir;

    const run = await executeWorkflowFile({
      pipelinePath: options.pipelinePath,
      overrides: {
        model: options.model,
        apiKey: options.apiKey,
        httpReferer: options.httpReferer,
        xTitle: options.xTitle,
        outputPath: options.outputPath,
        outputDir: options.outputDir,
        endpoint: options.endpoint,
        provider: options.provider,
        completionOptions: options.completionOptions,
        parallelism: options.parallelism,
        resumePath: options.resumePath,
        checkpointEvery: options.checkpointEvery,
      },
      initialContext,
      showThoughts: options.showThoughts,
      reporter,
      allowStreaming: true,
      writeArtifacts: true,
    });
    reportPath = run.reportPath;
    if (options.consoleMode !== "warnings") {
      reporter.printWarnings(run.warnings);
    }
    reporter.finishSuccess({
      outputJsonlPath: run.outputJsonlPath,
      reportPath: run.reportPath,
      warningsCount: run.warnings.length,
      durationMs: run.durationMs,
    });
    if (options.consoleMode === "full") {
      reporter.printFullReport(run.reportJson);
    }
    return run.result.ok ? EXIT_OK : EXIT_STAGE_EXECUTION_FAILED;
  } catch (error) {
    const endedAt = new Date();
    const classifiedRuntimeError = classifyTopLevelError(error);
    const errorType = error instanceof PipelineParseError
      ? "PipelineParseError"
      : error instanceof PipelineValidationError
      ? "PipelineValidationError"
      : error instanceof InputDatasetParseError
      ? "InputDatasetParseError"
      : error instanceof InputDatasetValidationError
      ? "InputDatasetValidationError"
      : error instanceof InputDatasetRemapError
      ? "InputDatasetRemapError"
      : classifiedRuntimeError.errorType;

    if (!loadedPipelineName || !loadedPipelineOutputDir) {
      try {
        const pipeline = await loadPipelineFromFile(options.pipelinePath);
        loadedPipelineName = pipeline.name;
        loadedPipelineOutputDir = pipeline.outputDir;
      } catch {
        // Keep fallback below when pipeline cannot be loaded.
      }
    }
    reportPath ??= (() => {
      const outputDir = options.outputDir ?? loadedPipelineOutputDir ?? "./output";
      const taskName = resolveTaskName(loadedPipelineName, options.pipelinePath);
      return `${outputDir.replace(/[\\/]+$/, "")}/${taskName}.report.json`;
    })();
    await ensureParentDir(reportPath);

    const failureReport = stringifyJson({
      ok: false,
      errorType,
      errorHint: classifiedRuntimeError.hint,
      error: toErrorDetail(error),
      pipeline: {
        source: options.pipelinePath,
        name: loadedPipelineName,
      },
      model: fallbackModel,
      startedAt: startedAt.toISOString(),
      endedAt: endedAt.toISOString(),
      durationMs: endedAt.getTime() - startedAt.getTime(),
    });

    await Deno.writeTextFile(reportPath, failureReport);
    reporter.finishFailure({
      errorType,
      hint: classifiedRuntimeError.hint,
      reportPath,
      durationMs: endedAt.getTime() - startedAt.getTime(),
    });
    if (options.consoleMode === "full") {
      reporter.printFullReport(failureReport, true);
    }
    return EXIT_USAGE_OR_CONFIG_ERROR;
  }
}

if (import.meta.main) {
  const code = await runMain(Deno.args);
  Deno.exit(code);
}
