import {
  loadPipelineFromFile,
} from "./PipelineLoader.ts";
import {
  StageExecutionEngine,
  type DelegatedWorkflowResult,
  type StageExecutionResult,
} from "./StageExecutionEngine.ts";
import {
  ChatSession,
  type ChatSessionBackendOptions,
  type ChatSessionDebugHooks,
  type CompletionSettings,
} from "./ChatSession.ts";
import {
  loadInputDataset,
  streamInputDataset,
} from "./InputDatasetLoader.ts";
import { rewriteConversationRecord } from "./ConversationRewrite.ts";
import { validateStageValue } from "./StageValidator.ts";
import type { PipelineProvider } from "../structures/TaskSchema.ts";
import type { StageInput } from "../structures/TaskSchema.ts";
import { getValueAtPath } from "./ObjectPath.ts";
import type { CliReporter } from "./CliReporter.ts";

export interface WorkflowRunOverrides {
  model?: string;
  apiKey?: string;
  httpReferer?: string;
  xTitle?: string;
  outputPath?: string;
  outputDir?: string;
  endpoint?: string;
  provider?: PipelineProvider;
  completionOptions?: CompletionSettings;
  parallelism?: number;
  resumePath?: string;
  checkpointEvery?: number;
}

export interface DelegationContext {
  depth: number;
  maxDepth: number;
  ancestryWorkflowPaths: string[];
}

export interface ExecuteWorkflowFileInput {
  pipelinePath: string;
  overrides: WorkflowRunOverrides;
  initialContext?: unknown;
  pipelineInputRecordsOverride?: unknown[];
  showThoughts?: boolean;
  reporter?: CliReporter;
  allowStreaming?: boolean;
  writeArtifacts?: boolean;
  delegation?: DelegationContext;
}

export interface WorkflowRunSuccess {
  ok: true;
  report: Record<string, unknown>;
  reportJson: string;
  reportPath: string;
  outputJsonlPath: string;
  result: StageExecutionResult;
  warnings: StageExecutionResult["warnings"];
  durationMs: number;
  pipelineName?: string;
  resolvedWorkflowPath: string;
  model: string;
  provider: PipelineProvider;
  endpoint?: string;
  finalStageKey: string;
}

interface RunCheckpoint {
  pipelinePath: string;
  inputPath?: string;
  stageIdentifier: string;
  nextRecordOffset: number;
  outputJsonlPath?: string;
  updatedAt: string;
}

function stringifyJson(value: unknown): string {
  return JSON.stringify(
    value,
    (_key, currentValue) => {
      if (currentValue instanceof Error) {
        return {
          name: currentValue.name,
          message: currentValue.message,
        };
      }
      return currentValue;
    },
    2,
  );
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

function resolveApiKey(overrides: WorkflowRunOverrides, pipelineApiKeyEnv?: string): string | undefined {
  if (overrides.apiKey?.trim()) {
    return overrides.apiKey.trim();
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

async function runStreamingRecordTransform(
  input: {
    stage: StageInput;
    stageIdentifier: string;
    stageIndex: number;
    inputRecords: AsyncGenerator<unknown>;
    chatSession: ChatSession;
    completionOptions: CompletionSettings;
    reporter?: CliReporter;
    outputJsonlPath: string;
    appendOutput: boolean;
    checkpointPath?: string;
    checkpointEvery?: number;
    checkpointBaseOffset: number;
    pipelinePath: string;
    inputPath?: string;
    writeArtifacts: boolean;
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

  if (input.writeArtifacts && !input.appendOutput) {
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

    if (input.writeArtifacts) {
      await Deno.writeTextFile(
        input.outputJsonlPath,
        `${JSON.stringify(rewriteResult.record)}\n`,
        { append: true },
      );
    }
    processedCount++;

    input.reporter?.onProgress({
      stageIdentifier: input.stageIdentifier,
      stageIndex: input.stageIndex,
      mode: "record_transform",
      current: processedCount,
      warningsSoFar: warnings.length,
    });

    if (
      input.writeArtifacts &&
      input.checkpointPath &&
      input.checkpointEvery &&
      processedCount % input.checkpointEvery === 0
    ) {
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

  if (input.writeArtifacts && input.checkpointPath) {
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

export async function executeWorkflowFile(input: ExecuteWorkflowFileInput): Promise<WorkflowRunSuccess> {
  const startedAt = new Date();
  const resolvedWorkflowPath = await Deno.realPath(input.pipelinePath);
  const pipeline = await loadPipelineFromFile(input.pipelinePath);
  const allowStreaming = input.allowStreaming ?? true;
  const writeArtifacts = input.writeArtifacts ?? true;

  const checkpoint = input.overrides.resumePath ? await loadRunCheckpoint(input.overrides.resumePath) : undefined;
  if (checkpoint && checkpoint.pipelinePath !== input.pipelinePath) {
    throw new Error(
      `Checkpoint pipelinePath '${checkpoint.pipelinePath}' does not match requested pipeline '${input.pipelinePath}'`,
    );
  }

  const baseInputOffset = pipeline.input?.offset ?? 0;
  const resumeOffset = checkpoint?.nextRecordOffset ?? baseInputOffset;
  const effectiveInput = pipeline.input
    ? {
      ...pipeline.input,
      offset: Math.max(baseInputOffset, resumeOffset),
    }
    : undefined;

  const shouldUseStreamingPath = allowStreaming &&
    !!effectiveInput &&
    (effectiveInput.readMode === "stream" || !!input.overrides.resumePath || !!input.overrides.checkpointEvery) &&
    canUseStreamingRecordTransform(pipeline.stages);
  const loadedInput = shouldUseStreamingPath
    ? undefined
    : effectiveInput
    ? await loadInputDataset(effectiveInput)
    : undefined;

  const model = input.overrides.model ?? pipeline.model ??
    Deno.env.get("DATAGEN_MODEL") ??
    Deno.env.get("OLLAMA_MODEL");
  if (!model) {
    throw new Error(
      "Model is required. Set pipeline.model, pass --model, or set DATAGEN_MODEL (or OLLAMA_MODEL).",
    );
  }

  const provider = resolveProvider(input.overrides.provider, pipeline.provider);
  const endpoint = input.overrides.endpoint ?? pipeline.endpoint ??
    Deno.env.get("DATAGEN_OPENAI_ENDPOINT");
  const apiKey = resolveApiKey(input.overrides, pipeline.apiKeyEnv);
  const httpReferer = resolvePreferredValue(
    input.overrides.httpReferer,
    pipeline.httpReferer,
    ["DATAGEN_HTTP_REFERER"],
  );
  const xTitle = resolvePreferredValue(
    input.overrides.xTitle,
    pipeline.xTitle,
    ["DATAGEN_X_TITLE"],
  );
  const taskName = resolveTaskName(pipeline.name, input.pipelinePath);
  const outputDir = input.overrides.outputDir ?? pipeline.outputDir ?? "./output";
  if (writeArtifacts) {
    await Deno.mkdir(outputDir, { recursive: true });
  }
  const normalizedOutputDir = outputDir.replace(/[\\/]+$/, "");
  const outputJsonlPath = `${normalizedOutputDir}/${taskName}.jsonl`;
  const reportPath = input.overrides.outputPath ?? `${normalizedOutputDir}/${taskName}.report.json`;

  const debugHooks: ChatSessionDebugHooks = input.showThoughts
    ? {
      onThoughts: (thoughts) => input.reporter?.onThoughts(thoughts),
    }
    : {};
  const chatSession = new ChatSession(
    model,
    {
      max_tokens: pipeline.maxTokens,
      temperature: pipeline.temperature,
      reasoning_mode: pipeline.reasoningMode ?? "off",
    },
    {
      provider,
      endpoint,
      apiKey,
      httpReferer,
      xTitle,
    } satisfies ChatSessionBackendOptions,
    debugHooks,
  );

  const delegation = input.delegation ?? {
    depth: 0,
    maxDepth: 3,
    ancestryWorkflowPaths: [resolvedWorkflowPath],
  };

  const engine = new StageExecutionEngine({
    model,
    chatSession,
    globalParallelism: input.overrides.parallelism,
    completionOptions: {
      max_tokens: input.overrides.completionOptions?.max_tokens ?? pipeline.maxTokens,
      temperature: input.overrides.completionOptions?.temperature ?? pipeline.temperature,
      reasoning_mode: pipeline.reasoningMode ?? "off",
    },
    progress: {
      onProgress: (event) => input.reporter?.onProgress(event),
      onWarning: (warning) => input.reporter?.onWarning(warning),
    },
    runDelegatedWorkflow: async (request) => {
      const baseDir = resolvedWorkflowPath.replace(/[\\/][^\\/]+$/, "");
      const joinedPath = request.delegate.workflowPath.match(/^(?:[a-zA-Z]:)?[\\/]/)
        ? request.delegate.workflowPath
        : `${baseDir}/${request.delegate.workflowPath}`;
      const resolvedChildPath = await Deno.realPath(joinedPath);
      if (delegation.ancestryWorkflowPaths.includes(resolvedChildPath)) {
        throw new Error(`Delegation cycle detected for workflow '${resolvedChildPath}'`);
      }
      if (delegation.depth + 1 > delegation.maxDepth) {
        throw new Error(`Delegation max depth ${delegation.maxDepth} exceeded at '${resolvedChildPath}'`);
      }

      const inheritMode = request.delegate.inheritParentCli ?? "none";
      const childOverrides: WorkflowRunOverrides = (() => {
        if (inheritMode === "all") {
          return {
            ...input.overrides,
            outputPath: undefined,
            outputDir: undefined,
            resumePath: undefined,
            checkpointEvery: undefined,
          };
        }
        if (inheritMode === "completion") {
          return {
            completionOptions: input.overrides.completionOptions,
            parallelism: input.overrides.parallelism,
          };
        }
        return {};
      })();

      const childRun = await executeWorkflowFile({
        pipelinePath: resolvedChildPath,
        overrides: childOverrides,
        initialContext: (request.delegate.inputAs ?? "initial_context") === "initial_context"
          ? request.mappedInput
          : undefined,
        pipelineInputRecordsOverride: (request.delegate.inputAs ?? "initial_context") === "pipeline_input"
          ? request.mappedInput as unknown[]
          : undefined,
        reporter: undefined,
        showThoughts: false,
        allowStreaming: false,
        writeArtifacts: false,
        delegation: {
          depth: delegation.depth + 1,
          maxDepth: delegation.maxDepth,
          ancestryWorkflowPaths: [...delegation.ancestryWorkflowPaths, resolvedChildPath],
        },
      });

      const delegatedResult: DelegatedWorkflowResult = {
        ok: childRun.result.ok,
        workflowPath: request.delegate.workflowPath,
        resolvedWorkflowPath: resolvedChildPath,
        durationMs: childRun.durationMs,
        model: childRun.model,
        provider: childRun.provider,
        endpoint: childRun.endpoint,
        result: childRun.result,
        finalStageKey: childRun.finalStageKey,
      };
      return delegatedResult;
    },
  });

  input.reporter?.startRun({
    pipelineName: pipeline.name,
    pipelinePath: input.pipelinePath,
    model,
    provider,
    endpoint,
    inputPath: effectiveInput?.path,
    inputRecordCount: loadedInput?.records.length,
    outputJsonlPath,
    stageCount: pipeline.stages.length,
    firstStageMode: pipeline.stages[0]?.mode ?? "batch",
  });

  const checkpointPath = input.overrides.resumePath ??
    `${normalizedOutputDir}/${taskName}.checkpoint.json`;
  const result = shouldUseStreamingPath
    ? await runStreamingRecordTransform({
      stage: pipeline.stages[0],
      stageIdentifier: stageIdentifier(pipeline.stages[0], 0),
      stageIndex: 0,
      inputRecords: streamInputDataset(effectiveInput!),
      chatSession,
      completionOptions: {
        max_tokens: input.overrides.completionOptions?.max_tokens ?? pipeline.maxTokens,
        temperature: input.overrides.completionOptions?.temperature ?? pipeline.temperature,
        reasoning_mode: pipeline.reasoningMode ?? "off",
      },
      reporter: input.reporter,
      outputJsonlPath,
      appendOutput: Boolean(input.overrides.resumePath),
      checkpointPath: input.overrides.checkpointEvery ? checkpointPath : undefined,
      checkpointEvery: input.overrides.checkpointEvery,
      checkpointBaseOffset: effectiveInput?.offset ?? 0,
      pipelinePath: input.pipelinePath,
      inputPath: effectiveInput?.path,
      writeArtifacts,
    })
    : await engine.executeStages(
      pipeline.stages,
      input.initialContext,
      input.pipelineInputRecordsOverride ?? loadedInput?.records,
    );
  const endedAt = new Date();

  const lastStage = pipeline.stages[pipeline.stages.length - 1];
  const lastStageKey = stageIdentifier(
    lastStage,
    pipeline.stages.length - 1,
  );

  if (result.ok && !shouldUseStreamingPath && writeArtifacts) {
    const finalOutput = result.outputsByStage[lastStageKey];
    await Deno.writeTextFile(outputJsonlPath, `${toJsonl(finalOutput)}\n`);
  }

  const report = buildRunReport({
    pipelinePath: input.pipelinePath,
    pipelineName: pipeline.name,
    model,
    provider,
    endpoint,
    inputPath: effectiveInput?.path,
    inputFormat: loadedInput?.format,
    inputRecordCount: shouldUseStreamingPath
      ? (result as StageExecutionResult & { processedCount?: number }).processedCount
      : loadedInput?.records.length,
    outputJsonlPath,
    resumeFrom: shouldUseStreamingPath ? effectiveInput?.offset : undefined,
    processedCount: shouldUseStreamingPath
      ? (result as StageExecutionResult & { processedCount?: number }).processedCount
      : loadedInput?.records.length,
    remainingCount: shouldUseStreamingPath && effectiveInput?.limit !== undefined
      ? Math.max(
        0,
        effectiveInput.limit -
          ((result as StageExecutionResult & { processedCount?: number }).processedCount ?? 0),
      )
      : undefined,
    startedAt,
    endedAt,
    result,
  });

  const reportJson = stringifyJson(report);
  if (writeArtifacts) {
    await ensureParentDir(reportPath);
    await Deno.writeTextFile(reportPath, reportJson);
  }

  return {
    ok: true,
    report,
    reportJson,
    reportPath,
    outputJsonlPath,
    result,
    warnings: result.warnings,
    durationMs: endedAt.getTime() - startedAt.getTime(),
    pipelineName: pipeline.name,
    resolvedWorkflowPath,
    model,
    provider,
    endpoint,
    finalStageKey: lastStageKey,
  };
}
