import { ChatSession, type CompletionSettings } from "./ChatSession.ts";
import type {
  LuaRuntimeOptionsInput,
  StageInput,
} from "../structures/TaskSchema.ts";
import { constrainToZodSchema } from "./ConstrainToZod.ts";
import {
  type ConversationRewriteWarning,
  rewriteConversationRecord,
} from "./ConversationRewrite.ts";
import { validateStageValue, type ValidationIssue } from "./StageValidator.ts";
import {
  buildRetryFeedbackAppendix,
  type RetryFailureKind,
} from "./RetryFeedback.ts";
import { parseModelJson } from "./ModelJson.ts";
import { getValueAtPath } from "./ObjectPath.ts";
import { executeLuaStage } from "./LuaStageRuntime.ts";

export type StageExecutionErrorKind =
  | "invalid_json"
  | "invalid_iter_input"
  | "invalid_record_transform_input"
  | "delegated_workflow_failed"
  | "lua_execution_failed"
  | "lua_invalid_output"
  | "lua_script_load_failed"
  | "constrain_mismatch"
  | "validator_mismatch"
  | "model_call_failed";

export interface StageExecutionError {
  kind: StageExecutionErrorKind;
  stageIdentifier: string;
  stageIndex: number;
  message: string;
  retryable: boolean;
  rawModelOutput?: string;
  cause?: unknown;
}

export interface StageExecutionTrace {
  repeatIndex?: number;
  stageIdentifier: string;
  stageIndex: number;
  stageStatus?: "executed" | "skipped" | "blocked";
  attempt?: number;
  maxAttempts?: number;
  promptSnapshot: string;
  inputContextSnapshot: {
    initialContext?: unknown;
    currentIterItem?: unknown;
    stageInput?: unknown;
    priorStageOutputs: Array<{
      stageIdentifier: string;
      output: unknown;
    }>;
  };
  rawModelOutput?: string;
  parsedJsonOutput?: unknown;
  validationIssues?: ValidationIssue[];
  subtraces?: unknown[];
  delegatedRun?: {
    workflowPath: string;
    resolvedWorkflowPath: string;
    ok: boolean;
    durationMs: number;
    childModel?: string;
    childProvider?: string;
    childEndpoint?: string;
    childStageStatuses: Record<string, "executed" | "skipped" | "blocked">;
    childWarningsCount: number;
    childTraces: StageExecutionTrace[];
  };
  luaScriptSnapshot?: string;
  luaScriptPath?: string;
  luaMetrics?: Array<{ name: string; value: number }>;
  luaNotes?: Array<{ kind: string; value: unknown }>;
  luaDebugEntries?: Array<{ label: string; value: unknown }>;
  error?: StageExecutionError;
  success: boolean;
}

export interface StageExecutionWarning {
  repeatIndex?: number;
  stageIdentifier: string;
  stageIndex: number;
  recordIndex?: number;
  turnIndex?: number;
  attempt?: number;
  maxAttempts?: number;
  kind: string;
  message: string;
  validatorName?: string;
}

export interface StageExecutionResult {
  ok: boolean;
  traces: StageExecutionTrace[];
  outputsByStage: Record<string, unknown>;
  warnings: StageExecutionWarning[];
  stageMeta?: Record<string, {
    sampleCount: number;
    successCount: number;
    failureCount: number;
    warningCount: number;
    successRatePct: number;
    failureRatePct: number;
    warningRatePct: number;
  }>;
  stageStatuses: Record<string, "executed" | "skipped" | "blocked">;
  dependencyGraph: Record<string, string[]>;
  failedStage?: {
    repeatIndex?: number;
    stageIdentifier: string;
    stageIndex: number;
    error: StageExecutionError;
  };
}

export interface StageExecutionProgressEvent {
  stageIdentifier: string;
  stageIndex: number;
  mode: "batch" | "iter" | "record_transform" | "workflow_delegate" | "lua";
  current: number;
  total?: number;
  warningsSoFar: number;
}

export interface DelegatedWorkflowRequest {
  stageIdentifier: string;
  stageIndex: number;
  delegate: NonNullable<StageInput["delegate"]>;
  mappedInput: unknown;
}

export interface DelegatedWorkflowResult {
  ok: boolean;
  workflowPath: string;
  resolvedWorkflowPath: string;
  durationMs: number;
  model?: string;
  provider?: string;
  endpoint?: string;
  result: StageExecutionResult;
  finalStageKey: string;
  warningMessage?: string;
}

export interface StageExecutionEngineConfig {
  model: string;
  workflowPath?: string;
  luaRuntimeDefaults?: LuaRuntimeOptionsInput;
  chatSession?: ChatSession;
  completionOptions?: CompletionSettings;
  globalParallelism?: number;
  progress?: {
    onProgress?: (event: StageExecutionProgressEvent) => void;
    onWarning?: (warning: StageExecutionWarning) => void;
  };
  runDelegatedWorkflow?: (
    request: DelegatedWorkflowRequest,
  ) => Promise<DelegatedWorkflowResult>;
}

function stageIdentifier(stage: StageInput, index: number): string {
  return stage.id?.trim() || stage.name?.trim() || `stage-${index + 1}`;
}

function normalizeParallelism(value: number | undefined): number {
  if (!Number.isFinite(value)) return 1;
  const parsed = Math.floor(value ?? 1);
  return parsed >= 1 ? parsed : 1;
}

interface ResolvedStageNode {
  key: string;
  stage: StageInput;
  originalIndex: number;
  dependencies: string[];
}

function resolveStageGraph(stages: StageInput[]): {
  executionOrder: ResolvedStageNode[];
  dependencyGraph: Record<string, string[]>;
} {
  const nodes = stages.map((stage, index) => ({
    key: stageIdentifier(stage, index),
    stage,
    originalIndex: index,
  }));
  const nodeMap = new Map(nodes.map((node) => [node.key, node]));
  const dependencyGraph: Record<string, string[]> = {};
  const resolvedNodes: ResolvedStageNode[] = [];

  for (const node of nodes) {
    const explicitDeps = node.stage.dependsOn?.map((value) => value.trim())
      .filter(Boolean);
    let dependencies: string[];
    if (explicitDeps?.length) {
      dependencies = explicitDeps;
    } else if (node.originalIndex > 0) {
      dependencies = [nodes[node.originalIndex - 1].key];
    } else {
      dependencies = [];
    }

    for (const dep of dependencies) {
      if (!nodeMap.has(dep)) {
        throw new Error(
          `Stage '${node.key}' depends on unknown stage '${dep}'`,
        );
      }
      if (dep === node.key) {
        throw new Error(`Stage '${node.key}' cannot depend on itself`);
      }
    }

    dependencyGraph[node.key] = [...dependencies];
    resolvedNodes.push({ ...node, dependencies });
  }

  const inDegree = new Map<string, number>();
  const reverse: Record<string, string[]> = {};
  for (const node of resolvedNodes) {
    inDegree.set(node.key, node.dependencies.length);
    for (const dep of node.dependencies) {
      reverse[dep] ??= [];
      reverse[dep].push(node.key);
    }
  }

  const ready = resolvedNodes
    .filter((node) => (inDegree.get(node.key) ?? 0) === 0)
    .sort((a, b) => a.originalIndex - b.originalIndex);
  const ordered: ResolvedStageNode[] = [];

  while (ready.length > 0) {
    const node = ready.shift()!;
    ordered.push(node);
    for (const nextKey of reverse[node.key] ?? []) {
      const next = (inDegree.get(nextKey) ?? 0) - 1;
      inDegree.set(nextKey, next);
      if (next === 0) {
        const nextNode = resolvedNodes.find((candidate) =>
          candidate.key === nextKey
        )!;
        ready.push(nextNode);
        ready.sort((a, b) => a.originalIndex - b.originalIndex);
      }
    }
  }

  if (ordered.length !== resolvedNodes.length) {
    const unresolved = resolvedNodes.filter((node) =>
      !ordered.some((item) => item.key === node.key)
    );
    throw new Error(
      `Cycle detected in stage dependency graph involving: ${
        unresolved.map((node) => node.key).join(", ")
      }`,
    );
  }

  return {
    executionOrder: ordered,
    dependencyGraph,
  };
}

function evaluateStageWhen(
  stage: StageInput,
  context: {
    initialContext?: unknown;
    outputsByStage: Record<string, unknown>;
  },
): boolean {
  if (!stage.when) return true;

  let target: unknown;
  try {
    target = getValueAtPath({
      initialContext: context.initialContext,
      outputsByStage: context.outputsByStage,
    }, stage.when.path);
  } catch {
    return false;
  }

  if (stage.when.equals !== undefined) {
    return target === stage.when.equals;
  }

  if (stage.when.notEquals !== undefined) {
    return target !== stage.when.notEquals;
  }

  if (stage.when.any !== undefined) {
    return stage.when.any.some((candidate) => target === candidate);
  }

  if (stage.when.notAny !== undefined) {
    return !stage.when.notAny.some((candidate) => target === candidate);
  }

  return true;
}

function toBulletList(values: string[]): string {
  return values.map((value) => `- ${value}`).join("\n");
}

function resolveMaxAttempts(stage: StageInput): number {
  if (!stage.retry?.enabled) return 1;
  return stage.retry.maxAttempts ?? 2;
}

function isIterRetryEligible(kind: StageExecutionErrorKind): boolean {
  return kind === "invalid_json" || kind === "constrain_mismatch" ||
    kind === "validator_mismatch";
}

function isAuthorizationFailure(message: string): boolean {
  const normalized = message.toLowerCase();
  return normalized.includes("http 401") || normalized.includes("http 403") ||
    normalized.includes("unauthorized") || normalized.includes("forbidden") ||
    normalized.includes("invalid api key") ||
    normalized.includes("authentication");
}

function toValidatorWarning(
  issue: ValidationIssue,
): Pick<StageExecutionWarning, "kind" | "message" | "validatorName"> {
  return {
    kind: `validator_mismatch.${issue.kind}`,
    message: issue.ruleName?.trim()
      ? `[${issue.ruleName.trim()}] ${issue.message}`
      : issue.message,
    validatorName: issue.ruleName?.trim() || undefined,
  };
}

export function buildStagePrompt(
  stage: StageInput,
  context: {
    initialContext?: unknown;
    currentIterItem?: unknown;
    priorStageOutputs: Array<{
      stageIdentifier: string;
      output: unknown;
    }>;
  },
): string {
  const sections: string[] = [];

  if (stage.system?.trim()) {
    sections.push(`System:\n${stage.system.trim()}`);
  }

  sections.push(`Instructions:\n${stage.instructions.trim()}`);

  if (stage.rules?.include?.length) {
    sections.push(`Rules (include):\n${toBulletList(stage.rules.include)}`);
  }

  if (stage.rules?.exclude?.length) {
    sections.push(`Rules (exclude):\n${toBulletList(stage.rules.exclude)}`);
  }

  if (stage.history?.trim()) {
    sections.push(`History:\n${stage.history.trim()}`);
  }

  if (stage.examples?.length) {
    const examplesText = stage.examples.map((example, index) => {
      const input = JSON.stringify(example.input, null, 2);
      const output = JSON.stringify(example.output, null, 2);
      return [`Example ${index + 1}:`, `Input: ${input}`, `Output: ${output}`]
        .join("\n");
    }).join("\n\n");

    sections.push(`Examples:\n${examplesText}`);
  }

  if (context.initialContext !== undefined) {
    sections.push(
      `Initial Context (JSON):\n${
        JSON.stringify(context.initialContext, null, 2)
      }`,
    );
  }

  if (context.priorStageOutputs.length > 0) {
    sections.push(
      `Prior Stage Outputs For Chaining (JSON):\n${
        JSON.stringify(context.priorStageOutputs, null, 2)
      }`,
    );
  }

  if (context.currentIterItem !== undefined) {
    sections.push(
      `Current Iter Item (JSON):\n${
        JSON.stringify(context.currentIterItem, null, 2)
      }`,
    );
  }

  sections.push(
    [
      "Output Contract:",
      "- Respond with JSON only.",
      "- Do not include markdown, code fences, prose, or any non-JSON text.",
      "- Ensure the response is valid JSON that can be parsed by JSON.parse.",
    ].join("\n"),
  );

  return sections.join("\n\n");
}

export class StageExecutionEngine {
  private readonly chatSession: ChatSession;
  private readonly completionOptions?: CompletionSettings;
  private readonly globalParallelism: number;
  private readonly workflowPath: string;
  private readonly luaRuntimeDefaults?: LuaRuntimeOptionsInput;
  private readonly onProgress?: (event: StageExecutionProgressEvent) => void;
  private readonly onWarning?: (warning: StageExecutionWarning) => void;
  private readonly runDelegatedWorkflow?: (
    request: DelegatedWorkflowRequest,
  ) => Promise<DelegatedWorkflowResult>;

  constructor(config: StageExecutionEngineConfig) {
    this.chatSession = config.chatSession ?? new ChatSession(config.model);
    this.workflowPath = config.workflowPath ?? ".";
    this.luaRuntimeDefaults = config.luaRuntimeDefaults;
    this.completionOptions = config.completionOptions;
    this.globalParallelism = normalizeParallelism(config.globalParallelism);
    this.onProgress = config.progress?.onProgress;
    this.onWarning = config.progress?.onWarning;
    this.runDelegatedWorkflow = config.runDelegatedWorkflow;
  }

  private emitProgress(event: StageExecutionProgressEvent): void {
    this.onProgress?.(event);
  }

  private appendWarnings(
    warnings: StageExecutionWarning[],
    ...newWarnings: StageExecutionWarning[]
  ): void {
    if (newWarnings.length === 0) return;
    warnings.push(...newWarnings);
    for (const warning of newWarnings) {
      this.onWarning?.(warning);
    }
  }

  private resolveStageParallelism(stage: StageInput): number {
    const stageParallelism = normalizeParallelism(stage.parallelism);
    return Math.max(1, Math.min(stageParallelism, this.globalParallelism));
  }

  private async runWithConcurrency<T>(
    total: number,
    concurrency: number,
    worker: (index: number) => Promise<T>,
  ): Promise<T[]> {
    const results = new Array<T>(total);
    let nextIndex = 0;
    let firstError: unknown;

    const runWorker = async () => {
      while (true) {
        if (firstError) return;
        const current = nextIndex;
        nextIndex++;
        if (current >= total) return;

        try {
          results[current] = await worker(current);
        } catch (error) {
          firstError = error;
          return;
        }
      }
    };

    const workers = Array.from(
      { length: Math.min(total, concurrency) },
      () => runWorker(),
    );
    await Promise.all(workers);
    if (firstError) {
      throw firstError;
    }
    return results;
  }

  private async callStageOnce(
    stage: StageInput,
    stageIdentifierValue: string,
    stageIndex: number,
    inputContextSnapshot: {
      initialContext?: unknown;
      currentIterItem?: unknown;
      priorStageOutputs: Array<{ stageIdentifier: string; output: unknown }>;
    },
    expectObjectOutput: boolean,
    promptFeedbackAppendix?: string,
    attempt = 1,
    maxAttempts = 1,
  ): Promise<
    {
      ok: true;
      trace: StageExecutionTrace;
      parsedJsonOutput: unknown;
      validationIssues?: ValidationIssue[];
    } | {
      ok: false;
      trace: StageExecutionTrace;
      error: StageExecutionError;
    }
  > {
    const prompt = buildStagePrompt(stage, inputContextSnapshot);
    const finalPrompt = promptFeedbackAppendix?.trim()
      ? `${prompt}\n\n${promptFeedbackAppendix.trim()}`
      : prompt;
    const completionOptions = {
      ...this.completionOptions,
      think: stage.reasoning ?? false,
    };

    let rawModelOutput: string | undefined;
    let parsedJsonOutput: unknown;

    if (stage.constrain) {
      let constrainSchema;
      try {
        constrainSchema = constrainToZodSchema(stage.constrain);
      } catch (error) {
        const executionError: StageExecutionError = {
          kind: "constrain_mismatch",
          stageIdentifier: stageIdentifierValue,
          stageIndex,
          message: error instanceof Error
            ? `Invalid stage constrain declaration: ${error.message}`
            : "Invalid stage constrain declaration",
          retryable: true,
          cause: error,
        };

        return {
          ok: false,
          error: executionError,
          trace: {
            stageIdentifier: stageIdentifierValue,
            stageIndex,
            attempt,
            maxAttempts,
            promptSnapshot: finalPrompt,
            inputContextSnapshot,
            error: executionError,
            success: false,
          },
        };
      }

      try {
        parsedJsonOutput = await this.chatSession.sendStructured(
          finalPrompt,
          constrainSchema,
          completionOptions,
        );
        rawModelOutput = JSON.stringify(parsedJsonOutput);
      } catch (error) {
        const maybeRawOutput = error && typeof error === "object" &&
            "rawOutput" in error &&
            typeof (error as { rawOutput?: unknown }).rawOutput === "string"
          ? (error as { rawOutput: string }).rawOutput
          : undefined;
        const executionError: StageExecutionError = {
          kind: "constrain_mismatch",
          stageIdentifier: stageIdentifierValue,
          stageIndex,
          message: error instanceof Error
            ? `Model output does not satisfy stage constrain schema: ${error.message}`
            : "Model output does not satisfy stage constrain schema",
          retryable: true,
          rawModelOutput: maybeRawOutput,
          cause: error,
        };

        return {
          ok: false,
          error: executionError,
          trace: {
            stageIdentifier: stageIdentifierValue,
            stageIndex,
            attempt,
            maxAttempts,
            promptSnapshot: finalPrompt,
            inputContextSnapshot,
            rawModelOutput: maybeRawOutput,
            error: executionError,
            success: false,
          },
        };
      }
    } else {
      try {
        rawModelOutput = await this.chatSession.send(
          finalPrompt,
          completionOptions,
        );
      } catch (error) {
        const executionError: StageExecutionError = {
          kind: "model_call_failed",
          stageIdentifier: stageIdentifierValue,
          stageIndex,
          message: error instanceof Error ? error.message : "Model call failed",
          retryable: true,
          cause: error,
        };

        return {
          ok: false,
          error: executionError,
          trace: {
            stageIdentifier: stageIdentifierValue,
            stageIndex,
            attempt,
            maxAttempts,
            promptSnapshot: finalPrompt,
            inputContextSnapshot,
            error: executionError,
            success: false,
          },
        };
      }

      try {
        parsedJsonOutput = parseModelJson(rawModelOutput);
      } catch (error) {
        const executionError: StageExecutionError = {
          kind: "invalid_json",
          stageIdentifier: stageIdentifierValue,
          stageIndex,
          message: "Model output is not valid JSON",
          retryable: true,
          rawModelOutput,
          cause: error,
        };

        return {
          ok: false,
          error: executionError,
          trace: {
            stageIdentifier: stageIdentifierValue,
            stageIndex,
            attempt,
            maxAttempts,
            promptSnapshot: finalPrompt,
            inputContextSnapshot,
            rawModelOutput,
            error: executionError,
            success: false,
          },
        };
      }
    }

    if (
      expectObjectOutput &&
      (typeof parsedJsonOutput !== "object" || parsedJsonOutput === null ||
        Array.isArray(parsedJsonOutput))
    ) {
      const executionError: StageExecutionError = {
        kind: "invalid_json",
        stageIdentifier: stageIdentifierValue,
        stageIndex,
        message:
          "Iter stage output must be a single JSON object (not an array or primitive)",
        retryable: true,
        rawModelOutput,
      };

      return {
        ok: false,
        error: executionError,
        trace: {
          stageIdentifier: stageIdentifierValue,
          stageIndex,
          attempt,
          maxAttempts,
          promptSnapshot: finalPrompt,
          inputContextSnapshot,
          rawModelOutput,
          parsedJsonOutput,
          error: executionError,
          success: false,
        },
      };
    }

    if (stage.validate) {
      const validationResult = validateStageValue(
        parsedJsonOutput,
        stage.validate,
      );
      if (!validationResult.success) {
        if ((stage.validate.onFailure ?? "fail") === "warn") {
          return {
            ok: true,
            parsedJsonOutput,
            validationIssues: validationResult.issues,
            trace: {
              stageIdentifier: stageIdentifierValue,
              stageIndex,
              attempt,
              maxAttempts,
              promptSnapshot: finalPrompt,
              inputContextSnapshot,
              rawModelOutput,
              parsedJsonOutput,
              validationIssues: validationResult.issues,
              success: true,
            },
          };
        }

        const executionError: StageExecutionError = {
          kind: "validator_mismatch",
          stageIdentifier: stageIdentifierValue,
          stageIndex,
          message: `Model output failed stage validators: ${
            validationResult.issues.map((issue) => issue.message).join("; ")
          }`,
          retryable: true,
          rawModelOutput,
          cause: validationResult.issues,
        };

        return {
          ok: false,
          error: executionError,
          trace: {
            stageIdentifier: stageIdentifierValue,
            stageIndex,
            attempt,
            maxAttempts,
            promptSnapshot: finalPrompt,
            inputContextSnapshot,
            rawModelOutput,
            parsedJsonOutput,
            validationIssues: validationResult.issues,
            error: executionError,
            success: false,
          },
        };
      }
    }

    return {
      ok: true,
      parsedJsonOutput,
      trace: {
        stageIdentifier: stageIdentifierValue,
        stageIndex,
        attempt,
        maxAttempts,
        promptSnapshot: finalPrompt,
        inputContextSnapshot,
        rawModelOutput,
        parsedJsonOutput,
        success: true,
      },
    };
  }

  private async callStageWithRetry(
    stage: StageInput,
    stageIdentifierValue: string,
    stageIndex: number,
    inputContextSnapshot: {
      initialContext?: unknown;
      currentIterItem?: unknown;
      priorStageOutputs: Array<{ stageIdentifier: string; output: unknown }>;
    },
    expectObjectOutput: boolean,
  ): Promise<
    {
      ok: true;
      parsedJsonOutput: unknown;
      traces: StageExecutionTrace[];
      validationIssues?: ValidationIssue[];
      finalAttempt: number;
      maxAttempts: number;
    } | {
      ok: false;
      error: StageExecutionError;
      traces: StageExecutionTrace[];
      finalAttempt: number;
      maxAttempts: number;
    }
  > {
    const maxAttempts = resolveMaxAttempts(stage);
    const traces: StageExecutionTrace[] = [];
    let feedbackAppendix: string | undefined;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const run = await this.callStageOnce(
        stage,
        stageIdentifierValue,
        stageIndex,
        inputContextSnapshot,
        expectObjectOutput,
        feedbackAppendix,
        attempt,
        maxAttempts,
      );

      traces.push(run.trace);
      if (run.ok) {
        return {
          ok: true,
          parsedJsonOutput: run.parsedJsonOutput,
          traces,
          validationIssues: run.validationIssues,
          finalAttempt: attempt,
          maxAttempts,
        };
      }

      if (
        !stage.retry?.enabled ||
        attempt >= maxAttempts ||
        !isIterRetryEligible(run.error.kind)
      ) {
        return {
          ok: false,
          error: run.error,
          traces,
          finalAttempt: attempt,
          maxAttempts,
        };
      }

      feedbackAppendix = buildRetryFeedbackAppendix({
        attempt,
        maxAttempts,
        failureKind: run.error.kind as RetryFailureKind,
        reason: run.error.message,
        validationIssues: run.trace.validationIssues,
      });
    }

    throw new Error("Unreachable retry state");
  }

  async executeStages(
    stages: StageInput[],
    initialContext?: unknown,
    pipelineInputRecords?: unknown[],
  ): Promise<StageExecutionResult> {
    this.chatSession.clearHistory();

    const traces: StageExecutionTrace[] = [];
    const outputsByStage: Record<string, unknown> = {};
    const warnings: StageExecutionWarning[] = [];
    const stageStatuses: Record<string, "executed" | "skipped" | "blocked"> =
      {};
    const priorStageOutputs: Array<
      { stageIdentifier: string; output: unknown }
    > = [];
    let dependencyGraph: Record<string, string[]> = {};

    let resolvedGraph: {
      executionOrder: ResolvedStageNode[];
      dependencyGraph: Record<string, string[]>;
    };
    try {
      resolvedGraph = resolveStageGraph(stages);
      dependencyGraph = resolvedGraph.dependencyGraph;
    } catch (error) {
      const stageIndex = 0;
      const stageIdentifierValue = stages[0]
        ? stageIdentifier(stages[0], 0)
        : "stage-1";
      const executionError: StageExecutionError = {
        kind: "invalid_record_transform_input",
        stageIdentifier: stageIdentifierValue,
        stageIndex,
        message: error instanceof Error ? error.message : String(error),
        retryable: false,
      };
      traces.push({
        stageIdentifier: stageIdentifierValue,
        stageIndex,
        stageStatus: "blocked",
        promptSnapshot: "",
        inputContextSnapshot: {
          initialContext,
          priorStageOutputs: [],
        },
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
          stageIdentifier: stageIdentifierValue,
          stageIndex,
          error: executionError,
        },
      };
    }

    for (const node of resolvedGraph.executionOrder) {
      const stage = node.stage;
      const index = node.originalIndex;
      const identifier = node.key;
      const mode = stage.mode ?? "batch";

      const blockedDependency = node.dependencies.find((dep) =>
        stageStatuses[dep] === "blocked" || stageStatuses[dep] === "skipped"
      );
      if (blockedDependency) {
        stageStatuses[identifier] = "blocked";
        traces.push({
          stageIdentifier: identifier,
          stageIndex: index,
          stageStatus: "blocked",
          promptSnapshot: "",
          inputContextSnapshot: {
            initialContext,
            priorStageOutputs: [...priorStageOutputs],
          },
          success: true,
        });
        continue;
      }

      const shouldRun = evaluateStageWhen(stage, {
        initialContext,
        outputsByStage,
      });
      if (!shouldRun) {
        stageStatuses[identifier] = "skipped";
        traces.push({
          stageIdentifier: identifier,
          stageIndex: index,
          stageStatus: "skipped",
          promptSnapshot: "",
          inputContextSnapshot: {
            initialContext,
            priorStageOutputs: [...priorStageOutputs],
          },
          success: true,
        });
        continue;
      }

      stageStatuses[identifier] = "executed";

      if (mode === "workflow_delegate") {
        if (!stage.delegate) {
          const executionError: StageExecutionError = {
            kind: "delegated_workflow_failed",
            stageIdentifier: identifier,
            stageIndex: index,
            message: "workflow_delegate stage is missing delegate config",
            retryable: false,
          };
          traces.push({
            stageIdentifier: identifier,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot: {
              initialContext,
              priorStageOutputs: [...priorStageOutputs],
            },
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
              stageIdentifier: identifier,
              stageIndex: index,
              error: executionError,
            },
          };
        }

        if (!this.runDelegatedWorkflow) {
          const executionError: StageExecutionError = {
            kind: "delegated_workflow_failed",
            stageIdentifier: identifier,
            stageIndex: index,
            message: "workflow_delegate is not configured in this runtime",
            retryable: false,
          };
          traces.push({
            stageIdentifier: identifier,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot: {
              initialContext,
              priorStageOutputs: [...priorStageOutputs],
            },
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
              stageIdentifier: identifier,
              stageIndex: index,
              error: executionError,
            },
          };
        }

        let mappedInput: unknown;
        try {
          mappedInput = getValueAtPath({
            initialContext,
            outputsByStage,
          }, stage.delegate.inputFromPath);
        } catch (error) {
          const executionError: StageExecutionError = {
            kind: "delegated_workflow_failed",
            stageIdentifier: identifier,
            stageIndex: index,
            message: error instanceof Error
              ? `Failed to resolve delegate inputFromPath '${stage.delegate.inputFromPath}': ${error.message}`
              : `Failed to resolve delegate inputFromPath '${stage.delegate.inputFromPath}'`,
            retryable: false,
            cause: error,
          };

          if ((stage.delegate.onFailure ?? "fail") === "warn") {
            this.appendWarnings(warnings, {
              stageIdentifier: identifier,
              stageIndex: index,
              kind: "delegated_workflow_failed",
              message: executionError.message,
            });
            outputsByStage[identifier] = null;
            traces.push({
              stageIdentifier: identifier,
              stageIndex: index,
              stageStatus: "executed",
              promptSnapshot: "",
              inputContextSnapshot: {
                initialContext,
                priorStageOutputs: [...priorStageOutputs],
              },
              parsedJsonOutput: null,
              success: true,
            });
            continue;
          }

          traces.push({
            stageIdentifier: identifier,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot: {
              initialContext,
              priorStageOutputs: [...priorStageOutputs],
            },
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
              stageIdentifier: identifier,
              stageIndex: index,
              error: executionError,
            },
          };
        }

        if (
          (stage.delegate.inputAs ?? "initial_context") === "pipeline_input" &&
          !Array.isArray(mappedInput)
        ) {
          const executionError: StageExecutionError = {
            kind: "delegated_workflow_failed",
            stageIdentifier: identifier,
            stageIndex: index,
            message:
              "delegate inputAs=pipeline_input requires mapped input to be a JSON array",
            retryable: false,
          };
          if ((stage.delegate.onFailure ?? "fail") === "warn") {
            this.appendWarnings(warnings, {
              stageIdentifier: identifier,
              stageIndex: index,
              kind: "delegated_workflow_failed",
              message: executionError.message,
            });
            outputsByStage[identifier] = null;
            traces.push({
              stageIdentifier: identifier,
              stageIndex: index,
              stageStatus: "executed",
              promptSnapshot: "",
              inputContextSnapshot: {
                initialContext,
                priorStageOutputs: [...priorStageOutputs],
              },
              parsedJsonOutput: null,
              success: true,
            });
            continue;
          }
          traces.push({
            stageIdentifier: identifier,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot: {
              initialContext,
              priorStageOutputs: [...priorStageOutputs],
            },
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
              stageIdentifier: identifier,
              stageIndex: index,
              error: executionError,
            },
          };
        }

        const delegated = await this.runDelegatedWorkflow({
          stageIdentifier: identifier,
          stageIndex: index,
          delegate: stage.delegate,
          mappedInput,
        });

        this.emitProgress({
          stageIdentifier: identifier,
          stageIndex: index,
          mode: "workflow_delegate",
          current: 1,
          total: 1,
          warningsSoFar: warnings.length,
        });

        this.appendWarnings(
          warnings,
          ...delegated.result.warnings.map((warning) => ({
            ...warning,
            stageIdentifier: `${identifier}::${warning.stageIdentifier}`,
            stageIndex: index,
          })),
        );

        let selectedOutput: unknown;
        try {
          const outputFrom = stage.delegate.outputFrom ?? "final_stage_output";
          if (outputFrom === "stage_key") {
            selectedOutput =
              delegated.result.outputsByStage[stage.delegate.outputStageKey!];
          } else {
            selectedOutput =
              delegated.result.outputsByStage[delegated.finalStageKey];
          }

          if (stage.delegate.outputSelectPath?.trim()) {
            selectedOutput = getValueAtPath(
              selectedOutput,
              stage.delegate.outputSelectPath.trim(),
            );
          }
        } catch (error) {
          if ((stage.delegate.onFailure ?? "fail") === "warn") {
            this.appendWarnings(warnings, {
              stageIdentifier: identifier,
              stageIndex: index,
              kind: "delegated_workflow_failed",
              message: error instanceof Error ? error.message : String(error),
            });
            selectedOutput = null;
          } else {
            const executionError: StageExecutionError = {
              kind: "delegated_workflow_failed",
              stageIdentifier: identifier,
              stageIndex: index,
              message: error instanceof Error ? error.message : String(error),
              retryable: false,
              cause: error,
            };
            traces.push({
              stageIdentifier: identifier,
              stageIndex: index,
              stageStatus: "executed",
              promptSnapshot: "",
              inputContextSnapshot: {
                initialContext,
                priorStageOutputs: [...priorStageOutputs],
              },
              delegatedRun: {
                workflowPath: delegated.workflowPath,
                resolvedWorkflowPath: delegated.resolvedWorkflowPath,
                ok: delegated.ok,
                durationMs: delegated.durationMs,
                childModel: delegated.model,
                childProvider: delegated.provider,
                childEndpoint: delegated.endpoint,
                childStageStatuses: delegated.result.stageStatuses,
                childWarningsCount: delegated.result.warnings.length,
                childTraces: delegated.result.traces,
              },
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
                stageIdentifier: identifier,
                stageIndex: index,
                error: executionError,
              },
            };
          }
        }

        const delegatedExecutionError: StageExecutionError | undefined =
          delegated.ok ? undefined : {
            kind: "delegated_workflow_failed",
            stageIdentifier: identifier,
            stageIndex: index,
            message: delegated.warningMessage ??
              delegated.result.failedStage?.error.message ??
              "Delegated workflow execution failed",
            retryable: false,
          };

        if (
          delegatedExecutionError &&
          (stage.delegate.onFailure ?? "fail") === "fail"
        ) {
          traces.push({
            stageIdentifier: identifier,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot: {
              initialContext,
              priorStageOutputs: [...priorStageOutputs],
            },
            delegatedRun: {
              workflowPath: delegated.workflowPath,
              resolvedWorkflowPath: delegated.resolvedWorkflowPath,
              ok: delegated.ok,
              durationMs: delegated.durationMs,
              childModel: delegated.model,
              childProvider: delegated.provider,
              childEndpoint: delegated.endpoint,
              childStageStatuses: delegated.result.stageStatuses,
              childWarningsCount: delegated.result.warnings.length,
              childTraces: delegated.result.traces,
            },
            error: delegatedExecutionError,
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
              stageIdentifier: identifier,
              stageIndex: index,
              error: delegatedExecutionError,
            },
          };
        }

        if (
          delegatedExecutionError &&
          (stage.delegate.onFailure ?? "fail") === "warn"
        ) {
          this.appendWarnings(warnings, {
            stageIdentifier: identifier,
            stageIndex: index,
            kind: "delegated_workflow_failed",
            message: delegatedExecutionError.message,
          });
          selectedOutput = null;
        }

        outputsByStage[identifier] = selectedOutput;
        priorStageOutputs.push({
          stageIdentifier: identifier,
          output: selectedOutput,
        });
        traces.push({
          stageIdentifier: identifier,
          stageIndex: index,
          stageStatus: "executed",
          promptSnapshot: "",
          inputContextSnapshot: {
            initialContext,
            priorStageOutputs: [...priorStageOutputs],
          },
          parsedJsonOutput: selectedOutput,
          delegatedRun: {
            workflowPath: delegated.workflowPath,
            resolvedWorkflowPath: delegated.resolvedWorkflowPath,
            ok: delegated.ok,
            durationMs: delegated.durationMs,
            childModel: delegated.model,
            childProvider: delegated.provider,
            childEndpoint: delegated.endpoint,
            childStageStatuses: delegated.result.stageStatuses,
            childWarningsCount: delegated.result.warnings.length,
            childTraces: delegated.result.traces,
          },
          success: true,
        });
        continue;
      }

      if (mode === "batch") {
        const inputContextSnapshot = {
          initialContext,
          priorStageOutputs: [...priorStageOutputs],
        };

        const run = await this.callStageOnce(
          stage,
          identifier,
          index,
          inputContextSnapshot,
          false,
        );

        traces.push({ ...run.trace, stageStatus: "executed" });
        if (!run.ok) {
          return {
            ok: false,
            traces,
            outputsByStage,
            warnings,
            stageStatuses,
            dependencyGraph,
            failedStage: {
              stageIdentifier: identifier,
              stageIndex: index,
              error: run.error,
            },
          };
        }

        if (run.validationIssues?.length) {
          this.appendWarnings(
            warnings,
            ...run.validationIssues.map((issue) => ({
              stageIdentifier: identifier,
              stageIndex: index,
              ...toValidatorWarning(issue),
            })),
          );
        }

        outputsByStage[identifier] = run.parsedJsonOutput;
        priorStageOutputs.push({
          stageIdentifier: identifier,
          output: run.parsedJsonOutput,
        });
        continue;
      }

      if (mode === "lua") {
        const source = stage.input?.source ??
          (node.dependencies.length > 0
            ? "previous_stage"
            : pipelineInputRecords !== undefined
            ? "pipeline_input"
            : undefined);

        if (source === "previous_stage" && node.dependencies.length === 0) {
          const executionError: StageExecutionError = {
            kind: "lua_execution_failed",
            stageIdentifier: identifier,
            stageIndex: index,
            message:
              "lua stage cannot read previous_stage when no dependency stage exists",
            retryable: false,
          };
          traces.push({
            stageIdentifier: identifier,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot: {
              initialContext,
              stageInput: undefined,
              priorStageOutputs: [...priorStageOutputs],
            },
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
              stageIdentifier: identifier,
              stageIndex: index,
              error: executionError,
            },
          };
        }

        const dependencyKey = node.dependencies[node.dependencies.length - 1];
        const stageInput = source === "pipeline_input"
          ? pipelineInputRecords
          : source === "previous_stage" && dependencyKey
          ? outputsByStage[dependencyKey]
          : undefined;

        const run = await executeLuaStage({
          stage,
          stageIdentifier: identifier,
          stageIndex: index,
          workflowPath: this.workflowPath,
          pipelineRuntime: this.luaRuntimeDefaults,
          context: {
            initialContext,
            outputsByStage,
            stageInput,
            stageIdentifier: identifier,
            stageIndex: index,
          },
          llmRequest: async (prompt, options) => {
            const resolvedOptions: CompletionSettings = {
              ...this.completionOptions,
              think: stage.reasoning ?? false,
              ...options,
            };
            return await this.chatSession.send(prompt, resolvedOptions);
          },
        });

        if (run.warnings.length > 0) {
          this.appendWarnings(
            warnings,
            ...run.warnings.map((warning) => ({
              stageIdentifier: identifier,
              stageIndex: index,
              kind: warning.kind,
              message: warning.message,
            })),
          );
        }

        const luaTelemetry = {
          luaMetrics: run.metrics.length > 0 ? run.metrics : undefined,
          luaNotes: run.notes.length > 0 ? run.notes : undefined,
          luaDebugEntries: run.debugEntries.length > 0
            ? run.debugEntries
            : undefined,
        };

        if (!run.ok) {
          const executionError: StageExecutionError = {
            kind: run.errorKind,
            stageIdentifier: identifier,
            stageIndex: index,
            message: run.message,
            retryable: false,
            cause: run.cause,
          };
          traces.push({
            stageIdentifier: identifier,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot: {
              initialContext,
              stageInput,
              priorStageOutputs: [...priorStageOutputs],
            },
            luaScriptSnapshot: run.scriptText,
            luaScriptPath: run.resolvedFilePath,
            ...luaTelemetry,
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
              stageIdentifier: identifier,
              stageIndex: index,
              error: executionError,
            },
          };
        }

        let luaOutput: unknown = run.output;
        if (stage.constrain) {
          let constrainSchema;
          try {
            constrainSchema = constrainToZodSchema(stage.constrain);
          } catch (error) {
            const executionError: StageExecutionError = {
              kind: "constrain_mismatch",
              stageIdentifier: identifier,
              stageIndex: index,
              message: error instanceof Error
                ? `Invalid stage constrain declaration: ${error.message}`
                : "Invalid stage constrain declaration",
              retryable: false,
              cause: error,
            };
            traces.push({
              stageIdentifier: identifier,
              stageIndex: index,
              stageStatus: "executed",
              promptSnapshot: "",
              inputContextSnapshot: {
                initialContext,
                stageInput,
                priorStageOutputs: [...priorStageOutputs],
              },
              parsedJsonOutput: luaOutput,
              luaScriptSnapshot: run.scriptText,
              luaScriptPath: run.resolvedFilePath,
              ...luaTelemetry,
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
                stageIdentifier: identifier,
                stageIndex: index,
                error: executionError,
              },
            };
          }

          const parsed = constrainSchema.safeParse(luaOutput);
          if (!parsed.success) {
            const executionError: StageExecutionError = {
              kind: "constrain_mismatch",
              stageIdentifier: identifier,
              stageIndex: index,
              message: `Model output does not satisfy stage constrain schema: ${
                parsed.error.issues.map((issue) => issue.message).join("; ")
              }`,
              retryable: false,
              cause: parsed.error,
            };
            traces.push({
              stageIdentifier: identifier,
              stageIndex: index,
              stageStatus: "executed",
              promptSnapshot: "",
              inputContextSnapshot: {
                initialContext,
                stageInput,
                priorStageOutputs: [...priorStageOutputs],
              },
              parsedJsonOutput: luaOutput,
              luaScriptSnapshot: run.scriptText,
              luaScriptPath: run.resolvedFilePath,
              ...luaTelemetry,
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
                stageIdentifier: identifier,
                stageIndex: index,
                error: executionError,
              },
            };
          }
          luaOutput = parsed.data;
        }

        const validationResult = validateStageValue(luaOutput, stage.validate);
        if (
          !validationResult.success &&
          (stage.validate?.onFailure ?? "fail") === "fail"
        ) {
          const executionError: StageExecutionError = {
            kind: "validator_mismatch",
            stageIdentifier: identifier,
            stageIndex: index,
            message: validationResult.issues.map((issue) => issue.message).join(
              "; ",
            ),
            retryable: false,
            cause: validationResult.issues,
          };
          traces.push({
            stageIdentifier: identifier,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot: {
              initialContext,
              stageInput,
              priorStageOutputs: [...priorStageOutputs],
            },
            parsedJsonOutput: luaOutput,
            validationIssues: validationResult.issues,
            luaScriptSnapshot: run.scriptText,
            luaScriptPath: run.resolvedFilePath,
            ...luaTelemetry,
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
              stageIdentifier: identifier,
              stageIndex: index,
              error: executionError,
            },
          };
        }

        if (!validationResult.success) {
          this.appendWarnings(
            warnings,
            ...validationResult.issues.map((issue) => ({
              stageIdentifier: identifier,
              stageIndex: index,
              ...toValidatorWarning(issue),
            })),
          );
        }

        traces.push({
          stageIdentifier: identifier,
          stageIndex: index,
          stageStatus: "executed",
          promptSnapshot: "",
          inputContextSnapshot: {
            initialContext,
            stageInput,
            priorStageOutputs: [...priorStageOutputs],
          },
          parsedJsonOutput: luaOutput,
          validationIssues: validationResult.success
            ? undefined
            : validationResult.issues,
          luaScriptSnapshot: run.scriptText,
          luaScriptPath: run.resolvedFilePath,
          ...luaTelemetry,
          success: true,
        });
        outputsByStage[identifier] = luaOutput;
        priorStageOutputs.push({
          stageIdentifier: identifier,
          output: luaOutput,
        });
        continue;
      }

      if (mode === "record_transform") {
        const source = stage.input?.source ??
          (index === 0 ? "pipeline_input" : "previous_stage");

        let sourceRecords: unknown;
        if (source === "pipeline_input") {
          sourceRecords = pipelineInputRecords;
        } else {
          const dependencyKey = node.dependencies[node.dependencies.length - 1];
          if (!dependencyKey) {
            const executionError: StageExecutionError = {
              kind: "invalid_record_transform_input",
              stageIdentifier: identifier,
              stageIndex: index,
              message:
                "record_transform stage cannot read previous_stage when no previous stage exists",
              retryable: false,
            };
            traces.push({
              stageIdentifier: identifier,
              stageIndex: index,
              stageStatus: "executed",
              promptSnapshot: "",
              inputContextSnapshot: {
                initialContext,
                priorStageOutputs: [...priorStageOutputs],
              },
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
                stageIdentifier: identifier,
                stageIndex: index,
                error: executionError,
              },
            };
          }
          sourceRecords = outputsByStage[dependencyKey];
        }

        if (!Array.isArray(sourceRecords)) {
          const executionError: StageExecutionError = {
            kind: "invalid_record_transform_input",
            stageIdentifier: identifier,
            stageIndex: index,
            message: source === "pipeline_input"
              ? "record_transform requires pipeline input to be a loaded JSON array"
              : "record_transform requires previous stage output to be a JSON array",
            retryable: false,
          };
          traces.push({
            stageIdentifier: identifier,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot: {
              initialContext,
              priorStageOutputs: [...priorStageOutputs],
            },
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
              stageIdentifier: identifier,
              stageIndex: index,
              error: executionError,
            },
          };
        }

        if (
          !stage.transform || stage.transform.kind !== "conversation_rewrite"
        ) {
          const executionError: StageExecutionError = {
            kind: "invalid_record_transform_input",
            stageIdentifier: identifier,
            stageIndex: index,
            message:
              "record_transform stages require a valid conversation_rewrite transform config",
            retryable: false,
          };
          traces.push({
            stageIdentifier: identifier,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot: {
              initialContext,
              priorStageOutputs: [...priorStageOutputs],
            },
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
              stageIdentifier: identifier,
              stageIndex: index,
              error: executionError,
            },
          };
        }

        const transformedRecords: unknown[] = new Array(sourceRecords.length);
        const perRecord = await this.runWithConcurrency(
          sourceRecords.length,
          this.resolveStageParallelism(stage),
          async (recordIndex) => {
            const currentRecord = sourceRecords[recordIndex];
            const inputContextSnapshot = {
              initialContext,
              currentIterItem: currentRecord,
              priorStageOutputs: [...priorStageOutputs],
            };

            try {
              const rewriteResult = await rewriteConversationRecord(
                currentRecord,
                stage,
                () => this.chatSession.fork(),
                this.completionOptions,
                this.workflowPath,
              );
              const recordValidationResult = validateStageValue(
                rewriteResult.record,
                stage.validate,
                { skipInapplicablePaths: true },
              );

              return {
                ok: true as const,
                recordIndex,
                inputContextSnapshot,
                rewriteResult,
                recordValidationResult,
              };
            } catch (error) {
              return {
                ok: false as const,
                recordIndex,
                inputContextSnapshot,
                error,
              };
            }
          },
        );

        const recordLevelTraces: StageExecutionTrace[] = [];
        for (const item of perRecord) {
          if (!item.ok) {
            const executionError: StageExecutionError = {
              kind: "invalid_record_transform_input",
              stageIdentifier: identifier,
              stageIndex: index,
              message: item.error instanceof Error
                ? item.error.message
                : String(item.error),
              retryable: false,
              cause: item.error,
            };
            traces.push({
              stageIdentifier: `${identifier}[${item.recordIndex}]`,
              stageIndex: index,
              stageStatus: "executed",
              promptSnapshot: "",
              inputContextSnapshot: item.inputContextSnapshot,
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
                stageIdentifier: identifier,
                stageIndex: index,
                error: executionError,
              },
            };
          }

          const {
            recordIndex,
            rewriteResult,
            recordValidationResult,
            inputContextSnapshot,
          } = item;
          transformedRecords[recordIndex] = rewriteResult.record;

          if (
            !recordValidationResult.success &&
            (stage.validate?.onFailure ?? "fail") === "fail"
          ) {
            const executionError: StageExecutionError = {
              kind: "validator_mismatch",
              stageIdentifier: identifier,
              stageIndex: index,
              message: `Transformed record failed stage validators: ${
                recordValidationResult.issues.map((issue) => issue.message)
                  .join("; ")
              }`,
              retryable: false,
              cause: recordValidationResult.issues,
            };
            traces.push({
              stageIdentifier: `${identifier}[${recordIndex}]`,
              stageIndex: index,
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
                stageIdentifier: identifier,
                stageIndex: index,
                error: executionError,
              },
            };
          }

          recordLevelTraces.push({
            stageIdentifier: `${identifier}[${recordIndex}]`,
            stageIndex: index,
            stageStatus: "executed",
            promptSnapshot: "",
            inputContextSnapshot,
            parsedJsonOutput: rewriteResult.record,
            validationIssues: recordValidationResult.success
              ? undefined
              : recordValidationResult.issues,
            subtraces: rewriteResult.traces,
            success: true,
          });

          this.appendWarnings(
            warnings,
            ...rewriteResult.warnings.map((
              warning: ConversationRewriteWarning,
            ) => ({
              stageIdentifier: identifier,
              stageIndex: index,
              recordIndex,
              turnIndex: warning.turnIndex,
              attempt: warning.attempt,
              maxAttempts: warning.maxAttempts,
              kind: warning.kind,
              message: warning.message,
              validatorName: warning.validatorName,
            })),
          );
          const fatalRewriteWarning = rewriteResult.warnings.find((warning) =>
            warning.kind === "model_call_failed" &&
            isAuthorizationFailure(warning.message)
          );
          if (fatalRewriteWarning) {
            const executionError: StageExecutionError = {
              kind: "model_call_failed",
              stageIdentifier: identifier,
              stageIndex: index,
              message: fatalRewriteWarning.message,
              retryable: false,
            };
            traces.push(...recordLevelTraces, {
              stageIdentifier: `${identifier}[${recordIndex}]`,
              stageIndex: index,
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
                stageIdentifier: identifier,
                stageIndex: index,
                error: executionError,
              },
            };
          }
          if (!recordValidationResult.success) {
            this.appendWarnings(
              warnings,
              ...recordValidationResult.issues.map((issue) => ({
                stageIdentifier: identifier,
                stageIndex: index,
                recordIndex,
                ...toValidatorWarning(issue),
              })),
            );
          }
          this.emitProgress({
            stageIdentifier: identifier,
            stageIndex: index,
            mode: "record_transform",
            current: recordIndex + 1,
            total: sourceRecords.length,
            warningsSoFar: warnings.length,
          });
        }

        traces.push(...recordLevelTraces);
        outputsByStage[identifier] = transformedRecords;
        priorStageOutputs.push({
          stageIdentifier: identifier,
          output: transformedRecords,
        });
        continue;
      }

      if (index === 0) {
        const executionError: StageExecutionError = {
          kind: "invalid_iter_input",
          stageIdentifier: identifier,
          stageIndex: index,
          message: "Iter mode requires a previous stage output array",
          retryable: false,
        };
        traces.push({
          stageIdentifier: identifier,
          stageIndex: index,
          stageStatus: "executed",
          promptSnapshot: "",
          inputContextSnapshot: {
            initialContext,
            priorStageOutputs: [...priorStageOutputs],
          },
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
            stageIdentifier: identifier,
            stageIndex: index,
            error: executionError,
          },
        };
      }

      const previousIdentifier =
        node.dependencies[node.dependencies.length - 1];
      if (!previousIdentifier) {
        const executionError: StageExecutionError = {
          kind: "invalid_iter_input",
          stageIdentifier: identifier,
          stageIndex: index,
          message: "Iter mode requires a dependency stage output array",
          retryable: false,
        };
        traces.push({
          stageIdentifier: identifier,
          stageIndex: index,
          stageStatus: "executed",
          promptSnapshot: "",
          inputContextSnapshot: {
            initialContext,
            priorStageOutputs: [...priorStageOutputs],
          },
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
            stageIdentifier: identifier,
            stageIndex: index,
            error: executionError,
          },
        };
      }

      const previousOutput = outputsByStage[previousIdentifier];
      if (!Array.isArray(previousOutput)) {
        const executionError: StageExecutionError = {
          kind: "invalid_iter_input",
          stageIdentifier: identifier,
          stageIndex: index,
          message:
            `Iter mode requires previous stage '${previousIdentifier}' output to be a JSON array`,
          retryable: false,
        };
        traces.push({
          stageIdentifier: identifier,
          stageIndex: index,
          stageStatus: "executed",
          promptSnapshot: "",
          inputContextSnapshot: {
            initialContext,
            priorStageOutputs: [...priorStageOutputs],
          },
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
            stageIdentifier: identifier,
            stageIndex: index,
            error: executionError,
          },
        };
      }

      const iterResults: unknown[] = new Array(previousOutput.length);
      const nonArrayPriorStageOutputs = priorStageOutputs.filter((entry) =>
        !Array.isArray(entry.output)
      );
      const completedState = { count: 0 };
      const iterRuns = await this.runWithConcurrency(
        previousOutput.length,
        this.resolveStageParallelism(stage),
        async (itemIndex) => {
          const currentIterItem = previousOutput[itemIndex];
          const inputContextSnapshot = {
            initialContext,
            currentIterItem,
            priorStageOutputs: [
              ...nonArrayPriorStageOutputs,
              {
                stageIdentifier: previousIdentifier,
                output: currentIterItem,
              },
            ],
          };
          const run = await this.callStageWithRetry(
            stage,
            `${identifier}[${itemIndex}]`,
            index,
            inputContextSnapshot,
            true,
          );
          completedState.count++;
          this.emitProgress({
            stageIdentifier: identifier,
            stageIndex: index,
            mode: "iter",
            current: completedState.count,
            total: previousOutput.length,
            warningsSoFar: warnings.length,
          });
          return { itemIndex, run };
        },
      );

      for (const item of iterRuns) {
        traces.push(
          ...item.run.traces.map((trace) => ({
            ...trace,
            stageStatus: "executed" as const,
          })),
        );
        if (!item.run.ok) {
          return {
            ok: false,
            traces,
            outputsByStage,
            warnings,
            stageStatuses,
            dependencyGraph,
            failedStage: {
              stageIdentifier: identifier,
              stageIndex: index,
              error: {
                ...item.run.error,
                stageIdentifier: identifier,
                message:
                  `Iter item ${item.itemIndex} failed: ${item.run.error.message}`,
              },
            },
          };
        }

        if (item.run.validationIssues?.length) {
          this.appendWarnings(
            warnings,
            ...item.run.validationIssues.map((issue) => ({
              stageIdentifier: identifier,
              stageIndex: index,
              recordIndex: item.itemIndex,
              attempt: item.run.finalAttempt,
              maxAttempts: item.run.maxAttempts,
              ...toValidatorWarning(issue),
            })),
          );
        }
        iterResults[item.itemIndex] = item.run.parsedJsonOutput;
      }

      outputsByStage[identifier] = iterResults;
      priorStageOutputs.push({
        stageIdentifier: identifier,
        output: iterResults,
      });
    }

    return {
      ok: true,
      traces,
      outputsByStage,
      warnings,
      stageStatuses,
      dependencyGraph,
    };
  }
}
