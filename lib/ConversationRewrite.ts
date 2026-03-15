import {
  ChatSession,
  type CompletionSettings,
} from "./ChatSession.ts";
import type { StageInput } from "../structures/TaskSchema.ts";
import { getValueAtPath } from "./ObjectPath.ts";
import { executeLuaStage } from "./LuaStageRuntime.ts";
import {
  validateStageValue,
  type ValidationIssue,
} from "./StageValidator.ts";
import {
  buildRetryFeedbackAppendix,
  type RetryFailureKind,
} from "./RetryFeedback.ts";

export interface ConversationRewriteWarning {
  kind:
    | "missing_conversations_path"
    | "invalid_conversations_value"
    | "invalid_turn_shape"
    | "turn_preprocess_failed"
    | "invalid_turn_preprocess_output"
    | "model_call_failed"
    | "empty_output"
    | "validator_mismatch"
    | `validator_mismatch.${ValidationIssue["kind"]}`;
  message: string;
  turnIndex?: number;
  attempt?: number;
  maxAttempts?: number;
  validatorName?: string;
}

export interface ConversationRewriteTrace {
  turnIndex: number;
  attempt: number;
  maxAttempts: number;
  promptSnapshot: string;
  rawModelOutput?: string;
  validationIssues?: ValidationIssue[];
  failureKind?: string;
  success: boolean;
}

export interface ConversationRewriteResult {
  record: unknown;
  warnings: ConversationRewriteWarning[];
  traces: ConversationRewriteTrace[];
}

function evaluateTurnWhen(
  transform: NonNullable<StageInput["transform"]>,
  turn: unknown,
): boolean {
  if (!transform.turnWhen) return true;

  let target: unknown;
  try {
    target = getValueAtPath(turn, transform.turnWhen.path);
  } catch {
    return false;
  }

  if (transform.turnWhen.equals !== undefined) {
    return target === transform.turnWhen.equals;
  }

  if (transform.turnWhen.notEquals !== undefined) {
    return target !== transform.turnWhen.notEquals;
  }

  if (transform.turnWhen.any !== undefined) {
    return transform.turnWhen.any.some((candidate) => target === candidate);
  }

  if (transform.turnWhen.notAny !== undefined) {
    return !transform.turnWhen.notAny.some((candidate) => target === candidate);
  }

  return true;
}

async function preprocessTurn(
  input: {
    record: unknown;
    turn: unknown;
    turnIndex: number;
    priorTurns: unknown[];
    stage: StageInput;
    transform: NonNullable<StageInput["transform"]>;
    sessionFactory: () => ChatSession;
    completionOptions?: CompletionSettings;
    workflowPath?: string;
  },
): Promise<
  | {
    ok: true;
    turn: Record<string, unknown>;
  }
  | {
    ok: false;
    warning: ConversationRewriteWarning;
  }
> {
  if (!input.transform.turnPreprocess) {
    return { ok: true, turn: input.turn as Record<string, unknown> };
  }

  const luaStage: StageInput = {
    name: `${input.stage.name ?? input.stage.id ?? "conversation_rewrite"}_turn_preprocess`,
    instructions: "Preprocess the current target turn before conversation rewrite.",
    mode: "lua",
    reasoning: input.stage.reasoning,
    lua: input.transform.turnPreprocess,
  };

  const run = await executeLuaStage({
    stage: luaStage,
    stageIdentifier: luaStage.name ?? "conversation_rewrite_turn_preprocess",
    stageIndex: -1,
    workflowPath: input.workflowPath ?? ".",
    context: {
      initialContext: undefined,
      outputsByStage: {},
      stageInput: input.turn,
      stageIdentifier: luaStage.name ?? "conversation_rewrite_turn_preprocess",
      stageIndex: -1,
      record: input.record,
      turn: input.turn,
      turnIndex: input.turnIndex,
      priorTurns: input.priorTurns,
      transform: input.transform,
    },
    llmRequest: async (prompt, options) => {
      const session = input.sessionFactory();
      return await session.send(prompt, {
        ...input.completionOptions,
        think: input.stage.reasoning ?? input.completionOptions?.think ?? false,
        ...options,
      });
    },
  });

  if (!run.ok) {
    return {
      ok: false,
      warning: {
        kind: "turn_preprocess_failed",
        turnIndex: input.turnIndex,
        message: run.message,
      },
    };
  }

  if (!run.output || typeof run.output !== "object" || Array.isArray(run.output)) {
    return {
      ok: false,
      warning: {
        kind: "invalid_turn_preprocess_output",
        turnIndex: input.turnIndex,
        message: "turnPreprocess must return a JSON object representing the target turn",
      },
    };
  }

  return {
    ok: true,
    turn: run.output as Record<string, unknown>,
  };
}

function findPreviousTurnWithSameRole(
  turns: unknown[],
  turnIndex: number,
  roleField: string,
  role: string,
): Record<string, unknown> | undefined {
  for (let index = turnIndex - 1; index >= 0; index--) {
    const candidate = turns[index];
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
      continue;
    }

    const candidateObject = candidate as Record<string, unknown>;
    if (candidateObject[roleField] === role) {
      return candidateObject;
    }
  }

  return undefined;
}

function resolveMaxAttempts(stage: StageInput): number {
  if (!stage.retry?.enabled) return 1;
  return stage.retry.maxAttempts ?? 2;
}

function isRewriteRetryEligible(kind: ConversationRewriteWarning["kind"]): boolean {
  return kind === "empty_output" || kind === "validator_mismatch" ||
    kind.startsWith("validator_mismatch.");
}

function buildValidatorMismatchWarning(
  issues: ValidationIssue[],
): Pick<ConversationRewriteWarning, "kind" | "message" | "validatorName"> {
  const primaryIssue = issues[0];
  const kind = primaryIssue
    ? `validator_mismatch.${primaryIssue.kind}` as const
    : "validator_mismatch";
  const prefixedMessages = issues.map((issue) =>
    issue.ruleName?.trim()
      ? `[${issue.ruleName.trim()}] ${issue.message}`
      : issue.message
  );
  return {
    kind,
    message: prefixedMessages.join("; "),
    validatorName: primaryIssue?.ruleName?.trim() || undefined,
  };
}

function normalizeRewriteOutput(rawModelOutput: string): string {
  const trimmed = rawModelOutput.trim();
  const fencedMatch =
    /^```(?:[a-zA-Z0-9_-]+)?\s*\r?\n?([\s\S]*?)\r?\n?```$/s.exec(trimmed);
  if (fencedMatch) {
    return fencedMatch[1]?.trim() ?? "";
  }
  return trimmed;
}

export function buildConversationRewritePrompt(
  stage: StageInput,
  priorTurns: unknown[],
  currentTargetTurn?: unknown,
  promptFeedbackAppendix?: string,
): string {
  const sections: string[] = [];

  if (stage.system?.trim()) {
    sections.push(`System:\n${stage.system.trim()}`);
  }

  sections.push(
    `Prior Conversation Turns:\n${JSON.stringify(priorTurns, null, 2)}`,
  );

  if (currentTargetTurn !== undefined) {
    sections.push(
      `Current Target Turn:\n${JSON.stringify(currentTargetTurn, null, 2)}`,
    );
  }

  sections.push(`Rewrite Task:\n${stage.instructions.trim()}`);

  if (promptFeedbackAppendix?.trim()) {
    sections.push(promptFeedbackAppendix.trim());
  }

  sections.push(
    [
      "Rewrite Output Rules:",
      "- Return only the full rewritten target turn text.",
      "- Do not return JSON.",
      "- Do not wrap the response in markdown or code fences.",
      "- Do not include any text before or after the rewritten turn.",
      "- Preserve the target turn content while applying the requested rewrite.",
    ].join("\n"),
  );

  return sections.join("\n\n");
}

async function rewriteTurnOnce(
  input: {
    stage: StageInput;
    sessionFactory: () => ChatSession;
    completionOptions?: CompletionSettings;
    priorTurns: unknown[];
    currentTurn: unknown;
    turnIndex: number;
    transform: NonNullable<StageInput["transform"]>;
    promptFeedbackAppendix?: string;
    attempt: number;
    maxAttempts: number;
  },
): Promise<
  | {
    ok: true;
    rewrittenContent: string;
    trace: ConversationRewriteTrace;
  }
  | {
    ok: false;
    retryable: boolean;
    warning: ConversationRewriteWarning;
    trace: ConversationRewriteTrace;
  }
> {
  const session = input.sessionFactory();
  const prompt = buildConversationRewritePrompt(
    input.stage,
    input.priorTurns,
    input.transform.includeOriginalTargetTurn === false ? undefined : input.currentTurn,
    input.promptFeedbackAppendix,
  );

  let rawModelOutput = "";
  try {
    rawModelOutput = await session.send(prompt, {
      ...input.completionOptions,
      think: input.stage.reasoning ?? input.completionOptions?.think ?? false,
    });
  } catch (error) {
    return {
      ok: false,
      retryable: false,
      warning: {
        kind: "model_call_failed",
        turnIndex: input.turnIndex,
        attempt: input.attempt,
        maxAttempts: input.maxAttempts,
        message: error instanceof Error ? error.message : "Model call failed",
      },
      trace: {
        turnIndex: input.turnIndex,
        attempt: input.attempt,
        maxAttempts: input.maxAttempts,
        promptSnapshot: prompt,
        failureKind: "model_call_failed",
        success: false,
      },
    };
  }

  const rewrittenContent = normalizeRewriteOutput(rawModelOutput);
  if (rewrittenContent.length === 0) {
    return {
      ok: false,
      retryable: true,
      warning: {
        kind: "empty_output",
        turnIndex: input.turnIndex,
        attempt: input.attempt,
        maxAttempts: input.maxAttempts,
        message: `Turn ${input.turnIndex} rewrite output was empty`,
      },
      trace: {
        turnIndex: input.turnIndex,
        attempt: input.attempt,
        maxAttempts: input.maxAttempts,
        promptSnapshot: prompt,
        rawModelOutput,
        failureKind: "empty_output",
        success: false,
      },
    };
  }

  const currentTurnObject = input.currentTurn as Record<string, unknown>;
  const turnValidationTarget = {
    ...currentTurnObject,
    [input.transform.contentField]: rewrittenContent,
    content: rewrittenContent,
  };
  const role = currentTurnObject[input.transform.roleField];
  const originalContent = currentTurnObject[input.transform.contentField];
  const validationResult = validateStageValue(
    turnValidationTarget,
    input.stage.validate,
    {
      skipInapplicablePaths: true,
      refs: {
        original_target_turn: input.currentTurn,
        original_target_content: typeof originalContent === "string"
          ? originalContent
          : undefined,
        previous_turn: input.turnIndex > 0 ? input.priorTurns[input.turnIndex - 1] : undefined,
        previous_same_role_turn: typeof role === "string"
          ? findPreviousTurnWithSameRole(
            input.priorTurns,
            input.priorTurns.length,
            input.transform.roleField,
            role,
          )
          : undefined,
      },
    },
  );
  if (!validationResult.success) {
    const validatorWarning = buildValidatorMismatchWarning(validationResult.issues);
    return {
      ok: false,
      retryable: true,
      warning: {
        kind: validatorWarning.kind,
        turnIndex: input.turnIndex,
        attempt: input.attempt,
        maxAttempts: input.maxAttempts,
        message: validatorWarning.message,
        validatorName: validatorWarning.validatorName,
      },
      trace: {
        turnIndex: input.turnIndex,
        attempt: input.attempt,
        maxAttempts: input.maxAttempts,
        promptSnapshot: prompt,
        rawModelOutput,
        validationIssues: validationResult.issues,
        failureKind: "validator_mismatch",
        success: false,
      },
    };
  }

  return {
    ok: true,
    rewrittenContent,
    trace: {
      turnIndex: input.turnIndex,
      attempt: input.attempt,
      maxAttempts: input.maxAttempts,
      promptSnapshot: prompt,
      rawModelOutput,
      success: true,
    },
  };
}

export async function rewriteConversationRecord(
  record: unknown,
  stage: StageInput,
  sessionFactory: () => ChatSession,
  completionOptions?: CompletionSettings,
  workflowPath?: string,
): Promise<ConversationRewriteResult> {
  const transformedRecord = structuredClone(record);
  const warnings: ConversationRewriteWarning[] = [];
  const traces: ConversationRewriteTrace[] = [];
  const transform = stage.transform;
  const maxAttempts = resolveMaxAttempts(stage);

  if (!transform || transform.kind !== "conversation_rewrite") {
    throw new Error("conversation_rewrite transform config is required");
  }

  let conversationsValue: unknown;
  try {
    conversationsValue = getValueAtPath(
      transformedRecord,
      transform.conversationsPath,
    );
  } catch (error) {
    warnings.push({
      kind: "missing_conversations_path",
      message: error instanceof Error ? error.message : String(error),
    });
    return { record: transformedRecord, warnings, traces };
  }

  if (!Array.isArray(conversationsValue)) {
    warnings.push({
      kind: "invalid_conversations_value",
      message:
        `Path '${transform.conversationsPath}' must resolve to an array of turns`,
    });
    return { record: transformedRecord, warnings, traces };
  }

  const turns = conversationsValue as unknown[];
  for (let turnIndex = 0; turnIndex < turns.length; turnIndex++) {
    const currentTurn = turns[turnIndex];
    if (
      !currentTurn ||
      typeof currentTurn !== "object" ||
      Array.isArray(currentTurn)
    ) {
      warnings.push({
        kind: "invalid_turn_shape",
        turnIndex,
        message: `Turn ${turnIndex} is not an object`,
      });
      continue;
    }

    const currentTurnObject = currentTurn as Record<string, unknown>;
    const role = currentTurnObject[transform.roleField];
    const originalContent = currentTurnObject[transform.contentField];

    if (typeof role !== "string") {
      warnings.push({
        kind: "invalid_turn_shape",
        turnIndex,
        message:
          `Turn ${turnIndex} is missing string role field '${transform.roleField}'`,
      });
      continue;
    }

    if (!transform.targetRoles.includes(role)) {
      continue;
    }

    if (typeof originalContent !== "string") {
      warnings.push({
        kind: "invalid_turn_shape",
        turnIndex,
        message:
          `Turn ${turnIndex} is missing string content field '${transform.contentField}'`,
      });
      continue;
    }

    const preprocessedTurn = await preprocessTurn({
      record: transformedRecord,
      turn: currentTurnObject,
      turnIndex,
      priorTurns: turns.slice(0, turnIndex),
      stage,
      transform,
      sessionFactory,
      completionOptions,
      workflowPath,
    });
    if (!preprocessedTurn.ok) {
      warnings.push(preprocessedTurn.warning);
      continue;
    }

    turns[turnIndex] = preprocessedTurn.turn;
    const currentTargetTurn = turns[turnIndex] as Record<string, unknown>;

    const targetRole = currentTargetTurn[transform.roleField];
    const targetContent = currentTargetTurn[transform.contentField];
    if (typeof targetRole !== "string") {
      warnings.push({
        kind: "invalid_turn_shape",
        turnIndex,
        message:
          `Turn ${turnIndex} is missing string role field '${transform.roleField}' after turnPreprocess`,
      });
      continue;
    }

    if (typeof targetContent !== "string") {
      warnings.push({
        kind: "invalid_turn_shape",
        turnIndex,
        message:
          `Turn ${turnIndex} is missing string content field '${transform.contentField}' after turnPreprocess`,
      });
      continue;
    }

    if (!transform.targetRoles.includes(targetRole)) {
      continue;
    }

    if (!evaluateTurnWhen(transform, currentTargetTurn)) {
      continue;
    }

    let feedbackAppendix: string | undefined;
    let finalWarning: ConversationRewriteWarning | undefined;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const attemptResult = await rewriteTurnOnce({
        stage,
        sessionFactory,
        completionOptions,
        priorTurns: turns.slice(0, turnIndex),
        currentTurn: currentTargetTurn,
        turnIndex,
        transform,
        promptFeedbackAppendix: feedbackAppendix,
        attempt,
        maxAttempts,
      });

      traces.push(attemptResult.trace);
      if (attemptResult.ok) {
        currentTargetTurn[transform.contentField] = attemptResult.rewrittenContent;
        finalWarning = undefined;
        break;
      }

      finalWarning = attemptResult.warning;
      if (
        !stage.retry?.enabled ||
        attempt >= maxAttempts ||
        !attemptResult.retryable ||
        !isRewriteRetryEligible(attemptResult.warning.kind)
      ) {
        break;
      }

      feedbackAppendix = buildRetryFeedbackAppendix({
        attempt,
        maxAttempts,
        failureKind: attemptResult.warning.kind === "empty_output"
          ? "empty_output"
          : "validator_mismatch",
        reason: attemptResult.warning.message,
        validationIssues: attemptResult.trace.validationIssues,
        responseFormat: "text",
      });
    }

    if (finalWarning) {
      warnings.push(finalWarning);
    }
  }

  return {
    record: transformedRecord,
    warnings,
    traces,
  };
}
