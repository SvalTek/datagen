/**
 * Copyright (C) 2026 Theros <https://github.com/therosin>
 *
 * This file is part of Datagen.
 *
 * Datagen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Datagen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Datagen.  If not, see <https://www.gnu.org/licenses/>.
 */
import { z } from "zod";

export type PipelineReasoningMode = "off" | "think" | "openai";
export type PipelineProvider = "openai" | "ollama";
export type StructuredOutputMode = "object" | "json" | "json-array" | "off";

export interface TurnExample {
  /** The input for the example, which can be a string or an array of strings. */
  input: string | string[];
  /** The expected output for the example, which can be a string or an array of strings. */
  output: string | string[];
}

export interface Stage {
  // ─── Main Fields ─────────────────────────────────────────────────────

  /** OPTIONAL: The name of the stage. */
  name?: string;
  /** OPTIONAL: Stable stage identifier used for graph dependencies and reporting. */
  id?: string;
  /** OPTIONAL: The description of the stage. */
  description?: string;
  /** The instructions for the stage. */
  instructions: string;

  // ─── Optional Fields ─────────────────────────────────────────────────

  /** OPTIONAL: system prompt to guide the model's behavior during this stage. */
  system?: string;

  /** OPTIONAL: A set of rules that can be used to validate the model's output during this stage.
   * Include: "sentences that encourage the model to generate responses that are relevant, coherent, and aligned with the task's objectives."
   * Exclude: "sentences that discourage the model from generating responses that are irrelevant, incoherent, or misaligned with the task's objectives."
   * These rules can be used to guide the model's behavior and ensure that the generated responses meet certain criteria or standards.
   * The specific implementation of how these rules are applied will depend on the the model and Stage instructions, Not all models may enforce these rules,
   * but they can be used as a way to provide additional guidance and constraints on the model's output during this stage.
   * This field is optional and can be used as needed to enhance the quality and relevance of the model's responses during this stage.
   */
  rules?: {
    include?: string[];
    exclude?: string[];
  };

  /** OPTIONAL: A string representing the history of interactions or context for this stage.
   * Can be used to provide additional information or context to the model which may be relevant for generating responses.
   * (e.g., previous conversations, user preferences, or any relevant background information).
   */
  history?: string;

  /** OPTIONAL: A list of Example objects that provide sample interactions or data relevant to this stage.
   * Each Example can include input-output pairs or any relevant examples that can help guide the model's behavior during this stage.
   */
  examples?: TurnExample[];

  /** OPTIONAL: Enables model reasoning/thinking mode for this stage. Defaults to false. */
  reasoning?: boolean;
  /** OPTIONAL: Execution mode for this stage. Defaults to "batch". */
  mode?: "batch" | "iter" | "record_transform" | "workflow_delegate" | "lua";
  /** OPTIONAL: Explicit stage dependencies by stage id/name/key. */
  dependsOn?: string[];
  /** OPTIONAL: Minimal conditional execution gate for this stage. */
  when?: {
    path: string;
    equals?: unknown;
    notEquals?: unknown;
    any?: unknown[];
    notAny?: unknown[];
  };
  /** OPTIONAL: Per-stage parallelism for iter and record_transform. Defaults to 1. */
  parallelism?: number;

  /** OPTIONAL: Input source selection for stages with alternate runtime inputs. */
  input?: {
    source?: "pipeline_input" | "previous_stage";
  };

  /** OPTIONAL: Structured transform config for record transformation stages. */
  transform?: {
    kind: "conversation_rewrite";
    conversationsPath: string;
    roleField: string;
    contentField: string;
    targetRoles: string[];
    includeOriginalTargetTurn?: boolean;
    turnPreprocess?: {
      source: "inline" | "file";
      code?: string;
      filePath?: string;
      runtime?: {
        functionTimeoutMs?: number;
        openStandardLibs?: boolean;
        injectObjects?: boolean;
        enableProxy?: boolean;
        traceAllocations?: boolean;
      };
    };
    turnWhen?: {
      path: string;
      equals?: unknown;
      notEquals?: unknown;
      any?: unknown[];
      notAny?: unknown[];
    };
  };

  /** OPTIONAL: Delegated child-workflow execution config. */
  delegate?: {
    workflowPath: string;
    inputFromPath: string;
    inputAs?: "initial_context" | "pipeline_input";
    outputFrom?: "final_stage_output" | "stage_key";
    outputStageKey?: string;
    outputSelectPath?: string;
    onFailure?: "fail" | "warn";
    inheritParentCli?: "none" | "completion" | "all";
  };

  /** OPTIONAL: Lua execution config for lua stages. */
  lua?: {
    source: "inline" | "file";
    code?: string;
    filePath?: string;
    runtime?: {
      functionTimeoutMs?: number;
      openStandardLibs?: boolean;
      injectObjects?: boolean;
      enableProxy?: boolean;
      traceAllocations?: boolean;
    };
  };

  /** OPTIONAL: Declarative typed schema for this stage output. */
  constrain?: ConstrainSchemaNode;

  /** OPTIONAL: Semantic/content validation rules enforced at runtime. */
  validate?: {
    onFailure?: "fail" | "warn";
    rules: ValidationRule[];
  };

  /** OPTIONAL: Retry settings for iter and record_transform stages. */
  retry?: {
    enabled?: boolean;
    maxAttempts?: number;
  };

  // ─── Extra Fields ────────────────────────────────────────────────────

  /** A flexible field that can hold any additional information or metadata relevant to the stage.
   * This can be used to store any extra data that may be needed for processing or understanding the stage, such as tags, categories, or any other relevant information.
   * This will be copied into the dataset as-is and can be used for any custom processing or handling as needed.
   */
  metadata?: Record<string, any>;
}

export interface PipelineInput {
  /** Path to the source dataset file. */
  path: string;
  /** OPTIONAL: Dataset file format. Inferred from extension when omitted. */
  format?: "json" | "jsonl";
  /** OPTIONAL: Number of records to skip before loading records into the pipeline. */
  offset?: number;
  /** OPTIONAL: Maximum number of records to load after offset is applied. */
  limit?: number;
  /** OPTIONAL: Input-shape normalization applied after load/slice and before stages run. */
  remap?: InputRemap;
  /** OPTIONAL: Dataset read mode. stream is currently supported for JSONL. */
  readMode?: "eager" | "stream";
}

export interface PrefixedStringArrayRemap {
  kind: "prefixed_string_array";
  sourcePath: string;
  outputPath?: string;
  roleField?: string;
  contentField?: string;
  prefixes: {
    user: string;
    assistant: string;
    system?: string;
  };
  trimContent?: boolean;
}

export interface AlpacaRemap {
  kind: "alpaca";
  instructionField?: string;
  inputField?: string;
  outputField?: string;
  outputPath?: string;
  roleField?: string;
  contentField?: string;
}

export type InputRemap = PrefixedStringArrayRemap | AlpacaRemap;

export interface ValidationRule {
  name?: string;
  path?: string;
  hint?: string;
  when?: {
    path?: string;
    equals?: string | number | boolean;
    notEquals?: string | number | boolean;
  };
  scope?: {
    includePattern?: {
      pattern: string;
      flags?: string;
    };
    excludePatterns?: Array<{
      pattern: string;
      flags?: string;
    }>;
  };
  similarity?: {
    mode?: "fast" | "detailed";
  };
  kind:
    | "contains"
    | "not_contains"
    | "regex"
    | "not_regex"
    | "min_length"
    | "max_length"
    | "equals"
    | "not_equals"
    | "must_change_from_path"
    | "must_change_from_ref"
    | "not_equal_to_path"
    | "min_similarity_to_path"
    | "max_similarity_to_path"
    | "not_equal_to_ref"
    | "min_similarity_to_ref"
    | "max_similarity_to_ref"
    | "array_min_length"
    | "array_max_length";
  value?: string | number | boolean | null;
  pattern?: string;
  flags?: string;
  otherPath?: string;
  ref?: string;
  threshold?: number;
}

export type ConstrainPrimitiveValue = string | number | boolean | null;

export type ConstrainSchemaNode =
  | {
    type: "string";
    minLength?: number;
    maxLength?: number;
    pattern?: string;
    optional?: boolean;
    nullable?: boolean;
  }
  | {
    type: "number";
    min?: number;
    max?: number;
    int?: boolean;
    optional?: boolean;
    nullable?: boolean;
  }
  | {
    type: "boolean";
    optional?: boolean;
    nullable?: boolean;
  }
  | {
    type: "literal";
    value: ConstrainPrimitiveValue;
    optional?: boolean;
    nullable?: boolean;
  }
  | {
    type: "enum";
    values: ConstrainPrimitiveValue[];
    optional?: boolean;
    nullable?: boolean;
  }
  | {
    type: "array";
    items: ConstrainSchemaNode;
    minItems?: number;
    maxItems?: number;
    optional?: boolean;
    nullable?: boolean;
  }
  | {
    type: "object";
    shape: Record<string, ConstrainSchemaNode>;
    additionalProperties?: boolean;
    optional?: boolean;
    nullable?: boolean;
  };

export interface PipelineDocument {
  /** OPTIONAL: Schema/document version. */
  version?: string | number;
  /** OPTIONAL: Pipeline name. */
  name?: string;
  /** OPTIONAL: Pipeline description. */
  description?: string;
  /** OPTIONAL: Default model for this pipeline run. */
  model?: string;
  /** OPTIONAL: OpenAI-compatible base endpoint for this pipeline run. */
  endpoint?: string;
  /** OPTIONAL: Provider selection. Defaults to openai when omitted. */
  provider?: PipelineProvider;
  /** OPTIONAL: Structured-output strategy for constrained stages. Defaults to off when omitted. */
  structuredOutputMode?: StructuredOutputMode;
  /** OPTIONAL: Transport-level reasoning protocol. Defaults to off when omitted. */
  reasoningMode?: PipelineReasoningMode;
  /** OPTIONAL: Environment variable name containing an API key for the endpoint. */
  apiKeyEnv?: string;
  /** OPTIONAL: HTTP-Referer header value for OpenAI-compatible provider attribution. */
  httpReferer?: string;
  /** OPTIONAL: X-Title header value for OpenAI-compatible provider attribution. */
  xTitle?: string;
  /** OPTIONAL: Default max token budget for model completions in this pipeline run. */
  maxTokens?: number;
  /** OPTIONAL: Default completion temperature for this pipeline run. */
  temperature?: number;
  /** OPTIONAL: Source dataset input definition for transform pipelines. */
  input?: PipelineInput;
  /** OPTIONAL: Output directory where final JSONL is written. Defaults to ./output. */
  outputDir?: string;
  /** OPTIONAL: Repeat the whole workflow run this many times. Defaults to 1. */
  repeat?: number;
  /** OPTIONAL: Workflow-level default Lua runtime options for lua stages. */
  luaRuntime?: {
    functionTimeoutMs?: number;
    openStandardLibs?: boolean;
    injectObjects?: boolean;
    enableProxy?: boolean;
    traceAllocations?: boolean;
  };
  /** Ordered stage definitions that form the pipeline. */
  stages: Stage[];
}

export const TurnExampleSchema = z.object({
  input: z.union([z.string(), z.array(z.string())]),
  output: z.union([z.string(), z.array(z.string())]),
}).strict();

export const StageRulesSchema = z.object({
  include: z.array(z.string()).optional(),
  exclude: z.array(z.string()).optional(),
}).strict().optional();

export const StageInputSourceSchema = z.object({
  source: z.enum(["pipeline_input", "previous_stage"]).optional(),
}).strict().optional();

const StageWhenSchema = z.object({
  path: z.string().min(1),
  equals: z.unknown().optional(),
  notEquals: z.unknown().optional(),
  any: z.array(z.unknown()).min(1).optional(),
  notAny: z.array(z.unknown()).min(1).optional(),
}).strict().superRefine((value, ctx) => {
  const operatorCount = Number(value.equals !== undefined) +
    Number(value.notEquals !== undefined) +
    Number(value.any !== undefined) +
    Number(value.notAny !== undefined);
  if (operatorCount !== 1) {
    ctx.addIssue({
      code: "custom",
      message:
        "stage.when requires exactly one of equals, notEquals, any, or notAny",
    });
  }
});

const EmbeddedLuaSchema = z.object({
  source: z.enum(["inline", "file"]),
  code: z.string().min(1).optional(),
  filePath: z.string().min(1).optional(),
  runtime: z.object({
    functionTimeoutMs: z.number().int().nonnegative().optional(),
    openStandardLibs: z.boolean().optional(),
    injectObjects: z.boolean().optional(),
    enableProxy: z.boolean().optional(),
    traceAllocations: z.boolean().optional(),
  }).strict().optional(),
}).strict().superRefine((value, ctx) => {
  if (value.source === "inline") {
    if (!value.code?.trim()) {
      ctx.addIssue({
        code: "custom",
        message: "turnPreprocess.code is required when turnPreprocess.source is inline",
        path: ["code"],
      });
    }
    if (value.filePath !== undefined) {
      ctx.addIssue({
        code: "custom",
        message: "turnPreprocess.filePath is not allowed when turnPreprocess.source is inline",
        path: ["filePath"],
      });
    }
  }

  if (value.source === "file") {
    if (!value.filePath?.trim()) {
      ctx.addIssue({
        code: "custom",
        message: "turnPreprocess.filePath is required when turnPreprocess.source is file",
        path: ["filePath"],
      });
    }
    if (value.code !== undefined) {
      ctx.addIssue({
        code: "custom",
        message: "turnPreprocess.code is not allowed when turnPreprocess.source is file",
        path: ["code"],
      });
    }
  }
});

export const ConversationRewriteTransformSchema = z.object({
  kind: z.literal("conversation_rewrite"),
  conversationsPath: z.string().min(1),
  roleField: z.string().min(1),
  contentField: z.string().min(1),
  targetRoles: z.array(z.string().min(1)).min(1),
  includeOriginalTargetTurn: z.boolean().optional(),
  turnPreprocess: EmbeddedLuaSchema.optional(),
  turnWhen: StageWhenSchema.optional(),
}).strict();

export const WorkflowDelegateSchema = z.object({
  workflowPath: z.string().min(1),
  inputFromPath: z.string().min(1),
  inputAs: z.enum(["initial_context", "pipeline_input"]).optional(),
  outputFrom: z.enum(["final_stage_output", "stage_key"]).optional(),
  outputStageKey: z.string().min(1).optional(),
  outputSelectPath: z.string().min(1).optional(),
  onFailure: z.enum(["fail", "warn"]).optional(),
  inheritParentCli: z.enum(["none", "completion", "all"]).optional(),
}).strict().superRefine((value, ctx) => {
  if (
    (value.outputFrom ?? "final_stage_output") === "stage_key" &&
    !value.outputStageKey?.trim()
  ) {
    ctx.addIssue({
      code: "custom",
      message:
        "delegate.outputStageKey is required when outputFrom is stage_key",
      path: ["outputStageKey"],
    });
  }
});

export const LuaRuntimeOptionsSchema = z.object({
  functionTimeoutMs: z.number().int().nonnegative().optional(),
  openStandardLibs: z.boolean().optional(),
  injectObjects: z.boolean().optional(),
  enableProxy: z.boolean().optional(),
  traceAllocations: z.boolean().optional(),
}).strict();

export const LuaStageSchema = z.object({
  source: z.enum(["inline", "file"]),
  code: z.string().min(1).optional(),
  filePath: z.string().min(1).optional(),
  runtime: LuaRuntimeOptionsSchema.optional(),
}).strict().superRefine((value, ctx) => {
  if (value.source === "inline") {
    if (!value.code?.trim()) {
      ctx.addIssue({
        code: "custom",
        message: "lua.code is required when lua.source is inline",
        path: ["code"],
      });
    }
    if (value.filePath !== undefined) {
      ctx.addIssue({
        code: "custom",
        message: "lua.filePath is not allowed when lua.source is inline",
        path: ["filePath"],
      });
    }
  }

  if (value.source === "file") {
    if (!value.filePath?.trim()) {
      ctx.addIssue({
        code: "custom",
        message: "lua.filePath is required when lua.source is file",
        path: ["filePath"],
      });
    }
    if (value.code !== undefined) {
      ctx.addIssue({
        code: "custom",
        message: "lua.code is not allowed when lua.source is file",
        path: ["code"],
      });
    }
  }
});

const PrefixedStringArrayRemapSchema = z.object({
  kind: z.literal("prefixed_string_array"),
  sourcePath: z.string().min(1),
  outputPath: z.string().min(1).optional(),
  roleField: z.string().min(1).optional(),
  contentField: z.string().min(1).optional(),
  prefixes: z.object({
    user: z.string().min(1),
    assistant: z.string().min(1),
    system: z.string().min(1).optional(),
  }).strict(),
  trimContent: z.boolean().optional(),
}).strict();

const AlpacaRemapSchema = z.object({
  kind: z.literal("alpaca"),
  instructionField: z.string().min(1).optional(),
  inputField: z.string().min(1).optional(),
  outputField: z.string().min(1).optional(),
  outputPath: z.string().min(1).optional(),
  roleField: z.string().min(1).optional(),
  contentField: z.string().min(1).optional(),
}).strict();

export const InputRemapSchema = z.discriminatedUnion("kind", [
  PrefixedStringArrayRemapSchema,
  AlpacaRemapSchema,
]);

export const ValidationWhenSchema = z.object({
  path: z.string().min(1).optional(),
  equals: z.union([z.string(), z.number(), z.boolean()]).optional(),
  notEquals: z.union([z.string(), z.number(), z.boolean()]).optional(),
}).strict().superRefine((value, ctx) => {
  const operatorCount = Number(value.equals !== undefined) +
    Number(value.notEquals !== undefined);
  if (operatorCount !== 1) {
    ctx.addIssue({
      code: "custom",
      message: "validate.when requires exactly one of equals or notEquals",
    });
  }
});

const ValidationRuleBaseSchema = z.object({
  name: z.string().min(1).optional(),
  path: z.string().min(1).optional(),
  hint: z.string().min(1).optional(),
  when: ValidationWhenSchema.optional(),
});

const ValidationScopedPatternSchema = z.object({
  pattern: z.string().min(1),
  flags: z.string().optional(),
}).strict();

const ValidationScopeSchema = z.object({
  includePattern: ValidationScopedPatternSchema.optional(),
  excludePatterns: z.array(ValidationScopedPatternSchema).min(1).optional(),
}).strict().superRefine((value, ctx) => {
  if (!value.includePattern && !value.excludePatterns?.length) {
    ctx.addIssue({
      code: "custom",
      message: "validate.scope requires includePattern and/or excludePatterns",
    });
  }
});

const ValidationRuleContainsSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("contains"),
  value: z.string(),
});

const ValidationRuleNotContainsSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("not_contains"),
  value: z.string(),
});

const ValidationRuleRegexSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("regex"),
  pattern: z.string(),
  flags: z.string().optional(),
});

const ValidationRuleNotRegexSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("not_regex"),
  pattern: z.string(),
  flags: z.string().optional(),
});

const ValidationRuleMinLengthSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("min_length"),
  value: z.number().int().nonnegative(),
});

const ValidationRuleMaxLengthSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("max_length"),
  value: z.number().int().nonnegative(),
});

const ValidationRuleEqualsSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("equals"),
  value: z.union([z.string(), z.number(), z.boolean(), z.null()]),
});

const ValidationRuleNotEqualsSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("not_equals"),
  value: z.union([z.string(), z.number(), z.boolean(), z.null()]),
});

const ValidationRuleMustChangeFromPathSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("must_change_from_path"),
  otherPath: z.string().min(1),
  scope: ValidationScopeSchema.optional(),
});

const ValidationRuleMustChangeFromRefSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("must_change_from_ref"),
  ref: z.string().min(1),
  scope: ValidationScopeSchema.optional(),
});

const ValidationRuleNotEqualToPathSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("not_equal_to_path"),
  otherPath: z.string().min(1),
  scope: ValidationScopeSchema.optional(),
});

const ValidationRuleMaxSimilarityToPathSchema = ValidationRuleBaseSchema.extend(
  {
    kind: z.literal("max_similarity_to_path"),
    otherPath: z.string().min(1),
    threshold: z.number().min(0).max(1),
    scope: ValidationScopeSchema.optional(),
    similarity: z.object({
      mode: z.enum(["fast", "detailed"]).optional(),
    }).strict().optional(),
  },
);

const ValidationRuleMinSimilarityToPathSchema = ValidationRuleBaseSchema.extend(
  {
    kind: z.literal("min_similarity_to_path"),
    otherPath: z.string().min(1),
    threshold: z.number().min(0).max(1),
    scope: ValidationScopeSchema.optional(),
    similarity: z.object({
      mode: z.enum(["fast", "detailed"]).optional(),
    }).strict().optional(),
  },
);

const ValidationRuleNotEqualToRefSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("not_equal_to_ref"),
  ref: z.string().min(1),
  scope: ValidationScopeSchema.optional(),
});

const ValidationRuleMaxSimilarityToRefSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("max_similarity_to_ref"),
  ref: z.string().min(1),
  threshold: z.number().min(0).max(1),
  scope: ValidationScopeSchema.optional(),
  similarity: z.object({
    mode: z.enum(["fast", "detailed"]).optional(),
  }).strict().optional(),
});

const ValidationRuleMinSimilarityToRefSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("min_similarity_to_ref"),
  ref: z.string().min(1),
  threshold: z.number().min(0).max(1),
  scope: ValidationScopeSchema.optional(),
  similarity: z.object({
    mode: z.enum(["fast", "detailed"]).optional(),
  }).strict().optional(),
});

const ValidationRuleArrayMinLengthSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("array_min_length"),
  value: z.number().int().nonnegative(),
});

const ValidationRuleArrayMaxLengthSchema = ValidationRuleBaseSchema.extend({
  kind: z.literal("array_max_length"),
  value: z.number().int().nonnegative(),
});

export const ValidationRuleSchema = z.discriminatedUnion("kind", [
  ValidationRuleContainsSchema,
  ValidationRuleNotContainsSchema,
  ValidationRuleRegexSchema,
  ValidationRuleNotRegexSchema,
  ValidationRuleMinLengthSchema,
  ValidationRuleMaxLengthSchema,
  ValidationRuleEqualsSchema,
  ValidationRuleNotEqualsSchema,
  ValidationRuleMustChangeFromPathSchema,
  ValidationRuleMustChangeFromRefSchema,
  ValidationRuleNotEqualToPathSchema,
  ValidationRuleMinSimilarityToPathSchema,
  ValidationRuleMaxSimilarityToPathSchema,
  ValidationRuleNotEqualToRefSchema,
  ValidationRuleMinSimilarityToRefSchema,
  ValidationRuleMaxSimilarityToRefSchema,
  ValidationRuleArrayMinLengthSchema,
  ValidationRuleArrayMaxLengthSchema,
]);

export const StageValidateSchema = z.object({
  onFailure: z.enum(["fail", "warn"]).optional(),
  rules: z.array(ValidationRuleSchema).min(1),
}).strict().optional();

export const StageRetrySchema = z.object({
  enabled: z.boolean().optional(),
  maxAttempts: z.number().int().min(1).optional(),
}).strict().optional();

const ConstrainPrimitiveValueSchema = z.union([
  z.string(),
  z.number(),
  z.boolean(),
  z.null(),
]);

const ConstrainNodeBaseSchema = z.object({
  optional: z.boolean().optional(),
  nullable: z.boolean().optional(),
});

const ConstrainSchemaNodeSchema: z.ZodType<ConstrainSchemaNode> = z.lazy(() =>
  z.discriminatedUnion("type", [
    ConstrainNodeBaseSchema.extend({
      type: z.literal("string"),
      minLength: z.number().int().nonnegative().optional(),
      maxLength: z.number().int().nonnegative().optional(),
      pattern: z.string().optional(),
    }).strict(),
    ConstrainNodeBaseSchema.extend({
      type: z.literal("number"),
      min: z.number().optional(),
      max: z.number().optional(),
      int: z.boolean().optional(),
    }).strict(),
    ConstrainNodeBaseSchema.extend({
      type: z.literal("boolean"),
    }).strict(),
    ConstrainNodeBaseSchema.extend({
      type: z.literal("literal"),
      value: ConstrainPrimitiveValueSchema,
    }).strict(),
    ConstrainNodeBaseSchema.extend({
      type: z.literal("enum"),
      values: z.array(ConstrainPrimitiveValueSchema).min(1),
    }).strict(),
    ConstrainNodeBaseSchema.extend({
      type: z.literal("array"),
      items: ConstrainSchemaNodeSchema,
      minItems: z.number().int().nonnegative().optional(),
      maxItems: z.number().int().nonnegative().optional(),
    }).strict(),
    ConstrainNodeBaseSchema.extend({
      type: z.literal("object"),
      shape: z.record(z.string().min(1), ConstrainSchemaNodeSchema),
      additionalProperties: z.boolean().optional(),
    }).strict(),
  ])
);

export const StageSchema = z.object({
  name: z.string().min(1).optional(),
  id: z.string().min(1).optional(),
  description: z.string().optional(),
  instructions: z.string().min(1),
  system: z.string().optional(),
  rules: StageRulesSchema,
  history: z.string().optional(),
  examples: z.array(TurnExampleSchema).optional(),
  reasoning: z.boolean().optional(),
  mode: z.enum([
    "batch",
    "iter",
    "record_transform",
    "workflow_delegate",
    "lua",
  ]).optional(),
  dependsOn: z.array(z.string().min(1)).min(1).optional(),
  when: StageWhenSchema.optional(),
  parallelism: z.number().int().min(1).optional(),
  input: StageInputSourceSchema,
  transform: ConversationRewriteTransformSchema.optional(),
  delegate: WorkflowDelegateSchema.optional(),
  lua: LuaStageSchema.optional(),
  constrain: ConstrainSchemaNodeSchema.optional(),
  validate: StageValidateSchema,
  retry: StageRetrySchema,
  metadata: z.record(z.string(), z.unknown()).optional(),
}).strict().superRefine((value, ctx) => {
  if (value.mode === "record_transform" && !value.transform) {
    ctx.addIssue({
      code: "custom",
      message: "record_transform stages require a transform block",
      path: ["transform"],
    });
  }

  if (value.mode !== "record_transform" && value.transform) {
    ctx.addIssue({
      code: "custom",
      message: "transform is only supported for record_transform stages",
      path: ["transform"],
    });
  }

  if (value.mode === "workflow_delegate" && !value.delegate) {
    ctx.addIssue({
      code: "custom",
      message: "workflow_delegate stages require a delegate block",
      path: ["delegate"],
    });
  }

  if (value.mode !== "workflow_delegate" && value.delegate) {
    ctx.addIssue({
      code: "custom",
      message: "delegate is only supported for workflow_delegate stages",
      path: ["delegate"],
    });
  }

  if (value.mode === "lua" && !value.lua) {
    ctx.addIssue({
      code: "custom",
      message: "lua stages require a lua block",
      path: ["lua"],
    });
  }

  if (value.mode !== "lua" && value.lua) {
    ctx.addIssue({
      code: "custom",
      message: "lua is only supported for lua stages",
      path: ["lua"],
    });
  }
});

export const PipelineInputSchema = z.object({
  path: z.string().min(1),
  format: z.enum(["json", "jsonl"]).optional(),
  readMode: z.enum(["eager", "stream"]).optional(),
  offset: z.number().int().nonnegative().optional(),
  limit: z.number().int().nonnegative().optional(),
  remap: InputRemapSchema.optional(),
}).strict();

export const PipelineDocumentSchema = z.object({
  version: z.union([z.string(), z.number()]).optional(),
  name: z.string().optional(),
  description: z.string().optional(),
  model: z.string().min(1).optional(),
  endpoint: z.string().min(1).optional(),
  provider: z.enum(["openai", "ollama"]).optional(),
  structuredOutputMode: z.enum(["object", "json", "json-array", "off"]).optional(),
  reasoningMode: z.enum(["off", "think", "openai"]).optional(),
  apiKeyEnv: z.string().min(1).optional(),
  httpReferer: z.string().min(1).optional(),
  xTitle: z.string().min(1).optional(),
  maxTokens: z.number().int().nonnegative().optional(),
  temperature: z.number().optional(),
  input: PipelineInputSchema.optional(),
  outputDir: z.string().min(1).optional(),
  repeat: z.number().int().min(1).optional(),
  luaRuntime: LuaRuntimeOptionsSchema.optional(),
  stages: z.array(StageSchema).min(1),
}).strict().superRefine((value, ctx) => {
  const seen = new Set<string>();
  const seenIds = new Set<string>();
  const resolvedKeys = value.stages.map((stage, index) =>
    stage.id?.trim() || stage.name?.trim() || `stage-${index + 1}`
  );
  const resolvedKeySet = new Set(resolvedKeys);

  value.stages.forEach((stage, index) => {
    if (stage.id) {
      if (seenIds.has(stage.id)) {
        ctx.addIssue({
          code: "custom",
          message: `Duplicate stage id: ${stage.id}`,
          path: ["stages", index, "id"],
        });
      } else {
        seenIds.add(stage.id);
      }
    }

    if (!stage.name) return;
    if (seen.has(stage.name)) {
      ctx.addIssue({
        code: "custom",
        message: `Duplicate stage name: ${stage.name}`,
        path: ["stages", index, "name"],
      });
      return;
    }
    seen.add(stage.name);
  });

  value.stages.forEach((stage, index) => {
    if (stage.dependsOn?.length) {
      for (const dep of stage.dependsOn) {
        if (!resolvedKeySet.has(dep)) {
          ctx.addIssue({
            code: "custom",
            message: `Unknown stage dependency '${dep}'`,
            path: ["stages", index, "dependsOn"],
          });
        }
      }
    }

    if (stage.mode === "record_transform") {
      const source = stage.input?.source ??
        (index === 0 ? "pipeline_input" : "previous_stage");

      if (source === "pipeline_input" && !value.input) {
        ctx.addIssue({
          code: "custom",
          message:
            "record_transform stages using pipeline_input require pipeline.input to be configured",
          path: ["stages", index, "input", "source"],
        });
      }
    }

    if (stage.mode === "lua") {
      const source = stage.input?.source ??
        (index > 0
          ? "previous_stage"
          : value.input
          ? "pipeline_input"
          : undefined);

      if (source === "pipeline_input" && !value.input) {
        ctx.addIssue({
          code: "custom",
          message:
            "lua stages using pipeline_input require pipeline.input to be configured",
          path: ["stages", index, "input", "source"],
        });
      }

      if (source === "previous_stage" && index === 0) {
        ctx.addIssue({
          code: "custom",
          message:
            "first lua stage cannot use previous_stage without a dependency",
          path: ["stages", index, "input", "source"],
        });
      }
    }
  });
});

export type TurnExampleInput = z.infer<typeof TurnExampleSchema>;
export type StageInput = z.infer<typeof StageSchema>;
export type PipelineInputConfig = z.infer<typeof PipelineInputSchema>;
export type ValidationRuleInput = z.infer<typeof ValidationRuleSchema>;
export type StageValidateInput = z.infer<typeof StageValidateSchema>;
export type InputRemapInput = z.infer<typeof InputRemapSchema>;
export type LuaRuntimeOptionsInput = z.infer<typeof LuaRuntimeOptionsSchema>;
export type PipelineDocumentInput = z.infer<typeof PipelineDocumentSchema>;
