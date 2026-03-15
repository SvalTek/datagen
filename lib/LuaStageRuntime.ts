import { createLuaBridge } from "LuaBridge";
import type {
  LuaRuntimeOptionsInput,
  StageInput,
} from "../structures/TaskSchema.ts";
import { getValueAtPath, setValueAtPathClone } from "./ObjectPath.ts";
import type { CompletionSettings } from "./ChatSession.ts";
import { parseModelJson } from "./ModelJson.ts";

export interface LuaStageWarning {
  kind: string;
  message: string;
}

export interface LuaStageMetric {
  name: string;
  value: number;
}

export interface LuaStageNote {
  kind: string;
  value: unknown;
}

export interface LuaStageDebugEntry {
  label: string;
  value: unknown;
}

export interface LuaStageExecutionContext {
  initialContext?: unknown;
  outputsByStage: Record<string, unknown>;
  stageInput?: unknown;
  stageIdentifier: string;
  stageIndex: number;
  [key: string]: unknown;
}

export interface ExecuteLuaStageInput {
  stage: StageInput;
  stageIdentifier: string;
  stageIndex: number;
  workflowPath: string;
  context: LuaStageExecutionContext;
  pipelineRuntime?: LuaRuntimeOptionsInput;
  llmRequest?: (
    prompt: string,
    options?: CompletionSettings,
  ) => Promise<string>;
}

export type ExecuteLuaStageResult =
  | {
    ok: true;
    output: unknown;
    warnings: LuaStageWarning[];
    metrics: LuaStageMetric[];
    notes: LuaStageNote[];
    debugEntries: LuaStageDebugEntry[];
    scriptText: string;
    resolvedFilePath?: string;
  }
  | {
    ok: false;
    errorKind:
      | "lua_execution_failed"
      | "lua_invalid_output"
      | "lua_script_load_failed";
    message: string;
    cause?: unknown;
    warnings: LuaStageWarning[];
    metrics: LuaStageMetric[];
    notes: LuaStageNote[];
    debugEntries: LuaStageDebugEntry[];
    scriptText?: string;
    resolvedFilePath?: string;
  };

const LUA_RUNTIME_DEFAULTS = {
  functionTimeoutMs: 1000,
  openStandardLibs: false,
  injectObjects: true,
  enableProxy: true,
  traceAllocations: false,
} as const;
const LUA_STAGE_CONTEXT_GLOBAL = "__datagen_stage_ctx";

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isAbsolutePath(path: string): boolean {
  return /^(?:[a-zA-Z]:)?[\\/]/.test(path);
}

function resolveRelativePath(
  baseFilePath: string,
  relativePath: string,
): string {
  const baseDir = baseFilePath.replace(/[\\/][^\\/]+$/, "");
  return `${baseDir}/${relativePath}`;
}

function splitPath(path: string): string[] {
  return path.split(".").map((segment) => segment.trim()).filter(Boolean);
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function validateSerializable(
  value: unknown,
  path = "$",
  seen = new WeakSet<object>(),
): string | null {
  if (value === undefined) return `${path} is undefined`;
  if (typeof value === "function") return `${path} is a function`;
  if (typeof value === "symbol") return `${path} is a symbol`;
  if (typeof value === "bigint") return `${path} is a bigint`;
  if (typeof value === "number" && !Number.isFinite(value)) {
    return `${path} is not a finite number`;
  }
  if (value === null) return null;
  if (Array.isArray(value)) {
    if (seen.has(value)) return `${path} contains a circular reference`;
    seen.add(value);
    for (let index = 0; index < value.length; index++) {
      const issue = validateSerializable(value[index], `${path}[${index}]`, seen);
      if (issue) return issue;
    }
    return null;
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    if (seen.has(record)) return `${path} contains a circular reference`;
    seen.add(record);
    for (const [key, nested] of Object.entries(record)) {
      const issue = validateSerializable(nested, `${path}.${key}`, seen);
      if (issue) return issue;
    }
    return null;
  }
  return null;
}

function assertSerializable(value: unknown, label: string): void {
  const issue = validateSerializable(value, label);
  if (issue) {
    throw new Error(`${label} must be JSON-serializable: ${issue}`);
  }
  JSON.stringify(value);
}

function toSlug(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function normalizeTemplate(template: string): string {
  const leadingNormalized = template.startsWith("\n")
    ? template.slice(1)
    : template;
  return leadingNormalized.endsWith("\n")
    ? leadingNormalized.slice(0, -1)
    : leadingNormalized;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function toCompletionSettings(
  options: unknown,
): CompletionSettings | undefined {
  if (!isRecord(options)) return undefined;
  return {
    max_tokens: typeof options.max_tokens === "number"
      ? options.max_tokens
      : undefined,
    temperature: typeof options.temperature === "number"
      ? options.temperature
      : undefined,
    think: typeof options.think === "boolean" ? options.think : undefined,
    reasoning_mode: options.reasoning_mode === "off" ||
        options.reasoning_mode === "think" ||
        options.reasoning_mode === "openai"
      ? options.reasoning_mode
      : undefined,
  };
}

function renderTextTemplate(
  template: string,
  args: Record<string, unknown>,
  positional: unknown[],
): string {
  let out = normalizeTemplate(template);

  for (const [key, rawValue] of Object.entries(args)) {
    const pattern = new RegExp(`\\{${escapeRegExp(key)}\\}`, "g");
    const replacement = String(rawValue);
    out = out.replace(pattern, () => replacement);
  }

  out = out.replace(
    /\$\{\.\.\.\}/g,
    positional.map((item) => String(item)).join(" "),
  );
  out = out.replace(/\$(\d+)/g, (_match, indexRaw) => {
    const index = Number.parseInt(indexRaw, 10) - 1;
    return positional[index] !== undefined
      ? String(positional[index])
      : `$${indexRaw}`;
  });

  return out;
}

function normalizeWhitespace(value: unknown): string {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function truncateText(
  value: unknown,
  maxLen: number,
  suffix = "...",
): string {
  const text = String(value ?? "");
  if (!Number.isFinite(maxLen) || maxLen < 0) {
    throw new Error("Datagen.truncate requires maxLen >= 0");
  }
  if (text.length <= maxLen) return text;
  if (maxLen <= suffix.length) return suffix.slice(0, maxLen);
  return `${text.slice(0, maxLen - suffix.length)}${suffix}`;
}

function deepMerge(a: unknown, b: unknown): unknown {
  if (!isPlainRecord(a) || !isPlainRecord(b)) {
    return structuredClone(b);
  }

  const out = structuredClone(a);
  for (const [key, value] of Object.entries(b)) {
    const existing = out[key];
    out[key] = isPlainRecord(existing) && isPlainRecord(value)
      ? deepMerge(existing, value)
      : structuredClone(value);
  }
  return out;
}

function coerceList(value: unknown, helperName: string): unknown[] {
  if (Array.isArray(value)) return value;
  if (!value || typeof value !== "object") {
    throw new Error(`${helperName} requires an array-like table`);
  }

  const record = value as Record<string, unknown>;
  const numericKeys = Object.keys(record)
    .filter((key) => /^\d+$/.test(key))
    .map((key) => Number.parseInt(key, 10))
    .sort((a, b) => a - b);

  if (numericKeys.length === 0) {
    throw new Error(`${helperName} requires an array-like table`);
  }

  const out: unknown[] = [];
  for (let index = 0; index < numericKeys.length; index++) {
    const expected = index + 1;
    if (numericKeys[index] !== expected) {
      throw new Error(`${helperName} requires contiguous 1-based indices`);
    }
    out.push(record[String(expected)]);
  }
  return out;
}

function coerceStringList(value: unknown, helperName: string): string[] {
  return coerceList(value, helperName).map((item) => String(item));
}

function resolveKeyOrFn(
  item: unknown,
  keyOrFn: unknown,
  helperName: string,
  index: number,
  list: unknown[],
): unknown {
  if (typeof keyOrFn === "string") {
    return getValueAtPath(item, keyOrFn);
  }
  if (typeof keyOrFn === "function") {
    return keyOrFn(item, index + 1, list);
  }
  throw new Error(`${helperName} requires a path string or callback`);
}

function valueType(value: unknown): string {
  if (value === null || value === undefined) return "nil";
  if (Array.isArray(value) || typeof value === "object") return "table";
  if (typeof value === "string") return "string";
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "boolean";
  return typeof value;
}

function requireType(
  value: unknown,
  expectedType: string,
  message?: string,
): unknown {
  if (valueType(value) !== expectedType) {
    throw new Error(
      message ?? `Expected ${expectedType}, got ${valueType(value)}`,
    );
  }
  return value;
}

function setValueAtPathCreate(
  root: unknown,
  path: string,
  value: unknown,
): unknown {
  const clone = structuredClone(root ?? {});
  const parts = splitPath(path);
  if (parts.length === 0) {
    return structuredClone(value);
  }

  if (!clone || typeof clone !== "object" || Array.isArray(clone)) {
    throw new Error(`Path '${path}' requires an object root`);
  }

  let current: Record<string, unknown> = clone as Record<string, unknown>;
  for (let index = 0; index < parts.length - 1; index++) {
    const part = parts[index];
    const next = current[part];
    if (next === undefined) {
      current[part] = {};
    } else if (!next || typeof next !== "object" || Array.isArray(next)) {
      throw new Error(`Path '${path}' is not traversable at '${part}'`);
    }
    current = current[part] as Record<string, unknown>;
  }

  current[parts[parts.length - 1]] = structuredClone(value);
  return clone;
}

function deleteValueAtPath(root: unknown, path: string): unknown {
  const clone = structuredClone(root ?? {});
  const parts = splitPath(path);
  if (parts.length === 0) return clone;
  if (!clone || typeof clone !== "object" || Array.isArray(clone)) {
    throw new Error(`Path '${path}' requires an object root`);
  }

  let current: Record<string, unknown> = clone as Record<string, unknown>;
  for (let index = 0; index < parts.length - 1; index++) {
    const part = parts[index];
    const next = current[part];
    if (!next || typeof next !== "object" || Array.isArray(next)) {
      return clone;
    }
    current = next as Record<string, unknown>;
  }

  delete current[parts[parts.length - 1]];
  return clone;
}

function pickPaths(root: unknown, paths: unknown): Record<string, unknown> {
  let out: Record<string, unknown> = {};
  for (const path of coerceStringList(paths, "Datagen.pick")) {
    try {
      const value = getValueAtPath(root, path);
      out = setValueAtPathCreate(out, path, value) as Record<string, unknown>;
    } catch {
      // ignore missing paths
    }
  }
  return out;
}

function omitPaths(root: unknown, paths: unknown): unknown {
  let out = structuredClone(root);
  for (const path of coerceStringList(paths, "Datagen.omit")) {
    out = deleteValueAtPath(out, path);
  }
  return out;
}

async function resolveLuaScript(
  stage: StageInput,
  workflowPath: string,
): Promise<{ scriptText: string; resolvedFilePath?: string }> {
  if (!stage.lua) {
    throw new Error("lua stages require a lua block");
  }

  if (stage.lua.source === "inline") {
    return { scriptText: stage.lua.code ?? "" };
  }

  const filePath = stage.lua.filePath ?? "";
  const joinedPath = isAbsolutePath(filePath)
    ? filePath
    : resolveRelativePath(workflowPath, filePath);
  const resolvedFilePath = await Deno.realPath(joinedPath);
  const scriptText = await Deno.readTextFile(resolvedFilePath);
  return { scriptText, resolvedFilePath };
}

function parseJsonValue(raw: string, helperName: string): unknown {
  try {
    return parseModelJson(raw);
  } catch (error) {
    throw new Error(
      `${helperName} model output is not valid JSON: ${toErrorMessage(error)}`,
    );
  }
}

function parseRetryOptions(options: unknown): {
  maxAttempts: number;
  backoffMs: number;
} {
  if (!isRecord(options)) {
    return { maxAttempts: 2, backoffMs: 0 };
  }

  const maxAttempts = typeof options.maxAttempts === "number" &&
      Number.isInteger(options.maxAttempts) && options.maxAttempts >= 1
    ? options.maxAttempts
    : 2;
  const backoffMs = typeof options.backoffMs === "number" &&
      Number.isFinite(options.backoffMs) && options.backoffMs >= 0
    ? options.backoffMs
    : 0;
  return { maxAttempts, backoffMs };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function executeLuaStage(
  input: ExecuteLuaStageInput,
): Promise<ExecuteLuaStageResult> {
  const warnings: LuaStageWarning[] = [];
  const metrics: LuaStageMetric[] = [];
  const notes: LuaStageNote[] = [];
  const debugEntries: LuaStageDebugEntry[] = [];

  let scriptText = "";
  let resolvedFilePath: string | undefined;
  try {
    const resolved = await resolveLuaScript(input.stage, input.workflowPath);
    scriptText = resolved.scriptText;
    resolvedFilePath = resolved.resolvedFilePath;
  } catch (error) {
    return {
      ok: false,
      errorKind: "lua_script_load_failed",
      message: `Failed to load lua script: ${toErrorMessage(error)}`,
      cause: error,
      warnings,
      metrics,
      notes,
      debugEntries,
    };
  }

  const runtime = {
    ...LUA_RUNTIME_DEFAULTS,
    ...(input.pipelineRuntime ?? {}),
    ...(input.stage.lua?.runtime ?? {}),
  };

  const llmBinding = {
    generate: async (
      prompt: unknown,
      options?: unknown,
    ) => {
      if (!input.llmRequest) {
        throw new Error("LLM.generate is unavailable in this runtime context");
      }
      if (typeof prompt !== "string" || !prompt.trim()) {
        throw new Error("LLM.generate requires a non-empty prompt string");
      }
      return await input.llmRequest(prompt, toCompletionSettings(options));
    },
    generateObject: async (
      prompt: unknown,
      options?: unknown,
    ) => {
      if (typeof prompt !== "string" || !prompt.trim()) {
        throw new Error(
          "LLM.generateObject requires a non-empty prompt string",
        );
      }
      const raw = await llmBinding.generate(prompt, options);
      const parsed = parseJsonValue(raw, "LLM.generateObject");
      if (
        typeof parsed !== "object" || parsed === null || Array.isArray(parsed)
      ) {
        throw new Error(
          "LLM.generateObject requires the model output JSON to be an object",
        );
      }
      return parsed;
    },
    generateJson: async (
      prompt: unknown,
      options?: unknown,
    ) => {
      if (typeof prompt !== "string" || !prompt.trim()) {
        throw new Error("LLM.generateJson requires a non-empty prompt string");
      }
      const raw = await llmBinding.generate(prompt, options);
      return parseJsonValue(raw, "LLM.generateJson");
    },
    generateMany: async (
      prompts: unknown,
      options?: unknown,
    ) => {
      const promptList = coerceStringList(prompts, "LLM.generateMany");
      const out: string[] = [];
      for (const prompt of promptList) {
        out.push(await llmBinding.generate(prompt, options));
      }
      return out;
    },
    withRetry: async (
      prompt: unknown,
      options?: unknown,
      retry?: unknown,
    ) => {
      if (typeof prompt !== "string" || !prompt.trim()) {
        throw new Error("LLM.withRetry requires a non-empty prompt string");
      }
      const retryOptions = parseRetryOptions(retry);
      let lastError: unknown;
      for (let attempt = 1; attempt <= retryOptions.maxAttempts; attempt++) {
        try {
          return await llmBinding.generate(prompt, options);
        } catch (error) {
          lastError = error;
          if (attempt >= retryOptions.maxAttempts) break;
          if (retryOptions.backoffMs > 0) {
            await sleep(retryOptions.backoffMs);
          }
        }
      }
      throw lastError instanceof Error ? lastError : new Error(String(lastError));
    },
    generateObjectWithRetry: async (
      prompt: unknown,
      options?: unknown,
      retry?: unknown,
    ) => {
      if (typeof prompt !== "string" || !prompt.trim()) {
        throw new Error(
          "LLM.generateObjectWithRetry requires a non-empty prompt string",
        );
      }
      const retryOptions = parseRetryOptions(retry);
      let lastError: unknown;
      for (let attempt = 1; attempt <= retryOptions.maxAttempts; attempt++) {
        try {
          return await llmBinding.generateObject(prompt, options);
        } catch (error) {
          lastError = error;
          if (attempt >= retryOptions.maxAttempts) break;
          if (retryOptions.backoffMs > 0) {
            await sleep(retryOptions.backoffMs);
          }
        }
      }
      throw lastError instanceof Error ? lastError : new Error(String(lastError));
    },
  };

  const datagenBinding = {
    emitWarning: (kind: unknown, message: unknown) => {
      if (typeof kind !== "string" || !kind.trim()) return;
      warnings.push({
        kind: kind.trim(),
        message: typeof message === "string" ? message : String(message),
      });
    },
    emitMetric: (name: unknown, value: unknown) => {
      if (typeof name !== "string" || !name.trim()) {
        throw new Error("Datagen.emitMetric requires a non-empty metric name");
      }
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new Error(
          `Datagen.emitMetric requires a finite numeric value for '${name}'`,
        );
      }
      metrics.push({ name: name.trim(), value });
    },
    emitNote: (kind: unknown, value: unknown) => {
      if (typeof kind !== "string" || !kind.trim()) {
        throw new Error("Datagen.emitNote requires a non-empty note kind");
      }
      assertSerializable(value, "Datagen.emitNote value");
      notes.push({ kind: kind.trim(), value });
    },
    emitDebug: (label: unknown, value: unknown) => {
      if (typeof label !== "string" || !label.trim()) {
        throw new Error("Datagen.emitDebug requires a non-empty debug label");
      }
      assertSerializable(value, "Datagen.emitDebug value");
      debugEntries.push({ label: label.trim(), value });
    },
    stageInput: () => input.context.stageInput,
    initialContext: () => input.context.initialContext,
    output: (stageId: unknown) => {
      if (typeof stageId !== "string" || !stageId.trim()) return undefined;
      return input.context.outputsByStage[stageId.trim()];
    },
    outputs: () => input.context.outputsByStage,
    stageInfo: () => ({
      id: input.stageIdentifier,
      index: input.stageIndex,
      workflowPath: input.workflowPath,
      scriptPath: resolvedFilePath ?? null,
      runtime: structuredClone(runtime),
    }),
    assert: (condition: unknown, message: unknown) => {
      if (!condition) {
        throw new Error(
          typeof message === "string" && message.trim()
            ? message
            : "Datagen.assert failed",
        );
      }
      return true;
    },
    fail: (message: unknown) => {
      throw new Error(
        typeof message === "string" && message.trim()
          ? message
          : "Datagen.fail invoked",
      );
    },
    require: (value: unknown, message?: unknown) => {
      if (value === null || value === undefined) {
        throw new Error(
          typeof message === "string" && message.trim()
            ? message
            : "Required value is missing",
        );
      }
      return value;
    },
    requirePath: (root: unknown, path: unknown, message?: unknown) => {
      if (typeof path !== "string" || !path.trim()) {
        throw new Error("Datagen.requirePath requires a non-empty path string");
      }
      try {
        return getValueAtPath(root, path);
      } catch (error) {
        throw new Error(
          typeof message === "string" && message.trim()
            ? message
            : `Required path is missing: ${path} (${toErrorMessage(error)})`,
        );
      }
    },
    requireType: (value: unknown, expectedType: unknown, message?: unknown) => {
      if (typeof expectedType !== "string" || !expectedType.trim()) {
        throw new Error(
          "Datagen.requireType requires an expected type string",
        );
      }
      return requireType(
        value,
        expectedType.trim(),
        typeof message === "string" ? message : undefined,
      );
    },
    get: (
      root: unknown,
      path: unknown,
      defaultValue?: unknown,
    ) => {
      if (typeof path !== "string" || !path.trim()) return defaultValue;
      try {
        return getValueAtPath(root, path);
      } catch {
        return defaultValue;
      }
    },
    getOrThrow: (
      root: unknown,
      path: unknown,
      message?: unknown,
    ) => {
      if (typeof path !== "string" || !path.trim()) {
        throw new Error("Datagen.getOrThrow requires a non-empty path string");
      }
      try {
        return getValueAtPath(root, path);
      } catch (error) {
        throw new Error(
          typeof message === "string" && message.trim()
            ? message
            : `Path '${path}' is missing: ${toErrorMessage(error)}`,
        );
      }
    },
    has: (
      root: unknown,
      path: unknown,
    ) => {
      if (typeof path !== "string" || !path.trim()) return false;
      try {
        getValueAtPath(root, path);
        return true;
      } catch {
        return false;
      }
    },
    set: (
      root: unknown,
      path: unknown,
      value: unknown,
    ) => {
      if (typeof path !== "string" || !path.trim()) {
        throw new Error("Datagen.set requires a non-empty path string");
      }
      return setValueAtPathClone(root, path, value);
    },
    setOrCreate: (
      root: unknown,
      path: unknown,
      value: unknown,
    ) => {
      if (typeof path !== "string" || !path.trim()) {
        throw new Error(
          "Datagen.setOrCreate requires a non-empty path string",
        );
      }
      return setValueAtPathCreate(root, path, value);
    },
    delete: (
      root: unknown,
      path: unknown,
    ) => {
      if (typeof path !== "string" || !path.trim()) {
        throw new Error("Datagen.delete requires a non-empty path string");
      }
      return deleteValueAtPath(root, path);
    },
    merge: (a: unknown, b: unknown) => deepMerge(a, b),
    pick: (root: unknown, paths: unknown) => pickPaths(root, paths),
    omit: (root: unknown, paths: unknown) => omitPaths(root, paths),
    clone: (value: unknown) => structuredClone(value),
    map: (list: unknown, fn: unknown) => {
      if (typeof fn !== "function") {
        throw new Error("Datagen.map requires a callback");
      }
      return coerceList(list, "Datagen.map").map((item, index, values) =>
        fn(item, index + 1, values)
      );
    },
    filter: (list: unknown, fn: unknown) => {
      if (typeof fn !== "function") {
        throw new Error("Datagen.filter requires a callback");
      }
      return coerceList(list, "Datagen.filter").filter((item, index, values) =>
        Boolean(fn(item, index + 1, values))
      );
    },
    reduce: (list: unknown, initial: unknown, fn: unknown) => {
      if (typeof fn !== "function") {
        throw new Error("Datagen.reduce requires a callback");
      }
      return coerceList(list, "Datagen.reduce").reduce(
        (acc, item, index, values) => fn(acc, item, index + 1, values),
        initial,
      );
    },
    find: (list: unknown, fn: unknown) => {
      if (typeof fn !== "function") {
        throw new Error("Datagen.find requires a callback");
      }
      return coerceList(list, "Datagen.find").find((item, index, values) =>
        Boolean(fn(item, index + 1, values))
      );
    },
    flatMap: (list: unknown, fn: unknown) => {
      if (typeof fn !== "function") {
        throw new Error("Datagen.flatMap requires a callback");
      }
      const values = coerceList(list, "Datagen.flatMap");
      const out: unknown[] = [];
      for (const [index, item] of values.entries()) {
        const mapped = fn(item, index + 1, values);
        if (Array.isArray(mapped)) {
          out.push(...mapped);
        } else if (mapped !== null && mapped !== undefined) {
          out.push(mapped);
        }
      }
      return out;
    },
    groupBy: (list: unknown, keyOrFn: unknown) => {
      const values = coerceList(list, "Datagen.groupBy");
      const out: Record<string, unknown[]> = {};
      values.forEach((item, index) => {
        const key = String(
          resolveKeyOrFn(item, keyOrFn, "Datagen.groupBy", index, values),
        );
        out[key] ??= [];
        out[key].push(item);
      });
      return out;
    },
    indexBy: (list: unknown, keyOrFn: unknown) => {
      const values = coerceList(list, "Datagen.indexBy");
      const out: Record<string, unknown> = {};
      values.forEach((item, index) => {
        const key = String(
          resolveKeyOrFn(item, keyOrFn, "Datagen.indexBy", index, values),
        );
        out[key] = item;
      });
      return out;
    },
    pluck: (list: unknown, path: unknown) => {
      if (typeof path !== "string" || !path.trim()) {
        throw new Error("Datagen.pluck requires a non-empty path string");
      }
      return coerceList(list, "Datagen.pluck").map((item) => {
        try {
          return getValueAtPath(item, path);
        } catch {
          return null;
        }
      });
    },
    unique: (list: unknown) => {
      const out: unknown[] = [];
      const seen = new Set<string>();
      for (const item of coerceList(list, "Datagen.unique")) {
        const key = JSON.stringify(item);
        if (seen.has(key)) continue;
        seen.add(key);
        out.push(item);
      }
      return out;
    },
    compact: (list: unknown) =>
      coerceList(list, "Datagen.compact").filter((item) =>
        item !== null && item !== undefined
      ),
    countBy: (list: unknown, keyOrFn: unknown) => {
      const values = coerceList(list, "Datagen.countBy");
      const out: Record<string, number> = {};
      values.forEach((item, index) => {
        const key = String(
          resolveKeyOrFn(item, keyOrFn, "Datagen.countBy", index, values),
        );
        out[key] = (out[key] ?? 0) + 1;
      });
      return out;
    },
    toJson: (value: unknown) => JSON.stringify(value),
    prettyJson: (value: unknown) => JSON.stringify(value, null, 2),
    toJsonl: (list: unknown) =>
      coerceList(list, "Datagen.toJsonl").map((item) => JSON.stringify(item))
        .join("\n"),
    fromJson: (text: unknown, defaultValue?: unknown) => {
      if (typeof text !== "string") return defaultValue;
      try {
        return JSON.parse(text);
      } catch {
        return defaultValue;
      }
    },
    fromJsonl: (text: unknown) => {
      if (typeof text !== "string") {
        throw new Error("Datagen.fromJsonl requires a string");
      }
      return text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean).map(
        (line) => JSON.parse(line),
      );
    },
    trim: (value: unknown) => String(value ?? "").trim(),
    lower: (value: unknown) => String(value ?? "").toLowerCase(),
    upper: (value: unknown) => String(value ?? "").toUpperCase(),
    slug: (value: unknown) => toSlug(String(value ?? "")),
    isBlank: (value: unknown) => String(value ?? "").trim().length === 0,
    normalizeWhitespace: (value: unknown) => normalizeWhitespace(value),
    split: (value: unknown, separator?: unknown) =>
      String(value ?? "").split(
        separator === undefined ? /\s+/ : String(separator),
      ),
    join: (list: unknown, separator?: unknown) =>
      coerceList(list, "Datagen.join").map((item) => String(item)).join(
        separator === undefined ? "" : String(separator),
      ),
    startsWith: (value: unknown, prefix: unknown) =>
      String(value ?? "").startsWith(String(prefix ?? "")),
    endsWith: (value: unknown, suffix: unknown) =>
      String(value ?? "").endsWith(String(suffix ?? "")),
    contains: (value: unknown, needle: unknown) =>
      String(value ?? "").includes(String(needle ?? "")),
    truncate: (value: unknown, maxLen: unknown, suffix?: unknown) =>
      truncateText(
        value,
        typeof maxLen === "number" ? maxLen : Number(maxLen),
        suffix === undefined ? "..." : String(suffix),
      ),
    textTemplate: (
      template: unknown,
      args?: unknown,
      ...positional: unknown[]
    ) => {
      const templateValue = String(template ?? "");
      const argsValue = isRecord(args) ? args : {};
      return renderTextTemplate(templateValue, argsValue, positional);
    },
    bullets: (list: unknown) =>
      coerceList(list, "Datagen.bullets").map((item) => `- ${String(item)}`).join(
        "\n",
      ),
    numbered: (list: unknown) =>
      coerceList(list, "Datagen.numbered").map((item, index) =>
        `${index + 1}. ${String(item)}`
      ).join("\n"),
    codeFence: (text: unknown, lang?: unknown) => {
      const language = lang === undefined ? "" : String(lang);
      return `\`\`\`${language}\n${String(text ?? "")}\n\`\`\``;
    },
    prompt: (parts: unknown) =>
      coerceList(parts, "Datagen.prompt").map((item) => String(item ?? "").trim())
        .filter((item) => item.length > 0).join("\n\n"),
    LLM: llmBinding,
  };

  const datagenGlobals = {
    LLM: llmBinding,
    Datagen: datagenBinding,
  };

  const bridge = await createLuaBridge(
    datagenGlobals,
    {
      functionTimeout: runtime.functionTimeoutMs,
      openStandardLibs: runtime.openStandardLibs,
      injectObjects: runtime.injectObjects,
      enableProxy: runtime.enableProxy,
      traceAllocations: runtime.traceAllocations,
    },
  );

  try {
    bridge.setGlobal(LUA_STAGE_CONTEXT_GLOBAL, input.context);
    const wrappedScript = `
local __datagen_async_llm = LLM
LLM = {
  generate = function(prompt, options)
    return __datagen_async_llm.generate(prompt, options):await()
  end,
  generateObject = function(prompt, options)
    return __datagen_async_llm.generateObject(prompt, options):await()
  end,
  generateJson = function(prompt, options)
    return __datagen_async_llm.generateJson(prompt, options):await()
  end,
  generateMany = function(prompts, options)
    return __datagen_async_llm.generateMany(prompts, options):await()
  end,
  withRetry = function(prompt, options, retry)
    return __datagen_async_llm.withRetry(prompt, options, retry):await()
  end,
  generateObjectWithRetry = function(prompt, options, retry)
    return __datagen_async_llm.generateObjectWithRetry(prompt, options, retry):await()
  end
}
Datagen.LLM = LLM

local function __datagen_stage_main(...)
${scriptText}
end
return __datagen_stage_main(${LUA_STAGE_CONTEXT_GLOBAL})
`;
    const output = await bridge.execute(wrappedScript);

    const serializableIssue = validateSerializable(output);
    if (serializableIssue) {
      return {
        ok: false,
        errorKind: "lua_invalid_output",
        message:
          `Lua stage output must be JSON-serializable: ${serializableIssue}`,
        warnings,
        metrics,
        notes,
        debugEntries,
        scriptText,
        resolvedFilePath,
      };
    }

    try {
      JSON.stringify(output);
    } catch (error) {
      return {
        ok: false,
        errorKind: "lua_invalid_output",
        message: `Lua stage output must be JSON-serializable: ${
          toErrorMessage(error)
        }`,
        cause: error,
        warnings,
        metrics,
        notes,
        debugEntries,
        scriptText,
        resolvedFilePath,
      };
    }

    return {
      ok: true,
      output,
      warnings,
      metrics,
      notes,
      debugEntries,
      scriptText,
      resolvedFilePath,
    };
  } catch (error) {
    return {
      ok: false,
      errorKind: "lua_execution_failed",
      message: `Lua stage execution failed: ${toErrorMessage(error)}`,
      cause: error,
      warnings,
      metrics,
      notes,
      debugEntries,
      scriptText,
      resolvedFilePath,
    };
  } finally {
    try {
      bridge.setGlobal(LUA_STAGE_CONTEXT_GLOBAL, undefined);
    } catch {
      // ignore cleanup failures and close runtime below
    }
    bridge.close();
  }
}
