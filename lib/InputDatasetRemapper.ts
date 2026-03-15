import type {
  AlpacaRemap,
  InputRemap,
  PrefixedStringArrayRemap,
} from "../structures/TaskSchema.ts";
import { getValueAtPath } from "./ObjectPath.ts";

export class InputDatasetRemapError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "InputDatasetRemapError";
  }
}

function splitPath(path: string): string[] {
  return path.split(".").map((segment) => segment.trim()).filter(Boolean);
}

function setValueAtPathCloneOrCreate<T>(
  root: T,
  path: string,
  value: unknown,
): T {
  const clone = structuredClone(root);
  const parts = splitPath(path);

  if (parts.length === 0) {
    return value as T;
  }

  let current: unknown = clone;
  for (let index = 0; index < parts.length - 1; index++) {
    const part = parts[index];
    if (!current || typeof current !== "object" || Array.isArray(current)) {
      throw new Error(`Path '${path}' is not traversable at '${part}'`);
    }

    const currentObject = current as Record<string, unknown>;
    const next = currentObject[part];
    if (next === undefined) {
      currentObject[part] = {};
      current = currentObject[part];
      continue;
    }

    if (!next || typeof next !== "object" || Array.isArray(next)) {
      throw new Error(`Path '${path}' is not traversable at '${part}'`);
    }

    current = next;
  }

  const lastPart = parts[parts.length - 1];
  if (!current || typeof current !== "object" || Array.isArray(current)) {
    throw new Error(`Path '${path}' is not traversable at '${lastPart}'`);
  }

  (current as Record<string, unknown>)[lastPart] = value;
  return clone;
}

function formatRecordError(recordIndex: number, message: string): InputDatasetRemapError {
  return new InputDatasetRemapError(`Record ${recordIndex} ${message}`);
}

function mapPrefixedRole(prefixes: PrefixedStringArrayRemap["prefixes"]): Array<{
  sourcePrefix: string;
  outputRole: string;
}> {
  const mappings = [
    { sourcePrefix: prefixes.user, outputRole: "user" },
    { sourcePrefix: prefixes.assistant, outputRole: "assistant" },
  ];

  if (prefixes.system) {
    mappings.push({ sourcePrefix: prefixes.system, outputRole: "system" });
  }

  return mappings;
}

function remapPrefixedStringArrayRecord(
  record: unknown,
  remap: PrefixedStringArrayRemap,
  recordIndex: number,
): unknown {
  if (!record || typeof record !== "object" || Array.isArray(record)) {
    throw formatRecordError(recordIndex, "is not an object");
  }

  let sourceValue: unknown;
  try {
    sourceValue = getValueAtPath(record, remap.sourcePath);
  } catch (error) {
    throw formatRecordError(
      recordIndex,
      `path '${remap.sourcePath}' could not be resolved: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  if (!Array.isArray(sourceValue)) {
    throw formatRecordError(
      recordIndex,
      `path '${remap.sourcePath}' must resolve to an array`,
    );
  }

  const roleField = remap.roleField ?? "from";
  const contentField = remap.contentField ?? "value";
  const outputPath = remap.outputPath ?? "conversations";
  const trimContent = remap.trimContent ?? true;
  const mappings = mapPrefixedRole(remap.prefixes);

  const conversations = sourceValue.map((item, itemIndex) => {
    if (typeof item !== "string") {
      throw formatRecordError(
        recordIndex,
        `conversation item ${itemIndex} is not a string`,
      );
    }

    const trimmedStart = item.trimStart();
    const mapping = mappings.find(({ sourcePrefix }) =>
      trimmedStart.startsWith(sourcePrefix)
    );
    if (!mapping) {
      throw formatRecordError(
        recordIndex,
        `conversation item ${itemIndex} does not match any configured prefix`,
      );
    }

    let content = trimmedStart.slice(mapping.sourcePrefix.length);
    if (trimContent) {
      content = content.trim();
    }

    return {
      [roleField]: mapping.outputRole,
      [contentField]: content,
    };
  });

  try {
    return setValueAtPathCloneOrCreate(record, outputPath, conversations);
  } catch (error) {
    throw formatRecordError(
      recordIndex,
      `output path '${outputPath}' could not be written: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

function getRequiredStringField(
  record: Record<string, unknown>,
  recordIndex: number,
  fieldName: string,
  allowEmpty: boolean,
): string {
  const value = record[fieldName];
  if (value === undefined) {
    throw formatRecordError(recordIndex, `is missing required Alpaca field '${fieldName}'`);
  }
  if (typeof value !== "string") {
    throw formatRecordError(recordIndex, `field '${fieldName}' must be a string`);
  }
  if (!allowEmpty && value.trim().length === 0) {
    throw formatRecordError(recordIndex, `field '${fieldName}' must not be empty`);
  }
  return value;
}

function remapAlpacaRecord(
  record: unknown,
  remap: AlpacaRemap,
  recordIndex: number,
): unknown {
  if (!record || typeof record !== "object" || Array.isArray(record)) {
    throw formatRecordError(recordIndex, "is not an object");
  }

  const recordObject = record as Record<string, unknown>;
  const instructionField = remap.instructionField ?? "instruction";
  const inputField = remap.inputField ?? "input";
  const outputField = remap.outputField ?? "output";
  const outputPath = remap.outputPath ?? "conversations";
  const roleField = remap.roleField ?? "from";
  const contentField = remap.contentField ?? "value";

  const instruction = getRequiredStringField(
    recordObject,
    recordIndex,
    instructionField,
    false,
  );
  const output = getRequiredStringField(
    recordObject,
    recordIndex,
    outputField,
    false,
  );

  const inputValue = recordObject[inputField];
  if (inputValue !== undefined && typeof inputValue !== "string") {
    throw formatRecordError(recordIndex, `field '${inputField}' must be a string`);
  }

  const input = typeof inputValue === "string" ? inputValue : "";
  const hasInput = input.trim().length > 0;
  const conversations = hasInput
    ? [
      { [roleField]: "system", [contentField]: instruction },
      { [roleField]: "user", [contentField]: input },
      { [roleField]: "assistant", [contentField]: output },
    ]
    : [
      { [roleField]: "user", [contentField]: instruction },
      { [roleField]: "assistant", [contentField]: output },
    ];

  try {
    return setValueAtPathCloneOrCreate(record, outputPath, conversations);
  } catch (error) {
    throw formatRecordError(
      recordIndex,
      `output path '${outputPath}' could not be written: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

export function remapInputDataset(
  records: unknown[],
  remap: InputRemap,
): unknown[] {
  return records.map((record, index) => {
    const recordIndex = index + 1;

    if (remap.kind === "prefixed_string_array") {
      return remapPrefixedStringArrayRecord(record, remap, recordIndex);
    }

    return remapAlpacaRecord(record, remap, recordIndex);
  });
}
