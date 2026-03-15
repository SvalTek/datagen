import type { PipelineInputConfig } from "../structures/TaskSchema.ts";
import {
  InputDatasetRemapError,
  remapInputDataset,
} from "./InputDatasetRemapper.ts";

export class InputDatasetParseError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "InputDatasetParseError";
  }
}

export class InputDatasetValidationError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "InputDatasetValidationError";
  }
}

export { InputDatasetRemapError } from "./InputDatasetRemapper.ts";

export function inferInputFormat(
  filePath: string,
): "json" | "jsonl" {
  const normalized = filePath.toLowerCase();
  if (normalized.endsWith(".jsonl")) return "jsonl";
  if (normalized.endsWith(".json")) return "json";
  throw new InputDatasetValidationError(
    `Could not infer input format from path: ${filePath}`,
  );
}

async function* readNonEmptyLines(path: string): AsyncGenerator<{ line: string; lineNo: number }> {
  const file = await Deno.open(path, { read: true });
  const decoder = new TextDecoder();
  const buffer = new Uint8Array(64 * 1024);
  let carry = "";
  let lineNo = 0;

  try {
    while (true) {
      const readCount = await file.read(buffer);
      if (readCount === null) {
        break;
      }
      carry += decoder.decode(buffer.subarray(0, readCount), { stream: true });
      let newlineIndex = carry.indexOf("\n");
      while (newlineIndex >= 0) {
        const rawLine = carry.slice(0, newlineIndex).replace(/\r$/, "");
        carry = carry.slice(newlineIndex + 1);
        lineNo++;
        const line = rawLine.trim();
        if (line.length > 0) {
          yield { line, lineNo };
        }
        newlineIndex = carry.indexOf("\n");
      }
    }

    carry += decoder.decode();
    const tail = carry.replace(/\r$/, "").trim();
    if (tail.length > 0) {
      lineNo++;
      yield { line: tail, lineNo };
    }
  } finally {
    file.close();
  }
}

function parseJsonArray(text: string, filePath: string): unknown[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new InputDatasetParseError(
      `Failed to parse JSON input file: ${filePath}`,
      { cause: error },
    );
  }

  if (!Array.isArray(parsed)) {
    throw new InputDatasetValidationError(
      `JSON input file must contain a top-level array: ${filePath}`,
    );
  }

  return parsed;
}

function applyRecordSlice(
  records: unknown[],
  input: PipelineInputConfig,
): unknown[] {
  const offset = input.offset ?? 0;
  const limit = input.limit;

  if (limit === undefined) {
    return records.slice(offset);
  }

  return records.slice(offset, offset + limit);
}

function parseJsonl(
  text: string,
  filePath: string,
  input: PipelineInputConfig,
): unknown[] {
  const records: unknown[] = [];
  const lines = text.split(/\r?\n/);
  const offset = input.offset ?? 0;
  const limit = input.limit;
  let seenRecords = 0;

  for (let index = 0; index < lines.length; index++) {
    const line = lines[index].trim();
    if (!line) continue;

    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch (error) {
      throw new InputDatasetParseError(
        `Failed to parse JSONL input file ${filePath} at line ${index + 1}`,
        { cause: error },
      );
    }

    if (seenRecords >= offset) {
      if (limit === undefined || records.length < limit) {
        records.push(parsed);
      }
    }

    seenRecords++;
    if (limit !== undefined && records.length >= limit) {
      break;
    }
  }

  return records;
}

async function parseJsonlStreamed(
  filePath: string,
  input: PipelineInputConfig,
): Promise<unknown[]> {
  const records: unknown[] = [];
  const offset = input.offset ?? 0;
  const limit = input.limit;
  let seenRecords = 0;

  for await (const { line, lineNo } of readNonEmptyLines(filePath)) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch (error) {
      throw new InputDatasetParseError(
        `Failed to parse JSONL input file ${filePath} at line ${lineNo}`,
        { cause: error },
      );
    }

    if (seenRecords >= offset) {
      if (limit === undefined || records.length < limit) {
        records.push(parsed);
      }
    }

    seenRecords++;
    if (limit !== undefined && records.length >= limit) {
      break;
    }
  }

  return records;
}

export async function* streamInputDataset(
  input: PipelineInputConfig,
): AsyncGenerator<unknown> {
  const format = input.format ?? inferInputFormat(input.path);
  if (format !== "jsonl") {
    throw new InputDatasetValidationError(
      `readMode=stream is currently only supported for jsonl input: ${input.path}`,
    );
  }

  const offset = input.offset ?? 0;
  const limit = input.limit;
  let seenRecords = 0;
  let yieldedRecords = 0;

  for await (const { line, lineNo } of readNonEmptyLines(input.path)) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch (error) {
      throw new InputDatasetParseError(
        `Failed to parse JSONL input file ${input.path} at line ${lineNo}`,
        { cause: error },
      );
    }

    if (seenRecords >= offset) {
      const record = input.remap ? remapInputDataset([parsed], input.remap)[0] : parsed;
      yield record;
      yieldedRecords++;
      if (limit !== undefined && yieldedRecords >= limit) {
        break;
      }
    }

    seenRecords++;
  }
}

export async function loadInputDataset(
  input: PipelineInputConfig,
): Promise<{
  format: "json" | "jsonl";
  records: unknown[];
}> {
  const format = input.format ?? inferInputFormat(input.path);
  const records = await (() => {
    if (format === "json") {
      return Deno.readTextFile(input.path).then((fileText) =>
        applyRecordSlice(parseJsonArray(fileText, input.path), input)
      );
    }
    if (input.readMode === "stream") {
      return parseJsonlStreamed(input.path, input);
    }
    return Deno.readTextFile(input.path).then((fileText) =>
      parseJsonl(fileText, input.path, input)
    );
  })();

  if (!input.remap) {
    return {
      format,
      records,
    };
  }

  try {
    return {
      format,
      records: remapInputDataset(records, input.remap),
    };
  } catch (error) {
    if (error instanceof InputDatasetRemapError) {
      throw error;
    }
    throw new InputDatasetRemapError(
      "Failed to remap input dataset records",
      { cause: error },
    );
  }
}
