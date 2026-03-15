import { parse } from "jsr:@std/yaml";
import {
  PipelineDocumentSchema,
  type PipelineDocumentInput,
} from "../structures/TaskSchema.ts";

export class PipelineParseError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "PipelineParseError";
  }
}

export class PipelineValidationError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "PipelineValidationError";
  }
}

export function parsePipelineYaml(yamlText: string): PipelineDocumentInput {
  let parsed: unknown;

  try {
    parsed = parse(yamlText);
  } catch (error) {
    throw new PipelineParseError("Failed to parse pipeline YAML", { cause: error });
  }

  const validated = PipelineDocumentSchema.safeParse(parsed);
  if (!validated.success) {
    throw new PipelineValidationError("Pipeline YAML failed schema validation", {
      cause: validated.error,
    });
  }

  return validated.data;
}

export async function loadPipelineFromFile(
  filePath: string,
): Promise<PipelineDocumentInput> {
  const yamlText = await Deno.readTextFile(filePath);
  return parsePipelineYaml(yamlText);
}

