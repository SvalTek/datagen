import { z, type ZodTypeAny } from "zod";

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

const ConstrainNodeSchema: ZodTypeAny = z.lazy(() =>
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
      items: ConstrainNodeSchema,
      minItems: z.number().int().nonnegative().optional(),
      maxItems: z.number().int().nonnegative().optional(),
    }).strict(),
    ConstrainNodeBaseSchema.extend({
      type: z.literal("object"),
      shape: z.record(z.string().min(1), ConstrainNodeSchema),
      additionalProperties: z.boolean().optional(),
    }).strict(),
  ])
);

type ConstrainSchemaNode = {
  type: "string";
  minLength?: number;
  maxLength?: number;
  pattern?: string;
  optional?: boolean;
  nullable?: boolean;
} | {
  type: "number";
  min?: number;
  max?: number;
  int?: boolean;
  optional?: boolean;
  nullable?: boolean;
} | {
  type: "boolean";
  optional?: boolean;
  nullable?: boolean;
} | {
  type: "literal";
  value: string | number | boolean | null;
  optional?: boolean;
  nullable?: boolean;
} | {
  type: "enum";
  values: Array<string | number | boolean | null>;
  optional?: boolean;
  nullable?: boolean;
} | {
  type: "array";
  items: ConstrainSchemaNode;
  minItems?: number;
  maxItems?: number;
  optional?: boolean;
  nullable?: boolean;
} | {
  type: "object";
  shape: Record<string, ConstrainSchemaNode>;
  additionalProperties?: boolean;
  optional?: boolean;
  nullable?: boolean;
};

function formatPath(path: Array<string | number | symbol>): string {
  if (path.length === 0) return "root";
  return path.map((part) => String(part)).join(".");
}

function applyQualifiers(
  schema: ZodTypeAny,
  node: { optional?: boolean; nullable?: boolean },
): ZodTypeAny {
  let next = schema;
  if (node.nullable) next = next.nullable();
  if (node.optional) next = next.optional();
  return next;
}

function nodeToZod(
  node: ConstrainSchemaNode,
  path: Array<string | number>,
): ZodTypeAny {
  const location = formatPath(path);

  if (node.type === "string") {
    let schema = z.string();
    if (node.minLength !== undefined) schema = schema.min(node.minLength);
    if (node.maxLength !== undefined) schema = schema.max(node.maxLength);
    if (node.pattern !== undefined) {
      try {
        schema = schema.regex(new RegExp(node.pattern));
      } catch (error) {
        throw new Error(
          `Invalid constrain declaration at ${location}: invalid regex pattern`,
          { cause: error },
        );
      }
    }
    return applyQualifiers(schema, node);
  }

  if (node.type === "number") {
    let schema = z.number();
    if (node.int) schema = schema.int();
    if (node.min !== undefined) schema = schema.min(node.min);
    if (node.max !== undefined) schema = schema.max(node.max);
    return applyQualifiers(schema, node);
  }

  if (node.type === "boolean") {
    return applyQualifiers(z.boolean(), node);
  }

  if (node.type === "literal") {
    return applyQualifiers(z.literal(node.value), node);
  }

  if (node.type === "enum") {
    const values = node.values;
    const allStrings = values.every((value) => typeof value === "string");
    if (allStrings) {
      const stringValues = values as [string, ...string[]];
      return applyQualifiers(z.enum(stringValues), node);
    }
    const literalSchemas = values.map((value) => z.literal(value));
    const unionSchema = literalSchemas.length === 1
      ? literalSchemas[0]
      : z.union(
        literalSchemas as unknown as [ZodTypeAny, ZodTypeAny, ...ZodTypeAny[]],
      );
    return applyQualifiers(unionSchema, node);
  }

  if (node.type === "array") {
    let schema = z.array(nodeToZod(node.items, [...path, "items"]));
    if (node.minItems !== undefined) schema = schema.min(node.minItems);
    if (node.maxItems !== undefined) schema = schema.max(node.maxItems);
    return applyQualifiers(schema, node);
  }

  const shape: Record<string, ZodTypeAny> = {};
  for (const [key, value] of Object.entries(node.shape)) {
    shape[key] = nodeToZod(value, [...path, key]);
  }
  let objectSchema = z.object(shape);
  if (node.additionalProperties === false) {
    objectSchema = objectSchema.strict();
  } else if (node.additionalProperties === true) {
    objectSchema = objectSchema.passthrough();
  }
  return applyQualifiers(objectSchema, node);
}

export function constrainToZodSchema(constrain: unknown): ZodTypeAny {
  const parsed = (ConstrainNodeSchema as ZodTypeAny).safeParse(constrain);
  if (!parsed.success) {
    const message = parsed.error.issues
      .map((issue) => `${formatPath(issue.path)}: ${issue.message}`)
      .join("; ");
    throw new Error(
      `Invalid constrain declaration: ${message || "expected typed schema node"}`,
      { cause: parsed.error },
    );
  }

  return nodeToZod(parsed.data as ConstrainSchemaNode, []);
}
