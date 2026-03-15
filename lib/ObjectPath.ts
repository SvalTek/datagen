function splitPath(path: string): string[] {
  return path.split(".").map((segment) => segment.trim()).filter(Boolean);
}

export function getValueAtPath(root: unknown, path: string): unknown {
  const parts = splitPath(path);
  let current: unknown = root;

  for (const part of parts) {
    if (!current || typeof current !== "object" || Array.isArray(current)) {
      throw new Error(`Path '${path}' is not traversable at '${part}'`);
    }

    if (!(part in current)) {
      throw new Error(`Path '${path}' does not exist`);
    }

    current = (current as Record<string, unknown>)[part];
  }

  return current;
}

export function setValueAtPathClone<T>(
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
    if (!(part in current)) {
      throw new Error(`Path '${path}' does not exist`);
    }
    current = (current as Record<string, unknown>)[part];
  }

  const lastPart = parts[parts.length - 1];
  if (!current || typeof current !== "object" || Array.isArray(current)) {
    throw new Error(`Path '${path}' is not traversable at '${lastPart}'`);
  }
  if (!(lastPart in current)) {
    throw new Error(`Path '${path}' does not exist`);
  }

  (current as Record<string, unknown>)[lastPart] = value;
  return clone;
}
