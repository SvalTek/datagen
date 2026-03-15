export type ScopedStringFailureKind =
  | "invalid_scope_pattern"
  | "scope_no_match"
  | "scope_empty";

export type ScopedStringResult =
  | { ok: true; value: string }
  | { ok: false; kind: ScopedStringFailureKind; message: string };

export interface ValidationScopedPattern {
  pattern: string;
  flags?: string;
}

export interface ValidationScopeInput {
  includePattern?: ValidationScopedPattern;
  excludePatterns?: ValidationScopedPattern[];
}

function compilePattern(
  scopedPattern: ValidationScopedPattern,
): ScopedStringResult | RegExp {
  try {
    const flags = scopedPattern.flags ?? "";
    const globalFlags = flags.includes("g") ? flags : `${flags}g`;
    return new RegExp(scopedPattern.pattern, globalFlags);
  } catch (error) {
    return {
      ok: false,
      kind: "invalid_scope_pattern",
      message: error instanceof Error ? error.message : String(error),
    };
  }
}

function collapseWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

export function applyValidationScope(
  value: string,
  scope: ValidationScopeInput,
): ScopedStringResult {
  let scopedValue = value;

  if (scope.includePattern) {
    const pattern = compilePattern(scope.includePattern);
    if (!(pattern instanceof RegExp)) {
      return pattern;
    }

    const matches = [...scopedValue.matchAll(pattern)].map((match) => match[0]).filter(
      Boolean,
    );
    if (matches.length === 0) {
      return {
        ok: false,
        kind: "scope_no_match",
        message: "Scope includePattern did not match any content",
      };
    }

    scopedValue = matches.join(" ");
  }

  for (const excludePattern of scope.excludePatterns ?? []) {
    const pattern = compilePattern(excludePattern);
    if (!(pattern instanceof RegExp)) {
      return pattern;
    }

    scopedValue = scopedValue.replace(pattern, " ");
  }

  scopedValue = collapseWhitespace(scopedValue);
  if (!scopedValue) {
    return {
      ok: false,
      kind: "scope_empty",
      message: "Scoped comparison content is empty after applying scope patterns",
    };
  }

  return {
    ok: true,
    value: scopedValue,
  };
}
