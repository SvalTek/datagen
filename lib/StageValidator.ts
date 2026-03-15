import type {
  StageValidateInput,
  ValidationRuleInput,
} from "../structures/TaskSchema.ts";
import { getValueAtPath } from "./ObjectPath.ts";
import {
  calculateTextSimilarity,
  normalizeComparableText,
} from "./TextSimilarity.ts";
import { applyValidationScope } from "./ValidationScope.ts";

export type ValidationIssueKind =
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
  | "array_max_length"
  | "path_missing"
  | "ref_missing"
  | "invalid_target_type"
  | "invalid_regex_pattern"
  | "invalid_scope_pattern"
  | "scope_no_match"
  | "scope_empty";

export interface ValidationIssue {
  kind: ValidationIssueKind;
  path?: string;
  message: string;
  hint?: string;
  ruleName?: string;
  similarityScore?: number;
  similarityThreshold?: number;
  similarityDistance?: number;
  similarityStatus?: "close" | "far";
  similarityMode?: "fast" | "detailed";
}

export interface ValidationResult {
  success: boolean;
  issues: ValidationIssue[];
}

interface ValidationOptions {
  skipInapplicablePaths?: boolean;
  refs?: Record<string, unknown>;
}

function issueFromRule(
  rule: ValidationRuleInput,
  kind: ValidationIssueKind,
  message: string,
  path = rule.path,
): ValidationIssue {
  return {
    kind,
    path,
    message,
    hint: rule.hint,
    ruleName: rule.name,
  };
}

function resolveRuleTarget(
  value: unknown,
  rule: ValidationRuleInput,
  options: ValidationOptions,
): { applicable: boolean; target?: unknown; issues: ValidationIssue[] } {
  if (!rule.path) {
    return { applicable: true, target: value, issues: [] };
  }

  try {
    return {
      applicable: true,
      target: getValueAtPath(value, rule.path),
      issues: [],
    };
  } catch (error) {
    if (options.skipInapplicablePaths) {
      return { applicable: false, issues: [] };
    }

    return {
      applicable: true,
      issues: [{
        kind: "path_missing",
        path: rule.path,
        message: error instanceof Error ? error.message : String(error),
        hint: rule.hint,
        ruleName: rule.name,
      }],
    };
  }
}

function resolveComparisonPathTarget(
  value: unknown,
  otherPath: string,
  options: ValidationOptions,
): { applicable: boolean; target?: unknown; issues: ValidationIssue[] } {
  try {
    return {
      applicable: true,
      target: getValueAtPath(value, otherPath),
      issues: [],
    };
  } catch (error) {
    if (options.skipInapplicablePaths) {
      return { applicable: false, issues: [] };
    }

    return {
      applicable: true,
      issues: [{
        kind: "path_missing",
        path: otherPath,
        message: error instanceof Error ? error.message : String(error),
        ruleName: undefined,
      }],
    };
  }
}

function resolveReferenceTarget(
  ref: string,
  options: ValidationOptions,
): { applicable: boolean; target?: unknown; issues: ValidationIssue[] } {
  const [rootRef, ...pathParts] = ref.split(".").map((segment) => segment.trim()).filter(
    Boolean,
  );

  if (!rootRef) {
    if (options.skipInapplicablePaths) {
      return { applicable: false, issues: [] };
    }

    return {
      applicable: true,
      issues: [{
        kind: "ref_missing",
        message: "Validator reference must not be empty",
        hint: undefined,
        ruleName: undefined,
      }],
    };
  }

  const refs = options.refs ?? {};
  if (!(rootRef in refs) || refs[rootRef] === undefined) {
    if (options.skipInapplicablePaths) {
      return { applicable: false, issues: [] };
    }

    return {
      applicable: true,
      issues: [{
        kind: "ref_missing",
        path: ref,
        message: `Reference '${rootRef}' is not available in this validation context`,
        hint: undefined,
        ruleName: undefined,
      }],
    };
  }

  const baseValue = refs[rootRef];
  if (pathParts.length === 0) {
    return {
      applicable: true,
      target: baseValue,
      issues: [],
    };
  }

  const suffixPath = pathParts.join(".");
  try {
    return {
      applicable: true,
      target: getValueAtPath(baseValue, suffixPath),
      issues: [],
    };
  } catch (error) {
    if (options.skipInapplicablePaths) {
      return { applicable: false, issues: [] };
    }

    return {
      applicable: true,
      issues: [{
        kind: "path_missing",
        path: ref,
        message: error instanceof Error ? error.message : String(error),
        hint: undefined,
        ruleName: undefined,
      }],
    };
  }
}

function evaluateWhen(
  value: unknown,
  rule: ValidationRuleInput,
  options: ValidationOptions,
): { applicable: boolean; issues: ValidationIssue[] } {
  if (!rule.when) return { applicable: true, issues: [] };

  const whenPath = rule.when.path;
  let target: unknown = value;

  if (whenPath) {
    try {
      target = getValueAtPath(value, whenPath);
    } catch (error) {
      if (options.skipInapplicablePaths) {
        return { applicable: false, issues: [] };
      }
      return {
        applicable: true,
        issues: [{
          kind: "path_missing",
          path: whenPath,
          message: error instanceof Error ? error.message : String(error),
          hint: rule.hint,
          ruleName: rule.name,
        }],
      };
    }
  }

  if (rule.when.equals !== undefined) {
    return { applicable: target === rule.when.equals, issues: [] };
  }

  if (rule.when.notEquals !== undefined) {
    return { applicable: target !== rule.when.notEquals, issues: [] };
  }

  return { applicable: true, issues: [] };
}

function invalidType(rule: ValidationRuleInput, expected: string): ValidationIssue {
  return issueFromRule(
    rule,
    "invalid_target_type",
    `Validator '${rule.kind}' requires ${expected}`,
  );
}

function invalidComparisonType(
  rule: ValidationRuleInput,
  comparisonTarget: "path" | "ref",
): ValidationIssue {
  const comparisonDescriptor = comparisonTarget === "path"
    ? `string values at path '${rule.path ?? "<root>"}' and comparison path`
    : `string values at path '${rule.path ?? "<root>"}' and comparison ref`;

  return issueFromRule(
    rule,
    "invalid_target_type",
    `Validator '${rule.kind}' requires ${comparisonDescriptor}`,
  );
}

function issueFromSimilarityRule(
  rule: ValidationRuleInput,
  kind:
    | "max_similarity_to_path"
    | "min_similarity_to_path"
    | "max_similarity_to_ref"
    | "min_similarity_to_ref",
  message: string,
  similarityScore: number,
  similarityThreshold: number,
): ValidationIssue {
  const similarityDistance = Math.abs(similarityScore - similarityThreshold);
  const similarityMode = "similarity" in rule
    ? rule.similarity?.mode ?? "fast"
    : "fast";
  return {
    ...issueFromRule(rule, kind, message),
    similarityScore,
    similarityThreshold,
    similarityDistance,
    similarityStatus: similarityDistance <= 0.05 ? "close" : "far",
    similarityMode,
  };
}

function applyRuleScope(
  rule: ValidationRuleInput,
  value: string,
): { ok: true; value: string } | { ok: false; issues: ValidationIssue[] } {
  if (!("scope" in rule) || !rule.scope) {
    return { ok: true, value };
  }

  const scoped = applyValidationScope(value, rule.scope);
  if (scoped.ok) {
    return scoped;
  }

  return {
    ok: false,
    issues: [{
      kind: scoped.kind,
      path: rule.path,
      message: scoped.message,
      hint: rule.hint,
      ruleName: rule.name,
    }],
  };
}

function evaluateRule(
  value: unknown,
  rule: ValidationRuleInput,
  options: ValidationOptions,
): ValidationIssue[] {
  const whenResult = evaluateWhen(value, rule, options);
  if (whenResult.issues.length > 0) return whenResult.issues;
  if (!whenResult.applicable) return [];

  const targetResult = resolveRuleTarget(value, rule, options);
  if (targetResult.issues.length > 0) return targetResult.issues;
  if (!targetResult.applicable) return [];

  const target = targetResult.target;

  switch (rule.kind) {
    case "contains":
      if (typeof target !== "string") return [invalidType(rule, "a string target")];
      return target.includes(rule.value)
        ? []
        : [issueFromRule(
          rule,
          "contains",
          `Value does not contain required substring '${rule.value}'`,
        )];

    case "not_contains":
      if (typeof target !== "string") return [invalidType(rule, "a string target")];
      return target.includes(rule.value)
        ? [issueFromRule(
          rule,
          "not_contains",
          `Value contains forbidden substring '${rule.value}'`,
        )]
        : [];

    case "regex": {
      if (typeof target !== "string") return [invalidType(rule, "a string target")];
      let pattern: RegExp;
      try {
        pattern = new RegExp(rule.pattern, rule.flags);
      } catch (error) {
        return [issueFromRule(
          rule,
          "invalid_regex_pattern",
          error instanceof Error ? error.message : String(error),
        )];
      }
      return pattern.test(target)
        ? []
        : [issueFromRule(
          rule,
          "regex",
          `Value does not match regex '${rule.pattern}'`,
        )];
    }

    case "not_regex": {
      if (typeof target !== "string") return [invalidType(rule, "a string target")];
      let pattern: RegExp;
      try {
        pattern = new RegExp(rule.pattern, rule.flags);
      } catch (error) {
        return [issueFromRule(
          rule,
          "invalid_regex_pattern",
          error instanceof Error ? error.message : String(error),
        )];
      }
      return pattern.test(target)
        ? [issueFromRule(
          rule,
          "not_regex",
          `Value matches forbidden regex '${rule.pattern}'`,
        )]
        : [];
    }

    case "min_length":
      if (typeof target !== "string") return [invalidType(rule, "a string target")];
      return target.length >= rule.value
        ? []
        : [issueFromRule(
          rule,
          "min_length",
          `Value length ${target.length} is less than minimum ${rule.value}`,
        )];

    case "max_length":
      if (typeof target !== "string") return [invalidType(rule, "a string target")];
      return target.length <= rule.value
        ? []
        : [issueFromRule(
          rule,
          "max_length",
          `Value length ${target.length} exceeds maximum ${rule.value}`,
        )];

    case "equals":
      return target === rule.value
        ? []
        : [issueFromRule(rule, "equals", "Value is not equal to expected value")];

    case "not_equals":
      return target !== rule.value
        ? []
        : [issueFromRule(rule, "not_equals", "Value must not equal forbidden value")];

    case "must_change_from_path": {
      const otherTargetResult = resolveComparisonPathTarget(
        value,
        rule.otherPath,
        options,
      );
      if (otherTargetResult.issues.length > 0) return otherTargetResult.issues;
      if (!otherTargetResult.applicable) return [];
      if (typeof target !== "string" || typeof otherTargetResult.target !== "string") {
        return [invalidComparisonType(rule, "path")];
      }

      const scopedTargetResult = applyRuleScope(rule, target);
      if (!scopedTargetResult.ok) return scopedTargetResult.issues;
      const scopedOtherResult = applyRuleScope(rule, otherTargetResult.target);
      if (!scopedOtherResult.ok) return scopedOtherResult.issues;

      const normalizedTarget = normalizeComparableText(scopedTargetResult.value);
      const normalizedOther = normalizeComparableText(scopedOtherResult.value);
      return normalizedTarget !== normalizedOther
        ? []
        : [issueFromRule(
          rule,
          "must_change_from_path",
          `Value must differ from value at path '${rule.otherPath}' after scoped comparison`,
        )];
    }

    case "must_change_from_ref": {
      const refTargetResult = resolveReferenceTarget(rule.ref, options);
      if (refTargetResult.issues.length > 0) return refTargetResult.issues;
      if (!refTargetResult.applicable) return [];
      if (typeof target !== "string" || typeof refTargetResult.target !== "string") {
        return [invalidComparisonType(rule, "ref")];
      }

      const scopedTargetResult = applyRuleScope(rule, target);
      if (!scopedTargetResult.ok) return scopedTargetResult.issues;
      const scopedOtherResult = applyRuleScope(rule, refTargetResult.target);
      if (!scopedOtherResult.ok) return scopedOtherResult.issues;

      const normalizedTarget = normalizeComparableText(scopedTargetResult.value);
      const normalizedOther = normalizeComparableText(scopedOtherResult.value);
      return normalizedTarget !== normalizedOther
        ? []
        : [issueFromRule(
          rule,
          "must_change_from_ref",
          `Value must differ from ref '${rule.ref}' after scoped comparison`,
        )];
    }

    case "not_equal_to_path": {
      const otherTargetResult = resolveComparisonPathTarget(
        value,
        rule.otherPath,
        options,
      );
      if (otherTargetResult.issues.length > 0) return otherTargetResult.issues;
      if (!otherTargetResult.applicable) return [];
      if (typeof target !== "string" || typeof otherTargetResult.target !== "string") {
        return [invalidComparisonType(rule, "path")];
      }

      const scopedTargetResult = applyRuleScope(rule, target);
      if (!scopedTargetResult.ok) return scopedTargetResult.issues;
      const scopedOtherResult = applyRuleScope(rule, otherTargetResult.target);
      if (!scopedOtherResult.ok) return scopedOtherResult.issues;

      const normalizedTarget = normalizeComparableText(scopedTargetResult.value);
      const normalizedOther = normalizeComparableText(scopedOtherResult.value);
      return normalizedTarget !== normalizedOther
        ? []
        : [issueFromRule(
          rule,
          "not_equal_to_path",
          `Value must not equal value at path '${rule.otherPath}' after scoped comparison`,
        )];
    }

    case "max_similarity_to_path": {
      const otherTargetResult = resolveComparisonPathTarget(
        value,
        rule.otherPath,
        options,
      );
      if (otherTargetResult.issues.length > 0) return otherTargetResult.issues;
      if (!otherTargetResult.applicable) return [];
      if (typeof target !== "string" || typeof otherTargetResult.target !== "string") {
        return [invalidComparisonType(rule, "path")];
      }

      const scopedTargetResult = applyRuleScope(rule, target);
      if (!scopedTargetResult.ok) return scopedTargetResult.issues;
      const scopedOtherResult = applyRuleScope(rule, otherTargetResult.target);
      if (!scopedOtherResult.ok) return scopedOtherResult.issues;

      const similarity = calculateTextSimilarity(
        scopedTargetResult.value,
        scopedOtherResult.value,
        { mode: rule.similarity?.mode ?? "fast" },
      );
      return similarity.similarity <= rule.threshold
        ? []
        : [issueFromSimilarityRule(
          rule,
          "max_similarity_to_path",
          `Similarity ${similarity.similarity.toFixed(3)} exceeds maximum ${rule.threshold} when compared to path '${rule.otherPath}'`,
          similarity.similarity,
          rule.threshold,
        )];
    }

    case "min_similarity_to_path": {
      const otherTargetResult = resolveComparisonPathTarget(
        value,
        rule.otherPath,
        options,
      );
      if (otherTargetResult.issues.length > 0) return otherTargetResult.issues;
      if (!otherTargetResult.applicable) return [];
      if (typeof target !== "string" || typeof otherTargetResult.target !== "string") {
        return [invalidComparisonType(rule, "path")];
      }

      const scopedTargetResult = applyRuleScope(rule, target);
      if (!scopedTargetResult.ok) return scopedTargetResult.issues;
      const scopedOtherResult = applyRuleScope(rule, otherTargetResult.target);
      if (!scopedOtherResult.ok) return scopedOtherResult.issues;

      const similarity = calculateTextSimilarity(
        scopedTargetResult.value,
        scopedOtherResult.value,
        { mode: rule.similarity?.mode ?? "fast" },
      );
      return similarity.similarity >= rule.threshold
        ? []
        : [issueFromSimilarityRule(
          rule,
          "min_similarity_to_path",
          `Similarity ${similarity.similarity.toFixed(3)} is below minimum ${rule.threshold} when compared to path '${rule.otherPath}'`,
          similarity.similarity,
          rule.threshold,
        )];
    }

    case "not_equal_to_ref": {
      const refTargetResult = resolveReferenceTarget(rule.ref, options);
      if (refTargetResult.issues.length > 0) return refTargetResult.issues;
      if (!refTargetResult.applicable) return [];
      if (typeof target !== "string" || typeof refTargetResult.target !== "string") {
        return [invalidComparisonType(rule, "ref")];
      }

      const scopedTargetResult = applyRuleScope(rule, target);
      if (!scopedTargetResult.ok) return scopedTargetResult.issues;
      const scopedOtherResult = applyRuleScope(rule, refTargetResult.target);
      if (!scopedOtherResult.ok) return scopedOtherResult.issues;

      const normalizedTarget = normalizeComparableText(scopedTargetResult.value);
      const normalizedOther = normalizeComparableText(scopedOtherResult.value);
      return normalizedTarget !== normalizedOther
        ? []
        : [issueFromRule(
          rule,
          "not_equal_to_ref",
          `Value must not equal ref '${rule.ref}' after scoped comparison`,
        )];
    }

    case "max_similarity_to_ref": {
      const refTargetResult = resolveReferenceTarget(rule.ref, options);
      if (refTargetResult.issues.length > 0) return refTargetResult.issues;
      if (!refTargetResult.applicable) return [];
      if (typeof target !== "string" || typeof refTargetResult.target !== "string") {
        return [invalidComparisonType(rule, "ref")];
      }

      const scopedTargetResult = applyRuleScope(rule, target);
      if (!scopedTargetResult.ok) return scopedTargetResult.issues;
      const scopedOtherResult = applyRuleScope(rule, refTargetResult.target);
      if (!scopedOtherResult.ok) return scopedOtherResult.issues;

      const similarity = calculateTextSimilarity(
        scopedTargetResult.value,
        scopedOtherResult.value,
        { mode: rule.similarity?.mode ?? "fast" },
      );
      return similarity.similarity <= rule.threshold
        ? []
        : [issueFromSimilarityRule(
          rule,
          "max_similarity_to_ref",
          `Similarity ${similarity.similarity.toFixed(3)} exceeds maximum ${rule.threshold} when compared to ref '${rule.ref}'`,
          similarity.similarity,
          rule.threshold,
        )];
    }

    case "min_similarity_to_ref": {
      const refTargetResult = resolveReferenceTarget(rule.ref, options);
      if (refTargetResult.issues.length > 0) return refTargetResult.issues;
      if (!refTargetResult.applicable) return [];
      if (typeof target !== "string" || typeof refTargetResult.target !== "string") {
        return [invalidComparisonType(rule, "ref")];
      }

      const scopedTargetResult = applyRuleScope(rule, target);
      if (!scopedTargetResult.ok) return scopedTargetResult.issues;
      const scopedOtherResult = applyRuleScope(rule, refTargetResult.target);
      if (!scopedOtherResult.ok) return scopedOtherResult.issues;

      const similarity = calculateTextSimilarity(
        scopedTargetResult.value,
        scopedOtherResult.value,
        { mode: rule.similarity?.mode ?? "fast" },
      );
      return similarity.similarity >= rule.threshold
        ? []
        : [issueFromSimilarityRule(
          rule,
          "min_similarity_to_ref",
          `Similarity ${similarity.similarity.toFixed(3)} is below minimum ${rule.threshold} when compared to ref '${rule.ref}'`,
          similarity.similarity,
          rule.threshold,
        )];
    }

    case "array_min_length":
      if (!Array.isArray(target)) return [invalidType(rule, "an array target")];
      return target.length >= rule.value
        ? []
        : [issueFromRule(
          rule,
          "array_min_length",
          `Array length ${target.length} is less than minimum ${rule.value}`,
        )];

    case "array_max_length":
      if (!Array.isArray(target)) return [invalidType(rule, "an array target")];
      return target.length <= rule.value
        ? []
        : [issueFromRule(
          rule,
          "array_max_length",
          `Array length ${target.length} exceeds maximum ${rule.value}`,
        )];
  }
}

export function validateStageValue(
  value: unknown,
  validate?: StageValidateInput,
  options: ValidationOptions = {},
): ValidationResult {
  if (!validate?.rules?.length) {
    return { success: true, issues: [] };
  }

  const issues = validate.rules.flatMap((rule) =>
    evaluateRule(value, rule, options)
  );

  return {
    success: issues.length === 0,
    issues,
  };
}
