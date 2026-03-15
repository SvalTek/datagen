import { TxtProgressBar } from "./TxtProgress.ts";
import type {
  StageExecutionProgressEvent,
  StageExecutionWarning,
} from "./StageExecutionEngine.ts";

export type ConsoleMode = "quiet" | "summary" | "warnings" | "full";

export interface CliReporterOptions {
  consoleMode: ConsoleMode;
  progressEnabled: boolean;
  showThoughts: boolean;
}

interface CliReporterIo {
  writeStdoutLine?: (line: string) => void;
  writeStdoutRaw?: (text: string) => void;
  writeStderrLine?: (line: string) => void;
  writeStderrRaw?: (text: string) => void;
}

function formatDuration(durationMs: number): string {
  const totalSeconds = Math.max(0, Math.floor(durationMs / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }

  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function formatWarningLocation(warning: StageExecutionWarning): string {
  const parts = [
    `stage=${warning.stageIdentifier}`,
  ];

  if (warning.recordIndex !== undefined) {
    parts.push(`record=${warning.recordIndex}`);
  }

  if (warning.turnIndex !== undefined) {
    parts.push(`turn=${warning.turnIndex}`);
  }

  if (warning.attempt !== undefined) {
    parts.push(`attempt=${warning.attempt}`);
  }

  parts.push(`kind=${warning.kind}`);
  if (warning.validatorName?.trim()) {
    parts.push(`validator=${warning.validatorName.trim()}`);
  }
  return parts.join(" ");
}

export class CliReporter {
  private readonly stdoutLine: (line: string) => void;
  private readonly stdoutRaw: (text: string) => void;
  private readonly stderrLine: (line: string) => void;
  private readonly stderrRaw: (text: string) => void;
  private startedAt = 0;
  private progressBar?: TxtProgressBar;
  private lastProgressRender = 0;
  private activeProgressLine = "";
  private progressStageKey = "";

  constructor(
    private readonly options: CliReporterOptions,
    io: CliReporterIo = {},
  ) {
    this.stdoutLine = io.writeStdoutLine ?? ((line) => console.log(line));
    this.stdoutRaw = io.writeStdoutRaw ?? ((text) => {
      Deno.stdout.writeSync(new TextEncoder().encode(text));
    });
    this.stderrLine = io.writeStderrLine ?? ((line) => console.error(line));
    this.stderrRaw = io.writeStderrRaw ?? ((text) => {
      Deno.stderr.writeSync(new TextEncoder().encode(text));
    });
  }

  private shouldPrintSummaries(): boolean {
    return this.options.consoleMode === "summary" ||
      this.options.consoleMode === "warnings";
  }

  private writeProgress(text: string, final = false): void {
    const padded = text.length >= this.activeProgressLine.length
      ? text
      : text + " ".repeat(this.activeProgressLine.length - text.length);
    const suffix = final ? "\n" : "";
    this.stdoutRaw(`\r${padded}${suffix}`);
    this.activeProgressLine = final ? "" : padded;
  }

  private clearProgressIfNeeded(): void {
    if (!this.activeProgressLine) return;
    this.writeProgress("", true);
  }

  startRun(input: {
    pipelineName?: string;
    pipelinePath: string;
    model: string;
    provider?: string;
    endpoint?: string;
    inputPath?: string;
    inputRecordCount?: number;
    outputJsonlPath: string;
    stageCount: number;
    firstStageMode?: string;
  }): void {
    this.startedAt = Date.now();
    if (!this.shouldPrintSummaries()) return;

    const runName = input.pipelineName?.trim() || input.pipelinePath;
    this.stdoutLine(`Running ${runName}`);

    const modePart = input.stageCount === 1 && input.firstStageMode
      ? ` mode=${input.firstStageMode}`
      : "";
    this.stdoutLine(
      `model=${input.model}${input.provider ? ` provider=${input.provider}` : ""}${
        input.endpoint ? ` endpoint=${input.endpoint}` : ""
      }${modePart}`,
    );

    const inputSummary = input.inputPath
      ? `input=${input.inputPath}${
        input.inputRecordCount !== undefined ? ` records=${input.inputRecordCount}` : ""
      } `
      : "";
    this.stdoutLine(
      `${inputSummary}output=${input.outputJsonlPath} stages=${input.stageCount}`,
    );
  }

  onProgress(event: StageExecutionProgressEvent): void {
    if (!this.options.progressEnabled || this.options.consoleMode === "quiet") {
      return;
    }

    if (this.startedAt === 0) {
      this.startedAt = Date.now();
    }

    const now = Date.now();
    const stageKey = `${event.stageIdentifier}:${event.stageIndex}:${event.total ?? 0}`;
    if (
      this.activeProgressLine &&
      stageKey === this.progressStageKey &&
      now - this.lastProgressRender < 100 &&
      event.current < (event.total ?? event.current)
    ) {
      return;
    }

    if (!this.progressBar || stageKey !== this.progressStageKey) {
      this.progressBar = new TxtProgressBar({
        max: Math.max(event.total ?? event.current, 1),
        value: event.current,
      });
      this.progressStageKey = stageKey;
    } else {
      this.progressBar.update(event.current);
    }

    const elapsed = formatDuration(now - this.startedAt);
    const progressSummary = event.total !== undefined
      ? `${event.current}/${event.total}`
      : `${event.current}`;
    const line =
      `[${event.stageIdentifier}] ${progressSummary}  warnings=${event.warningsSoFar}  ${elapsed}  ${this.progressBar.get()}`;

    this.writeProgress(line, false);
    this.lastProgressRender = now;

    if (event.total !== undefined && event.current >= event.total) {
      this.writeProgress(
        `[${event.stageIdentifier}] ${progressSummary}  warnings=${event.warningsSoFar}  ${elapsed}  complete`,
        true,
      );
      this.progressStageKey = "";
      this.progressBar = undefined;
    }
  }

  printWarnings(warnings: StageExecutionWarning[]): void {
    for (const warning of warnings) {
      this.onWarning(warning);
    }
  }

  onWarning(warning: StageExecutionWarning): void {
    if (this.options.consoleMode !== "warnings") return;
    this.clearProgressIfNeeded();
    this.stdoutLine(`WARN ${formatWarningLocation(warning)}`);
    this.stdoutLine(`  ${warning.message}`);
  }

  onThoughts(thoughts: string): void {
    if (!this.options.showThoughts || !thoughts.trim()) return;
    this.clearProgressIfNeeded();
    this.stdoutLine("Model thoughts:");
    this.stdoutLine(thoughts);
  }

  finishSuccess(input: {
    outputJsonlPath: string;
    reportPath: string;
    warningsCount: number;
    durationMs: number;
  }): void {
    if (!this.shouldPrintSummaries()) return;
    this.clearProgressIfNeeded();
    this.stdoutLine("Done");
    this.stdoutLine(`output=${input.outputJsonlPath}`);
    this.stdoutLine(`report=${input.reportPath}`);
    this.stdoutLine(`warnings=${input.warningsCount}`);
    this.stdoutLine(`duration=${(input.durationMs / 1000).toFixed(1)}s`);
  }

  finishFailure(input: {
    errorType: string;
    reportPath: string;
    durationMs: number;
    hint?: string;
  }): void {
    this.clearProgressIfNeeded();
    if (this.options.consoleMode === "full") return;
    if (this.options.consoleMode === "quiet") {
      this.stderrLine("Failed");
      this.stderrLine(`report=${input.reportPath}`);
      return;
    }

    this.stderrLine("Failed");
    this.stderrLine(`errorType=${input.errorType}`);
    if (input.hint?.trim()) {
      this.stderrLine(`hint=${input.hint.trim()}`);
    }
    this.stderrLine(`report=${input.reportPath}`);
    this.stderrLine(`duration=${(input.durationMs / 1000).toFixed(1)}s`);
  }

  printFullReport(reportJson: string, isError = false): void {
    this.clearProgressIfNeeded();
    if (isError) {
      this.stderrRaw(`${reportJson}\n`);
      return;
    }
    this.stdoutRaw(`${reportJson}\n`);
  }
}
