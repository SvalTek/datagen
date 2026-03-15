/**
 * The rendered text produced for each non-active progress segment.
 */
export type ProgressTrackPiece = string;

/**
 * The rendered text produced for the active progress segment.
 */
export type ProgressTrackPoint = string;

/**
 * Options used to render a text progress bar.
 */
export interface ProgressBarRenderOptions {
  /**
   * Total number of segments to render.
   *
   * @defaultValue `15`
   */
  resolution?: number;

  /**
   * Text used for each inactive segment.
   *
   * @defaultValue `"=>"` for {@link createProgressBar}
   */
  trackPiece?: ProgressTrackPiece;

  /**
   * Text used for the active segment.
   *
   * @defaultValue `">>"` for {@link createProgressBar}
   */
  trackPoint?: ProgressTrackPoint;

  /**
   * Fixed grapheme width for each rendered segment.
   *
   * When omitted, the width is derived from the longest of `trackPiece` and
   * `trackPoint`, which keeps the rendered bar length stable even when the two
   * segment strings differ in length.
   */
  segmentWidth?: number;

  /**
   * Character used to right-pad segments shorter than `segmentWidth`.
   *
   * @defaultValue `" "`
   */
  padCharacter?: string;
}

/**
 * Options used to create a {@link TxtProgressBar} instance.
 */
export interface TxtProgressBarOptions extends ProgressBarRenderOptions {
  /**
   * Initial progress value.
   *
   * @defaultValue `0`
   */
  value?: number;

  /**
   * Maximum progress value.
   */
  max: number;
}

/**
 * Immutable view of a progress bar configuration and current value.
 */
export interface TxtProgressBarState {
  /**
   * Current progress value.
   */
  value: number;

  /**
   * Maximum progress value.
   */
  max: number;

  /**
   * Total number of rendered segments.
   */
  resolution: number;

  /**
   * Text used for inactive segments.
   */
  trackPiece: ProgressTrackPiece;

  /**
   * Text used for the active segment.
   */
  trackPoint: ProgressTrackPoint;

  /**
   * Fixed grapheme width for each rendered segment.
   */
  segmentWidth: number;

  /**
   * Character used to right-pad short segments.
   */
  padCharacter: string;
}

type InternalDefaultRenderOptions = Required<
  Omit<ProgressBarRenderOptions, "segmentWidth">
>;

const DEFAULT_RENDER_OPTIONS = {
  resolution: 15,
  trackPiece: "=>",
  trackPoint: ">>",
  padCharacter: " "
} satisfies InternalDefaultRenderOptions;

const DEFAULT_CLASS_OPTIONS = {
  resolution: 15,
  trackPiece: "⁃•⁃",
  trackPoint: "⟾➤⁃",
  padCharacter: " "
} satisfies InternalDefaultRenderOptions;

const graphemeSegmenter = new Intl.Segmenter(undefined, {
  granularity: "grapheme"
});

/**
 * Validates a numeric maximum value.
 *
 * @param max - Maximum progress value.
 * @throws {RangeError} Thrown when `max` is not greater than `0`.
 */
function assertMax(max: number): void {
  if (!Number.isFinite(max) || max <= 0) {
    throw new RangeError("max must be a finite number greater than 0");
  }
}

/**
 * Validates a rendering resolution.
 *
 * @param resolution - Total number of rendered segments.
 * @throws {RangeError} Thrown when `resolution` is not a positive integer.
 */
function assertResolution(resolution: number): void {
  if (!Number.isInteger(resolution) || resolution <= 0) {
    throw new RangeError("resolution must be a positive integer");
  }
}

/**
 * Validates a segment width.
 *
 * @param segmentWidth - Width of each rendered segment.
 * @throws {RangeError} Thrown when `segmentWidth` is not a positive integer.
 */
function assertSegmentWidth(segmentWidth: number): void {
  if (!Number.isInteger(segmentWidth) || segmentWidth <= 0) {
    throw new RangeError("segmentWidth must be a positive integer");
  }
}

/**
 * Splits a string into grapheme clusters.
 *
 * @param value - Text to split.
 * @returns The grapheme clusters for `value`.
 */
function splitGraphemes(value: string): string[] {
  return Array.from(graphemeSegmenter.segment(value), ({ segment }) => segment);
}

/**
 * Returns the grapheme length of a string.
 *
 * @param value - Text to measure.
 * @returns The number of grapheme clusters in `value`.
 */
function getGraphemeLength(value: string): number {
  return splitGraphemes(value).length;
}

/**
 * Fits a segment string to the configured width.
 *
 * Segments longer than the target width are truncated by grapheme. Shorter
 * segments are right-padded, which keeps every bar position aligned.
 *
 * @param value - Segment text.
 * @param segmentWidth - Target grapheme width.
 * @param padCharacter - Padding grapheme.
 * @returns A segment with a stable grapheme width.
 */
function fitSegment(
  value: string,
  segmentWidth: number,
  padCharacter: string
): string {
  const graphemes = splitGraphemes(value);

  if (graphemes.length >= segmentWidth) {
    return graphemes.slice(0, segmentWidth).join("");
  }

  const pad = splitGraphemes(padCharacter)[0] ?? " ";
  return graphemes.join("") + pad.repeat(segmentWidth - graphemes.length);
}

/**
 * Clamps a progress value to the valid range for rendering.
 *
 * The upper bound is exclusive so that a value equal to `max` still renders
 * the active point inside the final segment.
 *
 * @param value - Current progress value.
 * @param max - Maximum progress value.
 * @returns A value safe to convert into a segment index.
 */
function clampProgressValue(value: number, max: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }

  const lowerBounded = Math.max(0, value);
  return Math.min(lowerBounded, max - Number.EPSILON);
}

/**
 * Renders a text-based progress bar string for a given value and maximum.
 *
 * @param value - Current progress value.
 * @param max - Maximum progress value.
 * @param options - Rendering options.
 * @returns The rendered progress bar text.
 *
 * @example
 * ```ts
 * createProgressBar(4, 10, { resolution: 5, trackPiece: "..", trackPoint: "##" });
 * ```
 */
export function createProgressBar(
  value: number,
  max: number,
  options: ProgressBarRenderOptions = {}
): string {
  const {
    resolution = DEFAULT_RENDER_OPTIONS.resolution,
    trackPiece = DEFAULT_RENDER_OPTIONS.trackPiece,
    trackPoint = DEFAULT_RENDER_OPTIONS.trackPoint,
    segmentWidth = Math.max(
      getGraphemeLength(trackPiece),
      getGraphemeLength(trackPoint),
      1
    ),
    padCharacter = DEFAULT_RENDER_OPTIONS.padCharacter
  } = options;

  assertMax(max);
  assertResolution(resolution);
  assertSegmentWidth(segmentWidth);

  const safeValue = clampProgressValue(value, max);
  const segmentValue = max / resolution;
  const activeIndex = Math.min(
    resolution - 1,
    Math.floor(safeValue / segmentValue)
  );
  const inactiveSegment = fitSegment(trackPiece, segmentWidth, padCharacter);
  const activeSegment = fitSegment(trackPoint, segmentWidth, padCharacter);

  let output = "";

  for (let index = 0; index < resolution; index += 1) {
    output += index === activeIndex ? activeSegment : inactiveSegment;
  }

  return output;
}

/**
 * Mutable text-based progress bar helper.
 *
 * The class stores the current value and rendering configuration, and can
 * render the current state on demand with {@link get}.
 */
export class TxtProgressBar {
  /**
   * Current progress value.
   */
  public value: number;

  /**
   * Maximum progress value.
   */
  public readonly max: number;

  /**
   * Total number of rendered segments.
   */
  public readonly resolution: number;

  /**
   * Text used for inactive segments.
   */
  public readonly trackPiece: ProgressTrackPiece;

  /**
   * Text used for the active segment.
   */
  public readonly trackPoint: ProgressTrackPoint;

  /**
   * Fixed grapheme width for each rendered segment.
   */
  public readonly segmentWidth: number;

  /**
   * Character used to right-pad short segments.
   */
  public readonly padCharacter: string;

  /**
   * Creates a text-based progress bar instance.
   *
   * @param options - Initial state and rendering configuration.
   */
  constructor(options: TxtProgressBarOptions) {
    const {
      value = 0,
      max,
      resolution = DEFAULT_CLASS_OPTIONS.resolution,
      trackPiece = DEFAULT_CLASS_OPTIONS.trackPiece,
      trackPoint = DEFAULT_CLASS_OPTIONS.trackPoint,
      segmentWidth = Math.max(
        getGraphemeLength(trackPiece),
        getGraphemeLength(trackPoint),
        1
      ),
      padCharacter = DEFAULT_CLASS_OPTIONS.padCharacter
    } = options;

    assertMax(max);
    assertResolution(resolution);
    assertSegmentWidth(segmentWidth);

    this.value = value;
    this.max = max;
    this.resolution = resolution;
    this.trackPiece = trackPiece;
    this.trackPoint = trackPoint;
    this.segmentWidth = segmentWidth;
    this.padCharacter = padCharacter;
  }

  /**
   * Updates the current progress value.
   *
   * @param value - New progress value.
   * @returns The current instance for chaining.
   */
  public update(value: number): this {
    this.value = value;
    return this;
  }

  /**
   * Returns the current progress value.
   *
   * @returns The current value.
   */
  public current(): number {
    return this.value;
  }

  /**
   * Resets the current progress value to `0`.
   *
   * @returns The current instance for chaining.
   */
  public reset(): this {
    this.value = 0;
    return this;
  }

  /**
   * Returns a snapshot of the current configuration and value.
   *
   * @returns A serializable view of the progress bar state.
   */
  public toJSON(): TxtProgressBarState {
    return {
      value: this.value,
      max: this.max,
      resolution: this.resolution,
      trackPiece: this.trackPiece,
      trackPoint: this.trackPoint,
      segmentWidth: this.segmentWidth,
      padCharacter: this.padCharacter
    };
  }

  /**
   * Renders the current progress bar.
   *
   * @returns The rendered progress bar text.
   */
  public get(): string {
    return createProgressBar(this.value, this.max, {
      resolution: this.resolution,
      trackPiece: this.trackPiece,
      trackPoint: this.trackPoint,
      segmentWidth: this.segmentWidth,
      padCharacter: this.padCharacter
    });
  }
}

export { TxtProgressBar as txtProgressBar };
