/**
 * Copyright (C) 2025 Theros
 * GPLv3 or later — see <https://www.gnu.org/licenses/>
 */
import { Output, generateText, streamText } from "ai";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { createOllama } from "ai-sdk-ollama";
import type { ZodType } from "zod";
import type {
  PipelineProvider,
  PipelineReasoningMode,
} from "../structures/TaskSchema.ts";

// ============================================================================
// Types
// ============================================================================
export interface CompletionSettings {
  max_tokens?: number;
  temperature?: number;
  think?: boolean;
  reasoning_mode?: PipelineReasoningMode;
}

export interface ChatSessionDebugHooks {
  onThoughts?: (thoughts: string) => void;
}

export type CompletionRequestOptions = [
  prompt: string,
  model: string,
  options?: CompletionSettings,
];

export interface ChatTransportPayload {
  model: string;
  messages: any[];
  max_tokens?: number;
  temperature?: number;
  stream?: boolean;
  think?: boolean;
  extra_body?: Record<string, unknown>;
}

export interface ChatTransport {
  endpoint: string;

  request(payload: ChatTransportPayload): Promise<any>;

  stream?(
    payload: ChatTransportPayload,
    onToken: (token: string) => void,
  ): Promise<void>;
}

export interface OpenAICompatibleTransportOptions {
  apiKey?: string;
  httpReferer?: string;
  xTitle?: string;
}

export interface ChatSessionBackendOptions {
  provider?: PipelineProvider;
  endpoint?: string;
  apiKey?: string;
  httpReferer?: string;
  xTitle?: string;
}

const defaultBackendOptions: Required<
  Pick<ChatSessionBackendOptions, "provider" | "endpoint">
> = {
  provider: "openai",
  endpoint: "http://localhost:11434/",
};

function normalizeEndpoint(endpoint?: string): string {
  const value = endpoint?.trim() || defaultBackendOptions.endpoint;
  return value.endsWith("/") ? value : `${value}/`;
}

function resolveReasoningMode(
  value: PipelineReasoningMode | undefined,
): PipelineReasoningMode {
  return value ?? "off";
}

function applyReasoningFields(
  payload: ChatTransportPayload,
  reasoningRequested: boolean,
  reasoningMode: PipelineReasoningMode,
): ChatTransportPayload {
  const nextPayload: ChatTransportPayload = { ...payload };
  delete nextPayload.think;

  if (nextPayload.extra_body && "reasoning" in nextPayload.extra_body) {
    const extraBody = nextPayload.extra_body as Record<string, unknown>;
    const { reasoning: _reasoning, ...rest } = extraBody;
    nextPayload.extra_body = Object.keys(rest).length > 0 ? rest : undefined;
  }

  if (reasoningMode === "think") {
    nextPayload.think = reasoningRequested;
    return nextPayload;
  }

  if (reasoningMode === "openai") {
    nextPayload.extra_body = {
      ...(nextPayload.extra_body ?? {}),
      reasoning: { enabled: reasoningRequested },
    };
  }

  return nextPayload;
}

function extractReasoningText(value: unknown): string {
  if (typeof value === "string") return value.trim();
  if (Array.isArray(value)) {
    return value
      .map((item) => {
        if (typeof item === "string") return item;
        if (item && typeof item === "object") {
          const text = (item as Record<string, unknown>).text;
          if (typeof text === "string") return text;
        }
        return "";
      })
      .filter((item) => item.length > 0)
      .join("\n")
      .trim();
  }
  return "";
}

class StructuredOutputError extends Error {
  rawOutput?: string;

  constructor(message: string, options?: { cause?: unknown; rawOutput?: string }) {
    super(message, options);
    this.name = "StructuredOutputError";
    this.rawOutput = options?.rawOutput;
  }
}

// ============================================================================
// Legacy transport: OpenAI-compatible HTTP implementation (kept for compatibility)
// ============================================================================
export class OpenAICompatibleTransport implements ChatTransport {
  endpoint: string;
  private readonly apiKey?: string;
  private readonly httpReferer?: string;
  private readonly xTitle?: string;

  constructor(endpoint: string, options: OpenAICompatibleTransportOptions = {}) {
    if (!endpoint.endsWith("/")) endpoint += "/";
    this.endpoint = endpoint + "v1/chat/completions";
    this.apiKey = options.apiKey?.trim() || undefined;
    this.httpReferer = options.httpReferer?.trim() || undefined;
    this.xTitle = options.xTitle?.trim() || undefined;
  }

  private buildHeaders(): HeadersInit {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };

    if (this.apiKey) {
      headers.Authorization = `Bearer ${this.apiKey}`;
    }

    if (this.httpReferer) {
      headers["HTTP-Referer"] = this.httpReferer;
    }

    if (this.xTitle) {
      headers["X-Title"] = this.xTitle;
    }

    return headers;
  }

  async request(payload: ChatTransportPayload): Promise<any> {
    const res = await fetch(this.endpoint, {
      method: "POST",
      headers: this.buildHeaders(),
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const errBody = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status}${errBody ? `: ${errBody}` : ""}`);
    }
    return await res.json();
  }

  async stream(
    payload: ChatTransportPayload,
    onToken: (token: string) => void,
  ): Promise<void> {
    const res = await fetch(this.endpoint, {
      method: "POST",
      headers: this.buildHeaders(),
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const errBody = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status}${errBody ? `: ${errBody}` : ""}`);
    }

    if (!res.body) {
      throw new Error("HTTP stream response had no body");
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let eventData: string[] = [];
    let stopRequested = false;

    try {
      const flushEvent = () => {
        if (!eventData.length) return;
        const ssePayload = eventData.join("\n").trim();
        eventData = [];
        if (!ssePayload) return;
        if (ssePayload === "[DONE]") {
          stopRequested = true;
          return;
        }
        try {
          const json = JSON.parse(ssePayload);
          const token = json.choices?.[0]?.delta?.content;
          if (token) onToken(token);
        } catch {}
      };

      while (!stopRequested) {
        const { value, done } = await reader.read();
        if (done) {
          buffer += decoder.decode();
        } else {
          buffer += decoder.decode(value, { stream: true });
        }

        const lines = buffer.split(/\r?\n/);
        buffer = lines.pop() ?? "";

        for (const line of lines) {
          if (!line.trim()) {
            flushEvent();
            if (stopRequested) break;
            continue;
          }
          if (!line.startsWith("data:")) continue;
          eventData.push(line.slice(5).trimStart());
        }

        if (done) {
          if (buffer.trim().startsWith("data:")) {
            eventData.push(buffer.trim().slice(5).trimStart());
          }
          flushEvent();
          break;
        }
      }
    } finally {
      try {
        await reader.cancel();
      } catch {}
    }
  }
}

function toAiMessages(
  history: Array<{ role: "user" | "assistant" | "system"; content: string }>,
  systemMessage: string,
  content: string,
): Array<{ role: "user" | "assistant" | "system"; content: string }> {
  const messages: Array<{ role: "user" | "assistant" | "system"; content: string }> = [];
  if (systemMessage.trim()) {
    messages.push({ role: "system", content: systemMessage });
  }
  messages.push(...history);
  messages.push({ role: "user", content });
  return messages;
}

function resolveChatModel(
  model: string,
  backendOptions: ChatSessionBackendOptions,
): unknown {
  const provider = backendOptions.provider ?? defaultBackendOptions.provider;
  const endpoint = normalizeEndpoint(backendOptions.endpoint);

  if (provider === "openai") {
    const headers: Record<string, string> = {};
    if (backendOptions.httpReferer?.trim()) {
      headers["HTTP-Referer"] = backendOptions.httpReferer.trim();
    }
    if (backendOptions.xTitle?.trim()) {
      headers["X-Title"] = backendOptions.xTitle.trim();
    }
    const client = createOpenAICompatible({
      name: "openai-compatible",
      baseURL: `${endpoint}v1`,
      apiKey: backendOptions.apiKey?.trim() || undefined,
      headers: Object.keys(headers).length > 0 ? headers : undefined,
    } as any);

    if (typeof (client as { chatModel?: (value: string) => unknown }).chatModel === "function") {
      return (client as { chatModel: (value: string) => unknown }).chatModel(model);
    }
    if (typeof client === "function") {
      return (client as (value: string) => unknown)(model);
    }
    throw new Error("OpenAI-compatible provider could not resolve a chat model");
  }

  if (provider === "ollama") {
    const client = createOllama({
      baseURL: endpoint,
    } as any);
    if (typeof client === "function") {
      return (client as (value: string) => unknown)(model);
    }
    if (typeof (client as { chatModel?: (value: string) => unknown }).chatModel === "function") {
      return (client as { chatModel: (value: string) => unknown }).chatModel(model);
    }
    throw new Error("Ollama provider could not resolve a chat model");
  }

  throw new Error(
    `Unsupported provider: ${provider}. Expected one of: openai, ollama.`,
  );
}

function isTransport(value: unknown): value is ChatTransport {
  return !!value && typeof value === "object" &&
    typeof (value as { request?: unknown }).request === "function";
}

// ============================================================================
// High-Level API
// ============================================================================
export async function RequestChatCompletion(
  ...[prompt, model, options]: CompletionRequestOptions
): Promise<string> {
  const session = new ChatSession(model, options, {
    provider: "openai",
    endpoint: defaultBackendOptions.endpoint,
  });
  return await session.send(prompt, options);
}

export async function RequestChatStream(
  prompt: string,
  model: string,
  options: CompletionSettings = {},
  onToken: (token: string) => void,
): Promise<void> {
  const session = new ChatSession(model, options, {
    provider: "openai",
    endpoint: defaultBackendOptions.endpoint,
  });
  await session.stream(prompt, options, onToken);
}

// ============================================================================
// ChatSession
// ============================================================================
export class ChatSession {
  private history: {
    role: "user" | "assistant" | "system";
    content: string;
  }[] = [];
  private systemMessage = "";
  private readonly transport?: ChatTransport;
  private readonly backendOptions: ChatSessionBackendOptions;

  constructor(
    private model: string,
    private defaultOptions: CompletionSettings = {},
    transportOrBackend: ChatTransport | ChatSessionBackendOptions = {},
    private debugHooks: ChatSessionDebugHooks = {},
  ) {
    if (isTransport(transportOrBackend)) {
      this.transport = transportOrBackend;
      this.backendOptions = {};
      return;
    }
    this.backendOptions = transportOrBackend ?? {};
  }

  fork(): ChatSession {
    const clone = new ChatSession(
      this.model,
      { ...this.defaultOptions },
      this.transport ?? { ...this.backendOptions },
      this.debugHooks,
    );
    if (this.systemMessage) {
      clone.setSystemMessage(this.systemMessage);
    }
    return clone;
  }

  setSystemMessage(content: string) {
    this.systemMessage = content;
  }

  add(role: "user" | "assistant" | "system", content: string) {
    this.history.push({ role, content });
  }

  clearHistory() {
    this.history = [];
  }

  private buildMessages(content: string): any[] {
    return toAiMessages(this.history, this.systemMessage, content);
  }

  private resolveEffectiveOptions(options: CompletionSettings) {
    return {
      maxTokens: options.max_tokens ?? this.defaultOptions.max_tokens ?? 1000,
      temperature: options.temperature ?? this.defaultOptions.temperature ?? 0.7,
      think: options.think ?? this.defaultOptions.think ?? false,
      reasoningMode: resolveReasoningMode(
        options.reasoning_mode ?? this.defaultOptions.reasoning_mode,
      ),
    };
  }

  async send(
    content: string,
    options: CompletionSettings = {},
  ): Promise<string> {
    const messages = this.buildMessages(content);
    const resolvedOptions = this.resolveEffectiveOptions(options);

    if (this.transport) {
      const res = await this.transport.request(
        applyReasoningFields({
          model: this.model,
          messages,
          max_tokens: resolvedOptions.maxTokens,
          temperature: resolvedOptions.temperature,
          stream: false,
        }, resolvedOptions.think, resolvedOptions.reasoningMode),
      );

      const thoughts = (res.choices?.[0]?.message?.reasoning ?? "").toString();
      if (thoughts.trim()) {
        this.debugHooks.onThoughts?.(thoughts.trim());
      }

      const reply = (res.choices?.[0]?.message?.content ?? "").toString();
      this.add("user", content);
      this.add("assistant", reply);
      return reply.trim();
    }

    const modelInstance = resolveChatModel(this.model, this.backendOptions);
    const res = await generateText({
      model: modelInstance as any,
      messages,
      maxOutputTokens: resolvedOptions.maxTokens,
      temperature: resolvedOptions.temperature,
    });

    const thoughts = extractReasoningText((res as unknown as Record<string, unknown>).reasoning);
    if (thoughts) {
      this.debugHooks.onThoughts?.(thoughts);
    }

    const reply = (res.text ?? "").trim();
    this.add("user", content);
    this.add("assistant", reply);
    return reply;
  }

  async sendStructured<T>(
    content: string,
    schema: ZodType<T>,
    options: CompletionSettings = {},
  ): Promise<T> {
    if (this.transport) {
      const raw = await this.send(content, options);
      let parsed: unknown;
      try {
        parsed = JSON.parse(raw);
      } catch (error) {
        throw new StructuredOutputError(
          "Model output is not valid JSON",
          { cause: error, rawOutput: raw },
        );
      }

      const validationResult = schema.safeParse(parsed);
      if (!validationResult.success) {
        throw new StructuredOutputError(
          validationResult.error.issues.map((issue) => issue.message).join("; "),
          {
            cause: validationResult.error,
            rawOutput: raw,
          },
        );
      }

      return validationResult.data;
    }

    const messages = this.buildMessages(content);
    const resolvedOptions = this.resolveEffectiveOptions(options);
    const modelInstance = resolveChatModel(this.model, this.backendOptions);
    const res = await generateText({
      model: modelInstance as any,
      messages,
      maxOutputTokens: resolvedOptions.maxTokens,
      temperature: resolvedOptions.temperature,
      output: Output.object({ schema }),
    });

    const thoughts = extractReasoningText((res as unknown as Record<string, unknown>).reasoning);
    if (thoughts) {
      this.debugHooks.onThoughts?.(thoughts);
    }

    const output = (res as { output: T }).output;
    this.add("user", content);
    this.add("assistant", JSON.stringify(output));
    return output;
  }

  async stream(
    content: string,
    options: CompletionSettings = {},
    onToken: (token: string) => void,
  ): Promise<void> {
    const messages = this.buildMessages(content);
    const resolvedOptions = this.resolveEffectiveOptions(options);
    let assistantReply = "";

    if (this.transport) {
      if (this.transport.stream) {
        await this.transport.stream(
          applyReasoningFields({
            model: this.model,
            messages,
            max_tokens: resolvedOptions.maxTokens,
            temperature: resolvedOptions.temperature,
            stream: true,
          }, resolvedOptions.think, resolvedOptions.reasoningMode),
          (token) => {
            assistantReply += token;
            onToken(token);
          },
        );
      } else {
        const reply = await this.send(content, options);
        onToken(reply);
        return;
      }

      this.add("user", content);
      this.add("assistant", assistantReply);
      return;
    }

    const modelInstance = resolveChatModel(this.model, this.backendOptions);
    const result = streamText({
      model: modelInstance as any,
      messages,
      maxOutputTokens: resolvedOptions.maxTokens,
      temperature: resolvedOptions.temperature,
    });

    for await (const token of result.textStream) {
      assistantReply += token;
      onToken(token);
    }

    this.add("user", content);
    this.add("assistant", assistantReply);
  }
}

export function isStructuredOutputError(error: unknown): error is {
  message: string;
  rawOutput?: string;
} {
  return error instanceof StructuredOutputError;
}
