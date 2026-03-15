interface TemplateArgs {
  [key: string]: any;
}

export interface TextTemplate {
  (args?: TemplateArgs, ...positional: any[]): string;

  template: string;
  render(args?: TemplateArgs, positional?: any[]): Promise<string>;

  toString(): string;
  clone(): TextTemplate;

  indent(level: number): TextTemplate;
  trimLines(): TextTemplate;
}

function escapeRegExp(s: string) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export class TextTemplate extends Function {
  public template: string;

  constructor(template: string) {
    super();
    const normalized = template.startsWith("\n") ? template.slice(1) : template;
    this.template = normalized.endsWith("\n")
      ? normalized.slice(0, -1)
      : normalized;

    const self = this;

    const fn = async function (
      args: TemplateArgs = {},
      ...positional: any[]
    ) {
      return await self.render(args, positional);
    };

    const proxy = new Proxy(fn as any, {
      get(_t, prop) {
        const member = (self as any)[prop];
        return typeof member === "function" ? member.bind(self) : member;
      },
      apply(target, _thisArg, argArray) {
        return target(...argArray);
      }
    });

    Object.setPrototypeOf(proxy, new.target.prototype);
    return proxy as any;
  }

  public async render(
    args: TemplateArgs = {},
    positional: any[] = []
  ): Promise<string> {
    let out = this.template;

    const entries = Object.entries(args);

    // ---- Stage 1: resolve all values in parallel ----
    const resolvedEntries = await Promise.all(
      entries.map(async ([key, value]) => {
        let resolved = value;

        if (typeof value === "function") {
          const filtered = { ...args };
          delete (filtered as any)[key];
          resolved = value(filtered, ...positional);
        }

        resolved = await resolved;
        return [key, resolved] as const;
      })
    );

    // ---- Stage 2: apply replacements in deterministic order ----
    for (const [key, resolved] of resolvedEntries) {
      const pattern = new RegExp(`\\{${escapeRegExp(key)}\\}`, "g");
      const replacement = String(resolved);
      out = out.replace(pattern, () => replacement);
    }

    // Spread positional
    out = out.replace(/\$\{\.\.\.\}/g, positional.join(" "));

    // Indexed positional
    out = out.replace(/\$(\d+)/g, (_, idx) => {
      const i = parseInt(idx, 10) - 1;
      return positional[i] !== undefined ? positional[i] : `$${idx}`;
    });

    return out;
  }


  public override toString(): string {
    throw new Error("Use await template() instead of toString()");
  }

  public [Symbol.toPrimitive](hint: string) {
    if (hint === "string") {
      return this.template;
    }
    return this.template;
  }

  public clone(): TextTemplate {
    return new (this.constructor as any)(this.template);
  }

  public indent(level: number): TextTemplate {
    const pad = " ".repeat(level);
    return new (this.constructor as any)(
      this.template
        .split("\n")
        .map(line => (line.trim() ? pad + line : line))
        .join("\n")
    );
  }

  public trimLines(): TextTemplate {
    return new (this.constructor as any)(
      this.template.split("\n").map(l => l.trimEnd()).join("\n")
    );
  }
}

// Example usage:
// const tmpl = new TextTemplate(`Hello, {name}!`);
// console.log(await tmpl({ name: "Alice" }));
// Output:
// "Hello, Alice!"
//----
// Templates can be combined and nested, tey also support async functions and positional arguments
// This can be used to create complex templates that can be rendered with dynamic data.
// Example:
// const greeting = new TextTemplate(`Hello, {name}!`);
// const farewell = new TextTemplate(`Goodbye, {name}!`);
// const combined = new TextTemplate(`${greeting}\n${farewell}`);
// console.log(await combined({ name: "Bob" }));
// Output:
// Hello, Bob!
// Goodbye, Bob!
//----
// This allows to create some powerful templates that can be used in various contexts, such as generating emails, reports, or even code.
// The following example shows how to create a template that generates an email with dynamic content:
//
// const emailTemplate = new TextTemplate(`
// Dear {recipient},
// It has come to our attention that {issue} occurred on {date}.
// We are working to resolve this issue as soon as possible.
// Thank you for your patience.
// Best regards,
// {sender}
// `);
//
// Combine this with another template for the issue description:
//
// const issueTemplate = new TextTemplate(`
// The issue is described as follows:
// {description}
// `);
//
// Then you can render the email with the issue description:
//
// const emailContent = await emailTemplate({
//   recipient: "John Doe",
//   issue: issueTemplate({ description: "The server went down due to unexpected traffic spikes." }),
//   date: "2024-06-01",
//   sender: "Support Team"
// })
// console.log(emailContent);
// Output:
// Dear John Doe,
// It has come to our attention that The issue is described as follows:
// The server went down due to unexpected traffic spikes. occurred on 2024-06-01.
// We are working to resolve this issue as soon as possible.
// Thank you for your patience.
// Best regards,
// Support Team
//----
// This demonstrates how the TextTemplate class can create complex templates that can be rendered with dynamic data, including nested templates and async functions.


// const greeting = new TextTemplate(`Hello, {name}!`);
// const farewell = new TextTemplate(`Goodbye, {name}!`);
// const combined = new TextTemplate(`${greeting}\n${farewell}`);
// console.log(await combined({ name: "Bob" }));
