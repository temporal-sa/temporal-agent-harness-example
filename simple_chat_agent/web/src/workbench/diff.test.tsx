import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { DiffView } from "./DiffView";
import { diffFromEditOutput, editPreview, marker, parseUnifiedDiff } from "./diff";

const EDIT_OUTPUT = [
  "Edited refund_service/refunds.py (+1 -1).",
  "--- a/refund_service/refunds.py",
  "+++ b/refund_service/refunds.py",
  "@@ -31,3 +31,3 @@",
  "         self._check_refundable(order)",
  "-        amount = order.total_cents if amount_cents is None else amount_cents",
  "+        amount = order.balance_cents if amount_cents is None else amount_cents",
  "         if amount <= 0:",
  "",
].join("\n");

describe("unified diff parsing", () => {
  it("reads an edit_file result's diff and drops the file headers", () => {
    const d = diffFromEditOutput(EDIT_OUTPUT)!;
    expect(d.truncated).toBe(false);
    expect(d.lines.map((l) => l.kind)).toEqual(["hunk", "ctx", "del", "add", "ctx"]);
    expect(d.lines[2]!.text).toBe("        amount = order.total_cents if amount_cents is None else amount_cents");
  });

  it("flags a diff the tool shortened", () => {
    const d = diffFromEditOutput("Edited a.py (+900 -0).\n@@ -1 +1 @@\n+x\n[... 12 more chars; diff truncated]\n")!;
    expect(d.truncated).toBe(true);
    expect(d.lines.some((l) => l.text.includes("truncated"))).toBe(false);
  });

  it("previews an edit from its arguments", () => {
    expect(editPreview("a\nold\nz", "a\nnew\nz")).toEqual([
      { kind: "ctx", text: "a" },
      { kind: "del", text: "old" },
      { kind: "add", text: "new" },
      { kind: "ctx", text: "z" },
    ]);
  });

  it("parses + - and context lines", () => {
    expect(parseUnifiedDiff("@@ -1 +1 @@\n-a\n+b\n c\n\\ No newline at end of file").map((l) => l.kind)).toEqual(["hunk", "del", "add", "ctx", "meta"]);
  });
});

describe("diff renderer", () => {
  const html = renderToStaticMarkup(<DiffView lines={diffFromEditOutput(EDIT_OUTPUT)!.lines} label="Diff of refunds.py" />);

  it("marks deletions with a pink minus sign and a strikethrough element, additions with +", () => {
    expect(marker("del")).toBe("−");
    expect(marker("add")).toBe("+");
    expect(html).toMatch(/<del class="del"><span class="mk" aria-hidden="true">−<\/span><span class="sr-only">Removed: <\/span><span class="txt">/);
    expect(html).toMatch(/<ins class="add"><span class="mk" aria-hidden="true">\+<\/span><span class="sr-only">Added: <\/span>/);
  });

  it("never uses red: deletions are pink with a line-through", async () => {
    // Read the stylesheet from disk (Vitest stubs CSS imports). The app's tsconfig has no Node
    // types, so the module name is a variable.
    const fsName = "node:fs";
    const fs = (await import(/* @vite-ignore */ fsName)) as { readFileSync(p: URL, enc: "utf8"): string };
    const css = fs.readFileSync(new URL("./workbench.css", import.meta.url), "utf8");
    const diffRules = css
      .split("}")
      .filter((rule: string) => rule.includes(".wb-diff"))
      .join("}");
    expect(diffRules).not.toMatch(/--red|#ff5555|\bred\b/i);
    expect(diffRules).toMatch(/del\.del \{[^}]*color: var\(--pink\)/);
    expect(diffRules).toMatch(/del\.del \.txt \{[^}]*text-decoration: line-through/);
    expect(diffRules).toMatch(/ins\.add \{[^}]*color: var\(--green\)/);
  });
});
