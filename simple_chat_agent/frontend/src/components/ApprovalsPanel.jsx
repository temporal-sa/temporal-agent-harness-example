import { CodeBlock } from "./MarkdownContent.jsx";
import { languageFromFileName } from "../utils/code.js";

export function ApprovalsPanel({ workflowState, resolvingApprovals, onResolve }) {
  const approvals = (workflowState?.pending_approvals || []).filter(
    (approval) => !resolvingApprovals.has(approval.approval_id),
  );
  if (approvals.length === 0) return null;
  return (
    <section className="approval-panel">
      <div className="approval-panel-header">
        <span>Approval Required</span>
        <span className="approval-panel-count">
          {approvals.length === 1 ? "1 pending" : `${approvals.length} pending`}
        </span>
      </div>
      {approvals.map((approval) => (
        <ApprovalCard key={approval.approval_id} approval={approval} onResolve={onResolve} />
      ))}
    </section>
  );
}

function ApprovalCard({ approval, onResolve }) {
  return (
    <div className="approval-card">
      <div className="approval-title">{approval.summary || approval.tool_name}</div>
      <div className="approval-meta">
        <ApprovalMetaRow label="Tool" value={approval.tool_name} />
        <ApprovalMetaRow label="Scope" value={approval.memory_key || "one time"} />
      </div>
      <div className="approval-details bubble-content">
        <ApprovalArgs args={approval.tool_args || {}} />
      </div>
      <div className="approval-actions">
        <button
          type="button"
          className="allow"
          onClick={() => onResolve(approval.approval_id, "allow")}
        >
          Allow
        </button>
        <button
          type="button"
          className="always"
          onClick={() => onResolve(approval.approval_id, "always_allow")}
        >
          Always Allow
        </button>
        <button
          type="button"
          className="deny"
          onClick={() => onResolve(approval.approval_id, "deny")}
        >
          Deny
        </button>
      </div>
    </div>
  );
}

function ApprovalMetaRow({ label, value }) {
  return (
    <div>
      <strong>{label}: </strong>
      {value || "unknown"}
    </div>
  );
}

function ApprovalArgs({ args }) {
  if (typeof args.code === "string") {
    const rest = { ...args };
    delete rest.code;
    return (
      <>
        <CodeBlock source={args.code} languageHint="python" />
        {Object.keys(rest).length > 0 ? (
          <CodeBlock source={JSON.stringify(rest, null, 2)} languageHint="json" />
        ) : null}
      </>
    );
  }

  if (typeof args.content === "string" && typeof args.name === "string") {
    const metadata = { ...args };
    delete metadata.content;
    const truncated = args.content.length > 12000;
    const preview = truncated
      ? `${args.content.slice(0, 12000)}\n...[truncated for approval preview]`
      : args.content;
    return (
      <>
        <CodeBlock source={JSON.stringify(metadata, null, 2)} languageHint="json" />
        <CodeBlock source={preview} languageHint={languageFromFileName(args.name)} />
      </>
    );
  }

  return <CodeBlock source={JSON.stringify(args, null, 2)} languageHint="json" />;
}
