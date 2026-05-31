import { useRef } from "react";
import { mcpOAuthStartUrl, toolMetaText, toolPrefixFromLabel } from "../utils/tools.js";

export function ToolsWindow({
  open,
  tools,
  mcpFormOpen,
  mcpFormSubmitting,
  mcpFormError,
  mcpFormValues,
  onClose,
  onOpenMcpForm,
  onCancelMcpForm,
  onUpdateMcpForm,
  onSubmitMcpForm,
  onRefreshTools,
  onSetMcpEnabled,
  onDeleteMcp,
  setStatusNotice,
  post,
}) {
  const builtInTools = tools.filter((tool) => !tool.provider?.startsWith("mcp:"));
  const mcpTools = tools.filter((tool) => tool.provider?.startsWith("mcp:"));

  return (
    <section
      className="tools-overlay"
      hidden={!open}
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div
        className="tools-window"
        role="dialog"
        aria-modal="true"
        aria-labelledby="toolsWindowTitle"
      >
        <div className="tools-window-header">
          <div className="tools-window-title" id="toolsWindowTitle">
            Tools
          </div>
          <button type="button" onClick={onClose}>
            Close
          </button>
        </div>
        <div className="tools-window-body">
          <section className="tools-section">
            <ToolsSectionHeader title="Built-in tools" />
            <div className="tools-grid">
              {builtInTools.map((tool) => (
                <BuiltInToolCard
                  key={tool.provider || tool.label}
                  tool={tool}
                  onRefreshTools={onRefreshTools}
                  setStatusNotice={setStatusNotice}
                  post={post}
                />
              ))}
            </div>
          </section>
          <section className="tools-section">
            <ToolsSectionHeader
              title="MCP servers"
              actions={
                <button type="button" onClick={onOpenMcpForm}>
                  Add HTTP MCP
                </button>
              }
            />
            {mcpFormOpen ? (
              <McpForm
                values={mcpFormValues}
                submitting={mcpFormSubmitting}
                error={mcpFormError}
                onUpdate={onUpdateMcpForm}
                onSubmit={onSubmitMcpForm}
                onCancel={onCancelMcpForm}
              />
            ) : null}
            <div className="tools-grid">
              {mcpTools.map((tool) => (
                <McpToolCard
                  key={tool.provider || tool.label}
                  tool={tool}
                  onSetEnabled={onSetMcpEnabled}
                  onDelete={onDeleteMcp}
                />
              ))}
              {mcpTools.length === 0 ? (
                <div className="tool-meta">No MCP servers connected.</div>
              ) : null}
            </div>
          </section>
        </div>
      </div>
    </section>
  );
}

function ToolsSectionHeader({ title, actions = null }) {
  return (
    <div className="tools-section-header">
      <div className="tools-section-title">{title}</div>
      {actions ? <div className="tools-section-actions">{actions}</div> : null}
    </div>
  );
}

function BuiltInToolCard({ tool, onRefreshTools, setStatusNotice, post }) {
  return (
    <ToolCard
      tool={tool}
      status={tool.connected ? "Connected" : "Disconnected"}
      connected={Boolean(tool.connected)}
      disabled={false}
    >
      {tool.provider === "github" ? (
        <div className="tool-actions">
          <button
            type="button"
            disabled={!tool.configured}
            onClick={async () => {
              if (tool.connected) {
                await post("/api/tools/github/disconnect", {});
                setStatusNotice("GitHub disconnected");
                await onRefreshTools();
              } else {
                window.location.href = "/oauth/github/start";
              }
            }}
          >
            {tool.connected ? "Disconnect" : "Connect"}
          </button>
        </div>
      ) : null}
    </ToolCard>
  );
}

function McpToolCard({ tool, onSetEnabled, onDelete }) {
  const connected = Boolean(tool.connected);
  const enabled = Boolean(tool.enabled);
  return (
    <ToolCard
      tool={tool}
      status={connected ? (enabled ? "Enabled" : "Disabled") : "Disconnected"}
      connected={connected && enabled}
      disabled={!enabled}
    >
      <div className="tool-actions">
        {tool.auth_mode === "oauth" ? (
          <button
            type="button"
            onClick={() => {
              window.location.href = mcpOAuthStartUrl({
                label: tool.label,
                serverUrl: tool.server_url || tool.login || "",
                toolPrefix: tool.tool_prefix || "",
                serverId: tool.server_id || tool.provider.slice("mcp:".length),
              });
            }}
          >
            Reconnect
          </button>
        ) : null}
        <button type="button" onClick={() => onSetEnabled(tool, !enabled)}>
          {enabled ? "Disable" : "Enable"}
        </button>
        <button type="button" className="danger" onClick={() => onDelete(tool)}>
          Delete
        </button>
      </div>
    </ToolCard>
  );
}

function ToolCard({ tool, status, connected, disabled, children }) {
  return (
    <div className={`tool-card${connected ? " connected" : ""}${disabled ? " disabled" : ""}`}>
      <div className="tool-title">
        <span className="tool-label">{tool.label}</span>
        <span className="tool-status">{status}</span>
      </div>
      <div className="tool-meta">{toolMetaText(tool)}</div>
      {tool.available_tools?.length ? (
        <div className="tool-chip-list">
          {tool.available_tools.slice(0, 8).map((toolName) => (
            <span className="tool-chip" key={toolName}>
              {toolName}
            </span>
          ))}
          {tool.available_tools.length > 8 ? (
            <span className="tool-chip">+{tool.available_tools.length - 8}</span>
          ) : null}
        </div>
      ) : null}
      {children}
    </div>
  );
}

function McpForm({ values, submitting, error, onUpdate, onSubmit, onCancel }) {
  const prefixTouchedRef = useRef(false);
  return (
    <form
      className="mcp-form"
      onSubmit={(event) => onSubmit(event, values)}
      noValidate
    >
      <McpField
        label="Label"
        name="label"
        placeholder="Temporal docs"
        value={values.label}
        onChange={(value) => {
          const patch = { label: value };
          if (!prefixTouchedRef.current) patch.tool_prefix = toolPrefixFromLabel(value);
          onUpdate(patch);
        }}
      />
      <McpField
        label="HTTP URL"
        name="server_url"
        placeholder="https://example.com/mcp"
        value={values.server_url}
        onChange={(value) => onUpdate({ server_url: value })}
      />
      <McpField
        label="Tool prefix"
        name="tool_prefix"
        placeholder="temporal"
        value={values.tool_prefix}
        onChange={(value) => {
          prefixTouchedRef.current = true;
          onUpdate({ tool_prefix: value });
        }}
      />
      <div className="mcp-field">
        <label htmlFor="mcp-auth-mode">Auth</label>
        <select
          id="mcp-auth-mode"
          name="auth_mode"
          value={values.auth_mode}
          onChange={(event) => onUpdate({ auth_mode: event.currentTarget.value })}
        >
          <option value="none">No auth</option>
          <option value="oauth">OAuth authorization</option>
          <option value="bearer">Bearer token</option>
        </select>
      </div>
      <McpField
        label="Bearer token"
        name="bearer_token"
        placeholder=""
        type="password"
        value={values.bearer_token}
        hidden={values.auth_mode !== "bearer"}
        required={values.auth_mode === "bearer"}
        onChange={(value) => onUpdate({ bearer_token: value })}
      />
      {error ? <div className="mcp-error">{error}</div> : null}
      <div className="mcp-form-actions">
        <button type="submit" className="primary" disabled={submitting}>
          {submitting ? "Adding..." : "Add"}
        </button>
        <button type="button" disabled={submitting} onClick={onCancel}>
          Cancel
        </button>
      </div>
    </form>
  );
}

function McpField({
  label,
  name,
  placeholder,
  value,
  onChange,
  type = "text",
  hidden = false,
  required = true,
}) {
  return (
    <div className="mcp-field" data-field={name} hidden={hidden}>
      <label htmlFor={`mcp-${name}`}>{label}</label>
      <input
        id={`mcp-${name}`}
        name={name}
        type={type}
        placeholder={placeholder}
        value={value}
        required={required}
        onChange={(event) => onChange(event.currentTarget.value)}
      />
    </div>
  );
}
