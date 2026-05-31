export function toolMetaText(tool) {
  if (!tool.configured) {
    return "Set GITHUB_OAUTH_CLIENT_ID and GITHUB_OAUTH_CLIENT_SECRET.";
  }
  if (tool.provider?.startsWith("mcp:")) {
    return `${tool.login || "HTTP MCP"} | ${tool.available_tools?.length || 0} tools | ${tool.scopes}`;
  }
  if (tool.connected && tool.login) {
    return `@${tool.login} | ${tool.scopes || "no scopes returned"}`;
  }
  return `Scopes: ${tool.scopes || "none"}`;
}

export function mcpOAuthStartUrl({ label, serverUrl, toolPrefix, serverId = "" }) {
  const params = new URLSearchParams({
    label,
    server_url: serverUrl,
    tool_prefix: toolPrefix,
  });
  if (serverId) params.set("server_id", serverId);
  return `/api/mcp-servers/oauth/start?${params.toString()}`;
}

export function toolPrefixFromLabel(label) {
  return (
    label
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, "_")
      .replace(/^_+|_+$/g, "") || "mcp"
  );
}
