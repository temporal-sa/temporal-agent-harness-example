export function jsonHeaders() {
  return { "content-type": "application/json" };
}

export async function responseErrorText(response) {
  const text = await response.text();
  try {
    const body = JSON.parse(text);
    if (typeof body.detail === "string") return body.detail;
    if (body.detail) return JSON.stringify(body.detail);
  } catch (_error) {
  }
  return text || `${response.status} ${response.statusText}`;
}
