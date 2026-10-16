import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// The API (simple_chat_agent.api.main, port 8000) serves the built app at /app/. In development
// the dev server proxies the API and the harness console to it. The proxy keeps the browser's
// Host header (no changeOrigin), so the API's same-origin check on POSTs passes.
const API = process.env.AGENT_API_URL ?? "http://127.0.0.1:8000";

export default defineConfig({
  base: "/app/",
  plugins: [react()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: true,
  },
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      "/api": { target: API },
      "/console": { target: API },
    },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
