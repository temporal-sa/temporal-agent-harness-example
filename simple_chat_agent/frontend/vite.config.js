import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  base: "/static/dist/",
  plugins: [react()],
  build: {
    outDir: "static/dist",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8000",
      "/oauth": "http://127.0.0.1:8000",
      "/internal": "http://127.0.0.1:8000",
      "/static": "http://127.0.0.1:8000",
    },
  },
});
