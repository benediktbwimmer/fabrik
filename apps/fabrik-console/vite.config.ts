import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/admin": "http://127.0.0.1:3000",
      "/tenants": "http://127.0.0.1:3000",
      "/search": "http://127.0.0.1:3000"
    }
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./src/test-setup.ts"
  }
});
