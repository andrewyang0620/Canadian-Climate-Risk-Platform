import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";

const gisDataDir = fileURLToPath(new URL("../data", import.meta.url));

export default defineConfig({
  plugins: [react()],

  publicDir: gisDataDir,

  build: {
    copyPublicDir: false,
  },
});
