import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { fileURLToPath } from "node:url";

const gisDataDir = fileURLToPath(
  new URL("../data", import.meta.url),
);

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
  ],

  publicDir: gisDataDir,

  build: {
    copyPublicDir: false,
  },
});