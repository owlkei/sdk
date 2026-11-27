import { defineConfig } from "tsup";

export default defineConfig([
  {
    entry: { "ws/index": "src/ws/index.ts", "devtools/index": "src/devtools/index.ts" },
    format: ["esm"],
    target: "es2023",
    dts: true,
    sourcemap: true,
    clean: true,
    treeshake: true,
  },
  {
    entry: { "owlkei.ws": "src/ws/global.ts", "owlkei.devtools": "src/devtools/global.ts" },
    format: ["iife"],
    target: "es2020",
    minify: true,
    sourcemap: true,
    outExtension: () => ({ js: ".min.js" }),
  },
]);
