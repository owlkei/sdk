import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const root = new URL("../spec/", import.meta.url).pathname;

function* files(dir) {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) yield* files(path);
    else yield path;
  }
}

let checked = 0;
const failures = [];
for (const path of statSync(root, { throwIfNoEntry: false }) ? files(root) : []) {
  if (!path.endsWith(".json")) continue;
  checked++;
  try {
    JSON.parse(readFileSync(path, "utf8"));
  } catch (err) {
    failures.push(`${relative(root, path)}: ${err instanceof Error ? err.message : String(err)}`);
  }
}

if (failures.length > 0) {
  process.stderr.write(`spec: ${failures.length} invalid file(s)\n${failures.join("\n")}\n`);
  process.exit(1);
}
process.stdout.write(`spec: ${checked} JSON file(s) valid\n`);
