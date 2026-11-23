import { readdirSync, readFileSync, statSync } from "node:fs";
import { basename, dirname, join, relative } from "node:path";

const root = new URL("../spec/", import.meta.url).pathname;

function* files(dir) {
  for (const name of readdirSync(dir).sort()) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) yield* files(path);
    else yield path;
  }
}

const typeOf = (v) => {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  if (typeof v === "number") return Number.isInteger(v) ? "integer" : "number";
  return typeof v;
};

const typeMatches = (want, v) =>
  want === typeOf(v) || (want === "number" && typeOf(v) === "integer");

function validate(schema, value, path, errors) {
  if (schema.type !== undefined && !typeMatches(schema.type, value)) {
    errors.push(`${path}: want ${schema.type}, got ${typeOf(value)}`);
    return;
  }
  if ("const" in schema && JSON.stringify(schema.const) !== JSON.stringify(value)) {
    errors.push(`${path}: want ${JSON.stringify(schema.const)}`);
  }
  if (schema.enum !== undefined && !schema.enum.includes(value)) {
    errors.push(`${path}: want one of ${schema.enum.join(", ")}`);
  }
  if (typeof value === "string") {
    if (schema.minLength !== undefined && value.length < schema.minLength) errors.push(`${path}: too short`);
    if (schema.pattern !== undefined && !new RegExp(schema.pattern, "u").test(value)) {
      errors.push(`${path}: does not match ${schema.pattern}`);
    }
  }
  if (typeof value === "number") {
    if (schema.minimum !== undefined && value < schema.minimum) errors.push(`${path}: below ${schema.minimum}`);
    if (schema.maximum !== undefined && value > schema.maximum) errors.push(`${path}: above ${schema.maximum}`);
  }
  if (Array.isArray(value)) {
    if (schema.minItems !== undefined && value.length < schema.minItems) errors.push(`${path}: too few items`);
    if (schema.items !== undefined) value.forEach((item, i) => validate(schema.items, item, `${path}[${i}]`, errors));
  }
  if (typeOf(value) === "object") {
    const keys = Object.keys(value);
    for (const key of schema.required ?? []) {
      if (!(key in value)) errors.push(`${path}: missing ${key}`);
    }
    if (schema.minProperties !== undefined && keys.length < schema.minProperties) errors.push(`${path}: too few members`);
    if (schema.maxProperties !== undefined && keys.length > schema.maxProperties) errors.push(`${path}: too many members`);
    for (const key of keys) {
      const sub = schema.properties?.[key];
      if (sub !== undefined) validate(sub, value[key], `${path}.${key}`, errors);
      else if (schema.additionalProperties === false) errors.push(`${path}: unknown member ${key}`);
    }
  }
  for (const sub of schema.allOf ?? []) validate(sub, value, path, errors);
  if (schema.if !== undefined && passes(schema.if, value) && schema.then !== undefined) {
    validate(schema.then, value, path, errors);
  }
  if (schema.not !== undefined && passes(schema.not, value)) errors.push(`${path}: matches a forbidden shape`);
  if (schema.anyOf !== undefined && !schema.anyOf.some((sub) => passes(sub, value))) {
    errors.push(`${path}: matches none of the allowed shapes`);
  }
}

function passes(schema, value) {
  const errors = [];
  validate(schema, value, "", errors);
  return errors.length === 0;
}

const failures = [];
let checked = 0;
for (const path of files(root)) {
  if (!path.endsWith(".json") || basename(dirname(path)) !== "vectors") continue;
  checked++;
  const name = relative(root, path);
  let vector;
  try {
    vector = JSON.parse(readFileSync(path, "utf8"));
  } catch (err) {
    failures.push(`${name}: ${err instanceof Error ? err.message : String(err)}`);
    continue;
  }
  const schema = JSON.parse(readFileSync(join(dirname(dirname(path)), "vector.schema.json"), "utf8"));
  const errors = [];
  validate(schema, vector, "$", errors);
  const want = `${vector.kind}-${vector.name}.json`;
  if (basename(path) !== want) errors.push(`file name must be ${want}`);
  for (const err of errors) failures.push(`${name}: ${err}`);
}

if (failures.length > 0) {
  process.stderr.write(`spec: ${failures.length} problem(s)\n${failures.join("\n")}\n`);
  process.exit(1);
}
process.stdout.write(`spec: ${checked} vector(s) valid\n`);
