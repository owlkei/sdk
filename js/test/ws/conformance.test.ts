import { describe, expect, it } from "vitest";

import { runSession, sessionVectors } from "../session.js";

const url = process.env["OWLKEI_CONFORMANCE_URL"];
const vectors = sessionVectors("ws");

describe.skipIf(url === undefined)("conformance against OWLKEI_CONFORMANCE_URL", () => {
  it("has every session vector", () => {
    expect(vectors.length).toBeGreaterThanOrEqual(23);
  });

  it.each(vectors.map((v) => [v.name, v] as const))(
    "%s",
    async (_, vector) => {
      await runSession(url ?? "", vector);
    },
    15_000,
  );
});
