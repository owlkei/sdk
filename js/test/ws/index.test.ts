import { describe, expect, it } from "vitest";

import { PROTOCOL } from "../../src/ws/index.js";

describe("ws", () => {
  it("names the wire protocol it speaks", () => {
    expect(PROTOCOL).toBe("owlkei.v1");
  });
});
