import * as devtools from "./index.js";

const root = globalThis as unknown as { Owlkei?: Record<string, unknown> };

root.Owlkei = { ...root.Owlkei, devtools };
