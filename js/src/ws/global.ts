import * as ws from "./index.js";

declare global {
  var Owlkei: { ws: typeof ws } | undefined;
}

globalThis.Owlkei = { ...globalThis.Owlkei, ws };
