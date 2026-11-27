/** Where the app runs when `connect` is given no `app`. */
export const DEFAULT_APP = "http://localhost:3000";

const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);
const TOKEN = /^okd1_[A-Za-z0-9_-]{43}$/;

/** What a pairing link carries: the app's origin and its devtools token. */
export interface Pairing {
  readonly app: string;
  readonly token: string;
}

/**
 * Reads the pairing link the app prints on its terminal,
 * `https://devtools.owlkei.com/#app=http://localhost:3000&token=okd1_…`: the whole link, its
 * fragment (`#app=…&token=…`) or the fragment without `#`. Throws a `TypeError`, which never holds
 * the token, when a member is missing, the token is not `okd1_` and 43 base64url characters, or the
 * app is not an `http` or `https` origin on `localhost`, `127.0.0.1` or `[::1]` (spec T7).
 */
export function parsePairing(fragment: string): Pairing {
  const hash = fragment.indexOf("#");
  const params = new URLSearchParams(hash >= 0 ? fragment.slice(hash + 1) : fragment);
  const app = params.get("app");
  const token = params.get("token");
  if (app === null || token === null) {
    throw new TypeError("owlkei: the pairing link needs both app and token");
  }
  return { app: appOrigin(app), token: checkedToken(token) };
}

export function appOrigin(app: string): string {
  let url: URL;
  try {
    url = new URL(app);
  } catch {
    throw new TypeError("owlkei: the app is not a URL, such as http://localhost:3000");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new TypeError("owlkei: the app must be an http or https URL");
  }
  if (!LOOPBACK_HOSTS.has(url.hostname)) {
    throw new TypeError("owlkei: the app must run on this machine: localhost, 127.0.0.1 or [::1]");
  }
  if (
    url.username !== "" ||
    url.password !== "" ||
    url.pathname !== "/" ||
    url.search !== "" ||
    url.hash !== ""
  ) {
    throw new TypeError("owlkei: the app must be an origin, such as http://localhost:3000");
  }
  return url.origin;
}

export function checkedToken(token: string): string {
  if (!TOKEN.test(token)) {
    throw new TypeError("owlkei: the token is not okd1_ followed by 43 base64url characters");
  }
  return token;
}
