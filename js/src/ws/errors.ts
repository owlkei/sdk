/** One field's failure in a validation error (422), keyed by the field's name in `fields`. */
export interface FieldError {
  readonly rule: string;
  readonly message: string;
  readonly param?: string;
}

/** The REST error body the server sends inside an `error` message. */
export interface ErrorBody {
  readonly code: number;
  readonly error: string;
  readonly message: string;
  readonly fields?: Readonly<Record<string, FieldError>>;
  readonly requestId?: string;
}

/**
 * The server refused a message: `code` is an HTTP status code (403, 404, 405, 409, 415, 422, 429,
 * 500, 503…), `requestId` finds the server's log lines, `retryAfter` is in seconds when present.
 */
export class OwlkeiError extends Error {
  override readonly name = "OwlkeiError";
  readonly code: number;
  readonly status: string;
  readonly fields: Readonly<Record<string, FieldError>> | undefined;
  readonly requestId: string | undefined;
  readonly retryAfter: number | undefined;

  constructor(body: ErrorBody, requestId?: string, retryAfter?: number) {
    super(body.message);
    this.code = body.code;
    this.status = body.error;
    this.fields = body.fields;
    this.requestId = requestId ?? body.requestId;
    this.retryAfter = retryAfter;
  }
}

/**
 * The connection ended before the server answered, or the client was closed. `closeCode` is the
 * WebSocket close code when the server sent one.
 */
export class ConnectionError extends Error {
  override readonly name = "ConnectionError";
  readonly closeCode: number | undefined;

  constructor(message: string, closeCode?: number) {
    super(message);
    this.closeCode = closeCode;
  }
}

/** The server did not answer within `requestTimeout`. */
export class TimeoutError extends Error {
  override readonly name = "TimeoutError";
}

export function errorBody(value: unknown): ErrorBody {
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    if (typeof record["code"] === "number" && typeof record["message"] === "string") {
      return {
        code: record["code"],
        error: typeof record["error"] === "string" ? record["error"] : "",
        message: record["message"],
        ...(isFields(record["fields"]) ? { fields: record["fields"] } : {}),
        ...(typeof record["requestId"] === "string" ? { requestId: record["requestId"] } : {}),
      };
    }
  }
  return { code: 500, error: "Internal Server Error", message: "Internal Server Error" };
}

function isFields(value: unknown): value is Record<string, FieldError> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
