export {
  Client,
  PROTOCOL,
  connect,
  type ClientOptions,
  type EventHandler,
  type State,
  type Subscription,
  type WebSocketConstructor,
  type WebSocketLike,
} from "./client.js";
export {
  ConnectionError,
  OwlkeiError,
  TimeoutError,
  type ErrorBody,
  type FieldError,
} from "./errors.js";
