# Swift RPC (`szrpc`)

Lightweight ZeroMQ RPC framework supporting streaming replies, load-balanced workers, cancellation, broadcast events, and GUI main-loop integration.

## Language

**Broker**:
The central router (`Server`) that pairs frontend `Client`s with backend `Worker`s and optionally forwards `Broadcast Event`s.
_Avoid_: Proxy, coordinator

**Service**:
The RPC target exposing `remote__*` operations, optional lifecycle hooks (`on_start`, `on_shutdown`), and event publishing.
_Avoid_: Handler, controller

**Control Method**:
A `Service` operation decorated with `@control` that bypasses busy-worker queuing so it can execute even while a single worker is occupied.
_Avoid_: Priority call, admin endpoint

**Worker**:
An execution host that owns a `Service` instance, advertises supported methods via heartbeats, and runs dispatched `Request`s in a subprocess or thread.
_Avoid_: Runner, node

**Request**:
A single remote invocation carrying caller identity, method name, keyword arguments, and cancellation state.
_Avoid_: Command, job

**Response**:
A message emitted for a `Request`: `UPDATE` (streaming chunk), `DONE` (final value), `ERROR` (structured failure), `READY` (worker available while streaming), or `HEARTBEAT` (liveness).
_Avoid_: Reply packet

**Result**:
The client-side handle for an in-flight `Request`, tracking completion, `error_info`, signal callbacks, and explicit `cancel()`.
_Avoid_: Future, promise

**Dispatcher**:
The adapter at the `Client` seam that marshals `Result` and subscription callbacks onto a target execution context (direct, GLib, or Qt).
_Avoid_: Event loop wrapper

**Transport**:
The messaging seam between `Client`, `Broker`, and `Worker` (`Zmq*Transport` for production sockets, `InMemoryNetwork` for in-process tests).
_Avoid_: Socket layer

**Broadcast Event**:
An unsolicited `(topic, payload)` message published by a `Service` and delivered to subscribed `Client`s independently of RPC requests.
_Avoid_: Notification, push message

**Monitor**:
The introspection observer recording active calls, historical calls, and worker liveness.
_Avoid_: Dashboard singleton
