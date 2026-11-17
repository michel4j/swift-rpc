# 0002: Streaming-Ready and Control-Method Bypass in Balancing Mode

Under `proxy='balancing'`, a busy worker is removed from `worker_queue` until `DONE` or `ERROR`, which would stall `stop()`/`pause()`/`status()` when `instances=1` runs a long-lived streaming RPC.

We keep single-socket balancing and solve this with two opt-in mechanisms: streaming handlers call `request.ready()` (`ResponseType.READY`) to return the worker to `worker_queue` early, and `@control` methods bypass the empty-queue gate to run on any live worker.
