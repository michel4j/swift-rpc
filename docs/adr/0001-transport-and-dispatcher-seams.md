# 0001: Transport and Dispatcher Seams

`Client`, `Server`, and `Worker` accept injected `Transport` and `Dispatcher` ports instead of hard-coding ZeroMQ sockets and GUI toolkit subclasses (`GResult`/`QResult`) inside their implementations.

Two real adapters justify each seam (`Zmq*Transport` / `InMemoryNetwork` for messaging; `DirectDispatcher` / `GLibDispatcher` / `QtDispatcher` for main-loop marshaling), keeping protocol and state-machine logic testable in-process without TCP sockets or GUI event loops.
