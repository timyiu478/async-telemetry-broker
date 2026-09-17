# System Invariants & Safety Guarantees

This document defines the runtime invariants and system guarantees enforced by the async telemetry broker.

---

### 1. Connection Bound Invariant

$$\text{Active Connections} \le max_num_connections$$

* **Guarantee**: Active client connections will never exceed the configured upper bound.
* **Mechanism**: Enforced by an `Arc<Semaphore>`. The TCP accept loop must acquire an `OwnedSemaphorePermit` before spawning a handler. Excess incoming connections sit in the OS socket backlog without consuming server resources or file descriptors.

---

### 2. Frame Parsing Atomicity Invariant

$$\text{Channel Output} = \text{Fully Decoded Frame} \lor \emptyset$$

* **Guarantee**: Downstream channels receive only completely validated frames. Partial reads or corrupt headers are never broadcast.
* **Mechanism**: Connection handlers check `CancellationToken` signals strictly *between* complete frame decodes. If a connection drops mid-frame, the partial payload is discarded via `FrameCodec` boundary checks before reaching the broadcast queue.

---

### 3. Non-Blocking Ingress Liveness

$$\text{Ingress Latency} \perp \text{Worker Processing Speed}$$

* **Guarantee**: Socket read operations and frame ingestion runs at maximum wire speed regardless of downstream subscriber performance or backpressure.
* **Mechanism**: Powered by `tokio::sync::broadcast`. `broadcast::Sender::send` operates in $O(1)$ non-blocking time. Slow workers receive `RecvError::Lagged` and drop skipped frames locally without stalling the TCP ingestion loop.

---

### 4. Bounded Heap Allocations

$$\text{Max Queue Memory} = \text{Channel Capacity} \times \text{Max Frame Size}$$

* **Guarantee**: Memory consumption remains strictly capped under arbitrarily high traffic volume or forced downstream lagging.
* **Mechanism**: Frame payloads exceeding 8 MB trigger an immediate `FrameCodec` error, dropping the offending connection. Channel buffers are capped at a fixed capacity of 1,024 items, eliminating memory expansion risk.

---

### 5. Monotonic Bounded Shutdown

$$\text{Cancel Signal} \longrightarrow \text{Halt Accept Loop} \longrightarrow \text{Close Tracker} \longrightarrow \text{Drain In-Flight Tasks} \le t_{\text{shutdown timeout}}$$

* **Guarantee**: System teardown strictly moves forward without race conditions and is guaranteed to terminate within a bounded time limit ($t_{\text{shutdown timeout}}$).
* **Mechanism**: 
  1. `CancellationToken` cancels the `accept_loop` and drops `TcpListener`, immediately releasing the bound port.
  2. Control returns to `main()`, which invokes `tracker.close()` to seal task registration and prevent new task spawns.
  3. `tracker.wait()` is wrapped inside `tokio::time::timeout(config.shutdown_timeout, ...)`. 
  4. If in-flight tasks drain cleanly before the deadline, the process exits with `0`. If a task hangs or fails to yield, the timeout fires, aborting remaining tasks.
