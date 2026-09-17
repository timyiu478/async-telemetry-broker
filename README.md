# Async Telemetry Broker

A high-performance, asynchronous TCP broker built with Rust and Tokio. Designed to ingest telemetry over unstable network links, frame the binary streams, and route them to downstream processing services without blocking.

## Video Walkthrough

Watch the short system demonstration: **[Async Telemetry Broker Demonstration](https://drive.google.com/file/d/1peebYGbUjJuLJ_rKhv-nI80B-Kfx01vW/view)**

## Benchmarks

The repository includes a load generator (`examples/throughput_bench.rs`) designed to evaluate high-throughput frame ingestion and non-blocking channel behavior under heavy client concurrency and severe downstream subscriber backpressure.

### Performance Results

```text
=== Test Parameters ===
Clients: 40
Frames per client: 10000
Payload size: 1024 bytes
Downstream Worker Delay: 50 ms
=========================

=== Benchmark Results ===
Time Elapsed:  1.04s
Total Data:    392.15 MB (400000 frames)
Throughput:    376.96 MB/s
Frame Rate:    384503 frames/sec
=========================
```

### Key Findings

* **Empirically Proven Non-Blocking Ingress**: The test logs explicitly recorded multiple worker lag events (e.g., `event="worker_lagged" skipped_messages=31`), confirming that slow subscribers drop frames without backpressuring or stalling TCP socket reads.
* **Sustained High Ingestion**: Despite active downstream frame drops, the broker maintained a constant ingestion rate of **376.96 MB/s** (**384,503 frames/sec**) across 40 concurrent TCP streams.
* **Bounded Resource Usage**: Fixed broadcast channel capacity (`1024` frames) successfully prevented RAM bloat under the subscriber latency, enforcing backpressure safety at the message boundary.
* **Zero Frame Corruption**: All 400,000 frames were parsed cleanly by the length-prefixed binary codec without deserialization errors or partial read failures.

### Hardware & OS Specification

* CPU: 2.3 GHz Quad-Core Intel Core i5
* Memory: 8 GB 2133 MHz LPDDR3
* OS: macOS 15.3.1

### Reproducing the Benchmark

To run the load generator in release mode:

```Bash
cargo run --example throughput_bench --release
```

## Features

* **Custom Binary Framing:** Implements `tokio_util::codec` for length-prefixed binary frames (4-byte Big-Endian length + payload) with automatic DOS protection against oversized frames.
* **Non-Blocking Ingress:** Routes telemetry via `tokio::sync::broadcast`. Slow downstream consumers are actively warned (Lagged) but will never block the TCP ingress thread.
* **Concurrent Rate Limiting:** Strictly bounds memory allocation and file descriptors via an asynchronous semaphore, safely queuing excess connection attempts without crashing.
* **Idle Connection Timeouts**: Automatically detects and drops silent or hung TCP sockets when a client abruptly loses connectivity without executing a clean TCP shutdown sequence (sending a FIN packet).
* **Zero-Data-Loss Graceful Shutdown:** Utilizes `tokio_util::task::TaskTracker` and `CancellationToken`. Shutting down the broker ensures all active connections finish parsing their *current* in-progress frame and drain safely before the process exits.

## Protocol Specification

Clients must connect over TCP and stream binary frames using the following structure:

1. **Length Header:** 4 bytes, Big-Endian `u32` indicating the size of the payload.
2. **Payload:** Variable length byte sequence matching the size defined in the header.

*Note: Default maximum frame size is 8 MB to prevent OOM vulnerabilities.*

## Project Architecture

* `src/config.rs`: Centralized configuration for network bindings, channel capacities, and timeouts.
* `src/server.rs`: The core TCP acceptance loop and socket reading tasks. Uses RAII permits for hard connection limits.
* `src/frame.rs`: The `FrameCodec` responsible for translating raw fragmented TCP byte streams into discrete payload structures.
* `src/downstream.rs`: Simulated worker tasks that process telemetry data, demonstrating the broker's resilience to slow handlers.
* `src/main.rs`: Subsystem initialization, tracing setup, and graceful Ctrl+C teardown orchestration.

## Getting Started

### Prerequisites

* Rust 1.70+
* Cargo

### Running the Broker

Start the server using standard Cargo commands. It will bind to `0.0.0.0:7777` by default.

```bash
cargo run --release
```

Logging is powered by tracing and outputs structured JSON by default. You can adjust the log level via environment variables:

```bash
RUST_LOG=async_telemetry_broker=debug cargo run
```

### Running Tests

The project includes extensive unit and integration tests verifying connection limits, broadcast channel lagging, disconnects, and framing boundaries.

To run the entire test suite:

```bash
cargo test
```

### Configuration

Default values can be customized in `src/config.rs`:

* Listen Address: 0.0.0.0:7777
* Broadcast Capacity: 1024 frames
* Max Connections: 50 concurrent TCP sockets
* Connection Timeout: 30 seconds
* Shutdown Timeout: 10 seconds
