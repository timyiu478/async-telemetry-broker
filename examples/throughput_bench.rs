use async_telemetry_broker::{
    config::{Config, WorkerConfig},
    downstream,
    frame::Frame,
    server,
};
use bytes::{BufMut, BytesMut};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

// Benchmark Parameters
const NUM_CONCURRENT_CLIENTS: usize = 40; // Stays under the 48 max limit
const FRAMES_PER_CLIENT: usize = 10_000;
const PAYLOAD_SIZE_BYTES: usize = 1024; // 1 KB payload

#[tokio::main]
async fn main() {
    // 1. Initialize tracing but ONLY show errors. 
    // If we print a log for every dropped frame, console I/O will bottleneck the benchmark!
    tracing_subscriber::fmt()
        .with_env_filter("async_telemetry_broker=warn")
        .init();

    println!("Starting Async Telemetry Broker Benchmark...");
    println!("Clients: {}", NUM_CONCURRENT_CLIENTS);
    println!("Frames per client: {}", FRAMES_PER_CLIENT);
    println!("Payload size: {} bytes", PAYLOAD_SIZE_BYTES);

    let addr = "127.0.0.1:7778".parse().unwrap();
    let config = Config::default()
        .with_listen_addr(addr)
        .with_broadcast_capacity(1024);

    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    let (tx, _) = broadcast::channel::<Frame>(config.broadcast_capacity);

    // 2. Spawn a slow downstream worker (50ms delay) to guarantee channel overflow (Lagged)
    let slow_worker = WorkerConfig {
        id: 1,
        processing_delay: Duration::from_millis(50),
    };
    let rx = tx.subscribe();
    let cancel_worker = cancel.clone();
    tracker.spawn(async move {
        downstream::run_worker(slow_worker, rx, cancel_worker).await;
    });

    // 3. Spawn the TCP Broker
    let srv_tracker = tracker.clone();
    let srv_cancel = cancel.clone();
    let tx_srv = tx.clone();
    let srv_config = config.clone();
    tokio::spawn(async move {
        server::run(srv_config, tx_srv, srv_tracker, srv_cancel).await.unwrap();
    });

    // Give server a moment to bind
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 4. Pre-compute the raw binary frame [4-byte Length][Payload]
    let mut buf = BytesMut::with_capacity(4 + PAYLOAD_SIZE_BYTES);
    buf.put_u32(PAYLOAD_SIZE_BYTES as u32);
    buf.put_slice(&vec![0u8; PAYLOAD_SIZE_BYTES]);
    let raw_frame = buf.freeze();

    println!("Connecting clients and generating load...\n");

    let mut join_set = tokio::task::JoinSet::new();
    let start_time = Instant::now();

    // 5. Spawn concurrent clients to flood the broker
    for i in 0..NUM_CONCURRENT_CLIENTS {
        let frame_data = raw_frame.clone();
        join_set.spawn(async move {
            let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");
            for _ in 0..FRAMES_PER_CLIENT {
                stream.write_all(&frame_data).await.unwrap();
            }
            stream.flush().await.unwrap();
            i
        });
    }

    // 6. Wait for all clients to finish pushing data
    while let Some(res) = join_set.join_next().await {
        res.expect("Client task panicked");
    }

    let elapsed = start_time.elapsed();
    
    // 7. Calculate and display metrics
    let total_frames = NUM_CONCURRENT_CLIENTS * FRAMES_PER_CLIENT;
    let total_bytes = total_frames * (4 + PAYLOAD_SIZE_BYTES);
    let total_mb = total_bytes as f64 / 1_048_576.0;
    
    let frames_per_sec = total_frames as f64 / elapsed.as_secs_f64();
    let mb_per_sec = total_mb / elapsed.as_secs_f64();

    println!("=== Benchmark Results ===");
    println!("Time Elapsed:  {:.2?}", elapsed);
    println!("Total Data:    {:.2} MB ({} frames)", total_mb, total_frames);
    println!("Throughput:    {:.2} MB/s", mb_per_sec);
    println!("Frame Rate:    {:.0} frames/sec", frames_per_sec);
    println!("=========================");

    cancel.cancel();
}
