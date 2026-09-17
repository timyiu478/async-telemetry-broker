use async_telemetry_broker::{
    config::Config, config::WorkerConfig, downstream, frame::Frame, server,
};
use bytes::{BufMut, BytesMut};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Binds to port 0 to allocate an available OS port for test isolation.
async fn get_ephemeral_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// Helper to serialize a byte slice into wire-protocol frame format: [4-byte BE length] [payload]
fn encode_raw_frame(payload: &[u8]) -> BytesMut {
    let mut buf = BytesMut::with_capacity(4 + payload.len());
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf
}

#[tokio::test]
async fn test_48_concurrent_connections_and_routing() {
    let addr = get_ephemeral_addr().await;
    let config = Config::default().with_listen_addr(addr);
    let (tx, mut rx) = broadcast::channel::<Frame>(1024);
    let tracker = TaskTracker::new();
    let cancel = CancellationToken::new();

    let srv_tracker = tracker.clone();
    let srv_cancel = cancel.clone();
    tokio::spawn(async move {
        server::run(config, tx, srv_tracker, srv_cancel)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let payload = b"BROKER_CONCURRENCY_TEST";
    let frame_bytes = encode_raw_frame(payload);

    // Spawn 48 parallel client connections sending telemetry data
    let mut client_tasks = Vec::new();
    for _ in 0..48 {
        let frame_data = frame_bytes.clone();
        client_tasks.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.expect("TCP connect failed");
            stream
                .write_all(&frame_data)
                .await
                .expect("Write frame failed");
            stream.flush().await.expect("Flush stream failed");
        }));
    }

    for task in client_tasks {
        task.await.unwrap();
    }

    // Assert all 48 frames are received via the broadcast channel
    let mut received_count = 0;
    let timeout = Duration::from_secs(3);
    let start = tokio::time::Instant::now();

    while received_count < 48 && start.elapsed() < timeout {
        if let Ok(Ok(frame)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            assert_eq!(frame.payload, payload[..]);
            received_count += 1;
        }
    }

    assert_eq!(
        received_count, 48,
        "Broker dropped telemetry frames under 48-connection load"
    );
    cancel.cancel();
}

#[tokio::test]
async fn test_broker_handles_slow_downstream_worker_without_blocking() {
    let addr = get_ephemeral_addr().await;
    // Bounded channel capacity of 2 frames
    let config = Config::default()
        .with_listen_addr(addr)
        .with_broadcast_capacity(2);

    let (tx, _) = broadcast::channel::<Frame>(config.broadcast_capacity);
    let tracker = TaskTracker::new();
    let cancel = CancellationToken::new();

    // 1. Spawn real downstream worker with a 500ms processing delay
    let slow_worker_config = WorkerConfig {
        id: 1,
        processing_delay: Duration::from_millis(500),
    };
    let rx = tx.subscribe();
    let cancel_worker = cancel.clone();
    tracker.spawn(async move {
        downstream::run_worker(slow_worker_config, rx, cancel_worker).await;
    });

    // 2. Spawn real broker TCP server
    let srv_tracker = tracker.clone();
    let srv_cancel = cancel.clone();
    tokio::spawn(async move {
        server::run(config, tx, srv_tracker, srv_cancel)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Connect client and rapidly burst 10 telemetry frames over TCP
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let frame_bytes = encode_raw_frame(b"rapid_burst_frame");

    let write_start = tokio::time::Instant::now();
    for _ in 0..10 {
        stream.write_all(&frame_bytes).await.unwrap();
    }
    stream.flush().await.unwrap();

    // 4. Verification: Ingress writing must finish almost instantly (<50ms).
    // If the broker blocked on the 500ms worker, writing 10 frames would take ~5 seconds.
    assert!(
        write_start.elapsed() < Duration::from_millis(100),
        "Broker TCP ingress was blocked by slow downstream worker!"
    );

    cancel.cancel();
}

#[tokio::test]
async fn test_client_disconnect_and_reconnect_within_pass() {
    let addr = get_ephemeral_addr().await;
    let config = Config::default().with_listen_addr(addr);
    let (tx, mut rx) = broadcast::channel::<Frame>(1024);
    let tracker = TaskTracker::new();
    let cancel = CancellationToken::new();

    let srv_tracker = tracker.clone();
    let srv_cancel = cancel.clone();
    tokio::spawn(async move {
        server::run(config, tx, srv_tracker, srv_cancel)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Segment 1: Connect, send frame, disconnect unexpectedly
    {
        let mut stream1 = TcpStream::connect(addr).await.unwrap();
        stream1
            .write_all(&encode_raw_frame(b"session_1_frame"))
            .await
            .unwrap();
    }

    let frame1 = rx.recv().await.unwrap();
    assert_eq!(frame1.payload, &b"session_1_frame"[..]);

    // Segment 2: Reconnect during same pass, send frame
    {
        let mut stream2 = TcpStream::connect(addr).await.unwrap();
        stream2
            .write_all(&encode_raw_frame(b"session_2_frame"))
            .await
            .unwrap();
    }

    let frame2 = rx.recv().await.unwrap();
    assert_eq!(frame2.payload, &b"session_2_frame"[..]);

    cancel.cancel();
}

#[tokio::test]
async fn test_graceful_shutdown_drains_connection_tasks() {
    let addr = get_ephemeral_addr().await;
    let config = Config::default()
        .with_listen_addr(addr)
        .with_shutdown_timeout(Duration::from_secs(2));

    let (tx, _) = broadcast::channel::<Frame>(1024);
    let tracker = TaskTracker::new();
    let cancel = CancellationToken::new();

    let srv_tracker = tracker.clone();
    let srv_cancel = cancel.clone();
    tokio::spawn(async move {
        server::run(config, tx, srv_tracker, srv_cancel)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&encode_raw_frame(b"pre_shutdown_frame"))
        .await
        .unwrap();

    // Signal shutdown
    cancel.cancel();

    // Close socket to complete stream iteration
    stream.shutdown().await.unwrap();
    tracker.close();

    // Wait for task tracker drain within deadline
    let drain_result = tokio::time::timeout(Duration::from_secs(3), tracker.wait()).await;
    assert!(
        drain_result.is_ok(),
        "Broker failed to drain session tasks within shutdown timeout"
    );
}

#[tokio::test]
async fn test_idle_connection_timeout() {
    let addr = get_ephemeral_addr().await;
    // Set a very short timeout for fast test execution
    let config = Config::default()
        .with_listen_addr(addr)
        .with_broadcast_capacity(16);

    let mut config = config;
    config.connection_timeout = Duration::from_millis(200);

    let (tx, _) = broadcast::channel::<Frame>(config.broadcast_capacity);
    let tracker = TaskTracker::new();
    let cancel = CancellationToken::new();

    let srv_tracker = tracker.clone();
    let srv_cancel = cancel.clone();
    tokio::spawn(async move {
        server::run(config, tx, srv_tracker, srv_cancel)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect client, but send NO bytes (idle client)
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Wait past the 200ms timeout window
    tokio::time::sleep(Duration::from_millis(350)).await;

    // Attempting to read from socket should yield EOF (0 bytes read), confirming broker closed it
    // Because a socket read will wait/block indefinitely if the connection is still open and idle,
    // unblocking and returning 0 is the OS's explicit signal for EOF (End-of-File / Connection Closed).
    let mut buf = [0u8; 10];
    let bytes_read = stream.read(&mut buf).await.unwrap();
    assert_eq!(bytes_read, 0, "Broker failed to drop idle connection");

    cancel.cancel();
}

#[tokio::test]
async fn test_max_connections_rate_limiter() {
    let addr = get_ephemeral_addr().await;
    let config = Config::default()
        .with_listen_addr(addr)
        .with_max_num_connections(48);
    let (tx, mut rx) = broadcast::channel::<Frame>(1024);
    let tracker = TaskTracker::new();
    let cancel = CancellationToken::new();

    let srv_tracker = tracker.clone();
    let srv_cancel = cancel.clone();
    tokio::spawn(async move {
        server::run(config, tx, srv_tracker, srv_cancel)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 1. Fill all 48 connection permit slots
    let mut active_sockets = Vec::new();
    for _ in 0..48 {
        let stream = TcpStream::connect(addr).await.unwrap();
        active_sockets.push(stream);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 2. Open 49th connection and attempt to send telemetry
    let mut extra_socket = TcpStream::connect(addr).await.unwrap();
    let overflow_payload = b"overflow_client_frame";
    extra_socket
        .write_all(&encode_raw_frame(overflow_payload))
        .await
        .unwrap();
    extra_socket.flush().await.unwrap();

    // 3. Assert 49th frame is NOT processed (blocked waiting for semaphore permit)
    let recv_result = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(
        recv_result.is_err(),
        "Broker accepted 49th connection despite 48-connection rate limit!"
    );

    // 4. Drop one active connection to release a permit back to the pool
    drop(active_sockets.pop());

    // 5. Assert 49th connection unblocks and its queued frame gets processed
    let frame = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Timeout waiting for 49th connection frame after permit released")
        .unwrap();

    assert_eq!(frame.payload, &overflow_payload[..]);

    cancel.cancel();
}
