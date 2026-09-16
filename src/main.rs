use async_telemetry_broker::{config::Config, downstream, frame::Frame, server};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "async_telemetry_broker=info".into()),
        )
        .init();

    info!("Initializing async-telemetry-broker...");

    let config = Config::default();
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();

    // 1. Create the broadcast channel at the root level
    let (tx, _) = broadcast::channel::<Frame>(config.broadcast_capacity);

    // 2. Explicitly wire and spawn downstream subscribers
    for worker_config in config.workers.clone() {
        let rx = tx.subscribe();
        let cancel_clone = cancel.clone();

        tracker.spawn(async move {
            downstream::run_worker(worker_config, rx, cancel_clone).await;
        });
    }

    // 3. Register Ctrl+C shutdown handler
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("Ctrl+C received. Initiating shutdown sequence...");
            cancel_clone.cancel();
        }
    });

    // 4. Run the TCP server (passes tx and tracker into the network handler)
    server::run(config.clone(), tx, tracker.clone(), cancel.clone()).await?;

    // 5. Graceful Teardown: Close tracker and wait for all tasks (server + workers) to finish
    tracker.close();
    info!(
        timeout_sec = config.shutdown_timeout.as_secs(),
        "Waiting for active tasks to drain..."
    );

    match tokio::time::timeout(config.shutdown_timeout, tracker.wait()).await {
        Ok(_) => {
            info!(
                event = "shutdown_complete",
                "All subsystems shut down cleanly within deadline."
            );
        }
        Err(_) => {
            warn!(
                event = "shutdown_timeout",
                timeout_sec = config.shutdown_timeout.as_secs(),
                "Graceful shutdown deadline reached! Forcefully exiting."
            );
        }
    }

    Ok(())
}
