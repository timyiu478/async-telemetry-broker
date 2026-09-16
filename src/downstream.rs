use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn};

use crate::config::WorkerConfig;
use crate::frame::Frame;

#[instrument(skip(rx, cancel), fields(worker_id = worker_config.id))]
pub async fn run_worker(
    worker_config: WorkerConfig,
    mut rx: broadcast::Receiver<Frame>,
    cancel: CancellationToken,
) {
    let delay = worker_config.processing_delay;

    info!(
        event = "worker_started",
        delay_ms = delay.as_millis(),
        "Downstream worker started"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(event = "worker_shutdown", "Downstream worker stopping due to cancellation signal");
                break;
            }
            recv_result = rx.recv() => {
                match recv_result {
                    Ok(frame) => {
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }

                        // worker_id is automatically included in this JSON output via span context!
                        info!(
                            event = "frame_processed",
                            payload_len = frame.payload.len(),
                            "Worker processed frame successfully"
                        );
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped_count)) => {
                        warn!(
                            event = "worker_lagged",
                            skipped_messages = skipped_count,
                            "Subscriber fell behind broadcast channel capacity; dropped messages"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!(event = "channel_closed", "Broadcast channel closed; worker exiting loop");
                        break;
                    }
                }
            }
        }
    }
}
