use futures_util::StreamExt;
use std::error::Error;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, instrument, warn};

use crate::config::Config;
use crate::frame::{Frame, FrameCodec};

pub async fn run(
    config: Config,
    tx: broadcast::Sender<Frame>,
    tracker: TaskTracker,
    cancel: CancellationToken,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    info!(addr = %config.listen_addr, "TCP listener bound successfully");

    let timeout = config.connection_timeout;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Server accept loop stopping due to cancellation signal.");
                break;
            }
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((socket, peer_addr)) => {
                        info!(peer = %peer_addr, "Accepted new client TCP connection");
                        let tx_clone = tx.clone();
                        let cancel_clone = cancel.clone();

                        // Spawn and track client connection task
                        tracker.spawn(async move {
                            handle_connection(timeout, socket, tx_clone, cancel_clone).await;
                        });
                    }
                    Err(err) => {
                        warn!(error = %err, "Failed to accept TCP connection");
                    }
                }
            }
        }
    }

    Ok(())
}

#[instrument(skip(socket, tx, cancel))]
async fn handle_connection(
    connection_timeout: Duration,
    socket: TcpStream,
    tx: broadcast::Sender<Frame>,
    cancel: CancellationToken,
) {
    let peer_addr = socket.peer_addr().ok();
    let mut framed_reader = FramedRead::new(socket, FrameCodec::default());

    loop {
        let read_result = tokio::time::timeout(connection_timeout, framed_reader.next()).await;

        match read_result {
            // 1. Frame received successfully within timeout window
            Ok(Some(Ok(frame))) => {
                let _ = tx.send(frame);
            }
            // 2. Decoder error (malformed frame or protocol violation)
            Ok(Some(Err(err))) => {
                warn!(peer = ?peer_addr, error = %err, "Framing decode error; dropping client");
                break;
            }
            // 3. Clean disconnect (EOF from client)
            Ok(None) => {
                info!(peer = ?peer_addr, "Client disconnected cleanly (EOF)");
                break;
            }
            // 4. Idle connection timeout triggered
            Err(_) => {
                warn!(
                    peer = ?peer_addr,
                    timeout_sec = connection_timeout.as_secs(),
                    "Connection idle timeout reached; closing inactive socket"
                );
                break;
            }
        }

        // Check for graceful shutdown after handling the current frame
        if cancel.is_cancelled() {
            info!(peer = ?peer_addr, "Flushed current frame; stopping connection for shutdown");
            break;
        }
    }
}
