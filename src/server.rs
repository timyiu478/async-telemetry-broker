use futures_util::StreamExt;
use std::error::Error;
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
                            handle_connection(socket, tx_clone, cancel_clone).await;
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
    socket: TcpStream,
    tx: broadcast::Sender<Frame>,
    cancel: CancellationToken,
) {
    let peer_addr = socket.peer_addr().ok();
    let mut framed_reader = FramedRead::new(socket, FrameCodec::default());

    while let Some(frame_res) = framed_reader.next().await {
        match frame_res {
            Ok(frame) => {
                let _ = tx.send(frame);
            }
            Err(err) => {
                warn!(peer = ?peer_addr, error = %err, "Framing decode error");
                break;
            }
        }
        if cancel.is_cancelled() {
            info!(peer = ?peer_addr, "Flushed current frame; stopping connection for shutdown");
            break;
        }
    }
}
