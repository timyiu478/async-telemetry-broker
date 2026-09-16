use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub id: usize,
    pub processing_delay: Duration,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub broadcast_capacity: usize,
    pub connection_timeout: Duration,
    /// Maximum time to wait for active tasks to drain during graceful shutdown
    pub shutdown_timeout: Duration,
    pub workers: Vec<WorkerConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:7777"
                .parse()
                .expect("Failed to parse default listen address"),
            broadcast_capacity: 1024,
            connection_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(10),
            workers: vec![
                WorkerConfig {
                    id: 1,
                    processing_delay: Duration::from_millis(0),
                },
                WorkerConfig {
                    id: 2,
                    processing_delay: Duration::from_millis(1000),
                },
                WorkerConfig {
                    id: 3,
                    processing_delay: Duration::from_millis(110000),
                },
            ],
        }
    }
}

impl Config {
    pub fn with_listen_addr(mut self, addr: SocketAddr) -> Self {
        self.listen_addr = addr;
        self
    }

    pub fn with_broadcast_capacity(mut self, capacity: usize) -> Self {
        self.broadcast_capacity = capacity;
        self
    }

    /// Builder method to set graceful shutdown timeout
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }
}
