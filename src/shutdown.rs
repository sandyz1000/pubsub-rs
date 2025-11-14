use log::info;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, broadcast};

/// Shutdown coordinator for graceful shutdown
#[derive(Clone)]
pub struct ShutdownCoordinator {
    shutdown_signal: Arc<AtomicBool>,
    shutdown_tx: broadcast::Sender<()>,
    active_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1);
        Self {
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            shutdown_tx: tx,
            active_tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Check if shutdown has been initiated
    pub fn is_shutdown(&self) -> bool {
        self.shutdown_signal.load(Ordering::Relaxed)
    }

    /// Subscribe to shutdown signal
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Initiate shutdown
    pub async fn shutdown(&self) {
        info!("Initiating graceful shutdown...");
        self.shutdown_signal.store(true, Ordering::Relaxed);
        let _ = self.shutdown_tx.send(());

        // Wait for all active tasks to complete
        let mut tasks = self.active_tasks.lock().await;
        for task in tasks.drain(..) {
            let _ = task.await;
        }

        info!("Graceful shutdown complete");
    }

    /// Register a task for shutdown tracking
    pub async fn register_task(&self, task: tokio::task::JoinHandle<()>) {
        self.active_tasks.lock().await.push(task);
    }

    /// Create a shutdown guard that returns when shutdown is initiated
    pub async fn wait_for_shutdown(&self) {
        let mut rx = self.subscribe();
        let _ = rx.recv().await;
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}
