//! Bridge from the broadcast `event_tx` channel to [`MainOrchestrator`].
//!
//! Kept deliberately tiny — all event-handling logic now lives in
//! `riven_queue::main_orchestrator::MainOrchestrator::on_event`. This module
//! exists only to spawn the listener loop and translate broadcast channel
//! signals into orchestrator calls.

use std::sync::Arc;

use riven_queue::JobQueue;
use riven_queue::main_orchestrator::MainOrchestrator;
use tokio::sync::broadcast;

pub fn start(job_queue: Arc<JobQueue>) {
    let mut rx = job_queue.event_tx.subscribe();
    let orchestrator = MainOrchestrator::new(Arc::clone(&job_queue));
    tokio::spawn(async move {
        loop {
            let event = match rx.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        dropped = n,
                        "event controller lagged; {} events dropped — items may be stuck until the next retry cycle",
                        n
                    );
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            orchestrator.on_event(&event).await;
        }
    });
}
