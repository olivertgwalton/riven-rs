use std::sync::Arc;

use tokio::sync::watch;

use super::HttpResponseData;

/// Deduplicates concurrent in-flight requests. Uses `watch` so late subscribers
/// see the result immediately if the leader has already finished.
#[derive(Debug)]
pub(super) struct InFlightRequest {
    tx: watch::Sender<Option<Result<Arc<HttpResponseData>, String>>>,
}

impl InFlightRequest {
    pub(super) fn new() -> Self {
        let (tx, _) = watch::channel(None);
        Self { tx }
    }

    pub(super) fn finish(&self, result: Result<Arc<HttpResponseData>, String>) {
        drop(self.tx.send(Some(result)));
    }

    pub(super) async fn wait(&self) -> Result<Arc<HttpResponseData>, String> {
        let mut rx = self.tx.subscribe();
        rx.wait_for(|v| v.is_some())
            .await
            .map_err(|_e| "inflight leader cancelled before completing request".to_string())?
            .clone()
            .unwrap()
    }
}
