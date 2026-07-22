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
        self.tx.send_replace(Some(result));
    }

    pub(super) async fn wait(&self) -> Result<Arc<HttpResponseData>, String> {
        let mut rx = self.tx.subscribe();
        rx.wait_for(std::option::Option::is_some)
            .await
            .map_err(|_e| "inflight leader cancelled before completing request".to_string())?
            .clone()
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::InFlightRequest;

    #[tokio::test]
    async fn late_subscriber_receives_completed_result() {
        let request = Arc::new(InFlightRequest::new());
        let late_subscriber = Arc::clone(&request);

        request.finish(Err("completed before subscription".to_string()));

        let result = tokio::time::timeout(Duration::from_millis(50), late_subscriber.wait())
            .await
            .expect("late subscriber should not wait indefinitely");
        assert_eq!(result.unwrap_err(), "completed before subscription");
    }
}
