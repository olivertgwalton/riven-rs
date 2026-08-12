use std::sync::Arc;

use tokio::sync::watch;

use super::{HttpResponseData, RateLimitedError};

/// A leader's failure, in a form its deduped followers can share.
///
/// `anyhow::Error` is neither `Clone` nor reconstructible, so what crosses the
/// channel is the message plus the one distinction callers actually branch on:
/// a rate-limit deferral means "requeue me", every other error means "this
/// failed". Flattening both into a bare string (as this did) silently turned
/// every follower's deferral into a generic failure.
#[derive(Clone, Debug)]
pub(super) struct SharedError {
    message: String,
    rate_limited: bool,
}

impl SharedError {
    pub(super) fn new(error: &anyhow::Error) -> Self {
        Self {
            message: error.to_string(),
            rate_limited: error.is::<RateLimitedError>(),
        }
    }

    pub(super) fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            rate_limited: false,
        }
    }

    pub(super) fn into_anyhow(self) -> anyhow::Error {
        if self.rate_limited {
            RateLimitedError.into()
        } else {
            anyhow::Error::msg(self.message)
        }
    }
}

/// Deduplicates concurrent in-flight requests. Uses `watch` so late subscribers
/// see the result immediately if the leader has already finished.
#[derive(Debug)]
pub(super) struct InFlightRequest {
    tx: watch::Sender<Option<Result<Arc<HttpResponseData>, SharedError>>>,
}

impl InFlightRequest {
    pub(super) fn new() -> Self {
        let (tx, _) = watch::channel(None);
        Self { tx }
    }

    pub(super) fn finish(&self, result: Result<Arc<HttpResponseData>, SharedError>) {
        self.tx.send_replace(Some(result));
    }

    pub(super) async fn wait(&self) -> Result<Arc<HttpResponseData>, SharedError> {
        let mut rx = self.tx.subscribe();
        rx.wait_for(std::option::Option::is_some)
            .await
            .map_err(|_e| {
                SharedError::message("inflight leader cancelled before completing request")
            })?
            .clone()
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::{InFlightRequest, SharedError};

    #[tokio::test]
    async fn late_subscriber_receives_completed_result() {
        let request = Arc::new(InFlightRequest::new());
        let late_subscriber = Arc::clone(&request);

        request.finish(Err(SharedError::message("completed before subscription")));

        let result = tokio::time::timeout(Duration::from_millis(50), late_subscriber.wait())
            .await
            .expect("late subscriber should not wait indefinitely");
        assert_eq!(
            result.unwrap_err().into_anyhow().to_string(),
            "completed before subscription"
        );
    }

    /// A follower must learn that the leader *deferred*, not merely that it
    /// failed: the two lead to opposite handling (requeue vs give up), and
    /// sharing only the message collapsed them.
    #[tokio::test]
    async fn a_deferral_stays_a_deferral_for_followers() {
        let request = Arc::new(InFlightRequest::new());
        let follower = Arc::clone(&request);

        request.finish(Err(SharedError::new(&super::RateLimitedError.into())));

        let error = follower
            .wait()
            .await
            .expect_err("follower should see the leader's failure")
            .into_anyhow();
        assert!(
            error.is::<super::RateLimitedError>(),
            "follower lost the rate-limit signal: {error}"
        );
    }
}
