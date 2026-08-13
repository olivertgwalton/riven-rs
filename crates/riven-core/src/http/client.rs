use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

use super::inflight::{InFlightRequest, SharedError};
use super::rate_limit::ServiceState;
use super::response::HttpResponseData;
use super::retry::{BACKOFF_BASE_SECS, execute_with_retry, parse_rate_limit_pause};
use super::{HttpServiceProfile, RateLimitedError};

#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
    services: Arc<DashMap<String, Arc<ServiceState>>>,
    inflight: Arc<DashMap<String, Arc<InFlightRequest>>>,
}

impl HttpClient {
    pub fn new(inner: reqwest::Client) -> Self {
        Self {
            inner,
            services: Arc::new(DashMap::new()),
            inflight: Arc::new(DashMap::new()),
        }
    }

    pub fn raw(&self) -> &reqwest::Client {
        &self.inner
    }

    /// Fails with [`RateLimitedError`] when the service's limiter cannot supply
    /// a token promptly. Callers should requeue rather than retry inline — the
    /// point is to give the worker slot back, not to wait somewhere else.
    pub async fn send<F>(
        &self,
        profile: HttpServiceProfile,
        make_request: F,
    ) -> anyhow::Result<reqwest::Response>
    where
        F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    {
        let state = self.service_state(&profile);
        let response =
            execute_with_retry(&self.inner, Some(&state), profile.attempts, make_request).await?;

        if let Some(delay) = parse_rate_limit_pause(&profile, response.status(), response.headers())
        {
            state.register_retry_after(delay);
        }

        Ok(response)
    }

    pub async fn send_data<F>(
        &self,
        profile: HttpServiceProfile,
        dedupe_key: Option<String>,
        make_request: F,
    ) -> anyhow::Result<Arc<HttpResponseData>>
    where
        F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    {
        let Some(dedupe_key) = dedupe_key else {
            let response = self.send(profile, make_request).await?;
            return Ok(Arc::new(HttpResponseData::from_response(response).await?));
        };

        let (state, is_leader) = if let Some(existing) = self.inflight.get(&dedupe_key) {
            (existing.clone(), false)
        } else {
            let candidate = Arc::new(InFlightRequest::new());
            match self.inflight.entry(dedupe_key.clone()) {
                dashmap::mapref::entry::Entry::Occupied(entry) => (entry.get().clone(), false),
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    entry.insert(candidate.clone());
                    (candidate, true)
                }
            }
        };

        if is_leader {
            // RAII guard: a cancelled leader (caller future dropped mid-send)
            // must still publish failure and remove the dedupe entry, or every
            // future call with this key blocks on `state.wait()` forever.
            struct InflightGuard {
                state: Arc<InFlightRequest>,
                inflight: Arc<DashMap<String, Arc<InFlightRequest>>>,
                key: String,
                completed: bool,
            }
            impl Drop for InflightGuard {
                fn drop(&mut self) {
                    if !self.completed {
                        self.state.finish(Err(SharedError::message(
                            "inflight leader cancelled before completing request",
                        )));
                    }
                    self.inflight.remove(&self.key);
                }
            }

            let mut guard = InflightGuard {
                state: state.clone(),
                inflight: self.inflight.clone(),
                key: dedupe_key.clone(),
                completed: false,
            };

            let result = match self.send(profile, make_request).await {
                Ok(response) => HttpResponseData::from_response(response)
                    .await
                    .map(Arc::new)
                    .map_err(|e| SharedError::message(e.to_string())),
                Err(e) => Err(SharedError::new(&e)),
            };
            state.finish(result.clone());
            guard.completed = true;
            return result.map_err(SharedError::into_anyhow);
        }

        state.wait().await.map_err(SharedError::into_anyhow)
    }

    pub async fn get_json<T, F>(
        &self,
        profile: HttpServiceProfile,
        dedupe_key: String,
        make_request: F,
    ) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
        F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    {
        let response = self
            .send_data(profile.clone(), Some(dedupe_key), make_request)
            .await?;

        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            let delay = parse_rate_limit_pause(&profile, response.status(), response.headers())
                .unwrap_or_else(|| Duration::from_secs(BACKOFF_BASE_SECS));
            tracing::warn!(
                service = profile.name.as_ref(),
                delay_secs = delay.as_secs(),
                "rate limited (429); freeing worker slot and deferring to job-level retry"
            );
            return Err(RateLimitedError.into());
        }

        response.error_for_status_ref()?;
        response.json()
    }

    /// Hold back every later request on `profile` for `delay`, exactly as a
    /// 429 carrying that `Retry-After` would.
    ///
    /// [`send`](Self::send) can only recognise exhaustion that arrives as a
    /// status code. Some upstreams signal it in the body instead — Newznab
    /// reports a spent daily quota as HTTP 500 with an `<error>` document —
    /// and only the caller knows how to read that, so it needs a way to say
    /// so once it has.
    pub fn pause_service(&self, profile: &HttpServiceProfile, delay: Duration) {
        self.service_state(profile).register_retry_after(delay);
    }

    fn service_state(&self, profile: &HttpServiceProfile) -> Arc<ServiceState> {
        self.services
            .entry(profile.name.as_ref().to_owned())
            .or_insert_with(|| Arc::new(ServiceState::new(profile.clone())))
            .clone()
    }
}
