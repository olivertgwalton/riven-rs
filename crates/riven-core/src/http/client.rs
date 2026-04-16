use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

use super::inflight::InFlightRequest;
use super::rate_limit::ServiceState;
use super::response::HttpResponseData;
use super::retry::{
    BACKOFF_BASE_SECS, MAX_RETRY_AFTER_SECS, execute_with_retry, parse_retry_after,
};
use super::{HttpServiceProfile, RateLimitedError};

#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
    services: Arc<DashMap<&'static str, Arc<ServiceState>>>,
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

    pub async fn send<F>(
        &self,
        profile: HttpServiceProfile,
        make_request: F,
    ) -> reqwest::Result<reqwest::Response>
    where
        F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    {
        let state = self.service_state(profile);
        execute_with_retry(&self.inner, Some(&state), profile.attempts, make_request).await
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
            let result = match self.send(profile, make_request).await {
                Ok(response) => HttpResponseData::from_response(response)
                    .await
                    .map(Arc::new)
                    .map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            state.finish(result.clone());
            self.inflight.remove(&dedupe_key);
            return result.map_err(anyhow::Error::msg);
        }

        state.wait().await.map_err(anyhow::Error::msg)
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
            .send_data(profile, Some(dedupe_key), make_request)
            .await?;

        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            let delay = parse_retry_after(response.headers())
                .unwrap_or_else(|| Duration::from_secs(BACKOFF_BASE_SECS))
                .min(Duration::from_secs(MAX_RETRY_AFTER_SECS));
            self.service_state(profile).register_retry_after(delay);
            tracing::warn!(
                service = profile.name,
                delay_secs = delay.as_secs(),
                "rate limited (429); freeing worker slot and deferring to job-level retry"
            );
            return Err(RateLimitedError.into());
        }

        response.error_for_status_ref()?;
        response.json()
    }

    fn service_state(&self, profile: HttpServiceProfile) -> Arc<ServiceState> {
        self.services
            .entry(profile.name)
            .or_insert_with(|| Arc::new(ServiceState::new(profile)))
            .clone()
    }
}
