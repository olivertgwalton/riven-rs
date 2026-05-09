use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

use super::inflight::InFlightRequest;
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

    pub async fn send<F>(
        &self,
        profile: HttpServiceProfile,
        make_request: F,
    ) -> reqwest::Result<reqwest::Response>
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
            // RAII guard so a cancelled leader (caller's future dropped while
            // `self.send(...)` is awaiting) still publishes a failure to the
            // watch channel and removes the dedupe entry. Without this, the
            // entry would stay in `inflight` and any future call with the
            // same key would `state.wait().await` forever — observed when
            // an iOS HTTP query was cancelled mid-flight: subsequent calls
            // hung 90s+ with the request never reaching the resolver and no
            // backend log at all.
            struct InflightGuard {
                state: Arc<InFlightRequest>,
                inflight: Arc<DashMap<String, Arc<InFlightRequest>>>,
                key: String,
                completed: bool,
            }
            impl Drop for InflightGuard {
                fn drop(&mut self) {
                    if !self.completed {
                        self.state
                            .finish(Err("inflight leader cancelled before completing request"
                                .to_string()));
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
                    .map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            state.finish(result.clone());
            guard.completed = true;
            // Guard's Drop still runs to remove the entry; setting `completed`
            // just prevents the redundant failure-publish on the channel.
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

    fn service_state(&self, profile: &HttpServiceProfile) -> Arc<ServiceState> {
        self.services
            .entry(profile.name.as_ref().to_owned())
            .or_insert_with(|| Arc::new(ServiceState::new(profile.clone())))
            .clone()
    }
}
