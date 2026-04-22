use crate::settings::PluginSettings;

pub struct PluginContext {
    pub settings: PluginSettings,
    pub http: crate::http::HttpClient,
    pub db_pool: sqlx::PgPool,
    pub redis: redis::aio::ConnectionManager,
    pub vfs_mount_path: String,
}

impl PluginContext {
    pub fn new(
        settings: PluginSettings,
        http: crate::http::HttpClient,
        db_pool: sqlx::PgPool,
        redis: redis::aio::ConnectionManager,
        vfs_mount_path: String,
    ) -> Self {
        Self {
            settings,
            http,
            db_pool,
            redis,
            vfs_mount_path,
        }
    }

    pub fn require_setting(&self, key: &str) -> anyhow::Result<&str> {
        self.settings
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("{key} not configured"))
    }
}

pub async fn validate_api_key(
    http: &crate::http::HttpClient,
    settings: &PluginSettings,
    key_name: &str,
    url: &str,
    header: &str,
) -> anyhow::Result<bool> {
    let api_key = match settings.get(key_name) {
        Some(k) => k,
        None => return Ok(false),
    };
    let resp = http
        .send(
            crate::http::HttpServiceProfile::new("plugin_validation"),
            |client| client.get(url).header(header, api_key),
        )
        .await?;
    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return Ok(false);
    }
    Ok(true)
}
