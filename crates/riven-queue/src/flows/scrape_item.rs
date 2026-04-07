use crate::application::scrape as app;
use crate::{JobQueue, ParseScrapeResultsJob, ScrapeJob, ScrapePluginJob};

pub async fn run(id: i64, job: &ScrapeJob, queue: &JobQueue) {
    app::start(id, job, queue).await;
}

pub async fn run_plugin(job: &ScrapePluginJob, queue: &JobQueue) {
    app::handle_plugin(job, queue).await;
}

pub async fn finalize(id: i64, requested_title: &str, auto_download: bool, queue: &JobQueue) {
    app::finalize(id, requested_title, auto_download, queue).await;
}

pub async fn parse_results(id: i64, job: &ParseScrapeResultsJob, queue: &JobQueue) {
    app::parse_results(id, job, queue).await;
}
