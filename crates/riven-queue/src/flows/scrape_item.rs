use crate::application::scrape as app;
use crate::{JobQueue, ParseScrapeResultsJob, ScrapeJob, ScrapePluginJob};

pub async fn run(id: i64, job: &ScrapeJob, queue: &JobQueue) {
    app::start(id, job, queue).await;
}

pub async fn run_plugin(job: &ScrapePluginJob, queue: &JobQueue) {
    app::handle_plugin(job, queue).await;
}

pub async fn finalize(job: &ScrapeJob, queue: &JobQueue) {
    app::finalize(job, queue).await;
}

pub async fn parse_results(id: i64, job: &ParseScrapeResultsJob, queue: &JobQueue) {
    app::parse_results(id, job, queue).await;
}
