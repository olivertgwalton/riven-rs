use crate::application::scrape as app;
use crate::{JobQueue, ParseScrapeResultsJob};

pub async fn run(id: i64, job: &ParseScrapeResultsJob, queue: &JobQueue) {
    app::parse_results(id, job, queue).await;
}
