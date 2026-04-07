use crate::application::index as app;
use crate::{IndexJob, IndexPluginJob, JobQueue};

pub async fn run(job: &IndexJob, queue: &JobQueue) {
    app::start(job, queue).await;
}

pub async fn run_plugin(job: &IndexPluginJob, queue: &JobQueue) {
    app::handle_plugin(job, queue).await;
}

pub async fn finalize(id: i64, queue: &JobQueue) {
    app::finalize(id, queue).await;
}
