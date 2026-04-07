pub(crate) mod helpers;
pub(crate) mod persist;

use crate::application::download as app;
use crate::{DownloadJob, JobQueue};

pub async fn run(id: i64, job: &DownloadJob, queue: &JobQueue) {
    app::run(id, job, queue).await;
}
