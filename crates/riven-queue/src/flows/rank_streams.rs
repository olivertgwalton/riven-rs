use crate::application::download as app;
use crate::{JobQueue, RankStreamsJob};

pub async fn run(id: i64, job: &RankStreamsJob, queue: &JobQueue) {
    app::run_rank_streams(id, job, queue).await;
}
