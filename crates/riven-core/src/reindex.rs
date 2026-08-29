/// Runtime-configurable scheduling policy for delayed re-index jobs.
#[derive(Clone, Default)]
pub struct ReindexConfig {
    pub schedule_offset_minutes: u64,
    pub unknown_air_date_offset_days: u64,
    pub reconcile_scheduled_reindexes: bool,
}

impl From<&crate::settings::RivenSettings> for ReindexConfig {
    fn from(s: &crate::settings::RivenSettings) -> Self {
        Self {
            schedule_offset_minutes: s.schedule_offset_minutes,
            unknown_air_date_offset_days: s.unknown_air_date_offset_days,
            reconcile_scheduled_reindexes: s.reconcile_scheduled_reindexes,
        }
    }
}
