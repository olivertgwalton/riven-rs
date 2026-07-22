//! # Logging convention
//!
//! Every log line in the item pipeline starts with the stage it came from, so
//! a log can be followed (or grepped) without opening the source:
//!
//! - `index:` — fetching metadata from the metadata providers
//! - `scrape:` — asking the scrapers for releases
//! - `parse:` — parsing/ranking those releases into download candidates
//! - `download:` — picking a candidate and getting it from a debrid service
//! - `pipeline:` — the per-item state machine deciding what happens next
//! - `library sweep:` — the periodic pass that re-queues incomplete items
//!
//! Messages state what happened *and* its consequence for the item ("release
//! rejected, its files did not match this episode"), rather than naming the
//! internal function that failed. Anything identifying an item carries both
//! `id` and `title` — an id on its own means nothing to a reader.

pub mod download;
pub mod index;
pub mod process_media_item;
pub mod request_content;
pub mod scrape;
