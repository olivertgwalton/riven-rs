//! Degraded playback: serve a hole where a permanently dead article should
//! have been, instead of failing the read and stopping the stream.
//!
//! A single dead article in a 40 GB remux used to end playback. The read
//! returned `ArticleNotFound`, the FUSE read failed, and the player stopped —
//! even though the missing bytes are a fraction of a second of video that a
//! decoder would have skated over. Comet calls the same mechanism degraded
//! playback (`allow_degraded_playback` / `salvage_extents` in its
//! `session.rs`) and enables it by default; this is riven's version of it.
//!
//! Three rules keep it from turning a broken release into silent corruption:
//!
//! 1. **Only for articles confirmed dead on every provider.** A timeout, a
//!    transport error or a single provider's `430` is not a hole — it is a
//!    fetch that should be retried or failed. Only the pool's permanent-missing
//!    set qualifies, and it is only written when every provider agreed.
//! 2. **Only when the hole's exact length is known.** A hole of the wrong
//!    length shifts every byte after it within the read, which moves an MKV
//!    EBML signature or an MP4 `ftyp` box and fails the player's codec probe.
//!    Better to fail the read than to serve plausible-looking rubbish.
//! 3. **[`MAX_HOLES_PER_READ`] at most.** A couple of dead articles is a
//!    release worth playing; a read that needs more than that is a release
//!    worth re-grabbing, and it fails so that the usual read-failure handling
//!    gets to do exactly that.
//!
//! A salvaged read is deliberately **not** reported as a dead segment.
//! `report_dead_segment` drives read-time repair, which blacklists the release
//! and re-grabs it immediately, on the title currently being watched — so
//! reporting a hole would swap the file out from under the viewer and undo the
//! whole point of continuing. The scheduled availability scanner still finds
//! the release on its own tick, and defers its repair for as long as anything
//! is streaming it (see `ActiveStreams::is_streaming`), so the swap happens
//! once nobody is watching rather than never.

use std::sync::atomic::{AtomicBool, Ordering};

/// Holes one read may paper over before it gives up and fails.
///
/// One read is one read-ahead unit, which on an article origin is about one
/// article — so this is "skip a couple of dead articles", not "skip a couple
/// per file".
pub const MAX_HOLES_PER_READ: usize = 2;

/// On unless turned off, matching comet's `USENET_DEGRADED_PLAYBACK_ENABLED`.
/// The alternative default is that one dead article anywhere in a file ends
/// playback, which is the behaviour this exists to replace.
///
/// This is the value in force before settings are read, so it has to agree
/// with the `degradedplayback` default in `plugin-usenet` and with how
/// `riven-app` reads it — otherwise reads taken during startup would follow a
/// different policy from every read after.
static ENABLED: AtomicBool = AtomicBool::new(true);

/// Set from the `degradedplayback` setting. Read per-read, so a change takes
/// effect on the next read without restarting anything.
pub fn set_degraded_playback(enabled: bool) {
    if ENABLED.swap(enabled, Ordering::Relaxed) != enabled {
        tracing::info!(enabled, "usenet degraded playback setting changed");
    }
}

fn degraded_playback_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// One read's hole budget.
///
/// The enabled flag is snapshotted at construction so a settings flip mid-read
/// cannot make one half of an assembled buffer follow a different policy from
/// the other.
pub(crate) struct ReadSalvage {
    enabled: bool,
    used: usize,
}

impl ReadSalvage {
    pub(crate) fn new() -> Self {
        Self {
            enabled: degraded_playback_enabled(),
            used: 0,
        }
    }

    /// A budget that never permits a hole.
    ///
    /// Ingest and header probing take this one. They exist to decide whether a
    /// release is worth committing to, and a hole would answer that question
    /// with bytes riven invented — a dead release would pass ingest and fail
    /// later, which is the failure mode availability probing exists to prevent.
    pub(crate) fn refusing() -> Self {
        Self {
            enabled: false,
            used: 0,
        }
    }

    /// A budget that permits holes regardless of the process-wide flag, so a
    /// test can exercise both policies without mutating global state that
    /// every other test in the binary shares.
    #[cfg(test)]
    pub(crate) fn allowing() -> Self {
        Self {
            enabled: true,
            used: 0,
        }
    }

    /// Claim a hole of `len` bytes. `false` means the caller must propagate the
    /// original error: degraded playback is off, the length is unknown, or this
    /// read has already used its budget.
    pub(crate) fn claim(&mut self, len: u64) -> bool {
        if !self.enabled || len == 0 || self.used >= MAX_HOLES_PER_READ {
            return false;
        }
        self.used += 1;
        true
    }

    /// Holes this read has papered over.
    pub(crate) fn used(&self) -> usize {
        self.used
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_gets_a_couple_of_holes_and_no_more() {
        let mut salvage = ReadSalvage {
            enabled: true,
            used: 0,
        };
        for _ in 0..MAX_HOLES_PER_READ {
            assert!(salvage.claim(700_000));
        }
        assert!(
            !salvage.claim(700_000),
            "a read past its budget must fail rather than keep faking bytes"
        );
        assert_eq!(salvage.used(), MAX_HOLES_PER_READ);
    }

    /// A hole of unknown length would shift every byte after it in the read.
    #[test]
    fn an_unknown_length_is_never_salvaged() {
        let mut salvage = ReadSalvage {
            enabled: true,
            used: 0,
        };
        assert!(!salvage.claim(0));
        assert_eq!(salvage.used(), 0);
    }

    #[test]
    fn a_disabled_budget_refuses_everything() {
        let mut salvage = ReadSalvage::refusing();
        assert!(!salvage.claim(700_000));
    }
}
