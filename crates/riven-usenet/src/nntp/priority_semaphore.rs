//! Semaphore that serves waiters by priority.
//!
//! Streaming BODY fetches queue as `High`; background ingest operations queue
//! as `Low`. When both queues are non-empty, `HIGH_ODDS`% of releases go to
//! the high-priority queue — keeping streaming responsive without completely
//! starving ingest. Modelled on nzbdav's `PrioritizedSemaphore`.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// Priority passed to every pool acquisition.
#[derive(Clone, Copy, Debug)]
pub enum Priority {
    /// Real-time streaming reads — served before background work.
    High,
    /// Background ingest (availability probes, RAR header fetches).
    Low,
}

/// Percentage of releases awarded to High-priority waiters when both queues
/// are non-empty. 80 ≈ nzbdav default.
const HIGH_ODDS: i32 = 80;

struct Inner {
    available: usize,
    high: VecDeque<oneshot::Sender<()>>,
    low: VecDeque<oneshot::Sender<()>>,
    /// Accumulated "debt" toward serving a High waiter.
    /// Increments by `HIGH_ODDS` each contested release; when it reaches 100
    /// we serve High and subtract 100. This produces exactly `HIGH_ODDS`%
    /// High service over many releases without any randomness.
    accumulated: i32,
}

impl Inner {
    /// Find the next live waiter and wake it; if none remain, increment
    /// `available`. Skips senders whose receivers were dropped (task cancel).
    fn wake_next(&mut self) {
        loop {
            let has_high = !self.high.is_empty();
            let has_low = !self.low.is_empty();

            match (has_high, has_low) {
                (false, false) => {
                    self.available += 1;
                    return;
                }
                (true, false) => {
                    if let Some(tx) = self.high.pop_front()
                        && tx.send(()).is_ok()
                    {
                        return;
                    }
                }
                (false, true) => {
                    if let Some(tx) = self.low.pop_front()
                        && tx.send(()).is_ok()
                    {
                        return;
                    }
                }
                (true, true) => {
                    self.accumulated += HIGH_ODDS;
                    let serve_high = self.accumulated >= 100;
                    if serve_high {
                        self.accumulated -= 100;
                        if let Some(tx) = self.high.pop_front()
                            && tx.send(()).is_ok()
                        {
                            return;
                        }
                    } else if let Some(tx) = self.low.pop_front()
                        && tx.send(()).is_ok()
                    {
                        return;
                    }
                }
            }
        }
    }
}

pub struct PrioritizedSemaphore {
    inner: Mutex<Inner>,
}

impl PrioritizedSemaphore {
    pub fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                available: permits,
                high: VecDeque::new(),
                low: VecDeque::new(),
                accumulated: 0,
            }),
        })
    }

    /// Number of permits that can be acquired without blocking right now.
    pub fn available_permits(&self) -> usize {
        self.inner.lock().available
    }

    /// Acquire a permit only if one is immediately available, without
    /// parking the caller in a wait queue. Used to gate dialing a brand
    /// new connection versus waiting for one to be freed.
    pub fn try_acquire_owned(self: &Arc<Self>) -> Option<OwnedPermit> {
        let mut g = self.inner.lock();
        if g.available > 0 {
            g.available -= 1;
            Some(OwnedPermit { sem: self.clone() })
        } else {
            None
        }
    }

    /// Acquire one permit. Parks the caller in the appropriate queue if none
    /// are immediately available.
    pub async fn acquire_owned(self: &Arc<Self>, priority: Priority) -> OwnedPermit {
        let rx = {
            let mut g = self.inner.lock();
            if g.available > 0 {
                g.available -= 1;
                return OwnedPermit { sem: self.clone() };
            }
            let (tx, rx) = oneshot::channel::<()>();
            match priority {
                Priority::High => g.high.push_back(tx),
                Priority::Low => g.low.push_back(tx),
            }
            rx
        };
        let _woken = rx.await;
        OwnedPermit { sem: self.clone() }
    }

    pub(crate) fn release(&self) {
        self.inner.lock().wake_next();
    }
}

/// A held permit. Releases back to the semaphore on drop.
pub struct OwnedPermit {
    sem: Arc<PrioritizedSemaphore>,
}

impl Drop for OwnedPermit {
    fn drop(&mut self) {
        self.sem.release();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn try_acquire_owned_succeeds_while_permits_available() {
        let sem = PrioritizedSemaphore::new(2);
        assert_eq!(sem.available_permits(), 2);

        let p1 = sem.try_acquire_owned();
        assert!(p1.is_some());
        assert_eq!(sem.available_permits(), 1);

        let p2 = sem.try_acquire_owned();
        assert!(p2.is_some());
        assert_eq!(sem.available_permits(), 0);
    }

    #[test]
    fn try_acquire_owned_fails_without_parking_when_exhausted() {
        let sem = PrioritizedSemaphore::new(1);
        let _held = sem.try_acquire_owned().expect("first acquire succeeds");
        assert_eq!(sem.available_permits(), 0);

        // Must return None immediately rather than blocking/queuing the
        // caller — this is what lets `NntpPool::acquire` use it as a
        // non-blocking gate between "reuse idle" and "wait for hand-off".
        assert!(sem.try_acquire_owned().is_none());
        assert_eq!(sem.available_permits(), 0);
    }

    #[test]
    fn dropping_permit_makes_it_available_again() {
        let sem = PrioritizedSemaphore::new(1);
        let held = sem.try_acquire_owned().expect("acquire succeeds");
        assert_eq!(sem.available_permits(), 0);

        drop(held);
        assert_eq!(sem.available_permits(), 1);
        assert!(sem.try_acquire_owned().is_some());
    }

    #[tokio::test]
    async fn acquire_owned_wakes_on_release_rather_than_polling() {
        let sem = PrioritizedSemaphore::new(1);
        let held = sem.try_acquire_owned().expect("acquire succeeds");

        let waiter_sem = sem.clone();
        let waiter = tokio::spawn(async move { waiter_sem.acquire_owned(Priority::High).await });

        // Give the spawned task a chance to park in the wait queue before
        // the permit is released.
        tokio::task::yield_now().await;

        drop(held);
        let _woken_permit = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter should be woken promptly by release, not left parked")
            .expect("waiter task should not panic");
    }
}
