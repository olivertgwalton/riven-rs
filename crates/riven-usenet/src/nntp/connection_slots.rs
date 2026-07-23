//! Provider connection-slot accounting.
//!
//! Scheduling belongs to `NntpPool`; this type only enforces the provider's
//! configured socket ceiling. Idle sockets keep their permit until reaped.

use std::sync::Arc;

use parking_lot::Mutex;

pub struct ConnectionSlots {
    available: Mutex<usize>,
}

impl ConnectionSlots {
    pub fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            available: Mutex::new(permits),
        })
    }

    pub fn available_permits(&self) -> usize {
        *self.available.lock()
    }

    pub fn try_acquire_owned(self: &Arc<Self>) -> Option<OwnedPermit> {
        let mut available = self.available.lock();
        if *available == 0 {
            return None;
        }
        *available -= 1;
        Some(OwnedPermit {
            slots: self.clone(),
        })
    }

    fn release(&self) {
        *self.available.lock() += 1;
    }
}

pub struct OwnedPermit {
    slots: Arc<ConnectionSlots>,
}

impl Drop for OwnedPermit {
    fn drop(&mut self) {
        self.slots.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permits_track_open_connections() {
        let slots = ConnectionSlots::new(2);
        let first = slots.try_acquire_owned().expect("first slot");
        let second = slots.try_acquire_owned().expect("second slot");
        assert_eq!(slots.available_permits(), 0);
        assert!(slots.try_acquire_owned().is_none());

        drop(first);
        assert_eq!(slots.available_permits(), 1);
        drop(second);
        assert_eq!(slots.available_permits(), 2);
    }
}
