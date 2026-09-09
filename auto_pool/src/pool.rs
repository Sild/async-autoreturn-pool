use super::pool_object::PoolObject;
use crate::config::{AutoPoolConfig, PickStrategy};
use parking_lot::lock_api::{MutexGuard, RawMutex};
use parking_lot::{Condvar, Mutex};
use rand::Rng;
use std::time::{Duration, Instant};

/// A pool of caller-supplied objects, returned automatically when wrappers drop.
///
/// See the [crate documentation](crate) for synchronous and asynchronous examples.
/// Checked-out objects borrow this pool; no replacement objects are allocated.
pub struct AutoPool<T: Send> {
    config: AutoPoolConfig,
    storage: Mutex<Vec<T>>,
    condvar: Condvar,
    #[cfg(feature = "async")]
    available: event_listener::Event,
}

impl<T: Send + 'static> AutoPool<T> {
    /// Create a pool with unlimited waiting and LIFO selection.
    pub fn new(items: impl IntoIterator<Item = T>) -> Self { Self::new_with_config(AutoPoolConfig::default(), items) }

    /// Create a pool with the supplied checkout policy and initial objects.
    pub fn new_with_config(config: AutoPoolConfig, items: impl IntoIterator<Item = T>) -> Self {
        let objects = items.into_iter().collect();
        Self {
            config,
            storage: Mutex::new(objects),
            condvar: Condvar::new(),
            #[cfg(feature = "async")]
            available: event_listener::Event::new(),
        }
    }

    /// Take an object, returning `None` when the configured overall budget expires.
    /// Zero tries immediately. Unlimited waits can block forever on an empty pool.
    /// Scheduling and mutex reacquisition may delay completion past the deadline.
    pub fn get(&'_ self) -> Option<PoolObject<'_, T>> { self.get_with_timeout(self.config.wait_duration) }

    /// Wait asynchronously for an object, using the configured overall timeout.
    ///
    /// Exhaustion suspends the future without blocking a thread. The storage mutex
    /// is held briefly for checkout; no mutex guard is held across an await.
    /// Dropping a pending future cancels its wait without removing an object.
    /// This works on any executor; finite timeouts use smol's timer.
    #[cfg(feature = "async")]
    pub async fn get_async(&'_ self) -> Option<PoolObject<'_, T>> {
        if self.config.wait_duration.is_zero() {
            return self.get();
        }
        let deadline = Instant::now().checked_add(self.config.wait_duration);
        loop {
            // Available items need neither a listener allocation nor a timer.
            if let Some(object) = self.extract_object(self.storage.lock()) {
                return Some(object);
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return None;
            }

            // Register before rechecking so a concurrent return cannot be missed.
            let listener = self.available.listen();
            if let Some(object) = self.extract_object(self.storage.lock()) {
                return Some(object);
            }
            if let Some(deadline) = deadline {
                let notified = smol::future::race(
                    async {
                        listener.await;
                        true
                    },
                    async {
                        smol::Timer::at(deadline).await;
                        false
                    },
                )
                .await;
                if !notified {
                    return None;
                }
            } else {
                listener.await;
            }
        }
    }

    /// Add or return an object and wake waiting consumers.
    pub fn add(&self, item: T) {
        self.storage.lock().push(item);
        self.condvar.notify_one();
        #[cfg(feature = "async")]
        // Separate returns must wake separate waiters. A cancelled notified
        // listener forwards its notification to another listener on drop.
        self.available.notify_additional(1);
    }

    /// Return the number of available objects, excluding checked-out objects.
    pub fn size(&self) -> usize { self.storage.lock().len() }

    /// Shrink storage to the number of currently available objects.
    /// This holds the storage mutex while reallocating.
    pub fn shrink_to_fit(&self) { self.storage.lock().shrink_to_fit(); }

    fn get_with_timeout(&'_ self, timeout: Duration) -> Option<PoolObject<'_, T>> {
        let deadline = Instant::now().checked_add(timeout);
        let mut locked_storage = if timeout.is_zero() {
            self.storage.try_lock()?
        } else if let Some(deadline) = deadline {
            self.storage.try_lock_until(deadline)?
        } else {
            self.storage.lock()
        };
        while locked_storage.is_empty() {
            if let Some(deadline) = deadline {
                if Instant::now() >= deadline || self.condvar.wait_until(&mut locked_storage, deadline).timed_out() {
                    return None;
                }
            } else {
                self.condvar.wait(&mut locked_storage);
            }
        }
        self.extract_object(locked_storage)
    }

    fn extract_object<R>(&'_ self, mut locked_storage: MutexGuard<R, Vec<T>>) -> Option<PoolObject<'_, T>>
    where
        R: RawMutex,
    {
        let inner = match self.config.pick_strategy {
            PickStrategy::LIFO => locked_storage.pop(),
            PickStrategy::RANDOM => match locked_storage.len() {
                0 => None,
                1 => locked_storage.pop(),
                items_cnt => {
                    let index = rand::rng().next_u64() as usize % items_cnt;
                    Some(locked_storage.swap_remove(index))
                }
            },
        };
        inner.map(|inner| PoolObject::new(inner, self))
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_notifications_do_not_restart_sync_budget() {
        let pool = AutoPool::<u8>::new_with_config(
            AutoPoolConfig {
                wait_duration: Duration::from_millis(40),
                ..Default::default()
            },
            [],
        );
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..25 {
                    std::thread::sleep(Duration::from_millis(10));
                    pool.condvar.notify_all();
                }
            });
            let start = Instant::now();
            assert!(pool.get().is_none());
            assert!(start.elapsed() >= Duration::from_millis(40));
            assert!(start.elapsed() < Duration::from_millis(200));
        });
    }

    #[test]
    fn test_sync_budget_includes_mutex_acquisition() {
        let pool = AutoPool::<u8>::new_with_config(
            AutoPoolConfig {
                wait_duration: Duration::from_millis(20),
                ..Default::default()
            },
            [1],
        );
        std::thread::scope(|scope| {
            let (tx, rx) = std::sync::mpsc::channel();
            let pool = &pool;
            scope.spawn(move || {
                let _guard = pool.storage.lock();
                tx.send(()).unwrap();
                std::thread::sleep(Duration::from_millis(200));
            });
            rx.recv().unwrap();
            let start = Instant::now();
            assert!(pool.get().is_none());
            assert!(start.elapsed() < Duration::from_millis(100));
        });
    }
}
