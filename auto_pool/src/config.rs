use std::time::Duration;

/// How an available object is selected; this does not order waiting consumers.
#[derive(Clone, Debug, Copy)]
pub enum PickStrategy {
    /// Select the object that was added or returned last.
    LIFO,
    /// Select an available object using a random index.
    RANDOM,
}

/// Checkout policy. Public fields support struct literals and update syntax.
#[derive(Clone, Debug, Copy)]
pub struct AutoPoolConfig {
    /// Overall checkout budget. Zero tries immediately; `Duration::MAX` and
    /// durations that overflow the platform's `Instant` wait indefinitely.
    pub wait_duration: Duration,
    /// Legacy async polling setting, retained for compatibility and ignored.
    pub lock_duration: Duration,
    /// Legacy async polling setting, retained for compatibility and ignored.
    pub sleep_duration: Duration,
    /// Selection among available objects, independent of waiter scheduling.
    pub pick_strategy: PickStrategy,
}

impl Default for AutoPoolConfig {
    fn default() -> Self {
        Self {
            wait_duration: Duration::MAX,
            lock_duration: Duration::from_millis(1),
            sleep_duration: Duration::from_millis(5),
            pick_strategy: PickStrategy::LIFO,
        }
    }
}
