# auto_pool

A small, thread-safe pool of caller-supplied objects. `AutoPool<T>` stores a
`Mutex<Vec<T>>`; checkout returns a borrowed `PoolObject<'_, T>` that implements
`Deref` and `DerefMut`. Dropping the wrapper returns the object, including any
mutations. Calling `release()` takes ownership permanently instead.

```toml
[dependencies]
auto_pool = "0.3.3"
# Enable asynchronous checkout when needed:
# auto_pool = { version = "0.3.3", features = ["async"] }
```

```rust
use auto_pool::config::AutoPoolConfig;
use auto_pool::pool::AutoPool;
use std::time::Duration;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let pool = AutoPool::new_with_config(
    AutoPoolConfig {
        wait_duration: Duration::from_millis(20),
        ..Default::default()
    },
    [String::with_capacity(1024)],
);
{
    let mut buffer = pool.get().ok_or("pool exhausted")?;
    buffer.push_str("reused allocation");
} // the buffer returns here
let buffer = pool.get().ok_or("pool exhausted")?.release();
assert_eq!(buffer, "reused allocation");
assert_eq!(pool.size(), 0);
pool.add(buffer);
# Ok(())
# }
```

`AutoPool::new(items)` uses the default configuration. Constructors are
infallible. `add(item)` supplies or returns an item; `size()` counts only
available items; `shrink_to_fit()` shrinks available storage, not checked-out
objects. Items must be `Send + 'static`. The wrapper borrows its pool, so the
pool must outlive every checkout. Share the pool across threads with `Arc` or
scoped threads. The pool never creates replacement objects.

## Waiting and selection

`get()` waits up to `wait_duration` and returns `None` on timeout. Retries use
one overall deadline, including initial synchronous mutex acquisition.
`Duration::ZERO` performs an immediate attempt; `Duration::MAX` (and other
values that overflow the platform's `Instant`) means an unlimited wait.

Timeouts are not hard real-time guarantees: scheduling and mutex reacquisition
can delay completion. An object available when a waiter resumes may win a race
with its deadline. There is no FIFO fairness guarantee between consumers.
Avoid holding all objects while requesting another with an unlimited wait.

`PickStrategy::LIFO` is the default. `PickStrategy::RANDOM` selects from the
available items using a random index; it changes object selection, not waiter
priority.

## Async feature

The optional `async` feature adds `get_async()`. It works on any executor;
finite deadlines use smol's timer. Exhausted pools suspend through
[event-listener](https://docs.rs/event-listener/5.4.2/event_listener/struct.Event.html)
notifications, with no blocking item wait or sleep polling. Checkout still
briefly locks the storage mutex, as do `add()` and wrapper drop. Avoid expensive
pool maintenance on latency-sensitive executor threads.

```rust
# #[cfg(feature = "async")]
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# smol::block_on(async {
use auto_pool::pool::AutoPool;
let pool = AutoPool::new([vec![0u8; 1024]]);
let mut buffer = pool.get_async().await.ok_or("pool exhausted")?;
buffer[0] = 42;
drop(buffer); // also wakes waiting consumers
# Ok(())
# })
# }
# #[cfg(not(feature = "async"))]
# fn main() {}
```

Dropping a pending checkout cancels it without consuming an item. Sync and
async consumers can use the same pool. `lock_duration` and `sleep_duration`
remain public for source compatibility but are ignored; `wait_duration` is
the only waiting setting. Their old defaults remain unchanged.

## Toolchains and development

The library requires **Rust 1.85**, matching its existing `rand 0.10`
dependency. The old 1.81 declaration was inaccurate. Development tests and
benchmarks use current stable Rust (Criterion 0.8 requires at least 1.86), and
formatting uses nightly. Release-plz manages crate versions.

See [AGENTS.md](AGENTS.md) for maintainer checks and
[BENCHMARKS.md](BENCHMARKS.md) for benchmark workloads and measurement limits.
