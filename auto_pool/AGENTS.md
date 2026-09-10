# auto_pool maintainer guide

- This public crate owns caller-supplied object storage and borrowed RAII
  checkout. Keep `Mutex<Vec<T>>`, `pool::AutoPool`, `config::AutoPoolConfig`,
  `config::PickStrategy`, and `pool_object::PoolObject` as the canonical paths.
  No factories, background workers, unsafe storage, or alternate public APIs.
- Every successful checkout removes exactly one item. Drop returns its mutated
  value exactly once; `release()` removes it permanently. The wrapper borrows
  the pool. Never hold a storage guard across an await or user work.
- Async waiters register then recheck storage. Each return sends an additional
  notification; cancellation must forward an unconsumed notification. Sync
  and async consumers can race for items, with no promised waiter fairness.
- Use one deadline across retries. Zero attempts immediately; an unrepresentable
  deadline means unlimited waiting. Document scheduling/short mutex limitations.
  Do not reintroduce polling via the ignored public `lock_duration` or
  `sleep_duration` fields or remove these fields in a compatible release.
- The `async` feature is opt-in and executor-independent. Do not add a self
  dev-dependency that enables it. Gate async examples, docs, and tests explicitly.
- Library MSRV is 1.85 because rand 0.10 requires it; dev targets use current
  stable (Criterion needs 1.86+). Keep the workspace manifest and README aligned.
  Cargo.lock is intentionally ignored. Release-plz owns version changes.
- Extend existing config/methods only for a demonstrated contract. Public fields,
  paths, bounds and feature names are compatibility commitments. Update README,
  rustdoc, examples, tests and this guide together when contracts change.
- Fast checks: `cargo test -p auto_pool --no-default-features` and
  `cargo test -p auto_pool --no-default-features --features async`.
- Full affected checks, from the workspace root:
  ```sh
  cargo test --workspace
  cargo test --workspace --all-features
  cargo test -p auto_pool --examples --all-features
  cargo run -p auto_pool --example main
  cargo run -p auto_pool --example main --features async
  cargo +nightly fmt --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  RUSTDOCFLAGS="-D warnings" cargo doc -p auto_pool --no-deps --all-features
  RUSTDOCFLAGS="-D warnings" cargo doc -p auto_pool --no-deps --no-default-features
  cargo +1.85.0 check -p auto_pool --lib --no-default-features
  cargo +1.85.0 check -p auto_pool --lib --all-features
  cargo package -p auto_pool --list
  cargo package -p auto_pool
  git diff --check
  ```
- For dependency/package changes, extract `target/package/auto_pool-*.crate`
  into a temporary directory outside the workspace. Run a consumer on Rust
  1.85 with default dependency features, `default-features = false`, and that
  setting plus `features = ["async"]`. Exercise struct literals, checkout,
  mutation, drop, add and release. Inspect the normalized manifest and package
  inventory. Remove the temporary consumer afterward.
- Require one independent review for substantive concurrency/dependency changes.
  Benchmarks in BENCHMARKS.md are scoped measurements, not universal speed claims.
