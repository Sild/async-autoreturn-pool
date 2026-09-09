# Benchmark workloads

Run a bounded local sample from the workspace root:

```sh
cargo bench -p auto_pool --bench multithread_push_pop --all-features -- \
  --warm-up-time 0.2 --measurement-time 0.5 --sample-size 10 --noplot
```

- `uncontended/{lifo,random,mutex_stack,allocate_1k}` measures checkout, a
  mutation of a reused 1 KiB byte buffer, and return. Pools have 64 objects.
  The mutex stack takes two locks and has no waiting or RAII wrapper. Allocation
  includes zero-initialization, mutation and deallocation of a fresh buffer.
- `contention/auto_pool/{1,4}` uses four synchronized workers. One item forces
  consumers to compete for availability; four items isolate storage contention
  without item exhaustion. Each measured round is 400 operations (100 per
  worker). The mutex-stack baseline has four objects; allocation has no shared
  state. Thread creation and join are excluded from the returned measurement.
  Start and finish barrier overhead is included once per sample.
- `exhaustion/try_empty` measures an unsuccessful zero-budget checkout.
  `exhaustion/wait_1ms` includes the configured timeout and scheduler latency.
- `async/handoff_from_thread` registers an empty-pool future before requesting
  an item from a producer thread. It sums the interval from just before `add()`
  to consumer resumption and `release()`. Request-channel delay, listener setup,
  and thread creation/join are excluded. It uses smol's local `block_on`, not
  a loaded multi-task executor, and has no configured timeout.

The harness uses `std::hint::black_box`. Grow-on-demand third-party pools are
not compared because their exhaustion behavior differs. Compare only matching
workloads and report the feature set, machine, toolchain and sample settings.
These measurements do not establish a universal speedup, waiter fairness,
tail latency under load, or superiority over other pool implementations.
`swap_remove` is a readability simplification, not a measured optimization.

## Local sample: 2026-09-10

Apple M3 Max, macOS arm64, rustc 1.97.1, `--all-features`, Criterion 0.8.2;
10 samples, 0.2 s warmup and 0.5 s measurement per workload. Point estimates
from bounded runs (contention rerun after adding the untimed readiness
barrier; not a before/after comparison):

| Workload | Estimate |
| --- | ---: |
| Uncontended LIFO | 32.6 ns / operation |
| Uncontended random | 37.8 ns / operation |
| Uncontended mutex stack | 4.94 ns / operation |
| Uncontended allocate 1 KiB | 25.5 ns / operation |
| Four workers, one pooled item | 57.3 us / 400 operations |
| Four workers, four pooled items | 59.8 us / 400 operations |
| Four workers, mutex stack | 4.50 us / 400 operations |
| Four workers, allocation | 17.6 us / 400 operations |
| Empty, zero budget | 39.6 ns / attempt |
| Empty, 1 ms budget | 1.25 ms / attempt |
| Async handoff from a thread | 2.84 us / handoff |

The minimal stack and allocation baselines are cheaper in this deliberately
small workload. They omit waiting, notifications and borrowed RAII semantics.
The one-item and four-item cases also differ in scheduling and availability;
these results do not predict performance for a particular application.
