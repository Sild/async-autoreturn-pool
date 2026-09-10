use auto_pool::config::{AutoPoolConfig, PickStrategy};
use auto_pool::pool::AutoPool;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use parking_lot::Mutex;
use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

const WORKERS: usize = 4;
const BATCH: u64 = 100;
const BUFFER_SIZE: usize = 1024;

fn buffer() -> Vec<u8> { vec![0; BUFFER_SIZE] }

fn touch(buffer: &mut [u8]) {
    buffer[0] = buffer[0].wrapping_add(1);
    black_box(buffer);
}

// Thread startup and joining are excluded from the returned duration. The start
// and finish barriers are measured once per sample, amortized over all rounds.
fn parallel_rounds(rounds: u64, operation: impl Fn() + Sync) -> Duration {
    let ready = Barrier::new(WORKERS + 1);
    let start = Barrier::new(WORKERS + 1);
    let finish = Barrier::new(WORKERS + 1);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..WORKERS)
            .map(|_| {
                let (ready, start, finish, operation) = (&ready, &start, &finish, &operation);
                scope.spawn(move || {
                    ready.wait();
                    start.wait();
                    for _ in 0..rounds {
                        for _ in 0..BATCH {
                            operation();
                        }
                    }
                    finish.wait();
                })
            })
            .collect();
        ready.wait();
        let before = Instant::now();
        start.wait();
        finish.wait();
        let elapsed = before.elapsed();
        for worker in workers {
            worker.join().unwrap();
        }
        elapsed
    })
}

fn uncontended(c: &mut Criterion) {
    let mut group = c.benchmark_group("uncontended");
    for (name, strategy) in [("lifo", PickStrategy::LIFO), ("random", PickStrategy::RANDOM)] {
        let pool = AutoPool::new_with_config(
            AutoPoolConfig {
                pick_strategy: strategy,
                ..Default::default()
            },
            (0..64).map(|_| buffer()),
        );
        group.bench_function(name, |b| b.iter(|| touch(&mut pool.get().unwrap())));
    }
    let stack = Mutex::new((0..64).map(|_| buffer()).collect::<Vec<_>>());
    group.bench_function("mutex_stack", |b| {
        b.iter(|| {
            let mut item = stack.lock().pop().unwrap();
            touch(&mut item);
            stack.lock().push(item);
        });
    });
    group.bench_function("allocate_1k", |b| b.iter(|| touch(&mut black_box(buffer()))));
    group.finish();
}

fn contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention");
    group.throughput(Throughput::Elements(WORKERS as u64 * BATCH));
    for size in [1, WORKERS] {
        let pool = AutoPool::new((0..size).map(|_| buffer()));
        group.bench_with_input(BenchmarkId::new("auto_pool", size), &size, |b, _| {
            b.iter_custom(|rounds| parallel_rounds(rounds, || touch(&mut pool.get().unwrap())));
        });
    }
    // Enough items for every worker; this baseline has no exhaustion policy.
    let stack = Mutex::new((0..WORKERS).map(|_| buffer()).collect::<Vec<_>>());
    group.bench_function("mutex_stack_4", |b| {
        b.iter_custom(|rounds| {
            parallel_rounds(rounds, || {
                let mut item = stack.lock().pop().unwrap();
                touch(&mut item);
                stack.lock().push(item);
            })
        });
    });
    group.bench_function("allocate_1k", |b| {
        b.iter_custom(|rounds| parallel_rounds(rounds, || touch(&mut black_box(buffer()))));
    });
    group.finish();
}

fn exhaustion(c: &mut Criterion) {
    let pool = AutoPool::<Vec<u8>>::new_with_config(
        AutoPoolConfig {
            wait_duration: Duration::ZERO,
            ..Default::default()
        },
        [],
    );
    c.bench_function("exhaustion/try_empty", |b| b.iter(|| assert!(black_box(pool.get()).is_none())));
    let pool = AutoPool::<Vec<u8>>::new_with_config(
        AutoPoolConfig {
            wait_duration: Duration::from_millis(1),
            ..Default::default()
        },
        [],
    );
    c.bench_function("exhaustion/wait_1ms", |b| b.iter(|| assert!(black_box(pool.get()).is_none())));
}

#[cfg(feature = "async")]
fn async_handoff(c: &mut Criterion) {
    // Poll to Pending before asking the producer for an item. Measure from just
    // before add() to consumer resumption, excluding the request-channel delay.
    let pool = AutoPool::new([]);
    let (request, requests) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let pool = &pool;
        let producer = scope.spawn(move || {
            while requests.recv().is_ok() {
                pool.add(Instant::now());
            }
        });
        c.bench_function("async/handoff_from_thread", |b| {
            b.iter_custom(|iterations| {
                smol::block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let mut get = Box::pin(pool.get_async());
                        assert!(smol::future::poll_once(&mut get).await.is_none());
                        request.send(()).unwrap();
                        let sent = get.await.unwrap().release();
                        elapsed += sent.elapsed();
                        black_box(sent);
                    }
                    elapsed
                })
            });
        });
        drop(request);
        producer.join().unwrap();
    });
}

fn benchmarks(c: &mut Criterion) {
    uncontended(c);
    contention(c);
    exhaustion(c);
    #[cfg(feature = "async")]
    async_handoff(c);
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
