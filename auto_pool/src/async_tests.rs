use crate::config::AutoPoolConfig;
use crate::pool::AutoPool;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
}

fn poll<F: Future>(future: Pin<&mut F>, wake: &Arc<WakeCount>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(&Waker::from(wake.clone())))
}

#[test]
fn test_multiple_returns_wake_multiple_waiters() {
    let pool = AutoPool::new([]);
    let wakes: Vec<_> = (0..3).map(|_| Arc::new(WakeCount::default())).collect();
    let mut gets: Vec<_> = (0..3).map(|_| Box::pin(pool.get_async())).collect();
    for (get, wake) in gets.iter_mut().zip(&wakes) {
        assert!(poll(get.as_mut(), wake).is_pending());
    }
    for value in 0..3 {
        pool.add(value);
    }
    let mut values = Vec::new();
    for (get, wake) in gets.iter_mut().zip(&wakes) {
        assert!(wake.0.load(Ordering::SeqCst) > 0);
        match poll(get.as_mut(), wake) {
            Poll::Ready(Some(item)) => values.push(item.release()),
            _ => panic!("a returned object was stranded"),
        }
    }
    values.sort_unstable();
    assert_eq!(values, [0, 1, 2]);
    assert_eq!(pool.size(), 0);
}

#[test]
fn test_cancelling_notified_waiter_forwards_wakeup() {
    let pool = AutoPool::new([]);
    let wake1 = Arc::new(WakeCount::default());
    let wake2 = Arc::new(WakeCount::default());
    let mut first = Box::pin(pool.get_async());
    let mut second = Box::pin(pool.get_async());
    assert!(poll(first.as_mut(), &wake1).is_pending());
    assert!(poll(second.as_mut(), &wake2).is_pending());
    pool.add(42);
    assert!(wake1.0.load(Ordering::SeqCst) > 0);
    drop(first);
    assert!(wake2.0.load(Ordering::SeqCst) > 0);
    assert_eq!(smol::block_on(second).unwrap().release(), 42);
    assert_eq!(pool.size(), 0);
}

#[test]
fn test_cancelling_unnotified_waiter_preserves_future_returns() {
    let pool = AutoPool::new([]);
    let mut first = Box::pin(pool.get_async());
    assert!(smol::block_on(smol::future::poll_once(&mut first)).is_none());
    drop(first);
    pool.add(42);
    assert_eq!(smol::block_on(pool.get_async()).unwrap().release(), 42);
}

#[test]
fn test_sync_consumer_can_win_async_notification() {
    let pool = AutoPool::new([]);
    let wake = Arc::new(WakeCount::default());
    let mut get = Box::pin(pool.get_async());
    assert!(poll(get.as_mut(), &wake).is_pending());
    pool.add(1);
    assert_eq!(pool.get().unwrap().release(), 1);
    assert!(poll(get.as_mut(), &wake).is_pending());
    let prior_wakes = wake.0.load(Ordering::SeqCst);
    pool.add(2);
    assert!(wake.0.load(Ordering::SeqCst) > prior_wakes);
    assert_eq!(smol::block_on(get).unwrap().release(), 2);
}

#[test]
fn test_async_zero_and_max_timeouts() {
    for timeout in [Duration::ZERO, Duration::MAX] {
        let pool = AutoPool::new_with_config(
            AutoPoolConfig {
                wait_duration: timeout,
                ..Default::default()
            },
            [1],
        );
        assert_eq!(smol::block_on(pool.get_async()).unwrap().release(), 1);
        let mut get = Box::pin(pool.get_async());
        if timeout.is_zero() {
            assert!(smol::block_on(get).is_none());
        } else {
            assert!(smol::block_on(smol::future::poll_once(&mut get)).is_none());
            pool.add(2);
            assert_eq!(smol::block_on(get).unwrap().release(), 2);
        }
    }
}

#[test]
fn test_async_timeout_wakes_executor() {
    let timeout = Duration::from_millis(20);
    let pool = AutoPool::<u8>::new_with_config(
        AutoPoolConfig {
            wait_duration: timeout,
            sleep_duration: Duration::from_secs(60),
            ..Default::default()
        },
        [],
    );
    let start = Instant::now();
    assert!(smol::block_on(pool.get_async()).is_none());
    assert!(start.elapsed() >= timeout);
    assert!(start.elapsed() < Duration::from_millis(200));
}

#[test]
fn test_async_lost_handoffs_do_not_restart_timeout() {
    let timeout = Duration::from_millis(40);
    let pool = AutoPool::new_with_config(
        AutoPoolConfig {
            wait_duration: timeout,
            ..Default::default()
        },
        [],
    );
    let wake = Arc::new(WakeCount::default());
    let mut get = Box::pin(pool.get_async());
    let start = Instant::now();
    assert!(poll(get.as_mut(), &wake).is_pending());
    loop {
        pool.add(1);
        assert_eq!(pool.get().unwrap().release(), 1);
        match poll(get.as_mut(), &wake) {
            Poll::Ready(None) => break,
            Poll::Ready(Some(_)) => panic!("all returned items were already consumed"),
            Poll::Pending => {}
        }
        assert!(start.elapsed() < Duration::from_millis(200));
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(start.elapsed() >= timeout);
}

#[test]
fn test_contended_sync_async_return_handoff() {
    let pool = AutoPool::new_with_config(
        AutoPoolConfig {
            wait_duration: Duration::from_secs(2),
            ..Default::default()
        },
        [0usize],
    );
    let start = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let pool = &pool;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for _ in 0..200 {
                    let mut item = if worker % 2 == 0 {
                        pool.get()
                    } else {
                        smol::block_on(pool.get_async())
                    }
                    .unwrap();
                    *item += 1;
                    std::thread::yield_now();
                }
            });
        }
    });
    assert_eq!(pool.size(), 1);
    assert_eq!(pool.get().unwrap().release(), 800);
}
