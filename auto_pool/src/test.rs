use crate::config::{AutoPoolConfig, PickStrategy};
use crate::pool::AutoPool;
use std::ops::Deref;

#[test]
fn test_create() {
    let pool = AutoPool::new([1, 2, 3]);
    assert_eq!(pool.size(), 3);
}

#[test]
fn test_take() {
    let pool = AutoPool::new([1, 2, 3]);
    let obj1 = pool.get();
    assert_eq!(pool.size(), 2);
    assert_eq!(*obj1.as_ref().unwrap().deref(), 3);
}

#[tokio::test]
#[cfg(feature = "async")]
async fn test_take_async() {
    let pool = AutoPool::new([1, 2, 3]);
    let obj1 = pool.get_async().await;
    assert_eq!(pool.size(), 2);
    assert_eq!(*obj1.as_ref().unwrap().deref(), 3);
}

#[test]
fn test_add() {
    let pool = AutoPool::new([1]);
    pool.add(2);
    assert_eq!(pool.size(), 2);
}

#[test]
fn test_wait() {
    let wait_time = std::time::Duration::from_millis(20);
    let config = AutoPoolConfig {
        wait_duration: wait_time,
        ..Default::default()
    };
    let pool = AutoPool::new_with_config(config, [1]);
    let _obj1 = pool.get();
    assert_eq!(pool.size(), 0);
    let start_time = std::time::Instant::now();
    let obj2 = pool.get();
    assert!(start_time.elapsed() >= wait_time);
    assert!(obj2.is_none());
}

#[test]
fn test_workflow() {
    let config = AutoPoolConfig {
        wait_duration: std::time::Duration::from_millis(5),
        ..Default::default()
    };
    let pool = AutoPool::new_with_config(config, [1, 2, 3]);
    assert_eq!(pool.size(), 3);

    let obj1 = pool.get();
    assert_eq!(pool.size(), 2);
    assert_eq!(*obj1.as_ref().unwrap().deref(), 3);

    let obj2 = pool.get();
    assert_eq!(*obj2.as_ref().unwrap().deref(), 2);
    let obj3 = pool.get();
    assert_eq!(pool.size(), 0);
    assert_eq!(*obj3.as_ref().unwrap().deref(), 1);

    let obj4 = pool.get();
    assert!(obj4.is_none());
}

#[test]
fn test_pick_strategy_lifo() {
    let config = AutoPoolConfig {
        wait_duration: std::time::Duration::from_millis(5),
        pick_strategy: PickStrategy::LIFO,
        ..Default::default()
    };
    let pool = AutoPool::new_with_config(config, [1, 2, 3]);
    for _ in 0..1000 {
        let obj1 = pool.get();
        assert_eq!(*obj1.as_ref().unwrap().deref(), 3);
    }
}

#[test]
fn test_pick_strategy_random() {
    let config = AutoPoolConfig {
        wait_duration: std::time::Duration::from_millis(5),
        pick_strategy: PickStrategy::RANDOM,
        ..Default::default()
    };
    let pool = AutoPool::new_with_config(config, [1, 2, 3]);
    let mut values = Vec::new();
    for _ in 0..3 {
        values.push(pool.get().unwrap().release());
    }
    values.sort_unstable();
    assert_eq!(values, [1, 2, 3]);
    assert!(pool.get().is_none());
}

#[cfg(feature = "async")]
#[test]
fn test_async_first_poll_does_not_wait_for_an_item() {
    let pool = AutoPool::<u8>::new_with_config(
        AutoPoolConfig {
            wait_duration: std::time::Duration::from_millis(20),
            lock_duration: std::time::Duration::from_millis(200),
            ..Default::default()
        },
        [],
    );
    let mut get = Box::pin(pool.get_async());
    let start = std::time::Instant::now();
    assert!(smol::block_on(smol::future::poll_once(&mut get)).is_none());
    assert!(start.elapsed() < std::time::Duration::from_millis(100));
}

#[cfg(feature = "async")]
#[test]
fn test_async_add_wakes_without_polling_delay() {
    let pool = AutoPool::new_with_config(
        AutoPoolConfig {
            wait_duration: std::time::Duration::from_secs(1),
            lock_duration: std::time::Duration::ZERO,
            sleep_duration: std::time::Duration::from_secs(60),
            ..Default::default()
        },
        [],
    );
    let mut get = Box::pin(pool.get_async());
    assert!(smol::block_on(smol::future::poll_once(&mut get)).is_none());
    pool.add(42);
    let result = smol::block_on(smol::future::poll_once(&mut get));
    assert_eq!(result.flatten().map(|item| item.release()), Some(42));
}

#[test]
fn test_return_preserves_mutation_and_release_removes_item() {
    let pool = AutoPool::new([String::from("hello")]);
    {
        let mut item = pool.get().unwrap();
        item.push_str(" world");
        assert_eq!(pool.size(), 0);
    }
    assert_eq!(pool.size(), 1);
    assert_eq!(pool.get().unwrap().release(), "hello world");
    assert_eq!(pool.size(), 0);
}

#[test]
fn test_zero_timeout_checks_available_items() {
    let pool = AutoPool::new_with_config(
        AutoPoolConfig {
            wait_duration: std::time::Duration::ZERO,
            ..Default::default()
        },
        [7],
    );
    assert_eq!(pool.get().unwrap().release(), 7);
    assert!(pool.get().is_none());
}

#[test]
fn test_default_timeout_waits_for_return() {
    let pool = AutoPool::new([7]);
    let item = pool.get().unwrap();
    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| pool.get().unwrap().release());
        drop(item);
        assert_eq!(waiter.join().unwrap(), 7);
    });
}
