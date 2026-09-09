#![doc = include_str!("../README.md")]

/// Checkout timing and object selection.
pub mod config;
/// Pool storage and checkout operations.
pub mod pool;
/// Borrowed wrappers that return objects on drop.
pub mod pool_object;
#[cfg(test)]
mod test;

#[cfg(all(test, feature = "async"))]
mod async_tests;
