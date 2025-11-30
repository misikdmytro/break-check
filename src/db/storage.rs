use std::{ops::Deref, time::Duration};

use anyhow::Result;
use thiserror::Error;

use crate::db::{RateLimit, RateLimitConfig};

#[derive(Error, Debug)]
pub enum AcquireLockErr {
    #[error("Failed to acquire lock after maximum retries")]
    MaxRetriesExceeded,
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
}

pub trait Storage {
    type RateLimitImpl: RateLimit;
    type Guard: StorageLockGuard<Self::RateLimitImpl>;

    async fn acquire_rate_limit_lock(
        &mut self,
        rl: &RateLimitConfig,
        ttl: Duration,
        retry_interval: Duration,
        max_retries: u32,
    ) -> Result<Self::Guard, AcquireLockErr>;
}

pub trait StorageLockGuard<T>: AsRef<T> + AsMut<T> + Deref<Target = T> {}
