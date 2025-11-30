use std::time::{Duration, SystemTime};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RateLimitAcquireErr {
    #[error("Rate limit exceeded. Reset after {0:?}")]
    RateLimitExceeded(SystemTime),

    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
}

#[derive(Debug, Clone)]
pub struct AcquireRateLimitResult {
    pub remaining: u32,
    pub reset_after: SystemTime,
}

pub type AcquireResult = Result<AcquireRateLimitResult, RateLimitAcquireErr>;

impl AcquireRateLimitResult {
    pub fn new(remaining: u32, reset_after: SystemTime) -> Self {
        AcquireRateLimitResult {
            remaining,
            reset_after,
        }
    }
}

pub trait RateLimit {
    async fn acquire(&mut self) -> AcquireResult;
}

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub(in crate::db) key: String,
    pub(in crate::db) tokens: u32,
    pub(in crate::db) max_requests: u32,
    pub(in crate::db) window: Duration,
}

impl RateLimitConfig {
    pub fn new(key: String, tokens: u32, max_requests: u32, window: Duration) -> Self {
        RateLimitConfig {
            key,
            tokens,
            max_requests,
            window,
        }
    }
}
