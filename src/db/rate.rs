use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AcquireErr {
    #[error("Rate limit exceeded. Reset after {0:?}")]
    RateLimitExceeded(SystemTime),

    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TokensRemaining {
    pub remaining: u32,
    pub reset_after: SystemTime,
}

pub type AcquireResult = Result<TokensRemaining, AcquireErr>;

impl TokensRemaining {
    pub fn new(remaining: u32, reset_after: SystemTime) -> Self {
        TokensRemaining {
            remaining,
            reset_after,
        }
    }
}

#[async_trait]
pub trait RateLimitStore {
    async fn acquire(&mut self, config: &RateLimitConfig) -> AcquireResult;
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Default, Hash)]
pub struct RateLimitConfig {
    pub(in crate::db) resource_key: String,
    pub(in crate::db) tokens_to_acquire: u32,
    pub(in crate::db) max_tokens_per_window: u32,
    pub(in crate::db) window_duration: Duration,
}

impl RateLimitConfig {
    pub fn new(
        resource_key: String,
        tokens_to_acquire: u32,
        max_tokens_per_window: u32,
        window_duration: Duration,
    ) -> Self {
        RateLimitConfig {
            resource_key,
            tokens_to_acquire,
            max_tokens_per_window,
            window_duration,
        }
    }
}
