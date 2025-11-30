use std::time::SystemTime;

use crate::{
    common::{RateLimitAlgorithm, RateLimitAlgorithmErr, SlidingWindow, to_unix_millis},
    db::{TokensRemaining, AcquireResult, RateLimitStore, AcquireErr, RateLimitConfig},
};
use redis::aio::MultiplexedConnection;

#[derive(Debug, Clone)]
pub struct RedisRateLimit {
    conn: MultiplexedConnection,
    config: RateLimitConfig,
}

impl RedisRateLimit {
    pub fn new(conn: MultiplexedConnection, config: RateLimitConfig) -> Self {
        RedisRateLimit { conn, config }
    }
}

macro_rules! format_key {
    ($key:expr, $window:expr) => {
        format!("{}.rate_limit.window.{}", $key, $window)
    };
}

impl RateLimitStore for RedisRateLimit {
    async fn acquire(&mut self) -> AcquireResult {
        let now = to_unix_millis(SystemTime::now());

        let window_ms = self.config.window_duration.as_millis();
        let current_window = now / window_ms;
        let previous_window = current_window - 1;

        let current_key = format_key!(self.config.resource_key, current_window);
        let previous_key = format_key!(self.config.resource_key, previous_window);

        let mut current_conn = self.conn.clone();
        let tokens = self.config.tokens_to_acquire;
        let window_secs = self.config.window_duration.as_secs();
        let fetch_current = async move {
            let script = redis::Script::new(
                r#"
                    local key = KEYS[1]
                    local increment = tonumber(ARGV[1])
                    local ttl = tonumber(ARGV[2])
                    
                    local new_value = redis.call('INCRBY', key, increment)
                    redis.call('EXPIRE', key, ttl)
                    
                    return new_value - increment
                "#,
            );

            let value = script
                .key(&current_key)
                .arg(tokens)
                .arg(window_secs * 2) // TTL should be at least double the window
                .invoke_async(&mut current_conn)
                .await
                .map_err(|e| AcquireErr::RedisError(e))?;

            Ok::<u32, AcquireErr>(value)
        };

        let mut previous_conn = self.conn.clone();
        let fetch_previous = async move {
            let value: Option<u32> = redis::cmd("GET")
                .arg(previous_key)
                .query_async(&mut previous_conn)
                .await
                .map_err(|e| AcquireErr::RedisError(e))?;

            Ok::<u32, AcquireErr>(value.unwrap_or_default())
        };

        let (current, previous) = tokio::join!(fetch_current, fetch_previous);

        let current = current?;
        let previous = previous?;

        SlidingWindow::new(self.config.max_tokens_per_window, self.config.window_duration, previous, current)
            .try_acquire(self.config.tokens_to_acquire)
            .map(|(remaining, reset_after)| TokensRemaining::new(remaining, reset_after))
            .map_err(|e| match e {
                RateLimitAlgorithmErr::RateLimitExceeded(reset_after) => {
                    AcquireErr::RateLimitExceeded(reset_after)
                }
            })
    }
}
