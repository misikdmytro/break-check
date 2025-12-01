use std::time::SystemTime;

use crate::{
    common::{
        AcquireAttempt, RateLimitAlgorithm, RateLimitAlgorithmErr, to_unix_millis,
    },
    db::{AcquireErr, AcquireResult, RateLimitConfig, RateLimitStore, TokensRemaining},
};
use redis::aio::MultiplexedConnection;

#[derive(Debug, Clone)]
pub struct RedisRateLimit<A: RateLimitAlgorithm> {
    conn: MultiplexedConnection,
    algorithm: A,
}

impl<A: RateLimitAlgorithm> RedisRateLimit<A> {
    pub fn new(conn: MultiplexedConnection, algorithm: A) -> Self {
        RedisRateLimit { conn, algorithm }
    }
}

macro_rules! format_key {
    ($key:expr, $window:expr) => {
        format!("{}.rate_limit.window.{}", $key, $window)
    };
}

impl<A: RateLimitAlgorithm> RateLimitStore for RedisRateLimit<A> {
    async fn acquire(&mut self, config: &RateLimitConfig) -> AcquireResult {
        let now = to_unix_millis(SystemTime::now());

        let window_ms = config.window_duration.as_millis();
        let current_window = now / window_ms;
        let previous_window = current_window - 1;

        let current_key = format_key!(config.resource_key, current_window);
        let previous_key = format_key!(config.resource_key, previous_window);

        let mut current_conn = self.conn.clone();
        let tokens = config.tokens_to_acquire;
        let window_secs = config.window_duration.as_secs();
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

        let attempt = AcquireAttempt::new(
            config.tokens_to_acquire,
            config.max_tokens_per_window,
            config.window_duration,
            previous,
            current,
        );

        self.algorithm
            .try_acquire(&attempt)
            .map(|(remaining, reset_after)| TokensRemaining::new(remaining, reset_after))
            .map_err(|e| match e {
                RateLimitAlgorithmErr::RateLimitExceeded(reset_after) => {
                    AcquireErr::RateLimitExceeded(reset_after)
                }
            })
    }
}
