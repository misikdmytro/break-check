use std::time::SystemTime;

use crate::{
    common::{RateLimitAlgorithm, RateLimitErr, SlidingWindow, to_unix_millis},
    db::{AcquireRateLimitResult, AcquireResult, RateLimit, RateLimitAcquireErr, RateLimitConfig},
};
use redis::aio::MultiplexedConnection;

#[derive(Debug, Clone)]
pub struct RedisRateLimit {
    conn: MultiplexedConnection,
    rl: RateLimitConfig,
}

impl RedisRateLimit {
    pub(crate) fn new(conn: MultiplexedConnection, rl: RateLimitConfig) -> Self {
        RedisRateLimit { conn, rl }
    }
}

macro_rules! format_key {
    ($key:expr, $window:expr) => {
        format!("{}.rate_limit.window.{}", $key, $window)
    };
}

async fn get_value_or_zero(
    mut conn: MultiplexedConnection,
    key: &str,
) -> Result<u32, RateLimitAcquireErr> {
    let value: Option<u32> = redis::cmd("GET")
        .arg(key)
        .query_async(&mut conn)
        .await
        .map_err(|e| RateLimitAcquireErr::RedisError(e))?;

    Ok(value.unwrap_or_default())
}

impl RateLimit for RedisRateLimit {
    async fn acquire(&mut self) -> AcquireResult {
        let now = to_unix_millis(SystemTime::now());

        let window_ms = self.rl.window.as_millis();
        let current_window = now / window_ms;
        let previous_window = current_window - 1;

        let current_key = format_key!(self.rl.key, current_window);
        let previous_key = format_key!(self.rl.key, previous_window);

        let current_future = get_value_or_zero(self.conn.clone(), &current_key);
        let previous_future = get_value_or_zero(self.conn.clone(), &previous_key);

        let (current, previous) = tokio::join!(current_future, previous_future);

        let current: u32 = current?;
        let previous: u32 = previous?;

        let result = SlidingWindow::new(self.rl.max_requests, self.rl.window, previous, current)
            .try_acquire(self.rl.tokens)
            .map(|(remaining, reset_after)| AcquireRateLimitResult::new(remaining, reset_after))
            .map_err(|e| match e {
                RateLimitErr::RateLimitExceeded(reset_after) => {
                    RateLimitAcquireErr::RateLimitExceeded(reset_after)
                }
            });

        if let Ok(_) = result {
            let script = redis::Script::new(
                r#"
                    local key = KEYS[1]
                    local increment = tonumber(ARGV[1])
                    local ttl = tonumber(ARGV[2])
                    
                    local new_value = redis.call('INCRBY', key, increment)
                    redis.call('EXPIRE', key, ttl)
                    
                    return new_value
                "#,
            );

            let _: u32 = script
                .key(&current_key)
                .arg(self.rl.tokens)
                .arg(self.rl.window.as_secs() * 2) // TTL should be at least double the window
                .invoke_async(&mut self.conn)
                .await
                .map_err(|e| RateLimitAcquireErr::RedisError(e))?;
        };

        result
    }
}
