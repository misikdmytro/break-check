use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::{
    common::{AcquireAttempt, RateLimitAlgorithm, RateLimitAlgorithmErr, to_unix_millis},
    config::{PolicyDefinition, PolicyRule},
    db::{AcquireErr, AcquireResult, RateLimitConfig, RateLimitStore, TokensRemaining},
};

use async_trait::async_trait;
use log::{debug, info};
use redis::aio::MultiplexedConnection;

#[derive(Debug, Clone)]
pub struct RedisRateLimit<A: RateLimitAlgorithm> {
    conn: MultiplexedConnection,
    default_policy: Arc<PolicyDefinition>,
    policies: Arc<Vec<PolicyRule>>,
    algorithm: A,
}

impl<A: RateLimitAlgorithm> RedisRateLimit<A> {
    pub fn new(
        conn: MultiplexedConnection,
        default_policy: Arc<PolicyDefinition>,
        policies: Arc<Vec<PolicyRule>>,
        algorithm: A,
    ) -> Self {
        RedisRateLimit {
            conn,
            default_policy,
            policies,
            algorithm,
        }
    }
}

macro_rules! format_key {
    ($key:expr, $window:expr) => {
        format!("{}.rate_limit.window.{}", $key, $window)
    };
}

macro_rules! join_and_unwrap {
    ($fut1:expr, $fut2:expr) => {{
        let (res1, res2) = tokio::join!($fut1, $fut2);
        (res1?, res2?)
    }};
}

#[async_trait]
impl<A: RateLimitAlgorithm + Send> RateLimitStore for RedisRateLimit<A> {
    async fn acquire(&mut self, config: &RateLimitConfig) -> AcquireResult {
        let policy = self
            .policies
            .iter()
            .filter(|rule| match rule.pattern_type {
                crate::config::PatternType::Exact => rule.pattern == config.resource_key,
                crate::config::PatternType::Prefix => {
                    config.resource_key.starts_with(&rule.pattern)
                }
            })
            .max_by_key(|rule| rule.priority)
            .map(|rule| &rule.policy)
            .unwrap_or(&self.default_policy);

        info!(
            "Using policy for key '{}': max_tokens={}, window_secs={}",
            config.resource_key, policy.max_tokens, policy.window_secs
        );

        let now = to_unix_millis(SystemTime::now());

        let window_duration = Duration::from_secs(policy.window_secs);
        let window_ms = policy.window_secs as u128 * 1000;
        let current_window = now / window_ms;
        let previous_window = current_window - 1;

        let current_key = format_key!(config.resource_key, current_window);
        let previous_key = format_key!(config.resource_key, previous_window);

        let mut current_conn = self.conn.clone();
        let tokens = config.tokens_to_acquire;
        let window_secs = policy.window_secs;
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
                .map_err(AcquireErr::RedisError)?;

            Ok::<u32, AcquireErr>(value)
        };

        let mut previous_conn = self.conn.clone();
        let fetch_previous = async move {
            let value: Option<u32> = redis::cmd("GET")
                .arg(previous_key)
                .query_async(&mut previous_conn)
                .await
                .map_err(AcquireErr::RedisError)?;

            Ok::<u32, AcquireErr>(value.unwrap_or_default())
        };

        let (current, previous) = join_and_unwrap!(fetch_current, fetch_previous);
        debug!(
            "Current window requests: {}, Previous window requests: {}",
            current, previous
        );

        let attempt = AcquireAttempt::new(
            config.tokens_to_acquire,
            policy.max_tokens,
            window_duration,
            previous,
            current,
        );

        let result = self.algorithm
            .try_acquire(&attempt)
            .map(|(remaining, reset_after)| TokensRemaining::new(remaining, reset_after))
            .map_err(|e| match e {
                RateLimitAlgorithmErr::RateLimitExceeded(reset_after) => {
                    AcquireErr::RateLimitExceeded(reset_after)
                }
            });

        info!(
            "Acquire result for key '{}': {:?}",
            config.resource_key, result
        );

        result
    }
}
