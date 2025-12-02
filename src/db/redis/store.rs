use std::{
    fmt::Write,
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime},
};

use crate::{
    common::{AcquireAttempt, RateLimitAlgorithm, RateLimitAlgorithmErr, to_unix_millis},
    config::{PolicyDefinition, PolicyRule},
    db::{AcquireErr, AcquireResult, RateLimitConfig, RateLimitStore, TokensRemaining},
};

use async_trait::async_trait;
use log::debug;
use redis::aio::MultiplexedConnection;

#[derive(Debug, Clone)]
pub struct RedisRateLimit<A: RateLimitAlgorithm> {
    conn: MultiplexedConnection,
    timeout: Duration,
    default_policy: Arc<PolicyDefinition>,
    policies: Arc<Vec<PolicyRule>>,
    algorithm: A,
}

impl<A: RateLimitAlgorithm> RedisRateLimit<A> {
    pub fn new(
        conn: MultiplexedConnection,
        timeout: Duration,
        default_policy: Arc<PolicyDefinition>,
        policies: Arc<Vec<PolicyRule>>,
        algorithm: A,
    ) -> Self {
        RedisRateLimit {
            conn,
            timeout,
            default_policy,
            policies,
            algorithm,
        }
    }
}

macro_rules! format_key {
    ($key:expr, $window:expr) => {{
        let mut result = String::new();
        write!(&mut result, "{}.rate_limit.window.{}", $key, $window).unwrap();
        result
    }};
}

macro_rules! join_and_unwrap {
    ($fut1:expr, $fut2:expr) => {{
        let (res1, res2) = tokio::join!($fut1, $fut2);
        (res1?, res2?)
    }};
}

macro_rules! timeout {
    ($duration:expr, $fut:expr) => {{
        match tokio::time::timeout($duration, $fut).await {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(e)) => Err(AcquireErr::RedisError(e)),
            _ => Err(AcquireErr::Timeout),
        }
    }};
}

static SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r#"
    local key = KEYS[1]
    local increment = tonumber(ARGV[1])
    local ttl = tonumber(ARGV[2])

    local new_value = redis.call('INCRBY', key, increment)
    redis.call('EXPIRE', key, ttl)

    return new_value - increment
"#,
    )
});

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

        debug!(
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

        let mut conn = self.conn.clone();
        let fetch_current = async {
            timeout!(
                self.timeout,
                SCRIPT
                    .key(&current_key)
                    .arg(config.tokens_to_acquire)
                    .arg(policy.window_secs * 2) // TTL should be at least double the window
                    .invoke_async(&mut conn)
            )
        };

        let mut conn = self.conn.clone();
        let fetch_previous = async {
            timeout!(self.timeout, async {
                redis::cmd("GET")
                    .arg(previous_key)
                    .query_async::<Option<u32>>(&mut conn)
                    .await
                    .map(|opt| opt.unwrap_or(0))
            })
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

        let result = self
            .algorithm
            .try_acquire(&attempt)
            .map(|(remaining, reset_after)| TokensRemaining::new(remaining, reset_after))
            .map_err(|e| match e {
                RateLimitAlgorithmErr::RateLimitExceeded(reset_after) => {
                    AcquireErr::RateLimitExceeded(reset_after)
                }
            });

        debug!(
            "Acquire result for key '{}': {:?}",
            config.resource_key, result
        );

        result
    }
}
