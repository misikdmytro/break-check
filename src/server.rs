use std::time::Duration;

use crate::common::to_unix_millis;
use crate::db::{
    AcquireRateLimitResult, RateLimit, RateLimitAcquireErr, RateLimitConfig, RedisStorage, Storage,
};
use crate::proto::rate_limiter_server::RateLimiter;
use crate::proto::{AcquireRequest, AcquireResponse};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct RateLimiterImpl {
    storage: RedisStorage,
}

impl RateLimiterImpl {
    pub fn new(storage: RedisStorage) -> Self {
        RateLimiterImpl { storage }
    }
}

#[tonic::async_trait]
impl RateLimiter for RateLimiterImpl {
    async fn acquire(
        &self,
        request: Request<AcquireRequest>,
    ) -> Result<Response<AcquireResponse>, Status> {
        let mut storage = self.storage.clone();
        let request = request.get_ref();

        let rl = RateLimitConfig::new(
            request.key.to_string(),
            request.tokens as u32,
            10,
            Duration::from_secs(60),
        );

        let ttl = std::time::Duration::from_secs(100);
        let retry_interval = std::time::Duration::from_millis(100);
        let max_retries = 5;

        let mut lock = storage
            .acquire_rate_limit_lock(&rl, ttl, retry_interval, max_retries)
            .await
            .map_err(|e| Status::internal(format!("Failed to acquire rate limit lock: {}", e)))?;

        let result = lock.as_mut().acquire().await;

        match result {
            Ok(AcquireRateLimitResult {
                remaining,
                reset_after,
            }) => Ok(Response::new(AcquireResponse {
                remaining: remaining as i32,
                reset_after: to_unix_millis(reset_after) as i64,
                allowed: true,
            })),
            Err(e) => match e {
                RateLimitAcquireErr::RateLimitExceeded(reset_after) => {
                    Ok(Response::new(AcquireResponse {
                        remaining: 0,
                        reset_after: to_unix_millis(reset_after) as i64,
                        allowed: false,
                    }))
                }
                e => Err(Status::internal(format!(
                    "Failed to acquire rate limit: {:?}",
                    e
                ))),
            },
        }
    }
}
