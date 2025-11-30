use std::time::Duration;

use crate::common::to_unix_millis;
use crate::db::{
    AcquireRateLimitResult, RateLimit, RateLimitAcquireErr, RateLimitConfig, RedisRateLimit,
};
use crate::proto::rate_limiter_server::RateLimiter;
use crate::proto::{AcquireRequest, AcquireResponse};
use redis::aio::MultiplexedConnection;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct RateLimiterImpl {
    conn: MultiplexedConnection,
}

impl RateLimiterImpl {
    pub fn new(conn: MultiplexedConnection) -> Self {
        RateLimiterImpl { conn }
    }
}

#[tonic::async_trait]
impl RateLimiter for RateLimiterImpl {
    async fn acquire(
        &self,
        request: Request<AcquireRequest>,
    ) -> Result<Response<AcquireResponse>, Status> {
        let request = request.get_ref();

        let rl = RateLimitConfig::new(
            request.key.to_string(),
            request.tokens as u32,
            10,
            Duration::from_secs(60),
        );

        let result = RedisRateLimit::new(self.conn.clone(), rl).acquire().await;

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
                e => Err(Status::unavailable(format!(
                    "Failed to acquire rate limit: {:?}",
                    e
                ))),
            },
        }
    }
}
