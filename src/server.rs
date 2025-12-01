use std::time::Duration;

use crate::common::to_unix_millis;
use crate::db::{AcquireErr, RateLimitConfig, RateLimitStore, TokensRemaining};
use crate::proto::rate_limiter_server::RateLimiter;
use crate::proto::{AcquireRequest, AcquireResponse};
use tonic::{Request, Response, Status};

const DEFAULT_MAX_REQUESTS_PER_WINDOW: u32 = 10;
const DEFAULT_WINDOW_DURATION_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct RateLimiterImpl<R: RateLimitStore> {
    rate_limit: R,
}

impl<R: RateLimitStore> RateLimiterImpl<R> {
    pub fn new(rate_limit: R) -> Self {
        RateLimiterImpl { rate_limit }
    }
}

#[tonic::async_trait]
impl<R: RateLimitStore + 'static + Send + Sync + Clone> RateLimiter for RateLimiterImpl<R> {
    async fn acquire(
        &self,
        request: Request<AcquireRequest>,
    ) -> Result<Response<AcquireResponse>, Status> {
        let request = request.get_ref();

        let rl = RateLimitConfig::new(
            request.key.to_string(),
            request.tokens as u32,
            DEFAULT_MAX_REQUESTS_PER_WINDOW,
            Duration::from_secs(DEFAULT_WINDOW_DURATION_SECS),
        );

        let mut rate_limit = self.rate_limit.clone();
        let result = rate_limit.acquire(&rl).await;
        match result {
            Ok(TokensRemaining {
                remaining,
                reset_after,
            }) => Ok(Response::new(AcquireResponse {
                remaining: remaining as i32,
                reset_after: to_unix_millis(reset_after) as i64,
                allowed: true,
            })),
            Err(e) => match e {
                AcquireErr::RateLimitExceeded(reset_after) => Ok(Response::new(AcquireResponse {
                    remaining: 0,
                    reset_after: to_unix_millis(reset_after) as i64,
                    allowed: false,
                })),
                e => Err(Status::unavailable(format!(
                    "Failed to acquire rate limit: {:?}",
                    e
                ))),
            },
        }
    }
}
