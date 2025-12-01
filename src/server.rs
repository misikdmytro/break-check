use crate::common::to_unix_millis;
use crate::db::{AcquireErr, RateLimitConfig, RateLimitStore, TokensRemaining};
use crate::proto::rate_limiter_server::RateLimiter;
use crate::proto::{AcquireRequest, AcquireResponse};
use log::error;
use tonic::{Request, Response, Status};

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
        if request.tokens <= 0 {
            return Err(Status::invalid_argument(
                "Tokens to acquire must be greater than zero",
            ));
        }

        if request.key.is_empty() {
            return Err(Status::invalid_argument("Key must not be empty"));
        }

        let config = RateLimitConfig::new(request.key.to_string(), request.tokens as u32);

        let mut rate_limit = self.rate_limit.clone();
        let result = rate_limit.acquire(&config).await;
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
                AcquireErr::Timeout => {
                    error!("Rate limit acquisition timed out");

                    Err(Status::deadline_exceeded(
                        "Rate limit acquisition timed out",
                    ))
                }
                AcquireErr::RedisError(e) => {
                    error!("Redis error: {:?}", e);
                    Err(Status::unavailable("Failed to acquire rate limit"))
                }
            },
        }
    }
}
