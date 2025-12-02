use std::time::Duration;

use crate::proto::health_server::Health;
use crate::proto::{HealthCheckRequest, HealthCheckResponse};
use redis::aio::ConnectionLike;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct HealthCheckImpl<C: ConnectionLike> {
    timeout: Duration,
    conn: C,
}

impl<C: ConnectionLike> HealthCheckImpl<C> {
    pub fn new(conn: C, timeout: Duration) -> Self {
        HealthCheckImpl { conn, timeout }
    }
}

impl<C: ConnectionLike> HealthCheckImpl<C> {
    async fn check_health(conn: &mut C, timeout: Duration) -> HealthCheckResponse {
        match tokio::time::timeout(timeout, redis::cmd("PING").query_async::<String>(conn)).await {
            Ok(Ok(response)) if response == "PONG" => HealthCheckResponse { status: 1 }, // SERVING
            _ => HealthCheckResponse { status: 2 }, // NOT_SERVING
        }
    }
}

#[tonic::async_trait]
impl<C: ConnectionLike + Send + Sync + 'static + Clone> Health for HealthCheckImpl<C> {
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let mut conn = self.conn.clone();
        Ok(Response::new(
            HealthCheckImpl::check_health(&mut conn, self.timeout).await,
        ))
    }

    type WatchStream = ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let mut conn = self.conn.clone();
        let timeout = self.timeout;
        tokio::spawn(async move {
            loop {
                if tx
                    .send(Ok(HealthCheckImpl::check_health(&mut conn, timeout).await))
                    .await
                    .is_err()
                {
                    break;
                }

                sleep(Duration::from_secs(5)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
