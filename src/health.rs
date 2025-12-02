use std::time::Duration;

use crate::proto::health_server::Health;
use crate::proto::{HealthCheckRequest, HealthCheckResponse};
use redis::aio::MultiplexedConnection;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct HealthCheckImpl {
    conn: MultiplexedConnection,
}

impl HealthCheckImpl {
    pub fn new(conn: MultiplexedConnection) -> Self {
        HealthCheckImpl { conn }
    }
}

impl HealthCheckImpl {
    async fn check_health(conn: &mut MultiplexedConnection) -> HealthCheckResponse {
        match redis::cmd("PING").query_async::<String>(conn).await {
            Ok(response) if response == "PONG" => HealthCheckResponse { status: 1 }, // SERVING
            _ => HealthCheckResponse { status: 0 },                                  // NOT_SERVING
        }
    }
}

#[tonic::async_trait]
impl Health for HealthCheckImpl {
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let mut conn = self.conn.clone();
        Ok(Response::new(
            HealthCheckImpl::check_health(&mut conn).await,
        ))
    }

    type WatchStream = ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let mut conn = self.conn.clone();
        tokio::spawn(async move {
            loop {
                if tx
                    .send(Ok(HealthCheckImpl::check_health(&mut conn).await))
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
