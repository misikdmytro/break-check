mod common;
mod db;
mod proto;
mod server;

use std::env;

use proto::rate_limiter_server::RateLimiterServer;
use redis::AsyncConnectionConfig;
use server::RateLimiterImpl;

use crate::db::RedisStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::]:50051".parse()?;

    let conn = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let config = AsyncConnectionConfig::default();
    let storage = RedisStorage::new(conn, &config).await?;

    let rate_limiter = RateLimiterImpl::new(storage);

    println!("Server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(RateLimiterServer::new(rate_limiter))
        .serve(addr)
        .await?;

    Ok(())
}
