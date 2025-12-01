mod common;
mod config;
mod db;
mod proto;
mod server;

use std::sync::Arc;

use log::{LevelFilter, debug};
use proto::rate_limiter_server::RateLimiterServer;
use redis::AsyncConnectionConfig;
use server::RateLimiterImpl;
use simple_logger::SimpleLogger;
use tokio::signal;

use crate::{common::SlidingWindow, config::load_config, db::RedisRateLimit};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = log::set_boxed_logger(Box::new(SimpleLogger::new()))
        .map(|()| log::set_max_level(LevelFilter::Debug));

    let config = load_config("./config/config.toml")?;
    debug!("Loaded config: {:?}", config);

    let addr = config.server.address.parse()?;

    let redis_config = AsyncConnectionConfig::default();
    let client = redis::Client::open(config.server.redis_url)?;
    let conn = client
        .get_multiplexed_async_connection_with_config(&redis_config)
        .await?;

    let rate_limit = RedisRateLimit::new(
        conn,
        Arc::new(config.default_policy),
        Arc::new(config.policies),
        SlidingWindow::new(),
    );

    let rate_limiter = RateLimiterImpl::new(rate_limit);

    println!("Server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(RateLimiterServer::new(rate_limiter))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    println!("Server shutdown gracefully");

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    println!("Shutdown signal received, starting graceful shutdown...");
}
