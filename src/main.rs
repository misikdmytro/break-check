mod common;
mod config;
mod db;
mod health;
mod proto;
mod rate_limiter;

use std::{sync::Arc, time::Duration};

use log::{LevelFilter, debug};
use proto::rate_limiter_server::RateLimiterServer;
use simple_logger::SimpleLogger;
use tokio::signal;

use crate::{
    common::SlidingWindow, config::load_config, db::RedisRateLimit, health::HealthCheckImpl,
    proto::health_server::HealthServer, rate_limiter::RateLimiterImpl,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = log::set_boxed_logger(Box::new(SimpleLogger::new()))
        .map(|()| log::set_max_level(LevelFilter::Debug));

    let config = load_config("./config/config.toml")?;
    debug!("Loaded config: {:?}", config);

    let addr = config.server.address.parse()?;

    let timeout = Duration::from_millis(config.server.redis_timeout_ms);
    let redis_config = redis::aio::ConnectionManagerConfig::new()
        .set_response_timeout(timeout)
        .set_connection_timeout(Duration::from_secs(1))
        .set_factor(1000)
        .set_max_delay(10000)
        .set_exponent_base(2)
        .set_number_of_retries(5);

    let client = redis::Client::open(config.server.redis_url)?;
    let manager = tokio::time::timeout(
        Duration::from_secs(10),
        redis::aio::ConnectionManager::new_with_config(client, redis_config),
    )
    .await
    .map_err(|_| "Failed to connect to Redis: timeout")?
    .map_err(|e| format!("Failed to connect to Redis: {}", e))?;

    let rate_limit = RedisRateLimit::new(
        manager.clone(),
        timeout,
        Arc::new(config.default_policy),
        Arc::new(config.policies),
        SlidingWindow::new(),
    );

    let rate_limiter = RateLimiterImpl::new(rate_limit);
    let health = HealthCheckImpl::new(manager.clone(), timeout);

    println!("Server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(RateLimiterServer::new(rate_limiter))
        .add_service(HealthServer::new(health))
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
