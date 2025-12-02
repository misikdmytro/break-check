use break_check::proto::AcquireRequest;
use break_check::proto::rate_limiter_client::RateLimiterClient;
use break_check::proto::rate_limiter_server::RateLimiterServer;
use break_check::{
    common::SlidingWindow, config::PolicyDefinition, db::RedisRateLimit,
    rate_limiter::RateLimiterImpl,
};
use redis::AsyncConnectionConfig;
use std::time::SystemTime;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tonic::transport::{Channel, Server};

/// Helper function to setup a test gRPC server with Redis backend
async fn setup_test_server() -> (String, tokio::task::JoinHandle<()>) {
    // Use a random port for testing
    let addr: std::net::SocketAddr = "[::1]:0".parse().unwrap();

    // Connect to Redis (assumes Redis is running locally on default port)
    let redis_config = AsyncConnectionConfig::default();
    let client = redis::Client::open("redis://127.0.0.1/")
        .expect("Failed to connect to Redis. Ensure Redis is running on localhost:6379");

    let conn = client
        .get_multiplexed_async_connection_with_config(&redis_config)
        .await
        .expect("Failed to get Redis connection");

    // Setup test policies
    let default_policy = PolicyDefinition {
        max_tokens: 10,
        window_secs: 60,
    };

    let policies = vec![];

    let rate_limit = RedisRateLimit::new(
        conn,
        Duration::from_millis(200),
        Arc::new(default_policy),
        Arc::new(policies),
        SlidingWindow::new(),
    );

    let rate_limiter = RateLimiterImpl::new(rate_limit);

    // Start the server
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(RateLimiterServer::new(rate_limiter))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give the server a moment to start
    sleep(Duration::from_millis(100)).await;

    let server_url = format!("http://{}", local_addr);

    (server_url, server_handle)
}

/// Helper function to create a gRPC client
async fn create_client(server_url: String) -> RateLimiterClient<Channel> {
    RateLimiterClient::connect(server_url)
        .await
        .expect("Failed to connect to gRPC server")
}

/// Helper function to cleanup Redis keys after tests
async fn cleanup_redis_key(key: &str) {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: Result<(), _> = redis::cmd("DEL").arg(key).query_async(&mut conn).await;
}

fn unix_now_millis() -> u128 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_acquire_single_token() {
        let (server_url, _handle) = setup_test_server().await;
        let mut client = create_client(server_url).await;
        let now = unix_now_millis();

        let key = format!("test:single:{}", uuid::Uuid::new_v4());

        let request = AcquireRequest {
            key: key.clone(),
            tokens: 1,
        };

        let response = client.acquire(request).await.unwrap();
        let response = response.into_inner();

        assert_eq!(response.allowed, true);
        assert_eq!(response.remaining, 9); // 10 max tokens - 1 acquired
        assert!(response.reset_after > now as i64);
        assert!(response.reset_after <= (now + 60000) as i64); // within window

        cleanup_redis_key(&key).await;
    }

    #[tokio::test]
    async fn test_acquire_multiple_tokens() {
        let (server_url, _handle) = setup_test_server().await;
        let mut client = create_client(server_url).await;
        let now = unix_now_millis();

        let key = format!("test:multiple:{}", uuid::Uuid::new_v4());

        let request = AcquireRequest {
            key: key.clone(),
            tokens: 5,
        };

        let response = client.acquire(request).await.unwrap();
        let response = response.into_inner();

        assert_eq!(response.allowed, true);
        assert_eq!(response.remaining, 5); // 10 max tokens - 5 acquired
        assert!(response.reset_after > now as i64);
        assert!(response.reset_after <= (now + 60000) as i64); // within window

        cleanup_redis_key(&key).await;
    }

    #[tokio::test]
    async fn test_rate_limit_exceeded() {
        let (server_url, _handle) = setup_test_server().await;
        let mut client = create_client(server_url).await;
        let now = unix_now_millis();

        let key = format!("test:exceeded:{}", uuid::Uuid::new_v4());

        // First request: acquire all 10 tokens
        let request = AcquireRequest {
            key: key.clone(),
            tokens: 10,
        };
        let response = client.acquire(request).await.unwrap();
        assert_eq!(response.into_inner().allowed, true);

        // Second request: try to acquire 1 more token - should fail
        let request = AcquireRequest {
            key: key.clone(),
            tokens: 1,
        };
        let response = client.acquire(request).await.unwrap();
        let response = response.into_inner();

        assert_eq!(response.allowed, false);
        assert_eq!(response.remaining, 0);
        assert!(response.reset_after > now as i64);
        assert!(response.reset_after <= (now + 60000) as i64); // within window

        cleanup_redis_key(&key).await;
    }

    #[tokio::test]
    async fn test_multiple_sequential_requests() {
        let (server_url, _handle) = setup_test_server().await;
        let mut client = create_client(server_url).await;
        let now = unix_now_millis();

        let key = format!("test:sequential:{}", uuid::Uuid::new_v4());

        // Make 5 requests of 1 token each
        for i in 0..5 {
            let request = AcquireRequest {
                key: key.clone(),
                tokens: 1,
            };
            let response = client.acquire(request).await.unwrap();
            let response = response.into_inner();

            assert_eq!(response.allowed, true);
            assert_eq!(response.remaining, 9 - i); // Should decrease by 1 each time
            assert!(response.reset_after > now as i64);
            assert!(response.reset_after <= (now + 60000) as i64); // within window
        }

        cleanup_redis_key(&key).await;
    }

    #[tokio::test]
    async fn test_invalid_tokens_zero() {
        let (server_url, _handle) = setup_test_server().await;
        let mut client = create_client(server_url).await;

        let key = format!("test:zero:{}", uuid::Uuid::new_v4());

        let request = AcquireRequest {
            key: key.clone(),
            tokens: 0,
        };

        let result = client.acquire(request).await;
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "Tokens to acquire must be greater than zero"
        );

        cleanup_redis_key(&key).await;
    }

    #[tokio::test]
    async fn test_invalid_tokens_negative() {
        let (server_url, _handle) = setup_test_server().await;
        let mut client = create_client(server_url).await;

        let key = format!("test:negative:{}", uuid::Uuid::new_v4());

        let request = AcquireRequest {
            key: key.clone(),
            tokens: -1,
        };

        let result = client.acquire(request).await;
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        cleanup_redis_key(&key).await;
    }

    #[tokio::test]
    async fn test_empty_key() {
        let (server_url, _handle) = setup_test_server().await;
        let mut client = create_client(server_url).await;

        let request = AcquireRequest {
            key: String::new(),
            tokens: 1,
        };

        let result = client.acquire(request).await;
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "Key must not be empty");
    }

    #[tokio::test]
    async fn test_different_keys_independent() {
        let (server_url, _handle) = setup_test_server().await;
        let mut client = create_client(server_url).await;

        let key1 = format!("test:key1:{}", uuid::Uuid::new_v4());
        let key2 = format!("test:key2:{}", uuid::Uuid::new_v4());

        // Acquire 10 tokens for key1 (max out the limit)
        let request = AcquireRequest {
            key: key1.clone(),
            tokens: 10,
        };
        let response = client.acquire(request).await.unwrap();
        assert_eq!(response.into_inner().allowed, true);

        // key2 should still have full capacity
        let request = AcquireRequest {
            key: key2.clone(),
            tokens: 5,
        };
        let response = client.acquire(request).await.unwrap();
        let response = response.into_inner();

        assert_eq!(response.allowed, true);
        assert_eq!(response.remaining, 5);

        cleanup_redis_key(&key1).await;
        cleanup_redis_key(&key2).await;
    }

    #[tokio::test]
    async fn test_concurrent_requests() {
        let (server_url, _handle) = setup_test_server().await;

        let key = format!("test:concurrent:{}", uuid::Uuid::new_v4());

        // Spawn 5 concurrent requests
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let server_url = server_url.clone();
                let key = key.clone();
                tokio::spawn(async move {
                    let mut client = create_client(server_url).await;
                    let request = AcquireRequest {
                        key: key.clone(),
                        tokens: 2,
                    };
                    client.acquire(request).await
                })
            })
            .collect();

        // Wait for all requests to complete
        let mut allowed_count = 0;
        for handle in handles {
            let result = handle.await.unwrap().unwrap();
            if result.into_inner().allowed {
                allowed_count += 1;
            }
        }

        // With 10 max tokens and 2 tokens per request, maximum 5 can succeed
        assert!(allowed_count == 5);

        cleanup_redis_key(&key).await;
    }
}
