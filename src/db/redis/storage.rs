use std::{ops::Deref, time::Duration};

use anyhow::Result;
use redis::{AsyncConnectionConfig, IntoConnectionInfo, aio::MultiplexedConnection};
use tokio::time::sleep;
use uuid::Uuid;

use crate::db::redis::RedisRateLimit;
use crate::db::{AcquireLockErr, RateLimitConfig, Storage, StorageLockGuard};

const RATE_LIMIT_KEY: &str = "rate_limit_lock";

#[derive(Debug, Clone)]
pub struct RedisStorage {
    conn: MultiplexedConnection,
}

#[derive(Debug)]
pub struct RedisStorageLockGuard<T> {
    key: String,
    token: String,
    item: T,
    conn: MultiplexedConnection,
}

macro_rules! format_key {
    ($key:expr) => {
        format!("{}:{}", RATE_LIMIT_KEY, $key)
    };
}

impl<T> Drop for RedisStorageLockGuard<T> {
    fn drop(&mut self) {
        let mut conn = self.conn.clone();
        let key = self.key.clone();
        let token = self.token.clone();

        tokio::spawn(async move {
            let script = r"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
        ";

            let _: Result<i32, _> = redis::Script::new(script)
                .key(format_key!(key))
                .arg(token)
                .invoke_async(&mut conn)
                .await;
        });
    }
}

impl<T> Deref for RedisStorageLockGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

impl<T> AsRef<T> for RedisStorageLockGuard<T> {
    fn as_ref(&self) -> &T {
        &self.item
    }
}

impl<T> AsMut<T> for RedisStorageLockGuard<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.item
    }
}

impl<T> StorageLockGuard<T> for RedisStorageLockGuard<T> {}

impl RedisStorage {
    pub async fn new(
        conn: impl IntoConnectionInfo,
        config: &AsyncConnectionConfig,
    ) -> Result<Self> {
        let client = redis::Client::open(conn)?;
        let conn = client
            .get_multiplexed_async_connection_with_config(config)
            .await?;

        Ok(Self { conn })
    }
}

impl Storage for RedisStorage {
    type RateLimitImpl = RedisRateLimit;
    type Guard = RedisStorageLockGuard<Self::RateLimitImpl>;

    async fn acquire_rate_limit_lock(
        &mut self,
        rl: &RateLimitConfig,
        ttl: Duration,
        retry_interval: Duration,
        max_retries: u32,
    ) -> Result<Self::Guard, crate::db::AcquireLockErr> {
        let token = Uuid::new_v4().to_string();

        for _ in 0..max_retries {
            let result: Option<String> = redis::cmd("SET")
                .arg(format_key!(rl.key))
                .arg(&token)
                .arg("NX")
                .arg("PX")
                .arg(ttl.as_millis() as usize)
                .query_async(&mut self.conn)
                .await?;

            if result.is_some() {
                return Ok(RedisStorageLockGuard {
                    key: rl.key.clone(),
                    token,
                    item: RedisRateLimit::new(self.conn.clone(), rl.clone()),
                    conn: self.conn.clone(),
                });
            }

            sleep(retry_interval).await;
        }

        Err(AcquireLockErr::MaxRetriesExceeded)
    }
}
