use std::time::SystemTime;

#[cfg_attr(test, mockall::automock)]
pub trait Clock {
    fn now(&self) -> SystemTime;
}

#[derive(Debug, Clone)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

pub fn to_unix_millis(time: SystemTime) -> u128 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[cfg(test)]
pub fn from_unix_millis(millis: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(millis)
}
