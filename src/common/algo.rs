use std::time::{Duration, SystemTime};
use thiserror::Error;

use crate::common::{Clock, SystemClock, to_unix_millis};

#[derive(Error, Debug)]
pub enum RateLimitAlgorithmErr {
    #[error("Rate limit exceeded. Reset after {0:?}")]
    RateLimitExceeded(SystemTime),
}

pub trait RateLimitAlgorithm {
    fn try_acquire(&self, tokens: u32) -> Result<(u32, SystemTime), RateLimitAlgorithmErr>;
}

#[derive(Debug, Clone)]
pub struct SlidingWindow<C: Clock> {
    max_requests: u32,
    window: Duration,
    previous_requests: u32,
    current_requests: u32,
    clock: C,
}

impl SlidingWindow<SystemClock> {
    pub fn new(
        max_requests: u32,
        window: Duration,
        previous_requests: u32,
        current_requests: u32,
    ) -> Self {
        Self::with_clock(
            max_requests,
            window,
            previous_requests,
            current_requests,
            SystemClock,
        )
    }

    pub fn with_clock<C: Clock>(
        max_requests: u32,
        window: Duration,
        previous_requests: u32,
        current_requests: u32,
        clock: C,
    ) -> SlidingWindow<C> {
        SlidingWindow {
            max_requests,
            window,
            previous_requests,
            current_requests,
            clock,
        }
    }
}

impl<C: Clock> RateLimitAlgorithm for SlidingWindow<C> {
    fn try_acquire(&self, tokens: u32) -> Result<(u32, SystemTime), RateLimitAlgorithmErr> {
        let window_ms = self.window.as_millis();

        let now = self.clock.now();
        let unix_now = to_unix_millis(now);

        let time_in_current_window = unix_now % window_ms;
        let remaining_window_time = window_ms - time_in_current_window;

        let reset_after = now + Duration::from_millis(remaining_window_time as u64);

        if self.current_requests.saturating_add(tokens) > self.max_requests {
            return Err(RateLimitAlgorithmErr::RateLimitExceeded(reset_after));
        }

        let previous_window_weight = remaining_window_time as f64 / window_ms as f64;
        let used = self
            .current_requests
            .saturating_add((self.previous_requests as f64 * previous_window_weight).round() as u32);

        let possible_used = used.saturating_add(tokens);

        if possible_used > self.max_requests {
            Err(RateLimitAlgorithmErr::RateLimitExceeded(reset_after))
        } else {
            Ok((self.max_requests.saturating_sub(possible_used), reset_after))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{MockClock, from_unix_millis};
    use proptest::prelude::*;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(1761948300000, 10, 60, 9, 0, 1, 0)] // 2025-11-01 12:05:00 UTC
    #[case(1761948330000, 10, 60, 8, 5, 1, 0)] // 2025-11-01 12:05:30 UTC
    #[case(1761948359999, 10, 60, 10, 9, 1, 0)] // 2025-11-01 12:05:59.999 UTC
    #[case(1761948300000, 10, 60, 8, 0, 2, 0)] // 2025-11-01 12:05:00 UTC
    #[case(1761948330000, 10, 60, 8, 4, 2, 0)] // 2025-11-01 12:05:30 UTC
    #[case(1761948359999, 10, 60, 10, 8, 2, 0)] // 2025-11-01 12:05:59.999 UTC
    #[case(1761948300000, 10, 60, 0, 0, 10, 0)] // 2025-11-01 12:05:00 UTC
    fn test_sliding_window_allowed(
        #[case] now_millis: u64,
        #[case] max_requests: u32,
        #[case] window_secs: u64,
        #[case] previous_requests: u32,
        #[case] current_requests: u32,
        #[case] tokens: u32,
        #[case] expected_remaining: u32,
    ) {
        let mut clock = MockClock::new();
        let now = from_unix_millis(now_millis);
        clock.expect_now().return_const(now);

        let algorithm = SlidingWindow::with_clock(
            max_requests,
            Duration::from_secs(window_secs),
            previous_requests,
            current_requests,
            clock,
        );

        let (remaining, reset_after) = algorithm.try_acquire(tokens).unwrap();

        assert!(reset_after > now);
        assert_eq!(remaining, expected_remaining);
    }

    #[rstest]
    #[case(1761948300000, 10, 60, 10, 0, 1)] // 2025-11-01 12:05:00 UTC
    #[case(1761948330000, 10, 60, 10, 5, 1)] // 2025-11-01 12:05:30 UTC
    #[case(1761948359999, 10, 60, 0, 10, 1)] // 2025-11-01 12:05:59.999 UTC
    #[case(1761948300000, 10, 60, 9, 0, 2)] // 2025-11-01 12:05:00 UTC
    #[case(1761948330000, 10, 60, 10, 4, 2)] // 2025-11-01 12:05:30 UTC
    #[case(1761948359999, 10, 60, 0, 9, 2)] // 2025-11-01 12:05:59.999 UTC
    #[case(1761948300000, 10, 60, 0, 0, 11)] // 2025-11-01 12:05:00 UTC
    fn test_sliding_window_denied(
        #[case] now_millis: u64,
        #[case] max_requests: u32,
        #[case] window_secs: u64,
        #[case] previous_requests: u32,
        #[case] current_requests: u32,
        #[case] tokens: u32,
    ) {
        let mut clock = MockClock::new();
        let now = from_unix_millis(now_millis);
        clock.expect_now().return_const(now);

        let algorithm = SlidingWindow::with_clock(
            max_requests,
            Duration::from_secs(window_secs),
            previous_requests,
            current_requests,
            clock,
        );

        let result = algorithm.try_acquire(tokens);
        if let Err(RateLimitAlgorithmErr::RateLimitExceeded(reset_after)) = result {
            assert!(reset_after > now);
        } else {
            panic!("Expected RateLimitExceeded error");
        }
    }

    proptest! {
        #[test]
        fn test_sliding_window_proptest(
            max_requests in 1u32..4_294_967_295u32,
            previous_requests in 0u32..4_294_967_295u32,
            current_requests in 0u32..4_294_967_295u32,
            tokens in 1u32..4_294_967_295u32,
            window_secs in 1u64..3600,
            now_millis in 1u64..4102444800000, // up to year 2100
        ) {
            let mut clock = MockClock::new();
            let now = from_unix_millis(now_millis);
            clock.expect_now().return_const(now);

            let algorithm = SlidingWindow::with_clock(
                max_requests,
                Duration::from_secs(window_secs),
                previous_requests,
                current_requests,
                clock,
            );

            let _ = algorithm.try_acquire(tokens);
        }
    }
}
