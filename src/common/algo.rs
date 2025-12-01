use std::time::{Duration, SystemTime};
use thiserror::Error;

use crate::common::{Clock, SystemClock, to_unix_millis};

#[derive(Error, Debug)]
pub enum RateLimitAlgorithmErr {
    #[error("Rate limit exceeded. Reset after {0:?}")]
    RateLimitExceeded(SystemTime),
}

pub struct AcquireAttempt {
    pub(self) tokens_to_acquire: u32,
    pub(self) max_tokens_per_window: u32,
    pub(self) window_duration: Duration,
    pub(self) previous_window_requests: u32,
    pub(self) current_window_requests: u32,
}

impl AcquireAttempt {
    pub fn new(
        tokens_to_acquire: u32,
        max_tokens_per_window: u32,
        window_duration: Duration,
        previous_window_requests: u32,
        current_window_requests: u32,
    ) -> Self {
        AcquireAttempt {
            tokens_to_acquire,
            max_tokens_per_window,
            window_duration,
            previous_window_requests,
            current_window_requests,
        }
    }
}

pub trait RateLimitAlgorithm {
    fn try_acquire(
        &self,
        attempt: &AcquireAttempt,
    ) -> Result<(u32, SystemTime), RateLimitAlgorithmErr>;
}

#[derive(Debug, Clone)]
pub struct SlidingWindow<C: Clock> {
    clock: C,
}

impl SlidingWindow<SystemClock> {
    pub fn new() -> Self {
        Self::with_clock(SystemClock)
    }

    pub fn with_clock<C: Clock>(clock: C) -> SlidingWindow<C> {
        SlidingWindow { clock }
    }
}

impl<C: Clock> RateLimitAlgorithm for SlidingWindow<C> {
    fn try_acquire(
        &self,
        attempt: &AcquireAttempt,
    ) -> Result<(u32, SystemTime), RateLimitAlgorithmErr> {
        let window_ms = attempt.window_duration.as_millis();

        let now = self.clock.now();
        let unix_now = to_unix_millis(now);

        let time_in_current_window = unix_now % window_ms;
        let remaining_window_time = window_ms - time_in_current_window;

        let reset_after = now + Duration::from_millis(remaining_window_time as u64);

        if attempt
            .current_window_requests
            .saturating_add(attempt.tokens_to_acquire)
            > attempt.max_tokens_per_window
        {
            return Err(RateLimitAlgorithmErr::RateLimitExceeded(reset_after));
        }

        let previous_window_weight = remaining_window_time as f64 / window_ms as f64;
        let used = attempt.current_window_requests.saturating_add(
            (attempt.previous_window_requests as f64 * previous_window_weight).round() as u32,
        );

        let possible_used = used.saturating_add(attempt.tokens_to_acquire);
        if possible_used > attempt.max_tokens_per_window {
            Err(RateLimitAlgorithmErr::RateLimitExceeded(reset_after))
        } else {
            Ok((
                attempt.max_tokens_per_window.saturating_sub(possible_used),
                reset_after,
            ))
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
    #[case(1761948300000, 10, 60, 8, 0, 1, 1)] // 2025-11-01 12:05:00 UTC
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

        let algorithm = SlidingWindow::with_clock(clock);

        let attempt = AcquireAttempt {
            tokens_to_acquire: tokens,
            max_tokens_per_window: max_requests,
            window_duration: Duration::from_secs(window_secs),
            previous_window_requests: previous_requests,
            current_window_requests: current_requests,
        };

        let (remaining, reset_after) = algorithm.try_acquire(&attempt).unwrap();

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

        let algorithm = SlidingWindow::with_clock(clock);

        let attempt = AcquireAttempt {
            tokens_to_acquire: tokens,
            max_tokens_per_window: max_requests,
            window_duration: Duration::from_secs(window_secs),
            previous_window_requests: previous_requests,
            current_window_requests: current_requests,
        };

        let result = algorithm.try_acquire(&attempt);
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

            let attempt = AcquireAttempt {
                tokens_to_acquire: tokens,
                max_tokens_per_window: max_requests,
                window_duration: Duration::from_secs(window_secs),
                previous_window_requests: previous_requests,
                current_window_requests: current_requests,
            };

            let algorithm = SlidingWindow::with_clock(clock);

            let _ = algorithm.try_acquire(&attempt);
        }
    }
}
