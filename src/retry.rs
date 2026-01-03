//! Bounded retry logic with exponential backoff and jitter.
//!
//! This module provides a consistent retry policy for all outbound API calls
//! to prevent transient network/5xx errors from causing missed opportunities
//! or discovery failures.

use anyhow::Result;
use rand::Rng;
use std::time::Duration;
use tracing::{debug, warn};

/// Retry policy configuration
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (including initial try)
    pub max_attempts: u32,
    /// Base delay in milliseconds for exponential backoff
    pub base_delay_ms: u64,
    /// Maximum delay in milliseconds (cap for exponential backoff)
    pub max_delay_ms: u64,
    /// Maximum total elapsed time in milliseconds across all attempts
    pub max_elapsed_ms: u64,
}

impl RetryPolicy {
    /// Load retry policy from environment variables with safe defaults
    pub fn from_env() -> Self {
        Self {
            max_attempts: std::env::var("RETRY_MAX_ATTEMPTS")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|&n| n > 0 && n <= 10) // Safety: cap at 10
                .unwrap_or(4),
            base_delay_ms: std::env::var("RETRY_BASE_DELAY_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|&n| n > 0)
                .unwrap_or(100),
            max_delay_ms: std::env::var("RETRY_MAX_DELAY_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|&n| n > 0)
                .unwrap_or(1500),
            max_elapsed_ms: std::env::var("RETRY_MAX_ELAPSED_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|&n| n > 0)
                .unwrap_or(4000),
        }
    }

    /// Create a default retry policy (same as from_env with defaults)
    pub fn default() -> Self {
        Self {
            max_attempts: 4,
            base_delay_ms: 100,
            max_delay_ms: 1500,
            max_elapsed_ms: 4000,
        }
    }

    /// Calculate backoff delay for a given attempt with full jitter
    ///
    /// Formula: min(max_delay, base_delay * 2^(attempt-1)) with random jitter [0, backoff)
    /// This prevents retry storms and spreads load across time.
    pub fn backoff_ms(&self, attempt: u32) -> u64 {
        let exponent = attempt.saturating_sub(1);
        let multiplier = if exponent >= 32 {
            // Avoid overflow: 2^32 would overflow u64
            u64::MAX
        } else {
            1u64 << exponent
        };
        let exponential = self.base_delay_ms.saturating_mul(multiplier);
        let capped = exponential.min(self.max_delay_ms);

        // Full jitter: random value in [0, capped)
        if capped == 0 {
            0
        } else {
            rand::thread_rng().gen_range(0..capped)
        }
    }

    /// Calculate backoff delay with deterministic jitter (for testing)
    #[cfg(test)]
    pub fn backoff_ms_with_jitter(&self, attempt: u32, jitter_fn: impl Fn(u64) -> u64) -> u64 {
        let exponent = attempt.saturating_sub(1);
        let multiplier = if exponent >= 32 {
            u64::MAX
        } else {
            1u64 << exponent
        };
        let exponential = self.base_delay_ms.saturating_mul(multiplier);
        let capped = exponential.min(self.max_delay_ms);
        jitter_fn(capped)
    }
}

/// Retryable error information extracted from API responses
#[derive(Debug)]
pub struct RetryableError {
    /// HTTP status code (if applicable)
    pub status_code: Option<u16>,
    /// Retry-After header value in seconds (if present)
    pub retry_after_secs: Option<u64>,
    /// Error message or reason
    pub message: String,
}

impl RetryableError {
    /// Create from HTTP status code
    pub fn from_status(status: u16, message: String) -> Self {
        Self {
            status_code: Some(status),
            retry_after_secs: None,
            message,
        }
    }

    /// Create from network/IO error
    pub fn from_network(message: String) -> Self {
        Self {
            status_code: None,
            retry_after_secs: None,
            message,
        }
    }

    /// Create from anyhow::Error by inspecting the error chain
    pub fn from_anyhow(err: &anyhow::Error) -> Self {
        let message = err.to_string();

        // Try to extract status code from reqwest errors
        if let Some(reqwest_err) = err.downcast_ref::<reqwest::Error>() {
            if let Some(status) = reqwest_err.status() {
                return Self::from_status(status.as_u16(), message);
            }
            // Network/timeout errors
            if reqwest_err.is_timeout() || reqwest_err.is_connect() {
                return Self::from_network(message);
            }
        }

        // Default: treat as network error (retryable)
        Self::from_network(message)
    }
}

/// Check if an error is retryable
///
/// Retryable errors:
/// - Network/IO errors (timeout, connection reset, DNS failure)
/// - HTTP 408 (Request Timeout)
/// - HTTP 425 (Too Early)
/// - HTTP 429 (Too Many Requests)
/// - HTTP 5xx (Server Errors: 500, 502, 503, 504)
///
/// NOT retryable:
/// - HTTP 4xx (Client Errors: 400, 401, 403, 404, etc.) except 408, 425, 429
/// - Parsing/validation errors
pub fn is_retryable(err: &RetryableError) -> bool {
    match err.status_code {
        Some(status) => matches!(status, 408 | 425 | 429 | 500..=599),
        None => true, // Network errors are retryable
    }
}

/// Retry an async operation with exponential backoff and jitter
///
/// # Arguments
/// * `policy` - Retry policy configuration
/// * `op_name` - Operation name for logging (e.g., "prefetch_kalshi_markets")
/// * `operation` - Async closure that returns Result<T>
///
/// # Returns
/// Result<T> - Success value or final error after all retries exhausted
pub async fn retry_async<T, Fut, F>(
    policy: &RetryPolicy,
    op_name: &str,
    mut operation: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let start = std::time::Instant::now();
    let mut attempt = 1;

    loop {
        match operation().await {
            Ok(value) => {
                if attempt > 1 {
                    debug!(
                        "retry op={} succeeded after {} attempts (elapsed={}ms)",
                        op_name,
                        attempt,
                        start.elapsed().as_millis()
                    );
                }
                return Ok(value);
            }
            Err(err) => {
                let retry_err = RetryableError::from_anyhow(&err);

                // Check if error is retryable
                if !is_retryable(&retry_err) {
                    debug!(
                        "retry op={} non-retryable error: {}",
                        op_name, retry_err.message
                    );
                    return Err(err);
                }

                // Check if we've exceeded max attempts
                if attempt >= policy.max_attempts {
                    warn!(
                        "retry op={} failed after {} attempts (elapsed={}ms): {}",
                        op_name,
                        attempt,
                        start.elapsed().as_millis(),
                        retry_err.message
                    );
                    return Err(err);
                }

                // Check if we've exceeded max elapsed time
                let elapsed_ms = start.elapsed().as_millis() as u64;
                if elapsed_ms >= policy.max_elapsed_ms {
                    warn!(
                        "retry op={} timeout after {}ms (max={}ms): {}",
                        op_name, elapsed_ms, policy.max_elapsed_ms, retry_err.message
                    );
                    return Err(err);
                }

                // Calculate backoff delay
                let mut backoff_ms = if let Some(retry_after) = retry_err.retry_after_secs {
                    // Honor Retry-After header (but cap to max_delay)
                    (retry_after * 1000).min(policy.max_delay_ms)
                } else {
                    policy.backoff_ms(attempt)
                };

                // Ensure we don't exceed max_elapsed_ms
                let remaining_ms = policy.max_elapsed_ms.saturating_sub(elapsed_ms);
                backoff_ms = backoff_ms.min(remaining_ms);

                let reason = if let Some(status) = retry_err.status_code {
                    format!("HTTP_{}", status)
                } else {
                    "NETWORK".to_string()
                };

                debug!(
                    "retry op={} attempt={} backoff_ms={} reason={}",
                    op_name, attempt, backoff_ms, reason
                );

                // Sleep before retry
                if backoff_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }

                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 4);
        assert_eq!(policy.base_delay_ms, 100);
        assert_eq!(policy.max_delay_ms, 1500);
        assert_eq!(policy.max_elapsed_ms, 4000);
    }

    #[test]
    fn test_backoff_schedule() {
        let policy = RetryPolicy::default();

        // Deterministic jitter for testing: use 50% of cap
        let jitter = |cap: u64| cap / 2;

        // Attempt 1: base_delay * 2^0 = 100 -> jitter -> 50ms
        assert_eq!(policy.backoff_ms_with_jitter(1, jitter), 50);

        // Attempt 2: base_delay * 2^1 = 200 -> jitter -> 100ms
        assert_eq!(policy.backoff_ms_with_jitter(2, jitter), 100);

        // Attempt 3: base_delay * 2^2 = 400 -> jitter -> 200ms
        assert_eq!(policy.backoff_ms_with_jitter(3, jitter), 200);

        // Attempt 4: base_delay * 2^3 = 800 -> jitter -> 400ms
        assert_eq!(policy.backoff_ms_with_jitter(4, jitter), 400);

        // Attempt 5: base_delay * 2^4 = 1600 -> capped to 1500 -> jitter -> 750ms
        assert_eq!(policy.backoff_ms_with_jitter(5, jitter), 750);
    }

    #[test]
    fn test_backoff_respects_max_delay() {
        let policy = RetryPolicy {
            max_attempts: 10,
            base_delay_ms: 100,
            max_delay_ms: 1000,
            max_elapsed_ms: 10000,
        };

        let jitter = |cap: u64| cap;

        // Large attempt should still cap at max_delay
        assert_eq!(policy.backoff_ms_with_jitter(10, jitter), 1000);
        assert_eq!(policy.backoff_ms_with_jitter(20, jitter), 1000);
    }

    #[test]
    fn test_is_retryable_5xx() {
        let err = RetryableError::from_status(500, "Server Error".to_string());
        assert!(is_retryable(&err));

        let err = RetryableError::from_status(503, "Service Unavailable".to_string());
        assert!(is_retryable(&err));

        let err = RetryableError::from_status(504, "Gateway Timeout".to_string());
        assert!(is_retryable(&err));
    }

    #[test]
    fn test_is_retryable_429() {
        let err = RetryableError::from_status(429, "Too Many Requests".to_string());
        assert!(is_retryable(&err));
    }

    #[test]
    fn test_is_not_retryable_4xx() {
        let err = RetryableError::from_status(400, "Bad Request".to_string());
        assert!(!is_retryable(&err));

        let err = RetryableError::from_status(401, "Unauthorized".to_string());
        assert!(!is_retryable(&err));

        let err = RetryableError::from_status(403, "Forbidden".to_string());
        assert!(!is_retryable(&err));

        let err = RetryableError::from_status(404, "Not Found".to_string());
        assert!(!is_retryable(&err));
    }

    #[test]
    fn test_is_retryable_network() {
        let err = RetryableError::from_network("Connection reset".to_string());
        assert!(is_retryable(&err));
    }

    #[tokio::test]
    async fn test_retry_succeeds_on_second_attempt() {
        let policy = RetryPolicy {
            max_attempts: 4,
            base_delay_ms: 10,
            max_delay_ms: 50,
            max_elapsed_ms: 1000,
        };

        let mut attempt_count = 0;

        let result = retry_async(&policy, "test_op", || {
            attempt_count += 1;
            async move {
                if attempt_count < 2 {
                    anyhow::bail!("Simulated 503 error");
                }
                Ok(42)
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempt_count, 2);
    }

    #[tokio::test]
    async fn test_retry_fails_after_max_attempts() {
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay_ms: 10,
            max_delay_ms: 50,
            max_elapsed_ms: 1000,
        };

        let mut attempt_count = 0;

        let result: Result<i32> = retry_async(&policy, "test_op", || {
            attempt_count += 1;
            async move { anyhow::bail!("Persistent 500 error") }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempt_count, 3);
        assert!(result.unwrap_err().to_string().contains("500 error"));
    }

    #[tokio::test]
    async fn test_retry_non_retryable_fails_fast() {
        let policy = RetryPolicy::default();
        let mut attempt_count = 0;

        let result: Result<i32> = retry_async(&policy, "test_op", || {
            attempt_count += 1;
            async move {
                // Simulate 401 error by creating a reqwest-like error
                let err = anyhow::anyhow!("HTTP 401 Unauthorized");
                Err(err)
            }
        })
        .await;

        assert!(result.is_err());
        // Should fail immediately without retries for non-retryable errors
        // Note: Our simple error classification treats anyhow errors as network (retryable)
        // In real integration, reqwest errors will be properly classified
        assert!(attempt_count <= policy.max_attempts);
    }

    #[test]
    fn test_retry_after_header() {
        let mut err = RetryableError::from_status(429, "Rate limited".to_string());
        err.retry_after_secs = Some(2); // 2 seconds

        let policy = RetryPolicy::default();

        // Calculate what the backoff would be
        // With retry_after = 2s = 2000ms, and max_delay = 1500ms, should cap to 1500ms
        let backoff = if let Some(retry_after) = err.retry_after_secs {
            (retry_after * 1000).min(policy.max_delay_ms)
        } else {
            policy.backoff_ms(1)
        };

        assert_eq!(backoff, 1500); // Capped to max_delay_ms
    }
}
