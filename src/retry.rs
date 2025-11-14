use log::{debug, warn};
use std::time::Duration;

pub fn calculate_delay(attempt: u32) -> Duration {
    if attempt == 0 {
        return Duration::from_secs(0);
    }

    let delay_ms = 2.0_f64.powi((attempt - 1) as i32);
    let delay = Duration::from_millis(delay_ms as u64);

    delay
}

/// Retry a future with exponential backoff
pub async fn retry_with_backoff<F, Fut, T, E>(
    max_attempts: u32,
    operation_name: &str,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut last_error = None;

    for attempt in 0..max_attempts {
        if attempt > 0 {
            let delay = calculate_delay(attempt);
            debug!(
                "Retry attempt {} for '{}' after {:?}",
                attempt, operation_name, delay
            );
            tokio::time::sleep(delay).await;
        }

        match operation().await {
            Ok(result) => {
                if attempt > 0 {
                    debug!("'{}' succeeded after {} retries", operation_name, attempt);
                }
                return Ok(result);
            }
            Err(e) => {
                warn!(
                    "'{}' failed on attempt {}/{}",
                    operation_name,
                    attempt + 1,
                    e
                );
                last_error = Some(e);
            }
        }
    }

    Err(last_error.expect("Should have at least one error"))
}
