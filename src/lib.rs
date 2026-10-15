//! Composable, dependency-light resilience primitives.

use std::{fmt::Display, thread, time::Duration};

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RetryError<E> {
    pub attempts: u32,
    pub source: E,
}

impl<E: Display> Display for RetryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "operation failed after {} attempts: {}",
            self.attempts, self.source
        )
    }
}

pub fn retry<T, E, F>(policy: RetryPolicy, mut operation: F) -> Result<T, RetryError<E>>
where
    F: FnMut(u32) -> Result<T, E>,
{
    let attempts = policy.max_attempts.max(1);
    let mut delay = policy.initial_delay;
    for attempt in 1..=attempts {
        match operation(attempt) {
            Ok(value) => return Ok(value),
            Err(source) if attempt == attempts => {
                return Err(RetryError {
                    attempts: attempt,
                    source,
                });
            }
            Err(_) => {
                thread::sleep(delay);
                delay = delay.saturating_mul(2).min(policy.max_delay);
            }
        }
    }
    unreachable!()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

pub struct CircuitBreaker {
    threshold: u32,
    failures: u32,
    state: CircuitState,
}

impl CircuitBreaker {
    pub fn new(threshold: u32) -> Self {
        Self {
            threshold: threshold.max(1),
            failures: 0,
            state: CircuitState::Closed,
        }
    }
    pub fn state(&self) -> CircuitState {
        self.state
    }
    pub fn allow(&self) -> bool {
        self.state != CircuitState::Open
    }
    pub fn record_success(&mut self) {
        self.failures = 0;
        self.state = CircuitState::Closed;
    }
    pub fn record_failure(&mut self) {
        self.failures += 1;
        if self.failures >= self.threshold {
            self.state = CircuitState::Open;
        }
    }
    pub fn probe(&mut self) {
        if self.state == CircuitState::Open {
            self.state = CircuitState::HalfOpen;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_eventually_succeeds() {
        let mut calls = 0;
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        };
        let result: Result<u8, RetryError<&str>> = retry(policy, |_| {
            calls += 1;
            if calls < 3 { Err("transient") } else { Ok(42) }
        });
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls, 3);
    }
    #[test]
    fn circuit_opens_and_recovers() {
        let mut circuit = CircuitBreaker::new(2);
        circuit.record_failure();
        circuit.record_failure();
        assert_eq!(circuit.state(), CircuitState::Open);
        circuit.probe();
        circuit.record_success();
        assert_eq!(circuit.state(), CircuitState::Closed);
    }
}
