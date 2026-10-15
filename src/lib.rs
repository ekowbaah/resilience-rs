//! Composable, dependency-light resilience primitives.

use std::{
    fmt::Display,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

/// Cooperative cancellation which can be cloned into dependent operations.
#[derive(Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// A fair-enough, blocking bulkhead that bounds concurrent work without an async runtime.
pub struct Bulkhead {
    capacity: usize,
    in_flight: Mutex<usize>,
    changed: Condvar,
}
pub struct Permit<'a>(&'a Bulkhead);

impl Bulkhead {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            in_flight: Mutex::new(0),
            changed: Condvar::new(),
        }
    }
    pub fn try_acquire(&self) -> Option<Permit<'_>> {
        let mut count = self.in_flight.lock().expect("bulkhead poisoned");
        if *count >= self.capacity {
            return None;
        }
        *count += 1;
        Some(Permit(self))
    }
    pub fn acquire_timeout(&self, timeout: Duration) -> Option<Permit<'_>> {
        let deadline = Instant::now() + timeout;
        let mut count = self.in_flight.lock().expect("bulkhead poisoned");
        while *count >= self.capacity {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let (next, result) = self
                .changed
                .wait_timeout(count, remaining)
                .expect("bulkhead poisoned");
            count = next;
            if result.timed_out() && *count >= self.capacity {
                return None;
            }
        }
        *count += 1;
        Some(Permit(self))
    }
    pub fn in_flight(&self) -> usize {
        *self.in_flight.lock().expect("bulkhead poisoned")
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        *self.0.in_flight.lock().expect("bulkhead poisoned") -= 1;
        self.0.changed.notify_one();
    }
}

/// Deterministic token bucket. Callers provide time, which makes policy tests reproducible.
pub struct RateLimiter {
    capacity: u64,
    tokens: u64,
    refill_every: Duration,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(capacity: u64, refill_every: Duration, now: Instant) -> Self {
        Self {
            capacity: capacity.max(1),
            tokens: capacity.max(1),
            refill_every: refill_every.max(Duration::from_nanos(1)),
            last_refill: now,
        }
    }
    pub fn allow_at(&mut self, now: Instant) -> bool {
        let periods = now.saturating_duration_since(self.last_refill).as_nanos()
            / self.refill_every.as_nanos();
        if periods > 0 {
            self.tokens = self.capacity.min(
                self.tokens
                    .saturating_add(periods.min(u64::MAX as u128) as u64),
            );
            self.last_refill += self
                .refill_every
                .saturating_mul(periods.min(u32::MAX as u128) as u32);
        }
        if self.tokens == 0 {
            false
        } else {
            self.tokens -= 1;
            true
        }
    }
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
    #[test]
    fn bulkhead_releases_capacity_on_drop() {
        let b = Bulkhead::new(1);
        let permit = b.try_acquire().unwrap();
        assert!(b.try_acquire().is_none());
        assert_eq!(b.in_flight(), 1);
        drop(permit);
        assert!(b.try_acquire().is_some());
    }
    #[test]
    fn token_bucket_refills_at_a_defined_rate() {
        let now = Instant::now();
        let mut limiter = RateLimiter::new(2, Duration::from_secs(1), now);
        assert!(limiter.allow_at(now));
        assert!(limiter.allow_at(now));
        assert!(!limiter.allow_at(now));
        assert!(limiter.allow_at(now + Duration::from_secs(1)));
    }
    #[test]
    fn cancellation_is_visible_across_clones() {
        let token = CancellationToken::default();
        let child = token.clone();
        token.cancel();
        assert!(child.is_cancelled());
    }
}
