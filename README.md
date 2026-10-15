# resilience-rs

Synchronous resilience primitives for small Rust services and command-line tools.

The crate includes retries with bounded exponential backoff, a circuit breaker, a blocking bulkhead, a deterministic token-bucket rate limiter, and cooperative cancellation tokens. It has no async runtime dependency.

```rust
use resilience_rs::{retry, RetryPolicy};

let value = retry(RetryPolicy::default(), |_| Ok::<_, ()>(42))?;
```

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

The APIs are intentionally small and the state transitions are covered by tests.
