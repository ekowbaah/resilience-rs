# resilience-rs

Small, dependency-light resilience primitives for Rust services. The first release provides bounded exponential retry and an explicit circuit-breaker state machine.

```rust
use resilience_rs::{retry, RetryPolicy};
let value = retry(RetryPolicy::default(), |_| Ok::<_, ()>(42))?;
```

## Engineering goals

- Predictable behavior and inspectable state
- No hidden runtime or global executor
- Tests for transitions and retry limits
- Stable, documented API before `1.0`

Run `cargo test`. See [SECURITY.md](SECURITY.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

