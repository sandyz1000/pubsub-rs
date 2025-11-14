# pubsub-rs

A lightweight, Redis-backed pub/sub library for Rust with type-safe message routing and concurrent receiver handling.

## Features

- **Type-safe topics and keys** using generic parameters
- **Concurrent access** with DashMap for lock-free operations
- **Connection pooling** for better performance and load distribution
- **Flexible configuration** with customizable topic prefix and retry delays
- **Pattern-based subscriptions** using Redis PSUBSCRIBE
- **Graceful error handling** with automatic reconnection

## Quick Start

```rust
use pubsub::prelude::*;

let pubsub = PubSubRedis::<String, String>::new("redis://127.0.0.1:6379/0".to_string()).await?;
pubsub.set_receiver("topic".to_string(), Arc::new(MyReceiver));
pubsub.run_subscriber().await?;
pubsub.publish("topic".to_string(), "key".to_string(), message).await;
```

See `examples/` for complete usage examples.

