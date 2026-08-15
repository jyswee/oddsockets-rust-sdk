# OddSockets Rust SDK

[![Crates.io](https://img.shields.io/crates/v/oddsockets.svg)](https://crates.io/crates/oddsockets)
[![Documentation](https://docs.rs/oddsockets/badge.svg)](https://docs.rs/oddsockets)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.70+-blue.svg)](https://www.rust-lang.org)

Official Rust SDK for [OddSockets](https://oddsockets.com) - a high-performance real-time messaging platform.

## Features

- **High Performance**: Built on Tokio for maximum async performance with zero-cost abstractions
- **Type Safety**: Full Rust type safety with comprehensive error handling using `thiserror`
- **Real-time Messaging**: WebSocket-based real-time communication with automatic reconnection
- **Bulk Publishing**: Efficient multi-message publishing for high-throughput scenarios
- **Presence Tracking**: Real-time user presence information with channel-level granularity
- **Message History**: Retrieve historical messages with flexible filtering options
- **Auto Reconnection**: Intelligent reconnection with exponential backoff and jitter
- **Zero-Copy**: Efficient message handling with minimal allocations
- **Production Ready**: Comprehensive error handling, logging, and monitoring support

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
oddsockets = "0.1.0-beta.1"
tokio = { version = "1.0", features = ["full"] }
```

## 🏃 Quick Start

```rust
use oddsockets::{OddSocketsClient, OddSocketsConfig, message_types};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create and configure client
    let config = OddSocketsConfig::new("ak_your_api_key_here");
    let client = OddSocketsClient::new(config).await?;

    // Connect to OddSockets
    client.connect().await?;

    // Get a channel
    let channel = client.channel("my-channel");

    // Subscribe to messages
    let mut message_stream = channel.subscribe(Default::default()).await?;
    
    // Publish a message
    let message = message_types::chat_message("Hello, Rust!", "user123", None);
    channel.publish(message, Default::default()).await?;

    // Listen for messages
    while let Some(message) = message_stream.recv().await {
        println!("Received: {:?}", message);
    }

    Ok(())
}
```

## Configuration

### Basic Configuration

```rust
use oddsockets::OddSocketsConfig;

let config = OddSocketsConfig::new("ak_your_api_key_here");
```

### Advanced Configuration

```rust
use oddsockets::OddSocketsConfig;
use std::time::Duration;

let config = OddSocketsConfig::builder("ak_your_api_key_here")
    .manager_url("https://your-connect.oddsockets.tyga.network")
    .user_id("user123")
    .auto_connect(true)
    .heartbeat_interval(Duration::from_secs(30))
    .reconnect_attempts(5)
    .timeout(Duration::from_secs(10))
    .build()?;
```

### Environment-Specific Configurations

```rust
// Development environment
let config = OddSocketsConfig::builder("ak_your_api_key_here")
    .development()
    .build()?;

// Production environment
let config = OddSocketsConfig::builder("ak_your_api_key_here")
    .production()
    .build()?;

// High-performance scenarios
let config = OddSocketsConfig::builder("ak_your_api_key_here")
    .high_performance()
    .build()?;
```

## 📨 Publishing Messages

### Individual Messages

```rust
use oddsockets::{PublishOptions, message_types};

// Simple message
let message = message_types::chat_message("Hello!", "user123", None);
let result = channel.publish(message, PublishOptions::default()).await?;

// Message with options
let message = message_types::notification_message(
    "Alert", 
    "Something happened", 
    Some("system"), 
    Some("high"), 
    None
);
let options = PublishOptions::system_message();
let result = channel.publish(message, options).await?;
```

### Bulk Publishing

```rust
use oddsockets::{BulkMessage, message_types};

let messages = vec![
    BulkMessage::new(
        "channel1", 
        message_types::chat_message("Hello", "user1", None), 
        None
    ),
    BulkMessage::new(
        "channel2", 
        message_types::chat_message("World", "user2", None), 
        None
    ),
];

let results = client.publish_bulk(messages).await?;
for result in results {
    if result.is_successful() {
        println!("Message published successfully");
    } else {
        println!("Failed: {}", result.error_message("Unknown error"));
    }
}
```

## 🔔 Subscribing to Channels

### Basic Subscription

```rust
use oddsockets::SubscribeOptions;

let mut message_stream = channel.subscribe(SubscribeOptions::default()).await?;

while let Some(message) = message_stream.recv().await {
    println!("Received: {:?}", message);
}
```

### Subscription with Options

```rust
// Chat channel with presence and history
let options = SubscribeOptions::chat_channel();
let mut stream = channel.subscribe(options).await?;

// Notification channel (minimal options)
let options = SubscribeOptions::notification_channel();
let mut stream = channel.subscribe(options).await?;

// Data channel with history
let options = SubscribeOptions::data_channel();
let mut stream = channel.subscribe(options).await?;
```

## ⚡ Enhanced Features

Beyond core pub/sub, OddSockets ships a Slack-like **enhanced surface** — reactions,
typing indicators, threads, read receipts, presence/status, notifications, DMs,
channel management, message editing and search. It lives on the `EnhancedFeatures`
struct, which wraps a shared handle to your connected client. The pattern is always
the same:

1. **Send** an action with an `enhanced.*` method (snake_case, positional arguments,
   all `async`).
2. **Receive** the paired broadcast with `client.on("<event>", |data| { ... })` — the
   worker forwards every enhanced broadcast onto the client's raw event surface
   (delivered as a `serde_json::Value`).

`OddSocketsClient` is cheap to clone and every clone shares the same underlying
socket, so wrapping a clone in `Arc<RwLock<...>>` gives `EnhancedFeatures` a handle to
the very same connection your listeners are attached to.

```rust
use std::sync::Arc;
use tokio::sync::RwLock;
use oddsockets::{EnhancedFeatures, OddSocketsClient, OddSocketsConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = OddSocketsConfig::new("ak_your_api_key_here");
    let client = OddSocketsClient::new(config).await?;
    client.connect().await?;

    let channel = client.channel("room-42");
    let _stream = channel.subscribe(Default::default()).await?;

    // Receive-path: enhanced broadcasts arrive on the client's raw event surface
    client.on("user_typing",    |data| println!("someone is typing: {data}"));
    client.on("reaction_added", |data| println!("reaction added: {data}"));
    client.on("thread_reply",   |data| println!("new thread reply: {data}"));

    // Send-path: wrap a clone (same socket) for the enhanced surface
    let enhanced = EnhancedFeatures::new(Arc::new(RwLock::new(client.clone())));

    enhanced.start_typing("alice", "room-42").await?;
    enhanced.add_reaction("msg-1", "room-42", ":thumbsup:", "alice", "Alice").await?;

    // Request-style methods return the worker response as a serde_json::Value
    let reply = enhanced
        .thread_reply("room-42", "msg-1", "Replying in the thread", "alice", "Alice")
        .await?;
    println!("thread reply ack: {reply}");

    Ok(())
}
```

Fire-and-forget actions return `Result<(), OddSocketsError>`; query and request-style
methods (`get_*`, `search_*`, `thread_reply`, `create_channel`, …) await the worker
acknowledgement and return `Result<Value, OddSocketsError>`.

| Area | Requests (`enhanced.*`) | Broadcast events (`client.on`) |
|------|-------------------------|--------------------------------|
| Typing | `start_typing`, `stop_typing` | `user_typing`, `user_stopped_typing` |
| Reactions | `add_reaction`, `remove_reaction`, `get_reactions` | `reaction_added`, `reaction_removed` |
| Threads | `thread_reply`, `get_thread`, `subscribe_thread`, `follow_thread`, `unfollow_thread`, `mark_thread_read` | `thread_reply`, `thread_subscribed`, `thread_followed`, `thread_read_updated` |
| Read receipts | `mark_read`, `mark_all_read`, `get_unread_counts` | `user_read`, `unread_count_updated`, `all_marked_read` |
| Messages | `edit_message`, `delete_message`, `pin_message`, `unpin_message`, `get_pinned_messages` | `message_edited`, `message_deleted`, `message_pinned`, `message_unpinned` |
| Presence & status | `set_status`, `set_custom_status`, `clear_custom_status`, `set_dnd`, `clear_dnd`, `get_user_presence` | `user_status_changed`, `custom_status_updated`, `dnd_status_changed` |
| Channels | `create_channel`, `update_channel`, `archive_channel`, `invite_to_channel`, `join_channel`, `leave_channel`, `get_channel_members` | `channel_created`, `channel_updated`, `user_invited`, `user_joined_channel`, `user_left_channel` |
| DMs | `create_dm`, `send_dm`, `get_dm_conversations` | `dm_created`, `dm_received` |
| Notifications | `subscribe_notifications`, `get_notifications`, `mark_notification_read`, `clear_notifications` | `notification`, `notification_read`, `notifications_cleared` |
| Search | `search_messages`, `search_in_channel`, `search_by_user`, `filter_messages` | (query results returned as `Value`) |

For any worker event not wrapped above, subscribe with the raw
`client.on("<event>", |data| { ... })` API — all enhanced broadcasts are forwarded onto
the client surface.

## Message History

```rust
use oddsockets::HistoryOptions;

// Get recent messages
let history = channel.get_history(HistoryOptions::recent(50)).await?;

// Get messages from the last hour
let history = channel.get_history(HistoryOptions::last_hour(None)).await?;

// Get messages from the last day
let history = channel.get_history(HistoryOptions::last_day(Some(1000))).await?;

// Custom time range
let options = HistoryOptions {
    limit: Some(100),
    start: Some(chrono::Utc::now() - chrono::Duration::hours(2)),
    end: Some(chrono::Utc::now()),
    reverse: true,
};
let history = channel.get_history(options).await?;
```

## 👥 Presence Tracking

```rust
// Get current presence
let presence = channel.get_presence().await?;
println!("Channel has {} users", presence.count);

for user in &presence.users {
    println!("User: {}", user);
}

// Check if specific user is present
if presence.is_user_present("user123") {
    println!("User123 is online");
}
```

## 🔄 Connection Management

### Manual Connection Control

```rust
// Connect
client.connect().await?;

// Check connection state
if client.is_connected() {
    println!("Connected!");
}

// Disconnect
client.disconnect().await?;
```

### Connection State Monitoring

```rust
use oddsockets::ConnectionState;

let mut state_stream = client.connection_state_stream();

while let Some(state) = state_stream.recv().await {
    match state {
        ConnectionState::Connected => println!("Connected!"),
        ConnectionState::Disconnected => println!("Disconnected"),
        ConnectionState::Reconnecting => println!("Reconnecting..."),
        _ => {}
    }
}
```

## Message Types

The SDK provides convenient message type constructors:

```rust
use oddsockets::message_types;

// Chat message
let chat = message_types::chat_message("Hello!", "user123", Some("general"));

// Notification
let notification = message_types::notification_message(
    "Alert",
    "Something happened",
    Some("system"),
    Some("high"),
    Some(serde_json::json!({"extra": "data"}))
);

// System message
let system = message_types::system_message(
    "user_joined",
    "User joined the channel",
    Some(serde_json::json!({"userId": "user123"}))
);

// Data event
let data_event = message_types::data_event(
    "sensor_reading",
    serde_json::json!({"temperature": 23.5, "humidity": 45.2}),
    Some("sensor_01")
);
```

## Error Handling

The SDK uses comprehensive error types with recovery suggestions:

```rust
use oddsockets::{OddSocketsError, OddSocketsResultExt};

match client.connect().await {
    Ok(_) => println!("Connected successfully"),
    Err(OddSocketsError::InvalidApiKey { message }) => {
        println!("Invalid API key: {}", message);
        for suggestion in error.recovery_suggestions() {
            println!("  - {}", suggestion);
        }
    }
    Err(OddSocketsError::ConnectionFailed { message }) => {
        println!("Connection failed: {}", message);
        if error.should_reconnect() {
            // Implement retry logic
        }
    }
    Err(e) => println!("Other error: {}", e),
}
```

## Advanced Usage

### Custom Message Handling

```rust
use tokio::spawn;

let mut message_stream = channel.subscribe(SubscribeOptions::default()).await?;

spawn(async move {
    while let Some(message) = message_stream.recv().await {
        // Process message in background
        process_message(message).await;
    }
});
```

### Multiple Channels

```rust
use futures::future::join_all;

let channels = vec!["channel1", "channel2", "channel3"];
let mut streams = Vec::new();

for channel_name in channels {
    let channel = client.channel(channel_name);
    let stream = channel.subscribe(SubscribeOptions::default()).await?;
    streams.push(stream);
}

// Handle all streams concurrently
let handlers: Vec<_> = streams.into_iter().map(|mut stream| {
    spawn(async move {
        while let Some(message) = stream.recv().await {
            println!("Received on {}: {:?}", message.channel, message);
        }
    })
}).collect();

join_all(handlers).await;
```

### Graceful Shutdown

```rust
use tokio::signal;

// Set up graceful shutdown
let shutdown = async {
    signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
    println!("Shutting down gracefully...");
};

tokio::select! {
    _ = shutdown => {
        // Cleanup
        client.disconnect().await?;
        println!("Shutdown complete");
    }
    _ = message_handler => {
        // Message handling completed
    }
}
```

## Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use oddsockets::{OddSocketsConfig, OddSocketsClient};

    #[tokio::test]
    async fn test_client_creation() {
        let config = OddSocketsConfig::new("ak_test_key");
        let client = OddSocketsClient::new(config).await.unwrap();
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_message_creation() {
        let message = message_types::chat_message("test", "user", None);
        assert_eq!(message["text"], "test");
        assert_eq!(message["username"], "user");
    }
}
```

## Performance

The Rust SDK is designed for high performance:

- **Zero-copy message handling** where possible
- **Efficient WebSocket implementation** using `tokio-tungstenite`
- **Minimal allocations** in hot paths
- **Async-first design** for maximum concurrency
- **Connection pooling** and reuse
- **Batch operations** for bulk publishing

### Benchmarks

```bash
cargo bench
```

## Logging

Enable logging to debug issues:

```rust
env_logger::init();
```

Or with custom configuration:

```rust
use log::LevelFilter;

env_logger::Builder::from_default_env()
    .filter_level(LevelFilter::Debug)
    .init();
```

## Development

### Building

```bash
cargo build
```

### Running Tests

```bash
cargo test
```

### Running Examples

```bash
cargo run --example basic_usage
cargo run --example bulk_publishing
cargo run --example chat_application
```

### Documentation

```bash
cargo doc --open
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run `cargo test` and `cargo clippy`
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Get a Free API Key

AI agents can sign up with a verified email in two steps — no dashboard, no human required.

**Step 1:** Request a verification code
```bash
curl -X POST https://oddsockets.com/api/agent-signup \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "agentName": "my-agent", "platform": "rust"}'
```

**Step 2:** Verify the 6-digit code from your email and get your API key
```bash
curl -X POST https://oddsockets.com/api/agent-signup/verify \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "code": "123456", "agentName": "my-agent"}'
```

## Plans

| | Free | Starter | Pro |
|---|---|---|---|
| **Price** | $0/mo | $49.99/mo | $299/mo |
| **MAU** | 100 | 1,000 | 50,000 |
| **Concurrent connections** | 50 | 1,000 | Unlimited |
| **Messages/day** | 10,000 | 4,320,000 | Unlimited |
| **Messages/minute** | 100 | 3,000 | Unlimited |
| **Channels** | 10 | Unlimited | Unlimited |
| **Storage** | 100MB (24h) | 50GB (6 months) | Unlimited |

All limits are enforced in real time.

## Get Accredited

<a href="https://tyga.games/accreditation"><img src="https://prodmedia.tyga.host/public/tyga.cloud/landing/tyga.games/tygagames-black-words.svg" alt="tyga.games accreditation" height="44"></a>

Prove you can build and operate real-time features on OddSockets — channels, presence, pub/sub, delivery guarantees and production liveops — on the stack itself. Three tiers (**TCU / TCA / TCP**), certified through **tyga.games** and delivered on ClassaaS.

[**Get accredited on tyga.games →**](https://tyga.games/accreditation)

## Support

- [Documentation](https://docs.oddsockets.com/sdks/rust)
- [Issue Tracker](https://github.com/jyswee/oddsockets-rust-sdk/issues)
- [Email Support](mailto:support@oddsockets.com)

## License

MIT License - Copyright (c) 2026 Joe Wee, Tyga.Cloud Ltd. See [LICENSE](LICENSE) for details.
