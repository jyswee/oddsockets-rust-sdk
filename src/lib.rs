//! # OddSockets Rust SDK
//!
//! Official Rust SDK for OddSockets real-time messaging platform.
//!
//! This SDK provides a high-performance, async-first interface for connecting to OddSockets,
//! enabling real-time messaging, presence tracking, and message history features with
//! full Rust type safety and zero-cost abstractions.
//!
//! ## Features
//!
//! - **High Performance**: Built on Tokio for maximum async performance
//! - **Type Safety**: Full Rust type safety with comprehensive error handling
//! - **Real-time Messaging**: WebSocket-based real-time communication
//! - **Bulk Publishing**: Efficient multi-message publishing
//! - **Presence Tracking**: Real-time user presence information
//! - **Message History**: Retrieve historical messages with filtering
//! - **Auto Reconnection**: Intelligent reconnection with exponential backoff
//! - **Zero-Copy**: Efficient message handling with minimal allocations
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use oddsockets::{OddSocketsClient, OddSocketsConfig, message_types};
//! use tokio;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a client
//!     let config = OddSocketsConfig::new("ak_your_api_key_here");
//!     let client = OddSocketsClient::new(config).await?;
//!
//!     // Connect to OddSockets
//!     client.connect().await?;
//!
//!     // Get a channel
//!     let channel = client.channel("my-channel");
//!
//!     // Subscribe to messages
//!     let mut message_stream = channel.subscribe(Default::default()).await?;
//!     
//!     // Publish a message
//!     let message = message_types::chat_message("Hello, Rust!", "user123", None);
//!     channel.publish(message, Default::default()).await?;
//!
//!     // Listen for messages
//!     while let Some(message) = message_stream.recv().await {
//!         println!("Received: {:?}", message);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Configuration
//!
//! Use the configuration builder for advanced setups:
//!
//! ```rust,no_run
//! use oddsockets::OddSocketsConfig;
//! use std::time::Duration;
//!
//! let config = OddSocketsConfig::builder("ak_your_api_key_here")
//!     .high_performance() // Optimized for high-performance scenarios
//!     .heartbeat_interval(Duration::from_secs(60))
//!     .reconnect_attempts(3)
//!     .build()?;
//! # Ok::<(), oddsockets::error::OddSocketsError>(())
//! ```
//!
//! ## Bulk Publishing
//!
//! Efficiently publish multiple messages:
//!
//! ```rust,no_run
//! use oddsockets::{BulkMessage, message_types};
//! # use oddsockets::{OddSocketsClient, OddSocketsConfig};
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let config = OddSocketsConfig::new("ak_test");
//! # let client = OddSocketsClient::new(config).await?;
//!
//! let messages = vec![
//!     BulkMessage::new("channel1", message_types::chat_message("Hello", "user1", None), None),
//!     BulkMessage::new("channel2", message_types::chat_message("World", "user2", None), None),
//! ];
//!
//! let results = client.publish_bulk(messages).await?;
//! for result in results {
//!     if result.is_successful() {
//!         println!("Message published successfully");
//!     }
//! }
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]
#![allow(clippy::module_name_repetitions)]

pub mod error;
pub mod types;
pub mod manager_discovery;
pub mod message_size_validator;
pub mod enhanced_features;

mod client;
mod channel;

// Re-export main types for convenience
pub use client::OddSocketsClient;
pub use channel::OddSocketsChannel;
pub use enhanced_features::EnhancedFeatures;
pub use error::{OddSocketsError, Result};
pub use types::{
    constants, error_codes, message_types, utils, BulkMessage, BulkResult, ConnectionState,
    EventType, HistoryOptions, Message, OddSocketsConfig, OddSocketsConfigBuilder, PresenceInfo,
    PublishOptions, PublishResult, SubscribeOptions,
};

/// Prelude module for convenient imports.
///
/// This module re-exports the most commonly used types and traits,
/// allowing users to import everything they need with a single use statement.
///
/// ```rust
/// use oddsockets::prelude::*;
/// ```
pub mod prelude {
    pub use crate::client::OddSocketsClient;
    pub use crate::channel::OddSocketsChannel;
    pub use crate::error::{OddSocketsError, OddSocketsResultExt, Result};
    pub use crate::types::{
        message_types, utils, BulkMessage, BulkResult, ConnectionState, EventType, HistoryOptions,
        Message, OddSocketsConfig, OddSocketsConfigBuilder, PresenceInfo, PublishOptions,
        PublishResult, SubscribeOptions,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = OddSocketsConfig::new("ak_test_key");
        assert_eq!(config.api_key, "ak_test_key");
        assert!(config.auto_connect);
    }

    #[test]
    fn test_config_builder() {
        let config = OddSocketsConfig::builder("ak_test_key")
            .auto_connect(false)
            .reconnect_attempts(10)
            .build()
            .unwrap();

        assert_eq!(config.api_key, "ak_test_key");
        assert!(!config.auto_connect);
        assert_eq!(config.reconnect_attempts, 10);
    }

    #[test]
    fn test_message_creation() {
        let message = Message::new(
            "test-channel",
            serde_json::json!({"text": "Hello, World!"}),
            Some("user123".to_string()),
            None,
        );

        assert_eq!(message.channel, "test-channel");
        assert_eq!(message.user_id, Some("user123".to_string()));
        assert!(message.has_data());
    }

    #[test]
    fn test_bulk_message_creation() {
        let bulk_message = BulkMessage::new(
            "test-channel",
            serde_json::json!({"text": "Bulk message"}),
            Some(PublishOptions::with_history()),
        );

        assert_eq!(bulk_message.channel, "test-channel");
        assert!(bulk_message.options.is_some());
        assert!(bulk_message.options.unwrap().store_in_history);
    }

    #[test]
    fn test_subscribe_options() {
        let options = SubscribeOptions::chat_channel();
        assert!(options.enable_presence);
        assert!(options.retain_history);

        let options = SubscribeOptions::notification_channel();
        assert!(!options.enable_presence);
        assert!(!options.retain_history);
    }

    #[test]
    fn test_publish_options() {
        let options = PublishOptions::chat_message();
        assert!(options.store_in_history);
        assert!(options.metadata.is_some());

        let options = PublishOptions::with_ttl(3600);
        assert_eq!(options.ttl, Some(3600));
    }

    #[test]
    fn test_history_options() {
        let options = HistoryOptions::recent(50);
        assert_eq!(options.limit, Some(50));
        assert!(options.reverse);

        let options = HistoryOptions::last_hour(None);
        assert_eq!(options.limit, Some(100));
        assert!(options.start.is_some());
        assert!(options.reverse);
    }

    #[test]
    fn test_message_types() {
        let chat_msg = message_types::chat_message("Hello", "user123", None);
        assert_eq!(chat_msg["text"], "Hello");
        assert_eq!(chat_msg["username"], "user123");
        assert_eq!(chat_msg["messageType"], "chat");

        let notification = message_types::notification_message(
            "Alert",
            "Something happened",
            Some("system"),
            Some("high"),
            None,
        );
        assert_eq!(notification["title"], "Alert");
        assert_eq!(notification["category"], "system");
        assert_eq!(notification["priority"], "high");
    }

    #[test]
    fn test_utils() {
        let message_id = utils::generate_message_id();
        assert!(message_id.starts_with("msg_"));

        let user_id = utils::generate_user_id();
        assert!(user_id.starts_with("user_"));

        let bulk_msg = utils::bulk_message(
            "test",
            serde_json::json!({"test": true}),
            None,
        );
        assert_eq!(bulk_msg.channel, "test");
    }
}
