//! Core types and data structures for the OddSockets Rust SDK.
//!
//! This module provides all the essential types used throughout the SDK,
//! including configuration, messages, connection states, and utility types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

/// SDK version and metadata constants.
pub mod constants {
    pub const SDK_VERSION: &str = "0.1.0-beta.1";
    pub const SDK_NAME: &str = "OddSockets-Rust-SDK";
    pub const USER_AGENT: &str = concat!("OddSockets-Rust-SDK/", "0.1.0-beta.1");
    pub const DEFAULT_MANAGER_URL: &str = "https://manager1.oddsockets.tyga.network";
    pub const DEFAULT_TIMEOUT_SECS: u64 = 10;
    pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 30;
    pub const DEFAULT_RECONNECT_ATTEMPTS: u32 = 5;
    pub const MAX_MESSAGE_HISTORY_SIZE: usize = 100;
}

/// Represents the connection state of the OddSockets client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    /// Client is disconnected
    Disconnected,
    /// Client is attempting to connect
    Connecting,
    /// Client is connected and ready
    Connected,
    /// Client is attempting to reconnect
    Reconnecting,
    /// Connection failed
    Failed,
}

impl ConnectionState {
    /// Returns true if the client is connected.
    pub fn is_connected(self) -> bool {
        matches!(self, ConnectionState::Connected)
    }

    /// Returns true if the client is connecting or reconnecting.
    pub fn is_connecting(self) -> bool {
        matches!(self, ConnectionState::Connecting | ConnectionState::Reconnecting)
    }

    /// Returns true if the client is disconnected or failed.
    pub fn is_disconnected(self) -> bool {
        matches!(self, ConnectionState::Disconnected | ConnectionState::Failed)
    }
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "Disconnected"),
            ConnectionState::Connecting => write!(f, "Connecting"),
            ConnectionState::Connected => write!(f, "Connected"),
            ConnectionState::Reconnecting => write!(f, "Reconnecting"),
            ConnectionState::Failed => write!(f, "Failed"),
        }
    }
}

/// Represents different event types emitted by the OddSockets client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    /// Client connected successfully
    Connected,
    /// Client disconnected
    Disconnected,
    /// Client reconnected after a disconnection
    Reconnected,
    /// An error occurred
    Error,
    /// A message was received
    Message,
    /// Presence information updated
    Presence,
    /// Worker was assigned
    WorkerAssigned,
    /// Maximum reconnection attempts reached
    MaxReconnectAttemptsReached,
}

impl EventType {
    /// Returns true if this is a connection-related event.
    pub fn is_connection_event(self) -> bool {
        matches!(
            self,
            EventType::Connected
                | EventType::Disconnected
                | EventType::Reconnected
                | EventType::WorkerAssigned
                | EventType::MaxReconnectAttemptsReached
        )
    }

    /// Returns true if this is a message-related event.
    pub fn is_message_event(self) -> bool {
        matches!(self, EventType::Message | EventType::Presence)
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::Connected => write!(f, "Connected"),
            EventType::Disconnected => write!(f, "Disconnected"),
            EventType::Reconnected => write!(f, "Reconnected"),
            EventType::Error => write!(f, "Error"),
            EventType::Message => write!(f, "Message"),
            EventType::Presence => write!(f, "Presence"),
            EventType::WorkerAssigned => write!(f, "Worker Assigned"),
            EventType::MaxReconnectAttemptsReached => write!(f, "Max Reconnect Attempts Reached"),
        }
    }
}

/// Configuration for the OddSockets client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OddSocketsConfig {
    /// API key for authentication
    pub api_key: String,
    /// Manager URL for worker assignment
    pub manager_url: String,
    /// Optional user ID
    pub user_id: Option<String>,
    /// Whether to automatically connect on client creation
    pub auto_connect: bool,
    /// Number of reconnection attempts before giving up
    pub reconnect_attempts: u32,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Request timeout
    pub timeout: Duration,
}

impl OddSocketsConfig {
    /// Creates a new configuration with the given API key and default values.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            manager_url: constants::DEFAULT_MANAGER_URL.to_string(),
            user_id: None,
            auto_connect: true,
            reconnect_attempts: constants::DEFAULT_RECONNECT_ATTEMPTS,
            heartbeat_interval: Duration::from_secs(constants::DEFAULT_HEARTBEAT_INTERVAL_SECS),
            timeout: Duration::from_secs(constants::DEFAULT_TIMEOUT_SECS),
        }
    }

    /// Creates a builder for this configuration.
    pub fn builder(api_key: impl Into<String>) -> OddSocketsConfigBuilder {
        OddSocketsConfigBuilder::new(api_key)
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), crate::error::OddSocketsError> {
        if self.api_key.is_empty() {
            return Err(crate::error::OddSocketsError::InvalidConfiguration {
                message: "API key is required".to_string(),
            });
        }

        if !self.api_key.starts_with("ak_") {
            return Err(crate::error::OddSocketsError::InvalidApiKey {
                message: "Invalid API key format".to_string(),
            });
        }

        if self.manager_url.is_empty() {
            return Err(crate::error::OddSocketsError::InvalidConfiguration {
                message: "Manager URL is required".to_string(),
            });
        }

        if self.timeout.is_zero() {
            return Err(crate::error::OddSocketsError::InvalidConfiguration {
                message: "Timeout must be greater than zero".to_string(),
            });
        }

        if self.heartbeat_interval.is_zero() {
            return Err(crate::error::OddSocketsError::InvalidConfiguration {
                message: "Heartbeat interval must be greater than zero".to_string(),
            });
        }

        Ok(())
    }
}

/// Builder for creating OddSocketsConfig instances.
#[derive(Debug, Clone)]
pub struct OddSocketsConfigBuilder {
    config: OddSocketsConfig,
}

impl OddSocketsConfigBuilder {
    /// Creates a new builder with the given API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            config: OddSocketsConfig::new(api_key),
        }
    }

    /// Sets the manager URL.
    pub fn manager_url(mut self, url: impl Into<String>) -> Self {
        self.config.manager_url = url.into();
        self
    }

    /// Sets the user ID.
    pub fn user_id(mut self, user_id: impl Into<String>) -> Self {
        self.config.user_id = Some(user_id.into());
        self
    }

    /// Sets whether to auto-connect.
    pub fn auto_connect(mut self, auto_connect: bool) -> Self {
        self.config.auto_connect = auto_connect;
        self
    }

    /// Sets the number of reconnection attempts.
    pub fn reconnect_attempts(mut self, attempts: u32) -> Self {
        self.config.reconnect_attempts = attempts;
        self
    }

    /// Sets the heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Sets the timeout duration.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    /// Configures for development environment.
    pub fn development(mut self) -> Self {
        self.config.manager_url = "http://localhost:3001".to_string();
        self.config.timeout = Duration::from_secs(30);
        self.config.heartbeat_interval = Duration::from_secs(10);
        self
    }

    /// Configures for production environment.
    pub fn production(mut self) -> Self {
        self.config.manager_url = constants::DEFAULT_MANAGER_URL.to_string();
        self.config.timeout = Duration::from_secs(10);
        self.config.heartbeat_interval = Duration::from_secs(30);
        self
    }

    /// Configures for high-performance scenarios.
    pub fn high_performance(mut self) -> Self {
        self.config.heartbeat_interval = Duration::from_secs(60);
        self.config.timeout = Duration::from_secs(5);
        self.config.reconnect_attempts = 3;
        self
    }

    /// Builds the configuration.
    pub fn build(self) -> Result<OddSocketsConfig, crate::error::OddSocketsError> {
        self.config.validate()?;
        Ok(self.config)
    }
}

/// Represents a message in the OddSockets system.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// Unique message identifier
    pub id: String,
    /// Channel the message was sent to
    pub channel: String,
    /// Message data (can be any JSON value)
    pub data: serde_json::Value,
    /// When the message was created
    pub timestamp: DateTime<Utc>,
    /// Optional user ID of the sender
    pub user_id: Option<String>,
    /// Optional metadata
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

impl Message {
    /// Creates a new message with generated ID.
    pub fn new(
        channel: impl Into<String>,
        data: serde_json::Value,
        user_id: Option<String>,
        metadata: Option<HashMap<String, serde_json::Value>>,
    ) -> Self {
        Self {
            id: generate_message_id(),
            channel: channel.into(),
            data,
            timestamp: Utc::now(),
            user_id,
            metadata,
        }
    }

    /// Gets a metadata value by key.
    pub fn get_metadata<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.metadata
            .as_ref()?
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Checks if this message has metadata.
    pub fn has_metadata(&self) -> bool {
        self.metadata.as_ref().map_or(false, |m| !m.is_empty())
    }

    /// Checks if this message has data.
    pub fn has_data(&self) -> bool {
        !self.data.is_null()
    }
}

/// Represents presence information for a channel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PresenceInfo {
    /// Channel name
    pub channel: String,
    /// List of user IDs currently present
    pub users: Vec<String>,
    /// Total count of users present
    pub count: usize,
    /// When this presence info was created
    pub timestamp: DateTime<Utc>,
}

impl PresenceInfo {
    /// Creates new presence information.
    pub fn new(channel: impl Into<String>, users: Vec<String>) -> Self {
        let count = users.len();
        Self {
            channel: channel.into(),
            users,
            count,
            timestamp: Utc::now(),
        }
    }

    /// Checks if a specific user is present.
    pub fn is_user_present(&self, user_id: &str) -> bool {
        self.users.contains(&user_id.to_string())
    }

    /// Checks if the channel is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0 || self.users.is_empty()
    }

    /// Gets the presence ratio compared to a maximum capacity.
    pub fn presence_ratio(&self, max_capacity: usize) -> f64 {
        if max_capacity == 0 {
            0.0
        } else {
            self.count as f64 / max_capacity as f64
        }
    }
}

/// Represents the result of a publish operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishResult {
    /// Unique identifier for the published message
    pub message_id: String,
    /// When the message was published
    pub timestamp: DateTime<Utc>,
    /// The channel the message was published to
    pub channel: String,
    /// Whether the publish was successful
    pub success: bool,
}

impl PublishResult {
    /// Creates a new successful publish result.
    pub fn success(message_id: impl Into<String>, channel: impl Into<String>) -> Self {
        Self {
            message_id: message_id.into(),
            timestamp: Utc::now(),
            channel: channel.into(),
            success: true,
        }
    }

    /// Creates a new failed publish result.
    pub fn failure(message_id: impl Into<String>, channel: impl Into<String>) -> Self {
        Self {
            message_id: message_id.into(),
            timestamp: Utc::now(),
            channel: channel.into(),
            success: false,
        }
    }

    /// Returns true if the publish was successful.
    pub fn is_successful(&self) -> bool {
        self.success
    }

    /// Returns true if the publish failed.
    pub fn is_failed(&self) -> bool {
        !self.success
    }
}

/// Represents a message for bulk publishing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BulkMessage {
    /// The channel to publish to
    pub channel: String,
    /// The message data
    pub message: serde_json::Value,
    /// Optional publish options
    pub options: Option<PublishOptions>,
}

impl BulkMessage {
    /// Creates a new bulk message.
    pub fn new(
        channel: impl Into<String>,
        message: serde_json::Value,
        options: Option<PublishOptions>,
    ) -> Self {
        Self {
            channel: channel.into(),
            message,
            options,
        }
    }
}

/// Represents the result of a bulk publish operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BulkResult {
    /// Whether this individual result was successful
    pub success: bool,
    /// The publish result if successful
    pub result: Option<PublishResult>,
    /// Error message if failed
    pub error: Option<String>,
}

impl BulkResult {
    /// Creates a successful bulk result.
    pub fn success(result: PublishResult) -> Self {
        Self {
            success: true,
            result: Some(result),
            error: None,
        }
    }

    /// Creates a failed bulk result.
    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            success: false,
            result: None,
            error: Some(error.into()),
        }
    }

    /// Returns true if this result was successful.
    pub fn is_successful(&self) -> bool {
        self.success
    }

    /// Returns true if this result failed.
    pub fn is_failed(&self) -> bool {
        !self.success
    }

    /// Gets the error message with a default fallback.
    pub fn error_message(&self, default: &str) -> &str {
        self.error.as_deref().unwrap_or(default)
    }
}

/// Options for channel subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeOptions {
    /// Whether to enable presence tracking
    pub enable_presence: bool,
    /// Whether to retain message history
    pub retain_history: bool,
    /// Optional filter expression for messages
    pub filter_expression: Option<String>,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            enable_presence: false,
            retain_history: false,
            filter_expression: None,
        }
    }
}

impl SubscribeOptions {
    /// Creates options with presence enabled.
    pub fn with_presence() -> Self {
        Self {
            enable_presence: true,
            ..Default::default()
        }
    }

    /// Creates options with history enabled.
    pub fn with_history() -> Self {
        Self {
            retain_history: true,
            ..Default::default()
        }
    }

    /// Creates options with both presence and history enabled.
    pub fn with_presence_and_history() -> Self {
        Self {
            enable_presence: true,
            retain_history: true,
            ..Default::default()
        }
    }

    /// Creates options optimized for chat channels.
    pub fn chat_channel() -> Self {
        Self::with_presence_and_history()
    }

    /// Creates options optimized for notification channels.
    pub fn notification_channel() -> Self {
        Self::default()
    }

    /// Creates options optimized for data channels.
    pub fn data_channel() -> Self {
        Self::with_history()
    }
}

/// Options for message publishing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishOptions {
    /// Time-to-live in seconds
    pub ttl: Option<u64>,
    /// Optional metadata to attach to the message
    pub metadata: Option<HashMap<String, serde_json::Value>>,
    /// Whether to store this message in history
    pub store_in_history: bool,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            ttl: None,
            metadata: None,
            store_in_history: false,
        }
    }
}

impl PublishOptions {
    /// Creates options with history storage enabled.
    pub fn with_history() -> Self {
        Self {
            store_in_history: true,
            ..Default::default()
        }
    }

    /// Creates options with a specific TTL.
    pub fn with_ttl(seconds: u64) -> Self {
        Self {
            ttl: Some(seconds),
            ..Default::default()
        }
    }

    /// Creates options optimized for chat messages.
    pub fn chat_message() -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), serde_json::Value::String("chat".to_string()));
        
        Self {
            store_in_history: true,
            metadata: Some(metadata),
            ..Default::default()
        }
    }

    /// Creates options optimized for system messages.
    pub fn system_message() -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), serde_json::Value::String("system".to_string()));
        metadata.insert("priority".to_string(), serde_json::Value::String("high".to_string()));
        
        Self {
            store_in_history: true,
            metadata: Some(metadata),
            ..Default::default()
        }
    }
}

/// Options for retrieving message history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryOptions {
    /// Maximum number of messages to retrieve
    pub limit: Option<usize>,
    /// Start time for history retrieval
    pub start: Option<DateTime<Utc>>,
    /// End time for history retrieval
    pub end: Option<DateTime<Utc>>,
    /// Whether to return messages in reverse chronological order
    pub reverse: bool,
}

impl Default for HistoryOptions {
    fn default() -> Self {
        Self {
            limit: None,
            start: None,
            end: None,
            reverse: false,
        }
    }
}

impl HistoryOptions {
    /// Creates options with a specific limit.
    pub fn limit(count: usize) -> Self {
        Self {
            limit: Some(count),
            ..Default::default()
        }
    }

    /// Creates options for recent messages.
    pub fn recent(count: usize) -> Self {
        Self {
            limit: Some(count),
            reverse: true,
            ..Default::default()
        }
    }

    /// Creates options for the last hour.
    pub fn last_hour(count: Option<usize>) -> Self {
        Self {
            limit: count.or(Some(100)),
            start: Some(Utc::now() - chrono::Duration::hours(1)),
            reverse: true,
            ..Default::default()
        }
    }

    /// Creates options for the last day.
    pub fn last_day(count: Option<usize>) -> Self {
        Self {
            limit: count.or(Some(1000)),
            start: Some(Utc::now() - chrono::Duration::days(1)),
            reverse: true,
            ..Default::default()
        }
    }
}

/// Common message types for structured messaging.
pub mod message_types {
    use super::*;

    /// Creates a chat message structure.
    pub fn chat_message(
        text: impl Into<String>,
        username: impl Into<String>,
        message_type: Option<&str>,
    ) -> serde_json::Value {
        serde_json::json!({
            "text": text.into(),
            "username": username.into(),
            "messageType": message_type.unwrap_or("chat"),
            "timestamp": Utc::now().to_rfc3339(),
        })
    }

    /// Creates a notification message structure.
    pub fn notification_message(
        title: impl Into<String>,
        body: impl Into<String>,
        category: Option<&str>,
        priority: Option<&str>,
        data: Option<serde_json::Value>,
    ) -> serde_json::Value {
        let mut msg = serde_json::json!({
            "title": title.into(),
            "body": body.into(),
            "category": category.unwrap_or("general"),
            "priority": priority.unwrap_or("normal"),
            "timestamp": Utc::now().to_rfc3339(),
        });

        if let Some(data) = data {
            msg["data"] = data;
        }

        msg
    }

    /// Creates a system message structure.
    pub fn system_message(
        event: impl Into<String>,
        description: impl Into<String>,
        metadata: Option<serde_json::Value>,
    ) -> serde_json::Value {
        let mut msg = serde_json::json!({
            "event": event.into(),
            "description": description.into(),
            "timestamp": Utc::now().to_rfc3339(),
        });

        if let Some(metadata) = metadata {
            msg["metadata"] = metadata;
        }

        msg
    }

    /// Creates a data event message structure.
    pub fn data_event(
        event_type: impl Into<String>,
        payload: serde_json::Value,
        source: Option<&str>,
    ) -> serde_json::Value {
        let mut msg = serde_json::json!({
            "eventType": event_type.into(),
            "payload": payload,
            "timestamp": Utc::now().to_rfc3339(),
        });

        if let Some(source) = source {
            msg["source"] = serde_json::Value::String(source.to_string());
        }

        msg
    }
}

/// Utility functions for creating common data structures.
pub mod utils {
    use super::*;

    /// Generates a unique message ID.
    pub fn generate_message_id() -> String {
        format!("msg_{}", Uuid::new_v4().simple())
    }

    /// Generates a unique user ID.
    pub fn generate_user_id() -> String {
        format!("user_{}", Uuid::new_v4().simple())
    }

    /// Creates a bulk message.
    pub fn bulk_message(
        channel: impl Into<String>,
        message: serde_json::Value,
        options: Option<PublishOptions>,
    ) -> BulkMessage {
        BulkMessage::new(channel, message, options)
    }

    /// Creates multiple bulk messages for the same channel.
    pub fn bulk_messages(
        channel: impl Into<String>,
        messages: Vec<serde_json::Value>,
        options: Option<PublishOptions>,
    ) -> Vec<BulkMessage> {
        let channel = channel.into();
        messages
            .into_iter()
            .map(|message| BulkMessage::new(channel.clone(), message, options.clone()))
            .collect()
    }
}

// Re-export the generate_message_id function at the module level for convenience
pub use utils::generate_message_id;

/// Error codes used throughout the SDK.
pub mod error_codes {
    pub const INVALID_API_KEY: &str = "INVALID_API_KEY";
    pub const CONNECTION_FAILED: &str = "CONNECTION_FAILED";
    pub const AUTHENTICATION_FAILED: &str = "AUTHENTICATION_FAILED";
    pub const CHANNEL_ACCESS_DENIED: &str = "CHANNEL_ACCESS_DENIED";
    pub const MESSAGE_DELIVERY_FAILED: &str = "MESSAGE_DELIVERY_FAILED";
    pub const INVALID_CONFIGURATION: &str = "INVALID_CONFIGURATION";
    pub const WORKER_ASSIGNMENT_FAILED: &str = "WORKER_ASSIGNMENT_FAILED";
    pub const MAX_RECONNECT_ATTEMPTS_REACHED: &str = "MAX_RECONNECT_ATTEMPTS_REACHED";
    pub const OPERATION_TIMEOUT: &str = "OPERATION_TIMEOUT";
    pub const INVALID_CHANNEL_NAME: &str = "INVALID_CHANNEL_NAME";
    pub const WEBSOCKET_ERROR: &str = "WEBSOCKET_ERROR";
}
