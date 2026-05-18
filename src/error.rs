//! Error types and handling for the OddSockets Rust SDK.
//!
//! This module provides comprehensive error handling with structured error types,
//! recovery suggestions, and integration with the standard Rust error ecosystem.

use std::fmt;
use thiserror::Error;

/// Main error type for the OddSockets SDK.
///
/// This enum covers all possible errors that can occur during SDK operations,
/// with specific variants for different error conditions and recovery strategies.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum OddSocketsError {
    /// Invalid or missing API key
    #[error("Invalid API key: {message}")]
    InvalidApiKey { message: String },

    /// Connection to OddSockets failed
    #[error("Connection failed: {message}")]
    ConnectionFailed { message: String },

    /// Authentication failed
    #[error("Authentication failed: {message}")]
    AuthenticationFailed { message: String },

    /// Access to a channel was denied
    #[error("Channel access denied for '{channel}': {message}")]
    ChannelAccessDenied { channel: String, message: String },

    /// Message delivery failed
    #[error("Message delivery failed: {message}")]
    MessageDeliveryFailed {
        message: String,
        message_id: Option<String>,
        channel: Option<String>,
    },

    /// Invalid configuration
    #[error("Invalid configuration: {message}")]
    InvalidConfiguration { message: String },

    /// Worker assignment failed
    #[error("Worker assignment failed: {message}")]
    WorkerAssignmentFailed { message: String },

    /// Maximum reconnection attempts reached
    #[error("Maximum reconnection attempts ({attempts}) reached")]
    MaxReconnectAttemptsReached { attempts: u32 },

    /// Operation timed out
    #[error("Operation '{operation}' timed out after {timeout_secs} seconds")]
    OperationTimeout {
        operation: String,
        timeout_secs: u64,
    },

    /// Invalid channel name
    #[error("Invalid channel name '{channel_name}': {message}")]
    InvalidChannelName {
        channel_name: String,
        message: String,
    },

    /// WebSocket error
    #[error("WebSocket error: {message}")]
    WebSocketError {
        message: String,
        close_code: Option<u16>,
        close_reason: Option<String>,
    },

    /// HTTP request error
    #[error("HTTP request failed: {message}")]
    HttpError { message: String, status: Option<u16> },

    /// JSON serialization/deserialization error
    #[error("JSON error: {message}")]
    JsonError { message: String },

    /// IO error
    #[error("IO error: {message}")]
    IoError { message: String },

    /// Message exceeds size limit
    #[error("Message too large: {message}")]
    MessageTooLarge {
        size_kb: usize,
        max_size_kb: usize,
        message: String,
    },

    /// Generic error for unknown conditions
    #[error("Unknown error: {message}")]
    Unknown { message: String },
}

impl OddSocketsError {
    /// Returns the error code for this error.
    pub fn error_code(&self) -> &'static str {
        match self {
            OddSocketsError::InvalidApiKey { .. } => crate::types::error_codes::INVALID_API_KEY,
            OddSocketsError::ConnectionFailed { .. } => crate::types::error_codes::CONNECTION_FAILED,
            OddSocketsError::AuthenticationFailed { .. } => {
                crate::types::error_codes::AUTHENTICATION_FAILED
            }
            OddSocketsError::ChannelAccessDenied { .. } => {
                crate::types::error_codes::CHANNEL_ACCESS_DENIED
            }
            OddSocketsError::MessageDeliveryFailed { .. } => {
                crate::types::error_codes::MESSAGE_DELIVERY_FAILED
            }
            OddSocketsError::InvalidConfiguration { .. } => {
                crate::types::error_codes::INVALID_CONFIGURATION
            }
            OddSocketsError::WorkerAssignmentFailed { .. } => {
                crate::types::error_codes::WORKER_ASSIGNMENT_FAILED
            }
            OddSocketsError::MaxReconnectAttemptsReached { .. } => {
                crate::types::error_codes::MAX_RECONNECT_ATTEMPTS_REACHED
            }
            OddSocketsError::OperationTimeout { .. } => {
                crate::types::error_codes::OPERATION_TIMEOUT
            }
            OddSocketsError::InvalidChannelName { .. } => {
                crate::types::error_codes::INVALID_CHANNEL_NAME
            }
            OddSocketsError::WebSocketError { .. } => crate::types::error_codes::WEBSOCKET_ERROR,
            OddSocketsError::HttpError { .. } => "HTTP_ERROR",
            OddSocketsError::JsonError { .. } => "JSON_ERROR",
            OddSocketsError::IoError { .. } => "IO_ERROR",
            OddSocketsError::Unknown { .. } => "UNKNOWN_ERROR",
        }
    }

    /// Returns whether this error is recoverable.
    pub fn is_recoverable(&self) -> bool {
        match self {
            OddSocketsError::InvalidApiKey { .. } => false,
            OddSocketsError::ConnectionFailed { .. } => true,
            OddSocketsError::AuthenticationFailed { .. } => false,
            OddSocketsError::ChannelAccessDenied { .. } => false,
            OddSocketsError::MessageDeliveryFailed { .. } => true,
            OddSocketsError::InvalidConfiguration { .. } => false,
            OddSocketsError::WorkerAssignmentFailed { .. } => true,
            OddSocketsError::MaxReconnectAttemptsReached { .. } => false,
            OddSocketsError::OperationTimeout { .. } => true,
            OddSocketsError::InvalidChannelName { .. } => false,
            OddSocketsError::WebSocketError { close_code, .. } => {
                // Some WebSocket close codes indicate recoverable errors
                match close_code {
                    Some(1002) | Some(1003) => false, // Protocol or data errors
                    _ => true,
                }
            }
            OddSocketsError::HttpError { status, .. } => {
                // Some HTTP errors are recoverable
                match status {
                    Some(401) | Some(403) | Some(404) => false, // Auth/not found errors
                    Some(500..=599) => true,                     // Server errors
                    _ => true,
                }
            }
            OddSocketsError::JsonError { .. } => false,
            OddSocketsError::IoError { .. } => true,
            OddSocketsError::MessageTooLarge { .. } => false,
            OddSocketsError::Unknown { .. } => false,
        }
    }

    /// Returns whether this error should trigger a reconnection attempt.
    pub fn should_reconnect(&self) -> bool {
        match self {
            OddSocketsError::ConnectionFailed { .. } => true,
            OddSocketsError::WorkerAssignmentFailed { .. } => true,
            OddSocketsError::WebSocketError { .. } => self.is_recoverable(),
            OddSocketsError::HttpError { .. } => self.is_recoverable(),
            OddSocketsError::IoError { .. } => true,
            _ => false,
        }
    }

    /// Returns recovery suggestions for this error.
    pub fn recovery_suggestions(&self) -> Vec<&'static str> {
        match self {
            OddSocketsError::InvalidApiKey { .. } => vec![
                "Verify your API key is correct",
                "Check that your API key starts with 'ak_'",
                "Ensure your API key has not expired",
                "Contact support if the issue persists",
            ],
            OddSocketsError::ConnectionFailed { .. } => vec![
                "Check your internet connection",
                "Verify the manager URL is correct",
                "Try again in a few moments",
                "Check if OddSockets service is operational",
            ],
            OddSocketsError::AuthenticationFailed { .. } => vec![
                "Verify your API key is valid",
                "Check your account status",
                "Ensure your subscription is active",
                "Contact support for assistance",
            ],
            OddSocketsError::ChannelAccessDenied { .. } => vec![
                "Check channel permissions in your dashboard",
                "Verify the channel name is correct",
                "Ensure your API key has access to this channel",
                "Contact your administrator for access",
            ],
            OddSocketsError::MessageDeliveryFailed { .. } => vec![
                "Check your connection status",
                "Verify the channel exists and is accessible",
                "Try resending the message",
                "Check message size limits",
            ],
            OddSocketsError::InvalidConfiguration { .. } => vec![
                "Review your configuration parameters",
                "Check the documentation for valid values",
                "Ensure all required fields are provided",
                "Validate URL formats and timeouts",
            ],
            OddSocketsError::WorkerAssignmentFailed { .. } => vec![
                "Wait a moment and try reconnecting",
                "Check OddSockets service status",
                "Verify your subscription limits",
                "Contact support if the issue persists",
            ],
            OddSocketsError::MaxReconnectAttemptsReached { .. } => vec![
                "Check your internet connection",
                "Verify OddSockets service is operational",
                "Try connecting again later",
                "Consider increasing reconnection attempts in configuration",
            ],
            OddSocketsError::OperationTimeout { .. } => vec![
                "Check your internet connection speed",
                "Try increasing the timeout in configuration",
                "Retry the operation",
                "Check if the service is experiencing high load",
            ],
            OddSocketsError::InvalidChannelName { .. } => vec![
                "Channel names must be non-empty strings",
                "Avoid special characters in channel names",
                "Use alphanumeric characters, hyphens, and underscores",
                "Check the documentation for naming conventions",
            ],
            OddSocketsError::WebSocketError { close_code, .. } => {
                let mut suggestions = vec![
                    "Check your internet connection",
                    "Verify the WebSocket endpoint is accessible",
                    "Try reconnecting in a few moments",
                ];

                match close_code {
                    Some(1006) => suggestions.insert(0, "Connection was closed abnormally - check network"),
                    Some(1011) => suggestions.insert(0, "Server error - try again later"),
                    Some(1012) => suggestions.insert(0, "Service is restarting - wait and reconnect"),
                    _ => {}
                }

                suggestions
            }
            OddSocketsError::HttpError { .. } => vec![
                "Check your internet connection",
                "Verify the server endpoint is accessible",
                "Try the request again",
                "Check if the service is experiencing issues",
            ],
            OddSocketsError::JsonError { .. } => vec![
                "Check the data format being sent",
                "Ensure JSON is properly formatted",
                "Verify data types match expected schema",
                "Contact support if the issue persists",
            ],
            OddSocketsError::IoError { .. } => vec![
                "Check your internet connection",
                "Verify file permissions if applicable",
                "Try the operation again",
                "Check available disk space",
            ],
            OddSocketsError::MessageTooLarge { .. } => vec![
                "Reduce the message size to under 32KB",
                "Split large messages into smaller chunks",
                "Use message compression if applicable",
                "Consider using file uploads for large content",
            ],
            OddSocketsError::Unknown { .. } => vec![
                "Check the error message for specific details",
                "Verify your configuration and API key",
                "Try the operation again",
                "Contact support if the issue persists",
            ],
        }
    }

    /// Creates an error from an error code and message.
    pub fn from_error_code(
        code: &str,
        message: String,
        details: Option<std::collections::HashMap<String, serde_json::Value>>,
    ) -> Self {
        match code {
            crate::types::error_codes::INVALID_API_KEY => {
                OddSocketsError::InvalidApiKey { message }
            }
            crate::types::error_codes::CONNECTION_FAILED => {
                OddSocketsError::ConnectionFailed { message }
            }
            crate::types::error_codes::AUTHENTICATION_FAILED => {
                OddSocketsError::AuthenticationFailed { message }
            }
            crate::types::error_codes::CHANNEL_ACCESS_DENIED => {
                let channel = details
                    .as_ref()
                    .and_then(|d| d.get("channel"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                OddSocketsError::ChannelAccessDenied { channel, message }
            }
            crate::types::error_codes::MESSAGE_DELIVERY_FAILED => {
                let message_id = details
                    .as_ref()
                    .and_then(|d| d.get("messageId"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let channel = details
                    .as_ref()
                    .and_then(|d| d.get("channel"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                OddSocketsError::MessageDeliveryFailed {
                    message,
                    message_id,
                    channel,
                }
            }
            crate::types::error_codes::INVALID_CONFIGURATION => {
                OddSocketsError::InvalidConfiguration { message }
            }
            crate::types::error_codes::WORKER_ASSIGNMENT_FAILED => {
                OddSocketsError::WorkerAssignmentFailed { message }
            }
            crate::types::error_codes::MAX_RECONNECT_ATTEMPTS_REACHED => {
                let attempts = details
                    .as_ref()
                    .and_then(|d| d.get("attempts"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u32;
                OddSocketsError::MaxReconnectAttemptsReached { attempts }
            }
            crate::types::error_codes::OPERATION_TIMEOUT => {
                let operation = details
                    .as_ref()
                    .and_then(|d| d.get("operation"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let timeout_secs = details
                    .as_ref()
                    .and_then(|d| d.get("timeout"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                OddSocketsError::OperationTimeout {
                    operation,
                    timeout_secs,
                }
            }
            crate::types::error_codes::INVALID_CHANNEL_NAME => {
                let channel_name = details
                    .as_ref()
                    .and_then(|d| d.get("channelName"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                OddSocketsError::InvalidChannelName {
                    channel_name,
                    message,
                }
            }
            crate::types::error_codes::WEBSOCKET_ERROR => {
                let close_code = details
                    .as_ref()
                    .and_then(|d| d.get("closeCode"))
                    .and_then(|v| v.as_u64())
                    .map(|c| c as u16);
                let close_reason = details
                    .as_ref()
                    .and_then(|d| d.get("closeReason"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                OddSocketsError::WebSocketError {
                    message,
                    close_code,
                    close_reason,
                }
            }
            _ => OddSocketsError::Unknown { message },
        }
    }
}

// Implement conversions from common error types
impl From<reqwest::Error> for OddSocketsError {
    fn from(err: reqwest::Error) -> Self {
        let status = err.status().map(|s| s.as_u16());
        OddSocketsError::HttpError {
            message: err.to_string(),
            status,
        }
    }
}

impl From<serde_json::Error> for OddSocketsError {
    fn from(err: serde_json::Error) -> Self {
        OddSocketsError::JsonError {
            message: err.to_string(),
        }
    }
}

impl From<std::io::Error> for OddSocketsError {
    fn from(err: std::io::Error) -> Self {
        OddSocketsError::IoError {
            message: err.to_string(),
        }
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for OddSocketsError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        use tokio_tungstenite::tungstenite::Error as WsError;

        let (close_code, close_reason) = match &err {
            WsError::ConnectionClosed => (Some(1000), Some("Connection closed".to_string())),
            WsError::AlreadyClosed => (Some(1001), Some("Already closed".to_string())),
            WsError::Protocol(msg) => (Some(1002), Some(msg.clone())),
            WsError::Utf8 => (Some(1003), Some("UTF-8 error".to_string())),
            _ => (None, None),
        };

        OddSocketsError::WebSocketError {
            message: err.to_string(),
            close_code,
            close_reason,
        }
    }
}

impl From<url::ParseError> for OddSocketsError {
    fn from(err: url::ParseError) -> Self {
        OddSocketsError::InvalidConfiguration {
            message: format!("Invalid URL: {}", err),
        }
    }
}

/// Result type alias for OddSockets operations.
pub type Result<T> = std::result::Result<T, OddSocketsError>;

/// Extension trait for Result to add OddSockets-specific functionality.
pub trait OddSocketsResultExt<T> {
    /// Maps an error to a more specific OddSockets error with context.
    fn with_context(self, context: &str) -> Result<T>;

    /// Maps an error to a connection failure.
    fn as_connection_error(self) -> Result<T>;

    /// Maps an error to a message delivery failure.
    fn as_delivery_error(self, channel: Option<String>, message_id: Option<String>) -> Result<T>;
}

impl<T, E> OddSocketsResultExt<T> for std::result::Result<T, E>
where
    E: fmt::Display,
{
    fn with_context(self, context: &str) -> Result<T> {
        self.map_err(|e| OddSocketsError::Unknown {
            message: format!("{}: {}", context, e),
        })
    }

    fn as_connection_error(self) -> Result<T> {
        self.map_err(|e| OddSocketsError::ConnectionFailed {
            message: e.to_string(),
        })
    }

    fn as_delivery_error(self, channel: Option<String>, message_id: Option<String>) -> Result<T> {
        self.map_err(|e| OddSocketsError::MessageDeliveryFailed {
            message: e.to_string(),
            channel,
            message_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        let error = OddSocketsError::InvalidApiKey {
            message: "Test".to_string(),
        };
        assert_eq!(error.error_code(), "INVALID_API_KEY");
    }

    #[test]
    fn test_recoverable_errors() {
        let recoverable = OddSocketsError::ConnectionFailed {
            message: "Test".to_string(),
        };
        assert!(recoverable.is_recoverable());

        let non_recoverable = OddSocketsError::InvalidApiKey {
            message: "Test".to_string(),
        };
        assert!(!non_recoverable.is_recoverable());
    }

    #[test]
    fn test_should_reconnect() {
        let should_reconnect = OddSocketsError::ConnectionFailed {
            message: "Test".to_string(),
        };
        assert!(should_reconnect.should_reconnect());

        let should_not_reconnect = OddSocketsError::InvalidApiKey {
            message: "Test".to_string(),
        };
        assert!(!should_not_reconnect.should_reconnect());
    }

    #[test]
    fn test_recovery_suggestions() {
        let error = OddSocketsError::InvalidApiKey {
            message: "Test".to_string(),
        };
        let suggestions = error.recovery_suggestions();
        assert!(!suggestions.is_empty());
        assert!(suggestions.contains(&"Verify your API key is correct"));
    }

    #[test]
    fn test_from_error_code() {
        let error = OddSocketsError::from_error_code(
            "INVALID_API_KEY",
            "Test message".to_string(),
            None,
        );
        match error {
            OddSocketsError::InvalidApiKey { message } => {
                assert_eq!(message, "Test message");
            }
            _ => panic!("Expected InvalidApiKey error"),
        }
    }
}
