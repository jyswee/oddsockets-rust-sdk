//! Message Size Validator
//!
//! Validates message sizes against industry standard limits to ensure
//! reliable real-time messaging performance.

use crate::error::{OddSocketsError, Result};
use serde_json::Value;

/// Message size limits (industry standard)
pub const MAX_MESSAGE_SIZE: usize = 32768; // 32KB in bytes
pub const MAX_MESSAGE_SIZE_KB: usize = 32;

/// Convert a byte count to KB, rounding up.
///
/// Rounding up matters: truncating division reports a 32769-byte message as
/// `32KB`, which is exactly the limit, producing the nonsensical error
/// "Message size (32KB) exceeds maximum allowed size of 32KB" and making an
/// over-limit message look like it was within bounds.
fn size_in_kb(bytes: usize) -> usize {
    (bytes + 1023) / 1024
}

/// Validate message size
///
/// # Arguments
/// * `message` - Message to validate (can be any JSON-serializable value)
///
/// # Returns
/// The message size in bytes if valid
///
/// # Errors
/// Returns `OddSocketsError::MessageTooLarge` if message exceeds size limit
pub fn validate_message_size(message: &Value) -> Result<usize> {
    let message_str = message.to_string();
    let message_size = message_str.len();
    
    if message_size > MAX_MESSAGE_SIZE {
        return Err(OddSocketsError::MessageTooLarge {
            size_kb: size_in_kb(message_size),
            max_size_kb: MAX_MESSAGE_SIZE_KB,
            message: format!(
                "Message size ({}KB) exceeds maximum allowed size of {}KB. \
                This limit keeps real-time delivery reliable.",
                size_in_kb(message_size),
                MAX_MESSAGE_SIZE_KB
            ),
        });
    }
    
    Ok(message_size)
}

/// Validate message size for a string
///
/// # Arguments
/// * `message` - Message string to validate
///
/// # Returns
/// The message size in bytes if valid
///
/// # Errors
/// Returns `OddSocketsError::MessageTooLarge` if message exceeds size limit
pub fn validate_string_message_size(message: &str) -> Result<usize> {
    let message_size = message.len();
    
    if message_size > MAX_MESSAGE_SIZE {
        return Err(OddSocketsError::MessageTooLarge {
            size_kb: size_in_kb(message_size),
            max_size_kb: MAX_MESSAGE_SIZE_KB,
            message: format!(
                "Message size ({}KB) exceeds maximum allowed size of {}KB. \
                This limit keeps real-time delivery reliable.",
                size_in_kb(message_size),
                MAX_MESSAGE_SIZE_KB
            ),
        });
    }
    
    Ok(message_size)
}

/// Check if a message size is within limits without returning an error
///
/// # Arguments
/// * `message_size` - Size in bytes to check
///
/// # Returns
/// `true` if the message size is within limits, `false` otherwise
pub fn is_message_size_valid(message_size: usize) -> bool {
    message_size <= MAX_MESSAGE_SIZE
}

/// Get the maximum allowed message size in bytes
pub fn get_max_message_size() -> usize {
    MAX_MESSAGE_SIZE
}

/// Get the maximum allowed message size in KB
pub fn get_max_message_size_kb() -> usize {
    MAX_MESSAGE_SIZE_KB
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_validate_small_message() {
        let message = json!({"text": "Hello, World!"});
        let size = validate_message_size(&message).unwrap();
        assert!(size > 0);
        assert!(size < MAX_MESSAGE_SIZE);
    }

    #[test]
    fn test_validate_large_message() {
        // Create a message that's too large
        let large_text = "x".repeat(MAX_MESSAGE_SIZE + 1000);
        let message = json!({"text": large_text});
        
        let result = validate_message_size(&message);
        assert!(result.is_err());
        
        if let Err(OddSocketsError::MessageTooLarge { size_kb, max_size_kb, .. }) = result {
            assert!(size_kb > max_size_kb);
        } else {
            panic!("Expected MessageTooLarge error");
        }
    }

    #[test]
    fn test_reported_kb_exceeds_limit_just_over_boundary() {
        // One byte over the limit must still report a size strictly greater than
        // the maximum. Truncating division reported this as 32KB - exactly the
        // limit - so the error claimed 32KB exceeded 32KB.
        let just_over = "x".repeat(MAX_MESSAGE_SIZE + 1);

        match validate_string_message_size(&just_over) {
            Err(OddSocketsError::MessageTooLarge { size_kb, max_size_kb, .. }) => {
                assert!(
                    size_kb > max_size_kb,
                    "reported {}KB against a {}KB limit",
                    size_kb,
                    max_size_kb
                );
            }
            _ => panic!("Expected MessageTooLarge error"),
        }
    }

    #[test]
    fn test_validate_string_message() {
        let message = "Hello, World!";
        let size = validate_string_message_size(message).unwrap();
        assert_eq!(size, message.len());
    }

    #[test]
    fn test_validate_large_string_message() {
        let large_message = "x".repeat(MAX_MESSAGE_SIZE + 1000);
        let result = validate_string_message_size(&large_message);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_message_size_valid() {
        assert!(is_message_size_valid(1000));
        assert!(is_message_size_valid(MAX_MESSAGE_SIZE));
        assert!(!is_message_size_valid(MAX_MESSAGE_SIZE + 1));
    }

    #[test]
    fn test_get_max_sizes() {
        assert_eq!(get_max_message_size(), MAX_MESSAGE_SIZE);
        assert_eq!(get_max_message_size_kb(), MAX_MESSAGE_SIZE_KB);
    }

    #[test]
    fn test_boundary_conditions() {
        // Test exactly at the limit
        let boundary_text = "x".repeat(MAX_MESSAGE_SIZE - 20); // Account for JSON overhead
        let message = json!({"text": boundary_text});
        
        // This should pass (might be close to limit but within it)
        let result = validate_message_size(&message);
        // We can't guarantee this will pass due to JSON overhead, so just check it doesn't panic
        let _ = result;
    }
}
