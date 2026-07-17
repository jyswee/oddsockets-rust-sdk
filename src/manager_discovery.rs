//! Manager Discovery Service
//!
//! Simple manager discovery that always connects to the main manager endpoint
//! which handles all routing and load balancing transparently.

use crate::error::{OddSocketsError, Result};

/// Manager Discovery Service
///
/// Always connects to the main manager endpoint which handles
/// all routing and load balancing transparently.
pub struct ManagerDiscovery {
    manager_url: String,
}

impl ManagerDiscovery {
    /// Create a new ManagerDiscovery instance
    pub fn new() -> Self {
        Self {
            manager_url: "https://connect.oddsockets.tyga.network".to_string(),
        }
    }

    /// Get the manager URL (always returns the main endpoint)
    ///
    /// # Arguments
    /// * `api_key` - The OddSockets API key (not used, kept for compatibility)
    ///
    /// # Returns
    /// The manager URL
    pub async fn discover_manager_url(&self, _api_key: &str) -> Result<String> {
        Ok(self.manager_url.clone())
    }

    /// Clear cache (no-op, kept for compatibility)
    pub fn clear_cache(&self) {
        // No cache to clear in simplified version
    }
}

impl Default for ManagerDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

/// Global singleton instance
static MANAGER_DISCOVERY: std::sync::LazyLock<ManagerDiscovery> = 
    std::sync::LazyLock::new(ManagerDiscovery::new);

/// Get the global manager discovery instance
pub fn get_manager_discovery() -> &'static ManagerDiscovery {
    &MANAGER_DISCOVERY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_discover_manager_url() {
        let discovery = ManagerDiscovery::new();
        let url = discovery.discover_manager_url("ak_test").await.unwrap();
        assert_eq!(url, "https://connect.oddsockets.tyga.network");
    }

    #[test]
    fn test_clear_cache() {
        let discovery = ManagerDiscovery::new();
        discovery.clear_cache(); // Should not panic
    }

    #[tokio::test]
    async fn test_global_instance() {
        let discovery = get_manager_discovery();
        let url = discovery.discover_manager_url("ak_test").await.unwrap();
        assert_eq!(url, "https://connect.oddsockets.tyga.network");
    }
}
