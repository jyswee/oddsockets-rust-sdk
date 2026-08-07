//! Manager Discovery Service
//!
//! Resolves the manager endpoint that the client asks for a worker assignment.
//!
//! The resolved URL is used verbatim. There is deliberately no fallback to the
//! public endpoint when a configured manager is unreachable: silently redirecting
//! a self-hosted or staging deployment at production would make a misconfigured
//! client look healthy and would invalidate any test aimed at a non-production
//! manager.

use crate::error::{OddSocketsError, Result};
use crate::types::constants;

/// Environment variable consulted when no manager URL was configured.
pub const MANAGER_URL_ENV_VAR: &str = "ODDSOCKETS_MANAGER_URL";

/// Resolves the manager URL to use.
///
/// Precedence is explicit configuration, then the `ODDSOCKETS_MANAGER_URL`
/// environment variable, then [`constants::DEFAULT_MANAGER_URL`]. The default
/// only applies when nothing at all was configured.
///
/// # Arguments
/// * `configured` - The manager URL supplied by the caller, if any. Empty and
///   whitespace-only values count as "not configured".
///
/// # Errors
/// Returns [`OddSocketsError::InvalidConfiguration`] if the resolved value is
/// not an absolute `http://` or `https://` URL.
pub fn resolve_manager_url(configured: Option<&str>) -> Result<String> {
    let candidate = configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            std::env::var(MANAGER_URL_ENV_VAR)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| constants::DEFAULT_MANAGER_URL.to_string());

    validate_manager_url(&candidate)
}

/// Validates a manager URL and returns it without trailing slashes.
///
/// # Errors
/// Returns [`OddSocketsError::InvalidConfiguration`] if `value` is not an
/// absolute `http://` or `https://` URL.
pub fn validate_manager_url(value: &str) -> Result<String> {
    let normalized = value.trim().trim_end_matches('/');

    let is_absolute_http = url::Url::parse(normalized)
        .map(|parsed| {
            matches!(parsed.scheme(), "http" | "https")
                && parsed.host_str().is_some_and(|host| !host.is_empty())
        })
        .unwrap_or(false);

    if !is_absolute_http {
        return Err(OddSocketsError::InvalidConfiguration {
            message: format!("Invalid managerUrl: {}", value),
        });
    }

    Ok(normalized.to_string())
}

/// Manager Discovery Service.
///
/// Holds the single manager endpoint this client talks to.
#[derive(Debug, Clone)]
pub struct ManagerDiscovery {
    manager_url: String,
}

impl ManagerDiscovery {
    /// Creates a new `ManagerDiscovery` for the given configured manager URL.
    ///
    /// # Arguments
    /// * `configured` - The manager URL from the client configuration, if any.
    ///
    /// # Errors
    /// Returns [`OddSocketsError::InvalidConfiguration`] if the resolved URL is
    /// not an absolute `http://` or `https://` URL.
    pub fn new(configured: Option<&str>) -> Result<Self> {
        Ok(Self {
            manager_url: resolve_manager_url(configured)?,
        })
    }

    /// Returns the manager URL this instance was built with.
    ///
    /// # Returns
    /// The manager URL, without trailing slashes.
    pub async fn discover_manager_url(&self) -> Result<String> {
        Ok(self.manager_url.clone())
    }

    /// Returns the manager URL this instance was built with.
    pub fn manager_url(&self) -> &str {
        &self.manager_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_configured_url_is_used_verbatim() {
        let discovery = ManagerDiscovery::new(Some("https://example.invalid/custom")).unwrap();
        let url = discovery.discover_manager_url().await.unwrap();
        assert_eq!(url, "https://example.invalid/custom");
        assert_ne!(url, constants::DEFAULT_MANAGER_URL);
    }

    #[tokio::test]
    async fn test_trailing_slashes_are_stripped() {
        let discovery = ManagerDiscovery::new(Some("https://example.invalid/custom//")).unwrap();
        assert_eq!(
            discovery.discover_manager_url().await.unwrap(),
            "https://example.invalid/custom"
        );
    }

    #[test]
    fn test_invalid_url_is_rejected() {
        let err = ManagerDiscovery::new(Some("not-a-url")).unwrap_err();
        match err {
            OddSocketsError::InvalidConfiguration { message } => {
                assert_eq!(message, "Invalid managerUrl: not-a-url");
            }
            other => panic!("Expected InvalidConfiguration, got {:?}", other),
        }
    }

    #[test]
    fn test_non_http_scheme_is_rejected() {
        assert!(ManagerDiscovery::new(Some("ftp://example.invalid")).is_err());
        assert!(ManagerDiscovery::new(Some("/api/cluster")).is_err());
    }

    #[test]
    fn test_default_applies_only_when_nothing_configured() {
        // The environment variable is process-global, so only assert the
        // built-in default when the ambient environment has not set it.
        if std::env::var(MANAGER_URL_ENV_VAR).is_err() {
            assert_eq!(
                resolve_manager_url(None).unwrap(),
                constants::DEFAULT_MANAGER_URL
            );
        }
    }
}
