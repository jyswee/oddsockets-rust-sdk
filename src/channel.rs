//! Channel handle: pub/sub operations over the real Socket.IO transport.

use crate::client::{adapt_message, OddSocketsClient};
use crate::error::Result;
use crate::message_size_validator::validate_message_size;
use crate::types::{HistoryOptions, Message, PresenceInfo, PublishOptions, PublishResult, SubscribeOptions};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

/// A handle to a single channel on the OddSockets platform.
///
/// Obtain one with [`OddSocketsClient::channel`]. Cloning shares subscription
/// state with the originating handle.
#[derive(Clone)]
pub struct OddSocketsChannel {
    name: String,
    client: OddSocketsClient,
    subscribed: Arc<AtomicBool>,
}

impl OddSocketsChannel {
    pub(crate) fn new(name: String, client: OddSocketsClient) -> Self {
        Self {
            name,
            client,
            subscribed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The channel name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether this channel is currently subscribed.
    pub fn is_subscribed(&self) -> bool {
        self.subscribed.load(Ordering::SeqCst)
    }

    /// Subscribes to the channel and returns a receiver for delivered messages.
    ///
    /// The receiver is a [`broadcast::Receiver`]; every call yields an
    /// independent receiver so multiple consumers can observe the same stream.
    pub async fn subscribe(
        &self,
        options: SubscribeOptions,
    ) -> Result<broadcast::Receiver<Message>> {
        // Register the fan-out receiver before the round-trip so no delivered
        // message can race ahead of the subscription acknowledgement.
        let receiver = self.client.channel_receiver(&self.name);

        let mut opts = json!({
            "enablePresence": options.enable_presence,
            "retainHistory": options.retain_history,
            "maxHistory": 100,
        });
        if let Some(filter) = options.filter_expression {
            opts["filterExpression"] = json!(filter);
        }

        let payload = json!({ "channel": self.name, "options": opts });
        self.client
            .request("subscribe", payload, "subscribed", &self.name, 10)
            .await?;

        self.subscribed.store(true, Ordering::SeqCst);
        Ok(receiver)
    }

    /// Unsubscribes from the channel.
    pub async fn unsubscribe(&self) -> Result<()> {
        if !self.is_subscribed() {
            return Ok(());
        }
        let payload = json!({ "channel": self.name });
        self.client
            .request("unsubscribe", payload, "unsubscribed", &self.name, 5)
            .await?;
        self.subscribed.store(false, Ordering::SeqCst);
        self.client.drop_channel(&self.name);
        Ok(())
    }

    /// Publishes a message to the channel.
    pub async fn publish(
        &self,
        message: Value,
        options: PublishOptions,
    ) -> Result<PublishResult> {
        validate_message_size(&message)?;

        let mut opts = json!({ "storeInHistory": options.store_in_history });
        if let Some(ttl) = options.ttl {
            opts["ttl"] = json!(ttl);
        }
        if let Some(metadata) = options.metadata {
            opts["metadata"] = json!(metadata);
        }

        let payload = json!({
            "channel": self.name,
            "message": message,
            "options": opts,
        });
        let response = self
            .client
            .request("publish", payload, "published", &self.name, 10)
            .await?;

        let message_id = response
            .get("messageId")
            .or_else(|| response.get("message_id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_default();

        Ok(PublishResult::success(message_id, &self.name))
    }

    /// Retrieves message history for the channel.
    pub async fn get_history(&self, options: Option<HistoryOptions>) -> Result<Vec<Message>> {
        let options = options.unwrap_or_default();
        let mut payload = json!({
            "channel": self.name,
            "count": options.limit.unwrap_or(50),
        });
        if let Some(start) = options.start {
            payload["start"] = json!(start.to_rfc3339());
        }
        if let Some(end) = options.end {
            payload["end"] = json!(end.to_rfc3339());
        }

        let response = self
            .client
            .request("get_history", payload, "history", &self.name, 10)
            .await?;

        let messages = response
            .get("messages")
            .and_then(Value::as_array)
            .map(|arr| arr.iter().map(adapt_message).collect())
            .unwrap_or_default();
        Ok(messages)
    }

    /// Retrieves current presence information for the channel.
    pub async fn get_presence(&self) -> Result<PresenceInfo> {
        let payload = json!({ "channel": self.name });
        let response = self
            .client
            .request("get_presence", payload, "presence", &self.name, 5)
            .await?;

        let users: Vec<String> = response
            .get("occupants")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|o| o.get("userId").and_then(Value::as_str))
                    .map(str::to_string)
                    .collect()
            })
            .or_else(|| {
                response.get("users").and_then(Value::as_array).map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
            })
            .unwrap_or_default();

        let count = response
            .get("occupancy")
            .or_else(|| response.get("count"))
            .and_then(Value::as_u64)
            .map(|c| c as usize)
            .unwrap_or(users.len());

        Ok(PresenceInfo {
            channel: self.name.clone(),
            users,
            count,
            timestamp: chrono::Utc::now(),
        })
    }
}

impl std::fmt::Debug for OddSocketsChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OddSocketsChannel")
            .field("name", &self.name)
            .field("subscribed", &self.is_subscribed())
            .finish()
    }
}
