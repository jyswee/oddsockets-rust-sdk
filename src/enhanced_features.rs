use crate::error::OddSocketsError;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};

/// Enhanced Features for OddSockets Rust SDK
/// Provides 67 new Slack-like events with Tokio async/await
pub struct EnhancedFeatures {
    client: Arc<RwLock<crate::OddSocketsClient>>,
    timeout_duration: Duration,
}

impl EnhancedFeatures {
    pub fn new(client: Arc<RwLock<crate::OddSocketsClient>>) -> Self {
        Self {
            client,
            timeout_duration: Duration::from_secs(10),
        }
    }

    // MARK: - Thread Events

    pub async fn thread_reply(
        &self,
        channel: &str,
        parent_message_id: &str,
        message: &str,
        user_id: &str,
        user_name: &str,
    ) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "channel": channel,
            "parentMessageId": parent_message_id,
            "message": message,
            "userId": user_id,
            "userName": user_name,
        });

        client.emit("thread_reply", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("thread_reply_success"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    pub async fn get_thread(&self, thread_id: &str) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "threadId": thread_id });
        client.emit("get_thread", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("thread_data"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    pub async fn subscribe_thread(&self, thread_id: &str, user_id: &str) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "threadId": thread_id, "userId": user_id });
        client.emit("subscribe_thread", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("thread_subscribed"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    pub async fn mark_thread_read(&self, thread_id: &str, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "threadId": thread_id, "userId": user_id });
        client.emit("mark_thread_read", params).await
    }

    pub async fn follow_thread(&self, thread_id: &str, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "threadId": thread_id, "userId": user_id });
        client.emit("follow_thread", params).await
    }

    pub async fn unfollow_thread(&self, thread_id: &str, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "threadId": thread_id, "userId": user_id });
        client.emit("unfollow_thread", params).await
    }

    // MARK: - Reaction Events

    pub async fn add_reaction(
        &self,
        message_id: &str,
        channel: &str,
        emoji: &str,
        user_id: &str,
        user_name: &str,
    ) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "messageId": message_id,
            "channel": channel,
            "emoji": emoji,
            "userId": user_id,
            "userName": user_name,
        });

        client.emit("add_reaction", params).await
    }

    pub async fn remove_reaction(
        &self,
        message_id: &str,
        channel: &str,
        emoji: &str,
        user_id: &str,
    ) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "messageId": message_id,
            "channel": channel,
            "emoji": emoji,
            "userId": user_id,
        });

        client.emit("remove_reaction", params).await
    }

    pub async fn get_reactions(&self, message_id: &str) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "messageId": message_id });
        client.emit("get_reactions", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("message_reactions"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    // MARK: - Read Receipt Events

    pub async fn mark_read(
        &self,
        message_id: &str,
        channel: &str,
        user_id: &str,
        user_name: &str,
    ) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "messageId": message_id,
            "channel": channel,
            "userId": user_id,
            "userName": user_name,
        });

        client.emit("mark_read", params).await
    }

    pub async fn get_unread_counts(&self, user_id: &str, channels: Vec<String>) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userId": user_id, "channels": channels });
        client.emit("get_unread_counts", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("unread_counts"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    pub async fn mark_all_read(&self, channel: &str, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "channel": channel, "userId": user_id });
        client.emit("mark_all_read", params).await
    }

    // MARK: - Channel Events

    pub async fn create_channel(
        &self,
        name: &str,
        channel_type: &str,
        description: &str,
        topic: &str,
        created_by: &str,
        created_by_name: &str,
    ) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "name": name,
            "type": channel_type,
            "description": description,
            "topic": topic,
            "createdBy": created_by,
            "createdByName": created_by_name,
            "members": [],
        });

        client.emit("create_channel", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("channel_create_success"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    pub async fn update_channel(
        &self,
        channel_id: &str,
        updates: HashMap<String, Value>,
        user_id: &str,
    ) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "channelId": channel_id,
            "updates": updates,
            "userId": user_id,
        });

        client.emit("update_channel", params).await
    }

    pub async fn archive_channel(&self, channel_id: &str, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "channelId": channel_id, "userId": user_id });
        client.emit("archive_channel", params).await
    }

    pub async fn invite_to_channel(
        &self,
        channel_id: &str,
        invited_user_id: &str,
        invited_user_name: &str,
        invited_by: &str,
    ) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "channelId": channel_id,
            "invitedUserId": invited_user_id,
            "invitedUserName": invited_user_name,
            "invitedBy": invited_by,
        });

        client.emit("invite_to_channel", params).await
    }

    pub async fn remove_from_channel(
        &self,
        channel_id: &str,
        removed_user_id: &str,
        removed_by: &str,
    ) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "channelId": channel_id,
            "removedUserId": removed_user_id,
            "removedBy": removed_by,
        });

        client.emit("remove_from_channel", params).await
    }

    pub async fn join_channel(&self, channel_id: &str, user_id: &str, user_name: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "channelId": channel_id,
            "userId": user_id,
            "userName": user_name,
        });

        client.emit("join_channel", params).await
    }

    pub async fn leave_channel(&self, channel_id: &str, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "channelId": channel_id, "userId": user_id });
        client.emit("leave_channel", params).await
    }

    pub async fn get_channel_members(&self, channel_id: &str) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "channelId": channel_id });
        client.emit("get_channel_members", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("channel_members"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    // MARK: - Direct Message Events

    pub async fn create_dm(&self, user_ids: Vec<String>, dm_type: &str) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userIds": user_ids, "type": dm_type });
        client.emit("create_dm", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("dm_create_success"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    pub async fn send_dm(
        &self,
        conversation_id: &str,
        message: &str,
        user_id: &str,
        user_name: &str,
    ) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "conversationId": conversation_id,
            "message": message,
            "userId": user_id,
            "userName": user_name,
        });

        client.emit("send_dm", params).await
    }

    pub async fn get_dm_conversations(&self, user_id: &str, include_archived: bool) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userId": user_id, "includeArchived": include_archived });
        client.emit("get_dm_conversations", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("dm_conversations"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    // MARK: - Notification Events

    pub async fn subscribe_notifications(&self, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userId": user_id });
        client.emit("subscribe_notifications", params).await
    }

    pub async fn mark_notification_read(&self, notification_id: &str, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "notificationId": notification_id, "userId": user_id });
        client.emit("mark_notification_read", params).await
    }

    pub async fn mark_all_notifications_read(&self, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userId": user_id });
        client.emit("mark_all_notifications_read", params).await
    }

    pub async fn clear_notifications(&self, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userId": user_id });
        client.emit("clear_notifications", params).await
    }

    pub async fn get_notifications(
        &self,
        user_id: &str,
        limit: u32,
        status: Option<&str>,
    ) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let mut params = json!({ "userId": user_id, "limit": limit });
        if let Some(s) = status {
            params["status"] = json!(s);
        }

        client.emit("get_notifications", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("notifications_data"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    // MARK: - Presence Events

    pub async fn set_status(&self, user_id: &str, status: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userId": user_id, "status": status });
        client.emit("set_status", params).await
    }

    pub async fn set_custom_status(
        &self,
        user_id: &str,
        emoji: &str,
        text: &str,
        expires_at: Option<&str>,
    ) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let mut params = json!({ "userId": user_id, "emoji": emoji, "text": text });
        if let Some(exp) = expires_at {
            params["expiresAt"] = json!(exp);
        }

        client.emit("set_custom_status", params).await
    }

    pub async fn clear_custom_status(&self, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userId": user_id });
        client.emit("clear_custom_status", params).await
    }

    pub async fn set_dnd(&self, user_id: &str, until: Option<&str>) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let mut params = json!({ "userId": user_id });
        if let Some(u) = until {
            params["until"] = json!(u);
        }

        client.emit("set_dnd", params).await
    }

    pub async fn clear_dnd(&self, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userId": user_id });
        client.emit("clear_dnd", params).await
    }

    pub async fn start_typing(&self, user_id: &str, channel: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userId": user_id, "channel": channel });
        client.emit("start_typing", params).await
    }

    pub async fn stop_typing(&self, user_id: &str, channel: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userId": user_id, "channel": channel });
        client.emit("stop_typing", params).await
    }

    pub async fn get_user_presence(&self, user_ids: Vec<String>) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "userIds": user_ids });
        client.emit("get_user_presence", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("user_presence_data"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    // MARK: - Message Editing Events

    pub async fn edit_message(
        &self,
        message_id: &str,
        channel: &str,
        new_content: &str,
        user_id: &str,
    ) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({
            "messageId": message_id,
            "channel": channel,
            "newContent": new_content,
            "userId": user_id,
        });

        client.emit("edit_message", params).await
    }

    pub async fn delete_message(&self, message_id: &str, channel: &str, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "messageId": message_id, "channel": channel, "userId": user_id });
        client.emit("delete_message", params).await
    }

    pub async fn pin_message(&self, message_id: &str, channel: &str, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "messageId": message_id, "channel": channel, "userId": user_id });
        client.emit("pin_message", params).await
    }

    pub async fn unpin_message(&self, message_id: &str, channel: &str, user_id: &str) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "messageId": message_id, "channel": channel, "userId": user_id });
        client.emit("unpin_message", params).await
    }

    pub async fn get_pinned_messages(&self, channel: &str) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "channel": channel });
        client.emit("get_pinned_messages", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("pinned_messages"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    // MARK: - Search Events

    pub async fn search_messages(&self, query: &str, user_id: &str, limit: u32) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "query": query, "userId": user_id, "limit": limit });
        client.emit("search_messages", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("search_results"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    pub async fn filter_messages(&self, filters: HashMap<String, Value>) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        client.emit("filter_messages", json!(filters)).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("filter_results"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    pub async fn search_in_channel(&self, channel: &str, query: &str, limit: u32) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let params = json!({ "channel": channel, "query": query, "limit": limit });
        client.emit("search_in_channel", params).await?;
        
        timeout(self.timeout_duration, client.wait_for_event("channel_search_results"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    pub async fn search_by_user(
        &self,
        user_id: &str,
        query: Option<&str>,
        limit: u32,
    ) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        let mut params = json!({ "userId": user_id, "limit": limit });
        if let Some(q) = query {
            params["query"] = json!(q);
        }

        client.emit("search_by_user", params).await?;

        timeout(self.timeout_duration, client.wait_for_event("user_search_results"))
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    // MARK: - Challenge / Leaderboard / Achievement Events
    //
    // Server-authoritative challenge lifecycle. Progress and completions land on
    // the shared room envelope so every member (and any partner resultWebhookUrl)
    // sees challenge_progress / leaderboard_rank_change / challenge_complete /
    // achievement_unlock — subscribe with client.on("leaderboard_rank_change", ...).

    /// Shared request/ack helper for the challenge surface. Emits `event`, then
    /// awaits either the `success_event` ack or a worker `error` whose `event`
    /// field equals `err_key` (JS: socket.once('error', e => if e.event === ...)).
    /// Mirrors get_reactions' emit + wait_for_event pattern, but races the ack
    /// against the guarded error event so a rejection fails fast instead of
    /// waiting out the timeout.
    async fn request_ack(
        &self,
        client: &crate::OddSocketsClient,
        event: &str,
        params: Value,
        success_event: &str,
        err_key: &str,
    ) -> Result<Value, OddSocketsError> {
        client.emit(event, params).await?;

        let success_fut = client.wait_for_event(success_event);
        let ack = async {
            tokio::pin!(success_fut);
            loop {
                tokio::select! {
                    data = &mut success_fut => return data,
                    err = client.wait_for_event("error") => {
                        let err = err?;
                        // Only a matching error rejects this call; unrelated
                        // worker errors are ignored so we keep awaiting our ack.
                        if err.get("event").and_then(Value::as_str) == Some(err_key) {
                            let message = err
                                .get("message")
                                .and_then(Value::as_str)
                                .unwrap_or("Challenge request failed")
                                .to_string();
                            return Err(OddSocketsError::MessageDeliveryFailed {
                                message,
                                message_id: None,
                                channel: None,
                            });
                        }
                    }
                }
            }
        };

        timeout(self.timeout_duration, ack)
            .await
            .map_err(|_| OddSocketsError::Timeout)?
    }

    /// Create (register) a challenge run and its optional result-webhook target.
    /// params: { challengeId, metric, ranked?, channel?, resultWebhookUrl?, standingsUrl? }
    pub async fn create_challenge(&self, params: Value) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        self.request_ack(
            &client,
            "challenge_create",
            params,
            "challenge_create_success",
            "challenge_create",
        )
        .await
    }

    /// Report a progress value for the connected player. Fire-and-forget: the
    /// server echoes challenge_progress (and leaderboard_rank_change if the
    /// player moved) to the room via client.on(...). Pass a stable eventId to
    /// make retries idempotent.
    /// params: { challengeId, value, metric?, eventId?, cohort?, platform?, channel? }
    pub async fn report_progress(&self, params: Value) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        client.emit("challenge_progress", params).await
    }

    /// Complete the connected player's run. Resolves with the server-authoritative
    /// result; the room also receives a challenge_complete broadcast.
    /// params: { challengeId, outcome, eventId?, reward? }
    /// outcome ∈ { completed, failed, expired, conceded, tied }
    pub async fn complete_challenge(&self, params: Value) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        self.request_ack(
            &client,
            "challenge_complete",
            params,
            "challenge_complete_success",
            "challenge_complete",
        )
        .await
    }

    /// Unlock (or advance) an achievement for the connected player. Fire-and-forget:
    /// the room receives achievement_unlock (percentComplete >= 100 or omitted) or
    /// achievement_progress (percentComplete < 100) via client.on(...).
    /// params: { achievementId, name?, tier?, percentComplete?, challengeId?, channel? }
    pub async fn unlock_achievement(&self, params: Value) -> Result<(), OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        client.emit("achievement_unlock", params).await
    }

    /// Fetch a page of leaderboard standings for a challenge.
    /// params: { challengeId, limit?=20, offset?=0 }
    pub async fn get_standings(&self, params: Value) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        self.request_ack(
            &client,
            "challenge_standings",
            params,
            "challenge_standings_success",
            "challenge_standings",
        )
        .await
    }

    /// Query the connected player's achievement state.
    /// params: { achievementId? }
    pub async fn get_achievements(&self, params: Value) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        self.request_ack(
            &client,
            "achievement_query",
            params,
            "achievement_state",
            "achievement_query",
        )
        .await
    }

    /// Send a directed 1:1 challenge/invite to a specific player. The invitee
    /// receives a challenge_invited event via client.on(...).
    /// params: { toUserId, type?='match', payload?<=8KB, ttl?=300, channel?, inviteId? }
    pub async fn send_challenge_invite(&self, params: Value) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        self.request_ack(
            &client,
            "challenge_invite",
            params,
            "challenge_invite_success",
            "challenge_invite",
        )
        .await
    }

    /// Accept or decline a received invite. The original inviter is notified via
    /// client.on("challenge_reply_received", ...).
    /// params: { inviteId, accept, reason? }
    pub async fn reply_challenge_invite(&self, params: Value) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        self.request_ack(
            &client,
            "challenge_reply",
            params,
            "challenge_reply_success",
            "challenge_reply",
        )
        .await
    }

    /// Cancel a pending invite you sent. The invitee is notified via
    /// client.on("challenge_invite_cancelled", ...).
    /// params: { inviteId }
    pub async fn cancel_challenge_invite(&self, params: Value) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        self.request_ack(
            &client,
            "challenge_invite_cancel",
            params,
            "challenge_invite_cancel_success",
            "challenge_invite_cancel",
        )
        .await
    }

    /// Pull the connected player's pending invites (e.g. after reconnect).
    /// params: {} (empty)
    pub async fn get_challenge_invites(&self) -> Result<Value, OddSocketsError> {
        let client = self.client.read().await;
        if !client.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }

        self.request_ack(
            &client,
            "challenge_invites_query",
            json!({}),
            "challenge_invites",
            "challenge_invites_query",
        )
        .await
    }
}
