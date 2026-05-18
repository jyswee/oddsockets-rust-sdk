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
}
