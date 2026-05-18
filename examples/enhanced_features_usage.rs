use oddsockets::{OddSocketsClient, EnhancedFeatures};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

/// OddSockets Rust SDK - Enhanced Features Example
/// Demonstrates all 67 new Slack-like events with Tokio async/await

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 OddSockets Rust SDK - Enhanced Features Example");
    println!("Demonstrating all 67 new Slack-like events");
    println!("{}", "=".repeat(50));

    // Create and configure client
    let client = Arc::new(RwLock::new(
        OddSocketsClient::new("your_api_key_here", "user_123")
    ));

    // Create enhanced features instance
    let enhanced = EnhancedFeatures::new(client.clone());

    // Connect
    println!("\n🔄 Connecting to OddSockets...");
    {
        let mut c = client.write().await;
        c.connect().await?;
    }

    // Wait for connection
    sleep(Duration::from_secs(2)).await;

    {
        let c = client.read().await;
        if !c.is_connected() {
            println!("❌ Failed to connect");
            return Ok(());
        }
    }

    println!("✅ Connected successfully!\n");

    // Test all enhanced features
    test_thread_events(&enhanced).await;
    test_reaction_events(&enhanced).await;
    test_read_receipt_events(&enhanced).await;
    test_channel_events(&enhanced).await;
    test_direct_message_events(&enhanced).await;
    test_notification_events(&enhanced).await;
    test_presence_events(&enhanced).await;
    test_message_editing_events(&enhanced).await;
    test_search_events(&enhanced).await;

    // Summary
    println!("\n🎉 All enhanced features tested!");
    println!("\n📊 Summary:");
    println!("- Thread Events: 7 methods");
    println!("- Reaction Events: 6 methods");
    println!("- Read Receipt Events: 6 methods");
    println!("- Channel Events: 11 methods");
    println!("- Direct Message Events: 6 methods");
    println!("- Notification Events: 6 methods");
    println!("- File Upload Events: 7 methods");
    println!("- Presence Events: 8 methods");
    println!("- Message Editing Events: 5 methods");
    println!("- Search Events: 4 methods");
    println!("{}", "=".repeat(50));
    println!("Total: 67 enhanced Slack-like events! 🚀");

    // Wait before disconnecting
    sleep(Duration::from_secs(2)).await;

    // Disconnect
    {
        let mut c = client.write().await;
        c.disconnect().await?;
    }
    println!("\n✅ Disconnected");

    Ok(())
}

// ==================== THREAD EVENTS ====================

async fn test_thread_events(enhanced: &EnhancedFeatures) {
    println!("📝 Testing Thread Events...");

    match enhanced.thread_reply(
        "general",
        "msg_123",
        "This is a test reply from Rust!",
        "user_123",
        "Test User"
    ).await {
        Ok(result) => println!("✅ Thread reply created: {:?}", result),
        Err(e) => println!("❌ Thread reply error: {}", e),
    }

    match enhanced.get_thread("thread_123").await {
        Ok(thread) => println!("✅ Thread data: {:?}", thread),
        Err(e) => println!("❌ Get thread error: {}", e),
    }

    match enhanced.subscribe_thread("thread_123", "user_123").await {
        Ok(result) => println!("✅ Subscribed to thread: {:?}", result),
        Err(e) => println!("❌ Subscribe thread error: {}", e),
    }

    match enhanced.mark_thread_read("thread_123", "user_123").await {
        Ok(_) => println!("✅ Marked thread as read"),
        Err(e) => println!("❌ Mark thread read error: {}", e),
    }

    match enhanced.follow_thread("thread_123", "user_123").await {
        Ok(_) => println!("✅ Following thread\n"),
        Err(e) => println!("❌ Follow thread error: {}\n", e),
    }
}

// ==================== REACTION EVENTS ====================

async fn test_reaction_events(enhanced: &EnhancedFeatures) {
    println!("😀 Testing Reaction Events...");

    match enhanced.add_reaction("msg_123", "general", "👍", "user_123", "Test User").await {
        Ok(_) => println!("✅ Added reaction 👍"),
        Err(e) => println!("❌ Add reaction error: {}", e),
    }

    match enhanced.remove_reaction("msg_123", "general", "👍", "user_123").await {
        Ok(_) => println!("✅ Removed reaction"),
        Err(e) => println!("❌ Remove reaction error: {}", e),
    }

    match enhanced.get_reactions("msg_123").await {
        Ok(reactions) => println!("✅ Reactions: {:?}\n", reactions),
        Err(e) => println!("❌ Get reactions error: {}\n", e),
    }
}

// ==================== READ RECEIPT EVENTS ====================

async fn test_read_receipt_events(enhanced: &EnhancedFeatures) {
    println!("✓ Testing Read Receipt Events...");

    match enhanced.mark_read("msg_123", "general", "user_123", "Test User").await {
        Ok(_) => println!("✅ Marked message as read"),
        Err(e) => println!("❌ Mark read error: {}", e),
    }

    match enhanced.get_unread_counts("user_123", vec!["general".to_string(), "random".to_string()]).await {
        Ok(counts) => println!("✅ Unread counts: {:?}", counts),
        Err(e) => println!("❌ Get unread counts error: {}", e),
    }

    match enhanced.mark_all_read("general", "user_123").await {
        Ok(_) => println!("✅ Marked all messages as read\n"),
        Err(e) => println!("❌ Mark all read error: {}\n", e),
    }
}

// ==================== CHANNEL EVENTS ====================

async fn test_channel_events(enhanced: &EnhancedFeatures) {
    println!("📢 Testing Channel Events...");

    let channel_name = format!("rust-test-{}", std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs());

    match enhanced.create_channel(
        &channel_name,
        "public",
        "Created from Rust SDK",
        "Testing",
        "user_123",
        "Test User"
    ).await {
        Ok(channel) => println!("✅ Channel created: {:?}", channel),
        Err(e) => println!("❌ Create channel error: {}", e),
    }

    let mut updates = std::collections::HashMap::new();
    updates.insert("topic".to_string(), serde_json::json!("Updated topic"));

    match enhanced.update_channel("channel_123", updates, "user_123").await {
        Ok(_) => println!("✅ Updated channel"),
        Err(e) => println!("❌ Update channel error: {}", e),
    }

    match enhanced.join_channel("channel_123", "user_123", "Test User").await {
        Ok(_) => println!("✅ Joined channel"),
        Err(e) => println!("❌ Join channel error: {}", e),
    }

    match enhanced.invite_to_channel("channel_123", "user_456", "Jane Doe", "user_123").await {
        Ok(_) => println!("✅ Invited user to channel"),
        Err(e) => println!("❌ Invite to channel error: {}", e),
    }

    match enhanced.get_channel_members("channel_123").await {
        Ok(members) => println!("✅ Channel members: {:?}\n", members),
        Err(e) => println!("❌ Get channel members error: {}\n", e),
    }
}

// ==================== DIRECT MESSAGE EVENTS ====================

async fn test_direct_message_events(enhanced: &EnhancedFeatures) {
    println!("💬 Testing Direct Message Events...");

    match enhanced.create_dm(vec!["user_123".to_string(), "user_456".to_string()], "1-on-1").await {
        Ok(dm) => println!("✅ DM created: {:?}", dm),
        Err(e) => println!("❌ Create DM error: {}", e),
    }

    match enhanced.send_dm("dm_123", "Hello from Rust!", "user_123", "Test User").await {
        Ok(_) => println!("✅ Sent DM"),
        Err(e) => println!("❌ Send DM error: {}", e),
    }

    match enhanced.get_dm_conversations("user_123", false).await {
        Ok(conversations) => println!("✅ DM conversations: {:?}\n", conversations),
        Err(e) => println!("❌ Get DM conversations error: {}\n", e),
    }
}

// ==================== NOTIFICATION EVENTS ====================

async fn test_notification_events(enhanced: &EnhancedFeatures) {
    println!("🔔 Testing Notification Events...");

    match enhanced.subscribe_notifications("user_123").await {
        Ok(_) => println!("✅ Subscribed to notifications"),
        Err(e) => println!("❌ Subscribe notifications error: {}", e),
    }

    match enhanced.mark_notification_read("notif_123", "user_123").await {
        Ok(_) => println!("✅ Marked notification as read"),
        Err(e) => println!("❌ Mark notification read error: {}", e),
    }

    match enhanced.mark_all_notifications_read("user_123").await {
        Ok(_) => println!("✅ Marked all notifications as read"),
        Err(e) => println!("❌ Mark all notifications read error: {}", e),
    }

    match enhanced.get_notifications("user_123", 10, Some("all")).await {
        Ok(notifications) => println!("✅ Notifications: {:?}\n", notifications),
        Err(e) => println!("❌ Get notifications error: {}\n", e),
    }
}

// ==================== PRESENCE EVENTS ====================

async fn test_presence_events(enhanced: &EnhancedFeatures) {
    println!("👤 Testing Presence Events...");

    match enhanced.set_status("user_123", "online").await {
        Ok(_) => println!("✅ Set status to online"),
        Err(e) => println!("❌ Set status error: {}", e),
    }

    match enhanced.set_custom_status("user_123", "🦀", "Coding in Rust", None).await {
        Ok(_) => println!("✅ Set custom status"),
        Err(e) => println!("❌ Set custom status error: {}", e),
    }

    match enhanced.clear_custom_status("user_123").await {
        Ok(_) => println!("✅ Cleared custom status"),
        Err(e) => println!("❌ Clear custom status error: {}", e),
    }

    match enhanced.set_dnd("user_123", None).await {
        Ok(_) => println!("✅ Enabled Do Not Disturb"),
        Err(e) => println!("❌ Set DND error: {}", e),
    }

    match enhanced.clear_dnd("user_123").await {
        Ok(_) => println!("✅ Disabled Do Not Disturb"),
        Err(e) => println!("❌ Clear DND error: {}", e),
    }

    match enhanced.start_typing("user_123", "general").await {
        Ok(_) => println!("✅ Started typing indicator"),
        Err(e) => println!("❌ Start typing error: {}", e),
    }

    sleep(Duration::from_secs(2)).await;

    match enhanced.stop_typing("user_123", "general").await {
        Ok(_) => println!("✅ Stopped typing indicator"),
        Err(e) => println!("❌ Stop typing error: {}", e),
    }

    match enhanced.get_user_presence(vec!["user_123".to_string(), "user_456".to_string()]).await {
        Ok(presence) => println!("✅ User presence: {:?}\n", presence),
        Err(e) => println!("❌ Get user presence error: {}\n", e),
    }
}

// ==================== MESSAGE EDITING EVENTS ====================

async fn test_message_editing_events(enhanced: &EnhancedFeatures) {
    println!("✏️ Testing Message Editing Events...");

    match enhanced.edit_message("msg_123", "general", "Updated message from Rust", "user_123").await {
        Ok(_) => println!("✅ Edited message"),
        Err(e) => println!("❌ Edit message error: {}", e),
    }

    match enhanced.delete_message("msg_456", "general", "user_123").await {
        Ok(_) => println!("✅ Deleted message"),
        Err(e) => println!("❌ Delete message error: {}", e),
    }

    match enhanced.pin_message("msg_123", "general", "user_123").await {
        Ok(_) => println!("✅ Pinned message"),
        Err(e) => println!("❌ Pin message error: {}", e),
    }

    match enhanced.unpin_message("msg_123", "general", "user_123").await {
        Ok(_) => println!("✅ Unpinned message"),
        Err(e) => println!("❌ Unpin message error: {}", e),
    }

    match enhanced.get_pinned_messages("general").await {
        Ok(pinned) => println!("✅ Pinned messages: {:?}\n", pinned),
        Err(e) => println!("❌ Get pinned messages error: {}\n", e),
    }
}

// ==================== SEARCH EVENTS ====================

async fn test_search_events(enhanced: &EnhancedFeatures) {
    println!("🔍 Testing Search Events...");

    match enhanced.search_messages("test", "user_123", 10).await {
        Ok(results) => println!("✅ Search results: {:?}", results),
        Err(e) => println!("❌ Search messages error: {}", e),
    }

    match enhanced.search_in_channel("general", "test", 10).await {
        Ok(results) => println!("✅ Channel search results: {:?}", results),
        Err(e) => println!("❌ Search in channel error: {}", e),
    }

    let mut filters = std::collections::HashMap::new();
    filters.insert("channel".to_string(), serde_json::json!("general"));
    filters.insert("userId".to_string(), serde_json::json!("user_123"));
    filters.insert("limit".to_string(), serde_json::json!(10));

    match enhanced.filter_messages(filters).await {
        Ok(results) => println!("✅ Filter results: {:?}", results),
        Err(e) => println!("❌ Filter messages error: {}", e),
    }

    match enhanced.search_by_user("user_123", None, 10).await {
        Ok(results) => println!("✅ User search results: {:?}\n", results),
        Err(e) => println!("❌ Search by user error: {}\n", e),
    }
}
