//! Basic usage example for the OddSockets Rust SDK.
//!
//! This example demonstrates:
//! - Creating and configuring a client
//! - Connecting to OddSockets
//! - Subscribing to a channel
//! - Publishing messages
//! - Handling incoming messages
//! - Bulk publishing
//! - Graceful shutdown
//!
//! Run with: `cargo run --example basic_usage`

use oddsockets::{
    message_types, utils, BulkMessage, OddSocketsClient, OddSocketsConfig, PublishOptions,
    SubscribeOptions,
};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    println!("🦀 OddSockets Rust SDK - Basic Usage Example");
    println!("============================================");

    // Create configuration
    let config = OddSocketsConfig::builder("ak_your_api_key_here")
        .development() // Use development settings
        .user_id("rust_example_user")
        .heartbeat_interval(Duration::from_secs(30))
        .reconnect_attempts(3)
        .build()?;

    println!("📋 Configuration created");
    println!("   API Key: {}...", &config.api_key[..10]);
    println!("   Manager URL: {}", config.manager_url);
    println!("   Auto Connect: {}", config.auto_connect);

    // Create client
    let client = OddSocketsClient::new(config).await?;
    println!("🔧 Client created successfully");

    // Connect to OddSockets
    println!("🔌 Connecting to OddSockets...");
    client.connect().await?;
    println!("✅ Connected successfully!");

    // Get a channel
    let channel = client.channel("rust-example-channel");
    println!("📺 Channel 'rust-example-channel' created");

    // Subscribe to the channel with options
    let subscribe_options = SubscribeOptions::chat_channel(); // Enable presence and history
    let mut message_receiver = channel.subscribe(subscribe_options).await?;
    println!("🔔 Subscribed to channel with presence and history enabled");

    // Spawn a task to handle incoming messages
    let message_handler = tokio::spawn(async move {
        let mut message_count = 0;
        while let Ok(message) = message_receiver.recv().await {
            message_count += 1;
            println!("📨 Received message #{}: {:?}", message_count, message);
            
            // Stop after receiving 5 messages
            if message_count >= 5 {
                break;
            }
        }
        println!("🛑 Message handler finished");
    });

    // Wait a moment for subscription to be established
    sleep(Duration::from_secs(1)).await;

    // Publish some individual messages
    println!("\n📤 Publishing individual messages...");
    
    for i in 1..=3 {
        let message = message_types::chat_message(
            format!("Hello from Rust! Message #{}", i),
            "rust_user",
            Some("example"),
        );
        
        let publish_options = PublishOptions::chat_message();
        let result = channel.publish(message, publish_options).await?;
        
        println!("✅ Published message #{}: {}", i, result.message_id);
        sleep(Duration::from_millis(500)).await;
    }

    // Demonstrate bulk publishing
    println!("\n📦 Publishing bulk messages...");
    
    let bulk_messages = vec![
        BulkMessage::new(
            "rust-example-channel",
            message_types::notification_message(
                "System Alert",
                "Bulk message #1",
                Some("system"),
                Some("normal"),
                None,
            ),
            Some(PublishOptions::system_message()),
        ),
        BulkMessage::new(
            "rust-example-channel",
            message_types::data_event(
                "user_action",
                serde_json::json!({
                    "action": "bulk_publish_test",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "data": {"test": true, "count": 42}
                }),
                Some("rust_sdk"),
            ),
            Some(PublishOptions::with_history()),
        ),
    ];

    let bulk_results = client.publish_bulk(bulk_messages).await?;
    
    for (i, result) in bulk_results.iter().enumerate() {
        if result.is_successful() {
            if let Some(publish_result) = &result.result {
                println!("✅ Bulk message #{} published: {}", i + 1, publish_result.message_id);
            }
        } else {
            println!("❌ Bulk message #{} failed: {}", i + 1, result.error_message("Unknown error"));
        }
    }

    // Wait for message handler to finish
    println!("\n⏳ Waiting for message handler to finish...");
    let _ = message_handler.await;

    // Demonstrate getting message history
    println!("\n📚 Retrieving message history...");
    let history = channel.get_history(None).await?;
    println!("📖 Retrieved {} messages from history", history.len());
    
    for (i, msg) in history.iter().take(3).enumerate() {
        println!("   {}. [{}] {}: {:?}", 
                 i + 1, 
                 msg.timestamp.format("%H:%M:%S"), 
                 msg.user_id.as_deref().unwrap_or("unknown"),
                 msg.data);
    }

    // Demonstrate presence information
    println!("\n👥 Getting presence information...");
    match channel.get_presence().await {
        Ok(presence) => {
            println!("👤 Channel presence: {} users", presence.count);
            for user in &presence.users {
                println!("   - {}", user);
            }
        }
        Err(e) => {
            println!("⚠️  Could not get presence: {}", e);
        }
    }

    // Unsubscribe from the channel
    println!("\n🔕 Unsubscribing from channel...");
    channel.unsubscribe().await?;
    println!("✅ Unsubscribed successfully");

    // Disconnect from OddSockets
    println!("\n🔌 Disconnecting from OddSockets...");
    client.disconnect().await?;
    println!("✅ Disconnected successfully");

    println!("\n🎉 Example completed successfully!");
    println!("   - Connected to OddSockets ✅");
    println!("   - Subscribed to channel ✅");
    println!("   - Published individual messages ✅");
    println!("   - Published bulk messages ✅");
    println!("   - Received messages ✅");
    println!("   - Retrieved message history ✅");
    println!("   - Got presence information ✅");
    println!("   - Gracefully disconnected ✅");

    Ok(())
}
