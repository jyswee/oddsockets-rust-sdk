//! OddSockets Rust SDK - two-client honest regression demo
//!
//! Everything here runs through the REAL OddSockets platform: Manager -> Worker
//! discovery over HTTP, a genuine Socket.IO WebSocket per client, and live
//! broadcast fan-out between two SEPARATE connections. No mocks, no local echo.
//!
//! Scenario 1 - core pub/sub:
//!   alice subscribes, bob publishes a nonce-tagged message on a second
//!   connection, alice receives it on her broadcast receiver.
//!
//! Scenario 2 - enhanced (Slack-like) events:
//!   both clients subscribe to an enhanced channel. alice registers public
//!   on("user_typing") + on("reaction_added") listeners. bob fires
//!   enhanced.start_typing + enhanced.add_reaction. alice receives both
//!   broadcasts across the wire.
//!
//! Run:
//!   export ODDSOCKETS_API_KEY="ak_..."   # get a free key: see README
//!   cargo run
//!
//! Exit codes: 0 all green, 1 missing key / setup, 2 a scenario timed out.

use oddsockets::{
    message_types, EnhancedFeatures, OddSocketsClient, OddSocketsConfig, PublishOptions,
    SubscribeOptions,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio::time::{sleep, timeout};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = match std::env::var("ODDSOCKETS_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            eprintln!("Missing ODDSOCKETS_API_KEY. Get a free key (see README), then:");
            eprintln!("  export ODDSOCKETS_API_KEY=\"ak_...\"");
            std::process::exit(1);
        }
    };

    let nonce = format!("{:x}", unique());

    // Two independent clients on two independent connections.
    let alice = connect(&api_key, "alice").await?;
    let bob = connect(&api_key, "bob").await?;
    println!(
        "[connect] alice -> {}, bob -> {}",
        alice.worker_id().unwrap_or_else(|| "?".into()),
        bob.worker_id().unwrap_or_else(|| "?".into())
    );

    scenario_core(&alice, &bob, &nonce).await?;
    scenario_enhanced(&alice, &bob, &nonce).await?;

    let _ = alice.disconnect().await;
    let _ = bob.disconnect().await;

    println!("\nOK - all scenarios verified live through the OddSockets platform");
    Ok(())
}

async fn connect(api_key: &str, user_id: &str) -> Result<OddSocketsClient, Box<dyn std::error::Error>> {
    let config = OddSocketsConfig::builder(api_key).user_id(user_id).build()?;
    let client = OddSocketsClient::new(config).await?;
    client.connect().await?;
    Ok(client)
}

/// Scenario 1: bob publishes, alice (separate connection) receives.
async fn scenario_core(
    alice: &OddSocketsClient,
    bob: &OddSocketsClient,
    nonce: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel_name = format!("demo-core-{}", nonce);
    println!("\n=== Scenario 1: core pub/sub on {} ===", channel_name);

    let alice_ch = alice.channel(&channel_name);
    let mut receiver = alice_ch.subscribe(SubscribeOptions::default()).await?;
    println!("[alice] subscribed");

    let bob_ch = bob.channel(&channel_name);
    let _ = bob_ch.subscribe(SubscribeOptions::default()).await?;
    println!("[bob] subscribed");

    // Give the room membership a moment to settle before publishing.
    sleep(Duration::from_millis(500)).await;

    let needle = nonce.to_string();
    let waiter = tokio::spawn(async move {
        while let Ok(message) = receiver.recv().await {
            let rendered = format!("{:?}", message);
            println!("[alice recv] {}", rendered);
            if rendered.contains(&needle) {
                return true;
            }
        }
        false
    });

    let payload = message_types::chat_message(
        format!("hello from bob nonce={}", nonce),
        "bob",
        Some("demo"),
    );
    let result = bob_ch.publish(payload, PublishOptions::default()).await?;
    println!("[bob] published messageId={}", result.message_id);

    match timeout(Duration::from_secs(15), waiter).await {
        Ok(Ok(true)) => {
            println!("[PASS] alice received bob's message across separate connections");
            let _ = alice_ch.unsubscribe().await;
            let _ = bob_ch.unsubscribe().await;
            Ok(())
        }
        _ => {
            eprintln!("[FAIL] alice never received bob's message within 15s");
            std::process::exit(2);
        }
    }
}

/// Scenario 2: bob fires enhanced typing + reaction, alice receives both
/// broadcasts on her public on() surface.
async fn scenario_enhanced(
    alice: &OddSocketsClient,
    bob: &OddSocketsClient,
    nonce: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel_name = format!("demo-enh-{}", nonce);
    println!("\n=== Scenario 2: enhanced events on {} ===", channel_name);

    // Both join the room so worker broadcasts reach them.
    let alice_ch = alice.channel(&channel_name);
    let _ = alice_ch.subscribe(SubscribeOptions::default()).await?;
    let bob_ch = bob.channel(&channel_name);
    let _ = bob_ch.subscribe(SubscribeOptions::default()).await?;
    println!("[alice/bob] subscribed to enhanced channel");

    // alice listens on the PUBLIC transport surface for enhanced broadcasts.
    let typing_seen = Arc::new(AtomicBool::new(false));
    let reaction_seen = Arc::new(AtomicBool::new(false));
    let typing_note = Arc::new(Notify::new());
    let reaction_note = Arc::new(Notify::new());

    {
        let seen = typing_seen.clone();
        let note = typing_note.clone();
        alice.on("user_typing", move |payload| {
            println!("[alice on user_typing] {}", payload);
            seen.store(true, Ordering::SeqCst);
            note.notify_one();
        });
    }
    {
        let seen = reaction_seen.clone();
        let note = reaction_note.clone();
        alice.on("reaction_added", move |payload| {
            println!("[alice on reaction_added] {}", payload);
            seen.store(true, Ordering::SeqCst);
            note.notify_one();
        });
    }

    sleep(Duration::from_millis(500)).await;

    // bob needs a real messageId to react to: publish one first.
    let payload = message_types::chat_message(
        format!("enhanced anchor nonce={}", nonce),
        "bob",
        Some("demo"),
    );
    let anchor = bob_ch.publish(payload, PublishOptions::default()).await?;
    println!("[bob] published anchor messageId={}", anchor.message_id);

    // bob's enhanced surface shares bob's live connection.
    let bob_enhanced = EnhancedFeatures::new(Arc::new(RwLock::new(bob.clone())));

    bob_enhanced.start_typing("bob", &channel_name).await?;
    println!("[bob] enhanced.start_typing fired");

    bob_enhanced
        .add_reaction(&anchor.message_id, &channel_name, ":thumbsup:", "bob", "Bob")
        .await?;
    println!("[bob] enhanced.add_reaction fired");

    let typing_ok = timeout(Duration::from_secs(15), typing_note.notified())
        .await
        .is_ok()
        || typing_seen.load(Ordering::SeqCst);
    let reaction_ok = timeout(Duration::from_secs(15), reaction_note.notified())
        .await
        .is_ok()
        || reaction_seen.load(Ordering::SeqCst);

    let _ = alice_ch.unsubscribe().await;
    let _ = bob_ch.unsubscribe().await;

    if typing_ok && reaction_ok {
        println!("[PASS] alice received user_typing AND reaction_added from bob");
        Ok(())
    } else {
        eprintln!(
            "[FAIL] enhanced broadcasts missing (typing={}, reaction={})",
            typing_ok, reaction_ok
        );
        std::process::exit(2);
    }
}

/// Small unique id without pulling an extra direct dependency.
fn unique() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    now.as_nanos() ^ ((std::process::id() as u128) << 64)
}
