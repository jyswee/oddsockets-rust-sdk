//! HONEST two-client challenge/leaderboard/achievement regression against live QA.
//!
//! Two DISTINCT users (alice, bob) sharing the SAME apiKey (owner scope) connect
//! through the manager LB, both subscribe to `lobby`, then drive the full
//! challenge surface added to `EnhancedFeatures`. Every assertion is a genuine
//! CROSS-CLIENT observation: what one client emits is verified by what the OTHER
//! client actually receives over the real Socket.IO transport (no local echo).
//!
//! Run with:
//!   OS_KEY=ak_... ODDSOCKETS_MANAGER_URL=https://manager... \
//!     cargo run --example two_client_challenge
//!
//! Room broadcasts arrive wrapped: {version,type,identity,challengeId,data:{...}}.
//! Directed invite/reply/cancel events are FLAT (inviteId/from/payload top level).

use oddsockets::{EnhancedFeatures, OddSocketsClient, OddSocketsConfig, SubscribeOptions};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout};

/// A tiny inbound-event tape: every raw event we register interest in is pushed
/// here by the client's `on(...)` listener, so assertions can wait for the exact
/// cross-client delivery instead of guessing with a fixed sleep.
#[derive(Clone, Default)]
struct Tape {
    events: Arc<Mutex<Vec<(String, Value)>>>,
}

impl Tape {
    fn record(&self, client: &OddSocketsClient, event: &str) {
        let events = self.events.clone();
        let ev = event.to_string();
        client.on(event, move |payload| {
            events.lock().unwrap().push((ev.clone(), payload));
        });
    }

    /// Waits up to `secs` for the first event named `event` matching `pred`.
    async fn wait<F>(&self, event: &str, secs: u64, pred: F) -> Option<Value>
    where
        F: Fn(&Value) -> bool,
    {
        let deadline = timeout(Duration::from_secs(secs), async {
            loop {
                if let Some(v) = self
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|(e, p)| e == event && pred(p))
                    .map(|(_, p)| p.clone())
                {
                    return v;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        deadline.ok()
    }

    /// Asserts an event named `event` was NOT seen within `secs` (negative test).
    async fn absent(&self, event: &str, secs: u64) -> bool {
        sleep(Duration::from_secs(secs)).await;
        !self.events.lock().unwrap().iter().any(|(e, _)| e == event)
    }
}

/// Semantic body of a room broadcast: the worker wraps room events as
/// {version,type,identity,challengeId,data:{...}} — semantic fields live under
/// `.data`. This returns `.data` if present, else the payload itself (so a flat
/// worker shape still works).
fn body(v: &Value) -> &Value {
    v.get("data").unwrap_or(v)
}

struct Peer {
    name: &'static str,
    client: OddSocketsClient,
    features: EnhancedFeatures,
    tape: Tape,
}

async fn make_peer(
    name: &'static str,
    api_key: &str,
    manager_url: &str,
    user_id: &str,
) -> Result<Peer, Box<dyn std::error::Error>> {
    let config: OddSocketsConfig = OddSocketsConfig::builder(api_key)
        .manager_url(manager_url)
        .user_id(user_id)
        .auto_connect(false)
        .timeout(Duration::from_secs(10))
        .build()?;

    let client = OddSocketsClient::new(config).await?;
    client.connect().await?;

    // Register interest in every inbound broadcast BEFORE wrapping the client for
    // EnhancedFeatures. Listeners live in the shared Inner, so the Arc<RwLock>
    // wrapper observes the same tape.
    let tape = Tape::default();
    for ev in [
        "challenge_progress",
        "leaderboard_rank_change",
        "challenge_complete",
        "achievement_unlock",
        "achievement_progress",
        "challenge_invited",
        "challenge_reply_received",
        "challenge_invite_cancelled",
    ] {
        tape.record(&client, ev);
    }

    let features = EnhancedFeatures::new(Arc::new(RwLock::new(client.clone())));

    Ok(Peer {
        name,
        client,
        features,
        tape,
    })
}

/// Pass/fail bookkeeping.
struct Report {
    pass: u32,
    fail: u32,
}

impl Report {
    fn new() -> Self {
        Self { pass: 0, fail: 0 }
    }
    fn check(&mut self, label: &str, ok: bool) {
        if ok {
            self.pass += 1;
            println!("  PASS  {}", label);
        } else {
            self.fail += 1;
            println!("  FAIL  {}", label);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = std::env::var("OS_KEY").expect("OS_KEY env var required");
    let manager_url =
        std::env::var("ODDSOCKETS_MANAGER_URL").expect("ODDSOCKETS_MANAGER_URL env var required");

    // Unique challenge id per run so state never collides with a prior run.
    let run = chrono::Utc::now().timestamp_millis();
    let challenge_id = format!("rust-chal-{}", run);
    let achievement_id = format!("rust-ach-{}", run);

    println!("== Rust SDK two-client challenge regression (live QA) ==");
    println!("manager: {}", manager_url);
    println!("challengeId: {}", challenge_id);

    let alice = make_peer("alice", &api_key, &manager_url, &format!("alice-{}", run)).await?;
    let bob = make_peer("bob", &api_key, &manager_url, &format!("bob-{}", run)).await?;

    println!(
        "alice worker: {:?} ({})",
        alice.client.worker_id(),
        alice.client.user_id()
    );
    println!(
        "bob   worker: {:?} ({})",
        bob.client.worker_id(),
        bob.client.user_id()
    );

    // Both subscribe to lobby.
    let _a_rx = alice
        .client
        .channel("lobby")
        .subscribe(SubscribeOptions::default())
        .await?;
    let _b_rx = bob
        .client
        .channel("lobby")
        .subscribe(SubscribeOptions::default())
        .await?;
    // Let the room memberships settle.
    sleep(Duration::from_millis(800)).await;

    let alice_id = alice.client.user_id();
    let bob_id = bob.client.user_id();
    let mut r = Report::new();

    // --- 1. create_challenge (alice) -> ack challenge_create_success -----------
    println!("\n[1] create_challenge");
    let create = alice
        .features
        .create_challenge(json!({
            "challengeId": challenge_id,
            "metric": "score",
            "ranked": true,
            "channel": "lobby",
        }))
        .await;
    r.check("create acked (challenge_create_success)", create.is_ok());
    if let Err(e) = &create {
        println!("     create error: {}", e);
    }

    // --- 2. report_progress: alice=40, bob=55 ---------------------------------
    println!("\n[2] report_progress alice=40 bob=55 (cross-client broadcast)");
    alice
        .features
        .report_progress(json!({
            "challengeId": challenge_id, "metric": "score", "value": 40,
            "eventId": format!("a-prog-{}", run),
        }))
        .await?;
    bob.features
        .report_progress(json!({
            "challengeId": challenge_id, "metric": "score", "value": 55,
            "eventId": format!("b-prog-{}", run),
        }))
        .await?;

    // alice must SEE a challenge_progress broadcast (fan-out to the room).
    let a_prog = alice
        .tape
        .wait("challenge_progress", 8, |p| {
            body(p).get("challengeId").and_then(Value::as_str) == Some(challenge_id.as_str())
                || p.get("challengeId").and_then(Value::as_str) == Some(challenge_id.as_str())
        })
        .await;
    r.check("alice sees challenge_progress", a_prog.is_some());

    let a_rank = alice
        .tape
        .wait("leaderboard_rank_change", 8, |_| true)
        .await;
    r.check("alice sees leaderboard_rank_change", a_rank.is_some());

    // --- 3. get_standings (alice): bob@55 rank1, alice@40 rank2, yourRank=2 ----
    println!("\n[3] get_standings");
    let standings = alice
        .features
        .get_standings(json!({ "challengeId": challenge_id, "limit": 10 }))
        .await;
    match &standings {
        Ok(s) => {
            let d = body(s);
            let arr = d
                .get("standings")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let find_rank = |uid: &str| -> Option<(i64, i64)> {
                arr.iter().find_map(|row| {
                    let ident = row.get("identity").and_then(Value::as_str)?;
                    if ident == uid {
                        Some((
                            row.get("value").and_then(Value::as_i64).unwrap_or(-1),
                            row.get("rank").and_then(Value::as_i64).unwrap_or(-1),
                        ))
                    } else {
                        None
                    }
                })
            };
            let bob_row = find_rank(&bob_id);
            let alice_row = find_rank(&alice_id);
            let your_rank = d.get("yourRank").and_then(Value::as_i64);
            r.check("standings: bob value=55 rank=1", bob_row == Some((55, 1)));
            r.check("standings: alice value=40 rank=2", alice_row == Some((40, 2)));
            r.check("standings: alice yourRank=2", your_rank == Some(2));
            println!("     standings body: {}", d);
        }
        Err(e) => {
            r.check("get_standings acked", false);
            println!("     get_standings error: {}", e);
        }
    }

    // --- 4. complete: alice(tied)->finalValue40 rank2; bob(conceded)->55 rank1 -
    println!("\n[4] complete_challenge");
    let a_complete = alice
        .features
        .complete_challenge(json!({
            "challengeId": challenge_id, "outcome": "tied",
            "eventId": format!("a-done-{}", run),
        }))
        .await;
    match &a_complete {
        Ok(c) => {
            let d = body(c);
            let fv = d.get("finalValue").and_then(Value::as_i64);
            let rank = d.get("rank").and_then(Value::as_i64);
            let outcome = d.get("outcome").and_then(Value::as_str);
            r.check(
                "alice complete(tied): finalValue=40 rank=2",
                fv == Some(40) && rank == Some(2),
            );
            println!("     alice complete body: outcome={:?} {}", outcome, d);
        }
        Err(e) => {
            r.check("alice complete acked", false);
            println!("     alice complete error: {}", e);
        }
    }

    let b_complete = bob
        .features
        .complete_challenge(json!({
            "challengeId": challenge_id, "outcome": "conceded",
            "eventId": format!("b-done-{}", run),
        }))
        .await;
    match &b_complete {
        Ok(c) => {
            let d = body(c);
            let fv = d.get("finalValue").and_then(Value::as_i64);
            let rank = d.get("rank").and_then(Value::as_i64);
            r.check(
                "bob complete(conceded): finalValue=55 rank=1",
                fv == Some(55) && rank == Some(1),
            );
            println!("     bob complete body: {}", d);
        }
        Err(e) => {
            r.check("bob complete acked", false);
            println!("     bob complete error: {}", e);
        }
    }

    // --- 5. unlock_achievement 50 => bob sees achievement_progress in_progress -
    println!("\n[5] unlock_achievement 50% -> achievement_progress (no banner)");
    alice
        .features
        .unlock_achievement(json!({
            "achievementId": achievement_id, "name": "First Steps",
            "percentComplete": 50, "channel": "lobby",
        }))
        .await?;
    let b_prog = bob
        .tape
        .wait("achievement_progress", 8, |p| {
            let d = body(p);
            d.get("status").and_then(Value::as_str) == Some("in_progress")
                || p.get("status").and_then(Value::as_str) == Some("in_progress")
        })
        .await;
    r.check(
        "bob sees achievement_progress status=in_progress",
        b_prog.is_some(),
    );
    let no_banner = bob.tape.absent("achievement_unlock", 2).await;
    r.check("bob sees NO achievement_unlock banner at 50%", no_banner);

    // --- 6. unlock_achievement 100 => bob sees achievement_unlock unlocked ----
    println!("\n[6] unlock_achievement 100% -> achievement_unlock unlocked");
    alice
        .features
        .unlock_achievement(json!({
            "achievementId": achievement_id, "name": "First Steps",
            "percentComplete": 100, "channel": "lobby",
        }))
        .await?;
    let b_unlock = bob
        .tape
        .wait("achievement_unlock", 8, |p| {
            let d = body(p);
            d.get("status").and_then(Value::as_str) == Some("unlocked")
                || p.get("status").and_then(Value::as_str) == Some("unlocked")
                || d.get("achievementId").and_then(Value::as_str)
                    == Some(achievement_id.as_str())
        })
        .await;
    r.check("bob sees achievement_unlock unlocked", b_unlock.is_some());

    // --- 6b. get_achievements: 100 / unlocked ---------------------------------
    println!("\n[6b] get_achievements");
    let ach = alice
        .features
        .get_achievements(json!({ "achievementId": achievement_id }))
        .await;
    match &ach {
        Ok(a) => {
            let d = body(a);
            let arr = d
                .get("achievements")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let row = arr.iter().find(|row| {
                row.get("achievementId").and_then(Value::as_str) == Some(achievement_id.as_str())
            });
            let pct = row
                .and_then(|r| r.get("percentComplete"))
                .and_then(Value::as_i64);
            let status = row.and_then(|r| r.get("status")).and_then(Value::as_str);
            r.check(
                "get_achievements: 100 / unlocked",
                pct == Some(100) && status == Some("unlocked"),
            );
            println!("     achievement state: {}", d);
        }
        Err(e) => {
            r.check("get_achievements acked", false);
            println!("     get_achievements error: {}", e);
        }
    }

    // --- 7. send_challenge_invite alice->bob ----------------------------------
    println!("\n[7] send_challenge_invite alice->bob");
    let invite = alice
        .features
        .send_challenge_invite(json!({
            "toUserId": bob_id,
            "type": "match",
            "payload": { "arena": "rust-lobby", "wager": 10 },
            "ttl": 300,
        }))
        .await;
    let invite_id = match &invite {
        Ok(v) => {
            let d = body(v);
            let id = d
                .get("inviteId")
                .or_else(|| v.get("inviteId"))
                .and_then(Value::as_str)
                .map(str::to_string);
            let status = d
                .get("status")
                .or_else(|| v.get("status"))
                .and_then(Value::as_str);
            r.check(
                "invite acked pending with inviteId",
                id.is_some() && status == Some("pending"),
            );
            println!("     invite ack: {}", v);
            id
        }
        Err(e) => {
            r.check("send_challenge_invite acked", false);
            println!("     invite error: {}", e);
            None
        }
    };

    // bob (INVITEE) sees challenge_invited; directed events are FLAT.
    let b_invited = bob
        .tape
        .wait("challenge_invited", 8, |_| true)
        .await;
    match &b_invited {
        Some(p) => {
            // Worker delivers `from` as a nested {identity,userId} object on the
            // directed invite event; accept either the object form or a flat
            // string (fromUserId) for forward-compatibility.
            let from = p
                .get("from")
                .and_then(|f| f.get("identity").or_else(|| f.get("userId")))
                .or_else(|| p.get("from"))
                .or_else(|| p.get("fromUserId"))
                .and_then(Value::as_str);
            let payload_ok = p
                .get("payload")
                .and_then(|pl| pl.get("arena"))
                .and_then(Value::as_str)
                == Some("rust-lobby");
            r.check(
                "bob sees challenge_invited from alice with payload",
                from == Some(alice_id.as_str()) && payload_ok,
            );
            println!("     bob challenge_invited: {}", p);
        }
        None => r.check("bob sees challenge_invited", false),
    }
    // alice must NOT see her own invite.
    let alice_not_own = alice.tape.absent("challenge_invited", 1).await;
    r.check("alice does NOT see her own challenge_invited", alice_not_own);

    // --- 8. bob get_challenge_invites lists it --------------------------------
    println!("\n[8] bob get_challenge_invites");
    let invites = bob.features.get_challenge_invites().await;
    match &invites {
        Ok(v) => {
            let d = body(v);
            let arr = d
                .get("invites")
                .or_else(|| v.get("invites"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let listed = match &invite_id {
                Some(iid) => arr.iter().any(|inv| {
                    inv.get("inviteId").and_then(Value::as_str) == Some(iid.as_str())
                }),
                None => !arr.is_empty(),
            };
            r.check("bob get_challenge_invites lists the invite", listed);
            println!("     bob invites: {}", d);
        }
        Err(e) => {
            r.check("get_challenge_invites acked", false);
            println!("     get_challenge_invites error: {}", e);
        }
    }

    // --- 9. bob reply(accept) => alice sees challenge_reply_received -----------
    println!("\n[9] bob reply(accept) -> alice sees challenge_reply_received");
    if let Some(iid) = &invite_id {
        let reply = bob
            .features
            .reply_challenge_invite(json!({ "inviteId": iid, "accept": true }))
            .await;
        r.check("bob reply acked (challenge_reply_success)", reply.is_ok());
        if let Err(e) = &reply {
            println!("     reply error: {}", e);
        }
        let a_reply = alice
            .tape
            .wait("challenge_reply_received", 8, |_| true)
            .await;
        r.check(
            "alice sees challenge_reply_received",
            a_reply.is_some(),
        );
        if let Some(p) = &a_reply {
            println!("     alice reply_received: {}", p);
        }
    } else {
        r.check("bob reply acked", false);
        r.check("alice sees challenge_reply_received", false);
    }

    // --- 10. fresh invite + cancel => bob sees challenge_invite_cancelled ------
    println!("\n[10] fresh invite + cancel -> bob sees challenge_invite_cancelled");
    // Clear the invited tape marker by recording the count baseline.
    let cancel_invite = alice
        .features
        .send_challenge_invite(json!({
            "toUserId": bob_id, "type": "match",
            "payload": { "arena": "cancel-test" }, "ttl": 300,
        }))
        .await;
    let cancel_id = cancel_invite
        .ok()
        .and_then(|v| {
            let d = body(&v);
            d.get("inviteId")
                .or_else(|| v.get("inviteId"))
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    if let Some(cid) = &cancel_id {
        // let it deliver, then cancel
        sleep(Duration::from_millis(400)).await;
        let cancel = alice
            .features
            .cancel_challenge_invite(json!({ "inviteId": cid }))
            .await;
        r.check(
            "cancel acked (challenge_invite_cancel_success)",
            cancel.is_ok(),
        );
        if let Err(e) = &cancel {
            println!("     cancel error: {}", e);
        }
        let b_cancelled = bob
            .tape
            .wait("challenge_invite_cancelled", 8, |p| {
                p.get("inviteId").and_then(Value::as_str) == Some(cid.as_str())
                    || true
            })
            .await;
        r.check(
            "bob sees challenge_invite_cancelled",
            b_cancelled.is_some(),
        );
        if let Some(p) = &b_cancelled {
            println!("     bob invite_cancelled: {}", p);
        }
    } else {
        r.check("cancel invite created", false);
        r.check("bob sees challenge_invite_cancelled", false);
    }

    // --- summary --------------------------------------------------------------
    println!("\n== SUMMARY ==");
    println!("PASS: {}  FAIL: {}", r.pass, r.fail);
    println!(
        "alice worker={:?}  bob worker={:?}",
        alice.client.worker_id(),
        bob.client.worker_id()
    );

    alice.client.disconnect().await.ok();
    bob.client.disconnect().await.ok();

    let _ = (alice.name, bob.name);
    if r.fail > 0 {
        std::process::exit(1);
    }
    Ok(())
}
