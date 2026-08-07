//! OddSockets client: real-time transport over Engine.IO v4 / Socket.IO.
//!
//! The client speaks genuine Socket.IO to the assigned worker exactly like the
//! JavaScript and Python SDKs: it performs Manager -> Worker discovery over HTTP,
//! opens a WebSocket to `/socket.io/?EIO=4&transport=websocket`, completes the
//! Engine.IO handshake, sends the Socket.IO CONNECT packet with `{apiKey, userId}`
//! in `handshake.auth`, answers server PINGs, and frames application events as
//! `42["event", payload]`. No simulation, no local echo.

use crate::channel::OddSocketsChannel;
use crate::error::{OddSocketsError, Result};
use crate::types::{
    utils, BulkMessage, BulkResult, ConnectionState, Message, OddSocketsConfig,
};
use futures_util::stream::{SplitSink, StreamExt};
use futures_util::SinkExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, oneshot, Mutex as AsyncMutex};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Writer = SplitSink<WsStream, WsMessage>;

/// A callback registered via [`OddSocketsClient::on`].
type RawListener = Arc<dyn Fn(Value) + Send + Sync>;

/// Shared, reference-counted client state.
struct Inner {
    config: OddSocketsConfig,
    client_identifier: String,
    state: Mutex<ConnectionState>,
    writer: AsyncMutex<Option<Writer>>,
    /// Correlated request/response waiters keyed `"responseEvent:channel"`.
    pending: Mutex<HashMap<String, oneshot::Sender<Result<Value>>>>,
    /// One-shot event waiters (enhanced request/response) keyed by event name.
    once_waiters: Mutex<HashMap<String, Vec<oneshot::Sender<Value>>>>,
    /// Persistent raw event listeners keyed by event name.
    listeners: Mutex<HashMap<String, Vec<RawListener>>>,
    /// Per-channel broadcast fan-out for delivered `message` events.
    channels: Mutex<HashMap<String, broadcast::Sender<Message>>>,
    /// Completes once the Socket.IO CONNECT is acknowledged.
    connect_signal: Mutex<Option<oneshot::Sender<Result<()>>>>,
    worker_url: Mutex<Option<String>>,
    worker_id: Mutex<Option<String>>,
}

/// The main OddSockets client.
///
/// Cheap to clone: every clone shares the same underlying connection and state,
/// so a clone handed to [`crate::EnhancedFeatures`] operates on the same socket.
#[derive(Clone)]
pub struct OddSocketsClient {
    inner: Arc<Inner>,
}

impl OddSocketsClient {
    /// Creates a new client from the given configuration.
    pub async fn new(config: OddSocketsConfig) -> Result<Self> {
        config.validate()?;
        let client_identifier = config
            .user_id
            .clone()
            .unwrap_or_else(utils::generate_user_id);

        Ok(Self {
            inner: Arc::new(Inner {
                config,
                client_identifier,
                state: Mutex::new(ConnectionState::Disconnected),
                writer: AsyncMutex::new(None),
                pending: Mutex::new(HashMap::new()),
                once_waiters: Mutex::new(HashMap::new()),
                listeners: Mutex::new(HashMap::new()),
                channels: Mutex::new(HashMap::new()),
                connect_signal: Mutex::new(None),
                worker_url: Mutex::new(None),
                worker_id: Mutex::new(None),
            }),
        })
    }

    /// Returns the current connection state.
    pub fn get_state(&self) -> ConnectionState {
        *self.inner.state.lock().unwrap()
    }

    /// Returns true if the client is connected and ready.
    pub fn is_connected(&self) -> bool {
        self.get_state().is_connected()
    }

    /// Returns the assigned worker id, if connected.
    pub fn worker_id(&self) -> Option<String> {
        self.inner.worker_id.lock().unwrap().clone()
    }

    /// The user id this client authenticates as.
    pub fn user_id(&self) -> String {
        self.inner
            .config
            .user_id
            .clone()
            .unwrap_or_else(|| self.inner.client_identifier.clone())
    }

    /// Connects to OddSockets: discovers a worker, opens the WebSocket, completes
    /// the Engine.IO / Socket.IO handshake and authenticates.
    pub async fn connect(&self) -> Result<()> {
        {
            let state = *self.inner.state.lock().unwrap();
            if state.is_connected() || state == ConnectionState::Connecting {
                return Ok(());
            }
        }
        *self.inner.state.lock().unwrap() = ConnectionState::Connecting;

        let assignment = self.get_worker_assignment().await;
        let worker_url = match assignment {
            Ok(url) => url,
            Err(e) => {
                *self.inner.state.lock().unwrap() = ConnectionState::Disconnected;
                return Err(e);
            }
        };

        let ws_url = build_ws_url(&worker_url)?;
        let (ws_stream, _resp) = match connect_async(&ws_url).await {
            Ok(pair) => pair,
            Err(e) => {
                *self.inner.state.lock().unwrap() = ConnectionState::Disconnected;
                return Err(OddSocketsError::ConnectionFailed {
                    message: format!("WebSocket connect to worker failed: {}", e),
                });
            }
        };

        let (writer, reader) = ws_stream.split();
        *self.inner.writer.lock().await = Some(writer);

        // Arm the connect signal before the reader can complete it.
        let (tx, rx) = oneshot::channel();
        *self.inner.connect_signal.lock().unwrap() = Some(tx);

        // Spawn the read pump.
        let inner = self.inner.clone();
        tokio::spawn(async move {
            read_loop(inner, reader).await;
        });

        let timeout_dur = self.inner.config.timeout.max(Duration::from_secs(15));
        match timeout(timeout_dur, rx).await {
            Ok(Ok(Ok(()))) => {
                *self.inner.state.lock().unwrap() = ConnectionState::Connected;
                Ok(())
            }
            Ok(Ok(Err(e))) => {
                *self.inner.state.lock().unwrap() = ConnectionState::Disconnected;
                Err(e)
            }
            Ok(Err(_)) | Err(_) => {
                *self.inner.state.lock().unwrap() = ConnectionState::Disconnected;
                Err(OddSocketsError::ConnectionFailed {
                    message: "Timed out waiting for Socket.IO handshake".to_string(),
                })
            }
        }
    }

    /// Disconnects from OddSockets and tears down the socket.
    pub async fn disconnect(&self) -> Result<()> {
        *self.inner.state.lock().unwrap() = ConnectionState::Disconnected;
        // Best-effort Socket.IO + Engine.IO close, then drop the writer.
        if let Some(mut writer) = self.inner.writer.lock().await.take() {
            let _ = writer.send(WsMessage::Text("41".to_string())).await;
            let _ = writer.send(WsMessage::Close(None)).await;
            let _ = writer.flush().await;
        }
        // Fail any outstanding waiters so callers unblock.
        let drained: Vec<_> = self.inner.pending.lock().unwrap().drain().collect();
        for (_, tx) in drained {
            let _ = tx.send(Err(OddSocketsError::ConnectionFailed {
                message: "Client disconnected".to_string(),
            }));
        }
        self.inner.once_waiters.lock().unwrap().clear();
        Ok(())
    }

    /// Returns a channel handle for the given name.
    pub fn channel(&self, name: impl Into<String>) -> OddSocketsChannel {
        OddSocketsChannel::new(name.into(), self.clone())
    }

    /// Publishes a batch of messages, one request per message.
    pub async fn publish_bulk(&self, messages: Vec<BulkMessage>) -> Result<Vec<BulkResult>> {
        let mut results = Vec::with_capacity(messages.len());
        for bulk in messages {
            let channel = self.channel(&bulk.channel);
            let options = bulk.options.unwrap_or_default();
            match channel.publish(bulk.message, options).await {
                Ok(result) => results.push(BulkResult::success(result)),
                Err(e) => results.push(BulkResult::failure(e.to_string())),
            }
        }
        Ok(results)
    }

    /// Registers a persistent listener for a raw transport event.
    ///
    /// This is the public surface enhanced broadcasts (`user_typing`,
    /// `reaction_added`, ...) are delivered on.
    pub fn on<F>(&self, event: impl Into<String>, handler: F)
    where
        F: Fn(Value) + Send + Sync + 'static,
    {
        self.inner
            .listeners
            .lock()
            .unwrap()
            .entry(event.into())
            .or_default()
            .push(Arc::new(handler));
    }

    /// Emits a raw Socket.IO event to the worker (`42["event", payload]`).
    pub async fn emit(&self, event: &str, payload: Value) -> Result<()> {
        if !self.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }
        let frame = format!(
            "42{}",
            serde_json::to_string(&json!([event, prune_nulls(payload)]))?
        );
        self.send_frame(frame).await
    }

    /// Waits for the next occurrence of a raw event and returns its payload.
    /// Used by enhanced request/response helpers.
    pub async fn wait_for_event(&self, event: &str) -> Result<Value> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .once_waiters
            .lock()
            .unwrap()
            .entry(event.to_string())
            .or_default()
            .push(tx);
        rx.await.map_err(|_| OddSocketsError::ConnectionFailed {
            message: format!("Waiter for '{}' cancelled", event),
        })
    }

    // ---- internal request/response over the socket ------------------------

    /// Emits `event` and awaits the correlated `response_event` for `channel`.
    pub(crate) async fn request(
        &self,
        event: &str,
        payload: Value,
        response_event: &str,
        channel: &str,
        timeout_secs: u64,
    ) -> Result<Value> {
        if !self.is_connected() {
            return Err(OddSocketsError::NotConnected);
        }
        let key = format!("{}:{}", response_event, channel);
        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().unwrap().insert(key.clone(), tx);

        let frame = format!(
            "42{}",
            serde_json::to_string(&json!([event, prune_nulls(payload)]))?
        );
        if let Err(e) = self.send_frame(frame).await {
            self.inner.pending.lock().unwrap().remove(&key);
            return Err(e);
        }

        match timeout(Duration::from_secs(timeout_secs), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(OddSocketsError::ConnectionFailed {
                message: format!("Waiter for '{}' cancelled", response_event),
            }),
            Err(_) => {
                self.inner.pending.lock().unwrap().remove(&key);
                Err(OddSocketsError::OperationTimeout {
                    operation: event.to_string(),
                    timeout_secs,
                })
            }
        }
    }

    /// Registers (or reuses) the broadcast sender for a channel and returns a
    /// fresh receiver for delivered messages.
    pub(crate) fn channel_receiver(&self, channel: &str) -> broadcast::Receiver<Message> {
        let mut channels = self.inner.channels.lock().unwrap();
        let sender = channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(256).0);
        sender.subscribe()
    }

    pub(crate) fn drop_channel(&self, channel: &str) {
        self.inner.channels.lock().unwrap().remove(channel);
    }

    async fn send_frame(&self, frame: String) -> Result<()> {
        let mut guard = self.inner.writer.lock().await;
        let writer = guard.as_mut().ok_or(OddSocketsError::NotConnected)?;
        writer
            .send(WsMessage::Text(frame))
            .await
            .map_err(OddSocketsError::from)
    }

    async fn get_worker_assignment(&self) -> Result<String> {
        let manager_url = crate::manager_discovery::ManagerDiscovery::new(Some(
            self.inner.config.manager_url.as_str(),
        ))?
        .discover_manager_url()
        .await?;

        let http = reqwest::Client::new();
        let resp = http
            .get(format!("{}/api/cluster/select-worker", manager_url))
            .query(&[
                ("apiKey", self.inner.config.api_key.as_str()),
                ("userId", self.user_id().as_str()),
                ("clientIdentifier", self.inner.client_identifier.as_str()),
            ])
            .header("User-Agent", crate::types::constants::USER_AGENT)
            .timeout(self.inner.config.timeout)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(OddSocketsError::WorkerAssignmentFailed {
                message: format!("Manager returned HTTP {}", resp.status().as_u16()),
            });
        }

        let body: Value = resp.json().await?;
        let url = body
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| OddSocketsError::WorkerAssignmentFailed {
                message: "Invalid worker assignment response (no url)".to_string(),
            })?
            .to_string();

        if let Some(id) = body.get("workerId").and_then(Value::as_str) {
            *self.inner.worker_id.lock().unwrap() = Some(id.to_string());
        }
        *self.inner.worker_url.lock().unwrap() = Some(url.clone());
        Ok(url)
    }
}

// ---- read pump -----------------------------------------------------------

async fn read_loop(inner: Arc<Inner>, mut reader: futures_util::stream::SplitStream<WsStream>) {
    while let Some(frame) = reader.next().await {
        let text = match frame {
            Ok(WsMessage::Text(t)) => t,
            Ok(WsMessage::Binary(_)) | Ok(WsMessage::Ping(_)) | Ok(WsMessage::Pong(_)) => continue,
            Ok(WsMessage::Close(_)) | Err(_) => break,
            Ok(_) => continue,
        };
        if text.is_empty() {
            continue;
        }

        let engine_type = text.as_bytes()[0];
        match engine_type {
            b'0' => {
                // Engine.IO OPEN -> send Socket.IO CONNECT with auth.
                let auth = json!({
                    "apiKey": inner.config.api_key,
                    "userId": inner.config.user_id,
                });
                let connect_frame = format!("40{}", auth);
                send_raw(&inner, connect_frame).await;
            }
            b'2' => {
                // Engine.IO PING -> PONG.
                send_raw(&inner, "3".to_string()).await;
            }
            b'4' => {
                // Engine.IO MESSAGE -> Socket.IO packet.
                handle_socketio(&inner, &text[1..]).await;
            }
            b'1' => break, // Engine.IO CLOSE
            _ => {}
        }
    }

    *inner.state.lock().unwrap() = ConnectionState::Disconnected;
    // Unblock any pending connect waiter.
    if let Some(tx) = inner.connect_signal.lock().unwrap().take() {
        let _ = tx.send(Err(OddSocketsError::ConnectionFailed {
            message: "Socket closed before handshake completed".to_string(),
        }));
    }
}

async fn handle_socketio(inner: &Arc<Inner>, body: &str) {
    if body.is_empty() {
        return;
    }
    let sio_type = body.as_bytes()[0];
    match sio_type {
        b'0' => {
            // Socket.IO CONNECT acknowledged.
            if let Some(tx) = inner.connect_signal.lock().unwrap().take() {
                let _ = tx.send(Ok(()));
            }
        }
        b'2' => {
            // EVENT: strip optional numeric ack id, parse the JSON array.
            let json_part = body[1..].trim_start_matches(|c: char| c.is_ascii_digit());
            if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(json_part) {
                let event = arr.first().and_then(Value::as_str).unwrap_or("").to_string();
                let payload = arr.get(1).cloned().unwrap_or(Value::Null);
                dispatch(inner, &event, payload);
            }
        }
        b'4' => {
            // CONNECT_ERROR.
            if let Some(tx) = inner.connect_signal.lock().unwrap().take() {
                let _ = tx.send(Err(OddSocketsError::AuthenticationFailed {
                    message: format!("Socket.IO connect error: {}", &body[1..]),
                }));
            }
        }
        _ => {}
    }
}

fn dispatch(inner: &Arc<Inner>, event: &str, payload: Value) {
    let channel = payload
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    // 1. Correlated request/response.
    //
    // The worker emits "history" both as the explicit get_history RESPONSE
    // (query:true) and as a fire-and-forget on-join snapshot (~10 msgs, no query
    // flag). Only the query:true response may satisfy a pending get_history
    // waiter; ignore the snapshot here so it can't resolve get_history with the
    // wrong data. BUG-2026-0727-0012.
    let is_history_snapshot =
        event == "history" && payload.get("query").and_then(Value::as_bool) != Some(true);
    if !is_history_snapshot {
        let key = format!("{}:{}", event, channel);
        if let Some(tx) = inner.pending.lock().unwrap().remove(&key) {
            let _ = tx.send(Ok(payload.clone()));
        }
    }

    // 2. Delivered messages -> per-channel broadcast fan-out.
    if event == "message" {
        if let Some(sender) = inner.channels.lock().unwrap().get(&channel) {
            let _ = sender.send(adapt_message(&payload));
        }
    }

    // 3. Worker errors fail all outstanding correlated waiters.
    if event == "error" {
        let msg = payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Worker error")
            .to_string();
        let drained: Vec<_> = inner.pending.lock().unwrap().drain().collect();
        for (_, tx) in drained {
            let _ = tx.send(Err(OddSocketsError::MessageDeliveryFailed {
                message: msg.clone(),
                message_id: None,
                channel: None,
            }));
        }
    }

    // 4. One-shot event waiters (enhanced request/response).
    let waiters = inner.once_waiters.lock().unwrap().remove(event);
    if let Some(waiters) = waiters {
        for tx in waiters {
            let _ = tx.send(payload.clone());
        }
    }

    // 5. Persistent listeners (enhanced broadcasts, raw surface).
    let listeners = inner
        .listeners
        .lock()
        .unwrap()
        .get(event)
        .cloned()
        .unwrap_or_default();
    for listener in listeners {
        listener(payload.clone());
    }
}

async fn send_raw(inner: &Arc<Inner>, frame: String) {
    let mut guard = inner.writer.lock().await;
    if let Some(writer) = guard.as_mut() {
        let _ = writer.send(WsMessage::Text(frame)).await;
    }
}

// ---- helpers -------------------------------------------------------------

/// Builds the Engine.IO WebSocket URL from an `http(s)` worker URL.
fn build_ws_url(worker_url: &str) -> Result<String> {
    let base = if let Some(rest) = worker_url.strip_prefix("https://") {
        format!("wss://{}", rest)
    } else if let Some(rest) = worker_url.strip_prefix("http://") {
        format!("ws://{}", rest)
    } else if worker_url.starts_with("ws://") || worker_url.starts_with("wss://") {
        worker_url.to_string()
    } else {
        return Err(OddSocketsError::InvalidConfiguration {
            message: format!("Unsupported worker URL scheme: {}", worker_url),
        });
    };
    let base = base.trim_end_matches('/');
    Ok(format!("{}/socket.io/?EIO=4&transport=websocket", base))
}

/// Recursively removes null-valued keys so the worker's `options = {}` default
/// (which only triggers on `undefined`, not JSON `null`) is not defeated.
fn prune_nulls(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k, prune_nulls(v)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(prune_nulls).collect()),
        other => other,
    }
}

/// Adapts a worker `message` envelope into a strongly-typed [`Message`].
pub(crate) fn adapt_message(payload: &Value) -> Message {
    let channel = payload
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let data = payload
        .get("message")
        .or_else(|| payload.get("data"))
        .cloned()
        .unwrap_or_else(|| payload.clone());
    let user_id = payload
        .get("publisher")
        .and_then(|p| p.get("userId"))
        .and_then(Value::as_str)
        .or_else(|| payload.get("userId").and_then(Value::as_str))
        .map(str::to_string);
    let id = payload
        .get("messageId")
        .or_else(|| payload.get("id"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(utils::generate_message_id);

    Message {
        id,
        channel,
        data,
        timestamp: chrono::Utc::now(),
        user_id,
        metadata: None,
    }
}
