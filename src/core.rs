use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{self, Duration};

use crate::backend;
use crate::frontend::tcp::{ClientCommand, ClientDirectMap, ClientId, TcpFrontend};
use crate::node::state::{CachedChannel, CachedContact, NodeState};
use crate::protocol::decode::{
    DecodedValue, decode_command_payload, decode_response_payload, format_decoded,
};

const IN_FLIGHT_TIMEOUT: Duration = Duration::from_secs(5);
const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// Maximum age of a pending_acks entry before it is considered abandoned.
/// LoRa retransmissions inside the radio top out around ~30 s; five minutes
/// is comfortably above any realistic real ACK and short enough to bound
/// memory growth from channel sends (which never produce an ACK).
const PENDING_ACK_TTL: Duration = Duration::from_secs(300);

/// Response codes that are spontaneous push events from the radio and never
/// complete an in-flight request. They are always broadcast to every client.
/// ACK (0x82) is also a push from the in_flight slot's point of view but
/// has its own dedicated routing via `pending_acks`.
const PUSH_EVENT_CODES: &[u8] = &[
    0x80, // ADVERTISEMENT
    0x81, // PATH_UPDATE
    0x83, // MESSAGES_WAITING
    0x88, // LOG_DATA
    0x8A, // NEW_ADVERT
];

fn is_push_event(code: u8) -> bool {
    PUSH_EVENT_CODES.contains(&code)
}

/// What a request is waiting for, to know when to free the slot.
#[derive(Debug, Clone)]
enum Awaiting {
    /// Any single non-push response frame completes the request. This is
    /// the default for most commands: the radio replies with exactly one
    /// frame per command (OK / ERROR / DEVICE_INFO / CHANNEL_INFO / ...).
    Any,
    /// A multi-frame stream: any frame whose code is in `frame_codes`
    /// belongs to the response; the request completes with `terminator_code`.
    Stream {
        frame_codes: Vec<u8>,
        terminator_code: u8,
    },
}

#[derive(Debug)]
struct InFlight {
    client_id: ClientId,
    cmd_code: u8,
    sent_at: Instant,
    awaiting: Awaiting,
}

#[derive(Debug)]
struct Queued {
    client_id: ClientId,
    payload: Vec<u8>,
}

/// Map a client command code to the shape of the radio's reply.
fn awaiting_for(cmd_code: u8) -> Awaiting {
    match cmd_code {
        // GET_CONTACTS → CONTACT_START + N x CONTACT + CONTACT_END
        0x04 => Awaiting::Stream {
            frame_codes: vec![0x02, 0x03],
            terminator_code: 0x04,
        },
        // Every other command replies with a single non-push frame.
        // Routing is decided by "not a push event" rather than by an
        // explicit allow-list so unusual commands (SHARE_CONTACT,
        // GET_TELEMETRY, GET_STATS, ...) work without per-command entries.
        _ => Awaiting::Any,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    Serial,
    Ble,
    Tcp,
}

pub struct CoreConfig {
    pub backend_type: BackendType,
    pub serial_port: Option<String>,
    pub serial_baud: u32,
    pub ble_address: Option<String>,
    #[cfg_attr(not(feature = "ble"), allow(dead_code))]
    pub ble_pin: String,
    pub tcp_backend_host: Option<String>,
    pub tcp_backend_port: u16,
    pub tcp_frontend_host: String,
    pub tcp_frontend_port: u16,
    pub data_dir: PathBuf,
    pub event_log_level: LogLevel,
    pub event_log_json: bool,
    pub record_radio_rx: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Off,
    Summary,
    Verbose,
}

pub struct Core {
    config: CoreConfig,
    state: Arc<NodeState>,
    self_info_raw: Option<Vec<u8>>,
    device_info_raw: Option<Vec<u8>>,
    battery_info_raw: Option<Vec<u8>>,
    radio_pubkey: Option<[u8; 32]>,
    radio_name: Option<String>,
    max_channels: u8,
    radio_send_tx: Option<mpsc::UnboundedSender<Vec<u8>>>,
    radio_recv_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    shutdown_rx: watch::Receiver<bool>,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    command_rx: mpsc::UnboundedReceiver<ClientCommand>,
    broadcast_tx: broadcast::Sender<Vec<u8>>,
    client_channels: ClientDirectMap,
    // The radio handles one command at a time. We serialize: a single
    // request is in flight; further client commands wait in pending_queue.
    in_flight: Option<InFlight>,
    pending_queue: VecDeque<Queued>,
    // Clients waiting for the (possibly much later) LoRa-delivery ACK
    // (0x82), keyed by the 4-byte expected_ack the radio handed back in
    // the corresponding MSG_SENT. The Instant is the insertion time,
    // used to evict stale entries when a new ack is recorded. Channel
    // sends (0x03) never produce an ACK, so without eviction those
    // entries would accumulate forever.
    pending_acks: HashMap<[u8; 4], (ClientId, Instant)>,
}

impl Core {
    pub async fn new(config: CoreConfig, shutdown_rx: watch::Receiver<bool>) -> Result<Self> {
        let state = NodeState::open(config.data_dir.join("state.db")).await?;

        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(256);

        let (_, dummy_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        Ok(Self {
            config,
            state: Arc::new(state),
            self_info_raw: None,
            device_info_raw: None,
            battery_info_raw: None,
            radio_pubkey: None,
            radio_name: None,
            max_channels: 40,
            radio_send_tx: None,
            radio_recv_rx: dummy_rx,
            shutdown_rx,
            command_tx,
            command_rx,
            broadcast_tx,
            client_channels: Arc::new(Mutex::new(HashMap::new())),
            in_flight: None,
            pending_queue: VecDeque::new(),
            pending_acks: HashMap::new(),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        tracing::info!("starting coresplitter (MeshCore client multiplexer + cache)");

        if let Err(e) = self.connect_backend().await {
            tracing::warn!(error = %e, "initial radio connection failed, will retry");
            self.reconnect_radio().await;
        } else if let Err(e) = self.initialize_radio().await {
            tracing::warn!(error = %e, "initial radio initialization failed, will retry");
            self.radio_send_tx = None;
            self.reconnect_radio().await;
        }

        // Sync state from the physical radio (contacts, channels, etc.)
        self.sync_from_radio().await;

        // Load persisted blobs from kv_store if not already set (radio offline fallback)
        if self.self_info_raw.is_none()
            && let Ok(Some(blob)) = self.state.kv_get("self_info_raw").await
        {
            tracing::info!("restored SELF_INFO blob from kv_store");
            self.extract_radio_identity(&blob);
            self.self_info_raw = Some(blob);
        }
        if self.device_info_raw.is_none()
            && let Ok(Some(blob)) = self.state.kv_get("device_info_raw").await
        {
            tracing::info!("restored DEVICE_INFO blob from kv_store");
            self.extract_max_channels(&blob);
            self.device_info_raw = Some(blob);
        }
        if self.battery_info_raw.is_none()
            && let Ok(Some(blob)) = self.state.kv_get("battery_info_raw").await
        {
            tracing::info!("restored BATTERY blob from kv_store");
            self.battery_info_raw = Some(blob);
        }

        let frontend_addr = format!(
            "{}:{}",
            self.config.tcp_frontend_host, self.config.tcp_frontend_port
        );
        let shutdown_rx = self.shutdown_rx.clone();
        let mut frontend = TcpFrontend::new(
            frontend_addr,
            self.command_tx.clone(),
            self.broadcast_tx.clone(),
            self.client_channels.clone(),
            shutdown_rx,
        );
        tokio::spawn(async move {
            if let Err(e) = frontend.run().await {
                tracing::error!(error = %e, "TCP frontend error");
            }
        });

        tracing::info!("multiplexer ready, accepting clients");

        let mut timeout_tick = tokio::time::interval(TIMEOUT_CHECK_INTERVAL);
        timeout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;

                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("shutdown signal received, stopping");
                        break;
                    }
                }
                Some(cmd) = self.command_rx.recv() => {
                    self.handle_client_command(cmd).await;
                }
                data = self.radio_recv_rx.recv() => {
                    match data {
                        Some(payload) => self.handle_radio_rx(payload).await,
                        None => {
                            tracing::warn!("radio connection lost");
                            self.reconnect_radio().await;
                        }
                    }
                }
                _ = timeout_tick.tick() => {
                    self.check_in_flight_timeout().await;
                }
            }
        }

        tracing::info!("performing graceful shutdown");
        self.shutdown().await;

        Ok(())
    }

    async fn shutdown(&mut self) {
        tracing::info!("shutting down multiplexer");

        self.radio_send_tx = None;
        let (_, dummy_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        self.radio_recv_rx = dummy_rx;

        tracing::info!("shutdown complete");
    }

    async fn connect_backend(&mut self) -> Result<()> {
        let radio = match self.config.backend_type {
            BackendType::Serial => {
                let port = self
                    .config
                    .serial_port
                    .as_ref()
                    .context("serial port not specified")?;
                tracing::info!(port = %port, baud = self.config.serial_baud, "connecting to radio via serial");
                backend::serial::connect(port, self.config.serial_baud).await?
            }
            BackendType::Ble => {
                let addr = self
                    .config
                    .ble_address
                    .as_ref()
                    .context("BLE address not specified")?;
                tracing::info!(addr = %addr, "connecting to radio via BLE");
                #[cfg(not(feature = "ble"))]
                anyhow::bail!("BLE support not compiled (rebuild with --features ble)");
                #[cfg(feature = "ble")]
                backend::ble::connect(addr, &self.config.ble_pin).await?
            }
            BackendType::Tcp => {
                let host = self
                    .config
                    .tcp_backend_host
                    .as_ref()
                    .context("TCP backend host not specified")?;
                let port = self.config.tcp_backend_port;
                tracing::info!(host = %host, port = %port, "connecting to radio via TCP");
                backend::tcp::connect(host, port).await?
            }
        };

        let backend::RadioIo {
            send_tx,
            mut recv_rx,
            disconnect_rx,
            ..
        } = radio;

        let (publish_tx, new_recv_rx) = mpsc::unbounded_channel();
        self.radio_recv_rx = new_recv_rx;
        self.radio_send_tx = Some(send_tx);

        tokio::spawn(async move {
            let mut disconnect_rx = disconnect_rx;

            loop {
                tokio::select! {
                    data = recv_rx.recv() => {
                        match data {
                            Some(bytes) => {
                                if publish_tx.send(bytes).is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = &mut disconnect_rx => {
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn reconnect_radio(&mut self) {
        self.radio_send_tx = None;

        let mut delay = Duration::from_secs(1);
        let max_delay = Duration::from_secs(60);

        tracing::warn!("starting radio reconnection loop");

        loop {
            tokio::select! {
                biased;
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("shutdown during reconnection, aborting");
                        let (_, dummy_rx) = mpsc::unbounded_channel::<Vec<u8>>();
                        self.radio_recv_rx = dummy_rx;
                        return;
                    }
                }
                _ = time::sleep(delay) => {}
            }

            tracing::info!(
                delay_secs = delay.as_secs(),
                "attempting radio reconnection"
            );

            match self.connect_backend().await {
                Ok(()) => {
                    tracing::info!("radio reconnected");
                    match self.initialize_radio().await {
                        Ok(()) => {
                            tracing::info!("radio re-initialized successfully");
                            self.sync_from_radio().await;
                            return;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "re-initialization failed, will retry");
                            self.radio_send_tx = None;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "reconnection failed");
                }
            }

            delay = (delay * 2).min(max_delay);
        }
    }

    async fn initialize_radio(&mut self) -> Result<()> {
        let appstart = build_appstart();
        self.send_to_radio(&appstart).await?;

        // Wait for SELF_INFO (0x05) from the radio
        self.wait_for_self_info().await?;

        // Query device info to learn radio capabilities (max_channels, etc.)
        let device_query = vec![0x16];
        self.send_to_radio(&device_query).await?;

        self.wait_for_device_info().await?;

        Ok(())
    }

    async fn wait_for_self_info(&mut self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                anyhow::bail!("timeout waiting for radio SELF_INFO");
            }
            tokio::select! {
                biased;
                _ = self.shutdown_rx.changed() => {
                    anyhow::bail!("shutdown during radio initialization");
                }
                data = self.radio_recv_rx.recv() => {
                    match data {
                        Some(payload) if !payload.is_empty() && payload[0] == 0x05 => {
                            self.cache_response(0x05, &payload).await;
                            if self.self_info_raw.is_some() {
                                tracing::info!(
                                    name = self.radio_name.as_deref().unwrap_or("?"),
                                    pubkey = %self.radio_pubkey
                                        .as_ref()
                                        .map(|pk| hex::encode(&pk[..8]))
                                        .unwrap_or_default(),
                                    "radio initialized, received SELF_INFO"
                                );
                                return Ok(());
                            } else {
                                anyhow::bail!("failed to cache SELF_INFO");
                            }
                        }
                        Some(payload) => {
                            tracing::warn!(code = payload[0], "discarding unexpected response while waiting for SELF_INFO");
                            continue;
                        }
                        None => {
                            anyhow::bail!("radio disconnected during initialization");
                        }
                    }
                }
            }
        }
    }

    async fn wait_for_device_info(&mut self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                tracing::warn!(
                    "timeout waiting for DEVICE_INFO, using max_channels={}",
                    self.max_channels
                );
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = self.shutdown_rx.changed() => {
                    anyhow::bail!("shutdown during device query");
                }
                data = self.radio_recv_rx.recv() => {
                    match data {
                        Some(payload) if !payload.is_empty() && payload[0] == 0x0D => {
                            self.cache_response(0x0D, &payload).await;
                            return Ok(());
                        }
                        Some(payload) if !payload.is_empty() && payload[0] == 0x01 => {
                            tracing::warn!("radio returned error for DEVICE_QUERY, using default max_channels={}", self.max_channels);
                            return Ok(());
                        }
                        Some(payload) => {
                            tracing::warn!(code = payload[0], "discarding unexpected response while waiting for DEVICE_INFO");
                            continue;
                        }
                        None => {
                            anyhow::bail!("radio disconnected during device query");
                        }
                    }
                }
            }
        }
    }

    async fn sync_from_radio(&mut self) {
        tracing::info!("requesting contacts and channels from physical radio");

        // Fire-and-forget: tx all read commands without waiting. Responses
        // arrive on radio_recv_rx and are cached by cache_response() once
        // the main loop starts draining the channel.
        let _ = self.send_to_radio(&[0x04]).await;
        for idx in 0..self.max_channels {
            if self.send_to_radio(&[0x1F, idx]).await.is_err() {
                break;
            }
        }
    }

    async fn send_to_radio(&self, data: &[u8]) -> Result<()> {
        let tx = self.radio_send_tx.as_ref().context("radio not connected")?;
        tx.send(data.to_vec()).context("failed to send to radio")?;
        Ok(())
    }

    async fn handle_client_command(&mut self, cmd: ClientCommand) {
        if cmd.payload.is_empty() {
            return;
        }

        let cmd_code = cmd.payload[0];
        self.log_event("->", cmd_code, &cmd.payload);

        // 1. Cache-only paths: serve from RAM / SQLite, never touch the radio.
        if self.try_serve_from_cache(cmd_code, &cmd).await {
            return;
        }

        // 2. Anything else goes through the serialized radio queue.
        self.enqueue_or_dispatch(cmd.client_id, cmd.payload).await;
    }

    /// Returns true if the command was fully served from cache without
    /// forwarding to the radio.
    async fn try_serve_from_cache(&self, cmd_code: u8, cmd: &ClientCommand) -> bool {
        match cmd_code {
            0x01 => {
                if let Some(ref blob) = self.self_info_raw {
                    self.send_to_client(&cmd.client_id, blob.clone());
                    return true;
                }
            }
            0x16 => {
                if let Some(ref blob) = self.device_info_raw {
                    self.send_to_client(&cmd.client_id, blob.clone());
                    return true;
                }
            }
            0x14 => {
                if let Some(ref blob) = self.battery_info_raw {
                    self.send_to_client(&cmd.client_id, blob.clone());
                    return true;
                }
            }
            0x04 => {
                if let Ok(contacts) = self.state.get_contacts().await
                    && !contacts.is_empty()
                {
                    self.respond_contacts(&cmd.client_id, &contacts).await;
                    return true;
                }
            }
            0x1F => {
                let requested_idx = if cmd.payload.len() > 1 {
                    cmd.payload[1] as i64
                } else {
                    -1
                };
                if let Ok(channels) = self.state.get_channels().await
                    && let Some(ch) = channels.iter().find(|c| c.idx == requested_idx)
                {
                    self.respond_channel_info(&cmd.client_id, ch).await;
                    return true;
                }
            }
            _ => {}
        }
        false
    }

    /// Enqueue the command if the radio is busy with a previous request,
    /// otherwise dispatch it immediately.
    async fn enqueue_or_dispatch(&mut self, client_id: ClientId, payload: Vec<u8>) {
        if self.in_flight.is_none() {
            self.dispatch_to_radio(client_id, payload).await;
        } else {
            self.pending_queue.push_back(Queued { client_id, payload });
        }
    }

    /// Forward a command to the radio and set up the in_flight slot.
    /// On forward failure replies ERROR to the client and leaves the slot
    /// free. On success performs post-forward side effects (echo synthesis
    /// for SEND_*, cache invalidation for setters, etc).
    async fn dispatch_to_radio(&mut self, client_id: ClientId, payload: Vec<u8>) {
        let cmd_code = payload[0];

        if let Err(e) = self.send_to_radio(&payload).await {
            tracing::warn!(error = %e, cmd = format_args!("0x{cmd_code:02x}"), "forward failed");
            self.send_to_client(&client_id, vec![0x01, 0x01]);
            return;
        }

        match cmd_code {
            0x02 if payload.len() >= 14 => {
                self.synthesize_contact_echo(&client_id, &payload).await;
            }
            0x03 if payload.len() >= 8 => {
                self.synthesize_channel_echo(&client_id, &payload).await;
            }
            0x20 if payload.len() > 1 => {
                let idx = payload[1] as i64;
                let _ = self.state.delete_channel(idx).await;
            }
            // Setters that mutate SELF_INFO / DEVICE_INFO / BATTERY.
            0x08 | 0x0B | 0x0C | 0x0E | 0x15 | 0x25 | 0x26 | 0x33 => {
                self.self_info_raw = None;
                self.device_info_raw = None;
                self.battery_info_raw = None;
            }
            _ => {}
        }

        self.in_flight = Some(InFlight {
            client_id,
            cmd_code,
            sent_at: Instant::now(),
            awaiting: awaiting_for(cmd_code),
        });
    }

    /// Pop and dispatch the next queued command for which the originating
    /// client is still connected. Stops at the first one successfully
    /// dispatched, or when the queue empties.
    async fn advance_queue(&mut self) {
        while let Some(next) = self.pending_queue.pop_front() {
            if !self.client_is_alive(&next.client_id) {
                tracing::debug!(client = %next.client_id, "dropping queued request: client gone");
                continue;
            }
            self.dispatch_to_radio(next.client_id, next.payload).await;
            if self.in_flight.is_some() {
                return;
            }
            // dispatch failed; ERROR already sent. Try the next.
        }
    }

    fn client_is_alive(&self, client_id: &ClientId) -> bool {
        self.client_channels.lock().unwrap().contains_key(client_id)
    }

    /// Synthesize a CONTACT_MSG_RECV (0x07) for the rest of the clients so
    /// they see this message as if it had arrived over LoRa from the radio.
    async fn synthesize_contact_echo(&self, sender: &ClientId, payload: &[u8]) {
        let Some(pk) = self.radio_pubkey else {
            tracing::warn!("no radio pubkey known, skipping synthetic broadcast for SEND_MSG");
            return;
        };
        let from_key = &pk[..6];
        let msg_type = payload[1];
        let ts = u32::from_le_bytes(payload[3..7].try_into().unwrap_or([0; 4])) as i64;
        let text = String::from_utf8_lossy(&payload[13..]).to_string();

        let mut fake = Vec::with_capacity(13 + text.len());
        fake.push(0x07);
        fake.extend_from_slice(from_key);
        fake.push(0); // path_len
        fake.push(msg_type); // txt_type
        fake.extend_from_slice(&payload[3..7]); // timestamp
        fake.extend_from_slice(text.as_bytes());
        self.broadcast_to_others(Some(sender), &fake);

        let _ = self
            .state
            .insert_message("contact", Some(from_key), None, &text, ts)
            .await;
    }

    /// Synthesize a CHANNEL_MSG_RECV (0x08) for the rest of the clients.
    async fn synthesize_channel_echo(&self, sender: &ClientId, payload: &[u8]) {
        let Some(pk) = self.radio_pubkey else {
            tracing::warn!("no radio pubkey known, skipping synthetic broadcast for SEND_CHAN_MSG");
            return;
        };
        let from_key = &pk[..6];
        let channel = payload[2] as i64;
        let ts = u32::from_le_bytes(payload[3..7].try_into().unwrap_or([0; 4])) as i64;
        let text = String::from_utf8_lossy(&payload[7..]).to_string();

        let mut fake = Vec::with_capacity(8 + text.len());
        fake.push(0x08);
        fake.push(payload[2]); // channel
        fake.push(0); // path_len
        fake.push(0); // txt_type = text
        fake.extend_from_slice(&payload[3..7]); // timestamp
        fake.extend_from_slice(text.as_bytes());
        self.broadcast_to_others(Some(sender), &fake);

        let _ = self
            .state
            .insert_message("channel", Some(from_key), Some(channel), &text, ts)
            .await;
    }

    /// Called on every timeout tick. If the current in_flight has been
    /// pending longer than IN_FLIGHT_TIMEOUT, fail it with ERROR and
    /// advance the queue.
    async fn check_in_flight_timeout(&mut self) {
        let timed_out = self
            .in_flight
            .as_ref()
            .is_some_and(|f| f.sent_at.elapsed() >= IN_FLIGHT_TIMEOUT);
        if !timed_out {
            return;
        }
        let f = self.in_flight.take().expect("checked Some above");
        tracing::warn!(
            client = %f.client_id,
            cmd = format_args!("0x{:02x}", f.cmd_code),
            "in_flight timeout, replying ERROR"
        );
        self.send_to_client(&f.client_id, vec![0x01, 0x01]);
        self.advance_queue().await;
    }

    fn send_to_client(&self, client_id: &ClientId, payload: Vec<u8>) {
        let client_tx = self.client_channels.lock().unwrap().get(client_id).cloned();
        if let Some(tx) = client_tx {
            let _ = tx.send(payload);
        }
    }

    fn broadcast_to_others(&self, exclude: Option<&ClientId>, payload: &[u8]) {
        let channels = self.client_channels.lock().unwrap();
        for (id, tx) in channels.iter() {
            if let Some(exclude_id) = exclude
                && id == exclude_id
            {
                continue;
            }
            let _ = tx.send(payload.to_vec());
        }
    }

    fn extract_radio_identity(&mut self, blob: &[u8]) {
        // SELF_INFO layout: [0]=0x05, [1]=adv_type, [2]=tx_power, [3]=max_tx_power,
        // [4..36]=pubkey, ... name at the tail (offset depends on whether sf/cr
        // bytes are present; see decode_response_payload for the spec).
        if blob.len() >= 36 {
            let mut pk = [0u8; 32];
            pk.copy_from_slice(&blob[4..36]);
            self.radio_pubkey = Some(pk);
        }
        if let Some(decoded) = decode_response_payload(0x05, blob)
            && let Some(DecodedValue::String(name)) = decoded.get("name")
        {
            self.radio_name = Some(name.clone());
        }
    }

    fn extract_max_channels(&mut self, blob: &[u8]) {
        if let Some(decoded) = decode_response_payload(0x0D, blob)
            && let Some(DecodedValue::Integer(n)) = decoded.get("max_channels")
        {
            let max = (*n).min(255) as u8;
            if max > 0 {
                self.max_channels = max;
                tracing::info!(
                    max_channels = self.max_channels,
                    "radio reports max channels"
                );
            }
        }
    }

    async fn respond_contacts(&self, client_id: &ClientId, contacts: &[CachedContact]) {
        let count = contacts.len() as u32;
        let now = chrono::Utc::now().timestamp() as u32;

        let mut start = vec![0x02];
        start.extend_from_slice(&u32::to_le_bytes(count));
        self.send_to_client(client_id, start);

        for contact in contacts {
            let mut c = vec![0x03];
            let mut pk_b = [0u8; 32];
            let pklen = contact.public_key.len().min(32);
            pk_b[..pklen].copy_from_slice(&contact.public_key[..pklen]);
            c.extend_from_slice(&pk_b);
            c.push(contact.contact_type as u8);
            c.push(0); // flags
            c.push(0); // path_len
            c.extend_from_slice(&[0u8; 64]); // path
            let mut name_b = [0u8; 32];
            let nlen = contact.name.len().min(32);
            name_b[..nlen].copy_from_slice(&contact.name.as_bytes()[..nlen]);
            c.extend_from_slice(&name_b);
            let last_advert = contact.last_advert.unwrap_or(now as i64) as u32;
            c.extend_from_slice(&u32::to_le_bytes(last_advert));
            let lat_i = (contact.lat.unwrap_or(0.0) * 1_000_000.0) as i32;
            let lon_i = (contact.lon.unwrap_or(0.0) * 1_000_000.0) as i32;
            c.extend_from_slice(&i32::to_le_bytes(lat_i));
            c.extend_from_slice(&i32::to_le_bytes(lon_i));
            c.extend_from_slice(&[0u8; 4]); // last_mod

            self.send_to_client(client_id, c);
        }

        let mut end = vec![0x04];
        end.extend_from_slice(&u32::to_le_bytes(now));
        self.send_to_client(client_id, end);

        tracing::debug!(n = count, "responded with cached contacts");
    }

    async fn respond_channel_info(&self, client_id: &ClientId, channel: &CachedChannel) {
        tracing::info!(idx = channel.idx, name = %channel.name, "responding with cached CHANNEL_INFO");
        let mut payload = vec![0x12];
        payload.push(channel.idx as u8);
        let mut name_b = [0u8; 32];
        let nlen = channel.name.len().min(32);
        name_b[..nlen].copy_from_slice(&channel.name.as_bytes()[..nlen]);
        payload.extend_from_slice(&name_b);
        let mut secret_b = [0u8; 16];
        if let Some(ref secret) = channel.config {
            let slen = secret.len().min(16);
            secret_b[..slen].copy_from_slice(&secret[..slen]);
        }
        payload.extend_from_slice(&secret_b);
        self.send_to_client(client_id, payload);
    }

    async fn handle_radio_rx(&mut self, payload: Vec<u8>) {
        if payload.is_empty() {
            return;
        }

        let resp_code = payload[0];
        self.log_event("<-", resp_code, &payload);

        self.cache_response(resp_code, &payload).await;

        if self.config.record_radio_rx {
            let _ = self.state.insert_raw_rx(resp_code, &payload).await;
        }

        // 1. If the frame completes (or belongs to) the in_flight request,
        //    route it to that client.
        if self.try_route_to_in_flight(resp_code, &payload).await {
            return;
        }
        // 2. If it's a late ACK matching a previous MSG_SENT, route it.
        if self.try_route_ack(resp_code, &payload) {
            return;
        }
        // 3. Otherwise it's a spontaneous push event from the radio
        //    (MESSAGES_WAITING, ADVERTISEMENT, NEW_ADVERT, ...).
        let _ = self.broadcast_tx.send(payload);
    }

    /// Match the response against the current in_flight request. Returns
    /// true if it was routed (whether or not it terminated the request).
    async fn try_route_to_in_flight(&mut self, code: u8, payload: &[u8]) -> bool {
        let Some(in_flight) = self.in_flight.as_ref() else {
            return false;
        };

        // Push events never complete a request, even if a slot is open.
        if is_push_event(code) {
            return false;
        }

        let (matched, is_terminator) = match &in_flight.awaiting {
            Awaiting::Any => (true, true),
            Awaiting::Stream {
                frame_codes,
                terminator_code,
            } => {
                let in_stream = frame_codes.contains(&code) || code == *terminator_code;
                (in_stream, code == *terminator_code)
            }
        };

        if !matched {
            return false;
        }

        let client_id = in_flight.client_id;
        let cmd_code = in_flight.cmd_code;

        // If this is the MSG_SENT closing a SEND_*, stash the expected_ack
        // so the later 0x82 finds its way back. Lazy GC: purge stale
        // entries first so the map stays bounded (channel sends never
        // produce an ACK, so without this they'd accumulate forever).
        if code == 0x06
            && payload.len() >= 6
            && let Ok(ack) = <[u8; 4]>::try_from(&payload[2..6])
        {
            let now = Instant::now();
            self.pending_acks
                .retain(|_, (_, inserted_at)| now.duration_since(*inserted_at) < PENDING_ACK_TTL);
            self.pending_acks.insert(ack, (client_id, now));
        }

        self.send_to_client(&client_id, payload.to_vec());

        // An incoming LoRa message delivered as the reply to a GET_MSG is
        // a global event from the rest of the network's point of view:
        // every connected client should see it. Fan out to the other
        // clients (the originator just got it through send_to_client).
        if cmd_code == 0x0A && matches!(code, 0x07 | 0x08 | 0x10 | 0x11) {
            self.broadcast_to_others(Some(&client_id), payload);
        }

        if is_terminator {
            self.in_flight = None;
            self.advance_queue().await;
        }
        true
    }

    fn try_route_ack(&mut self, code: u8, payload: &[u8]) -> bool {
        if code != 0x82 || payload.len() < 5 {
            return false;
        }
        let Ok(ack) = <[u8; 4]>::try_from(&payload[1..5]) else {
            return false;
        };
        let Some((client_id, _)) = self.pending_acks.remove(&ack) else {
            return false;
        };
        self.send_to_client(&client_id, payload.to_vec());
        true
    }

    async fn cache_response(&mut self, code: u8, payload: &[u8]) {
        match code {
            0x03 if payload.len() >= 148 => {
                let pk = payload[1..33].to_vec();
                let contact_type = payload[33] as i64;
                let name = String::from_utf8_lossy(
                    &payload[100..][..payload[100..].iter().position(|&b| b == 0).unwrap_or(32)],
                )
                .to_string();
                let last_advert =
                    u32::from_le_bytes(payload[132..136].try_into().unwrap_or([0; 4])) as i64;
                let lat = f64::from(i32::from_le_bytes(
                    payload[136..140].try_into().unwrap_or([0; 4]),
                )) / 1_000_000.0;
                let lon = f64::from(i32::from_le_bytes(
                    payload[140..144].try_into().unwrap_or([0; 4]),
                )) / 1_000_000.0;

                if let Err(e) = self
                    .state
                    .upsert_contact(&CachedContact {
                        public_key: pk,
                        name,
                        contact_type,
                        last_advert: Some(last_advert),
                        lat: if lat != 0.0 { Some(lat) } else { None },
                        lon: if lon != 0.0 { Some(lon) } else { None },
                    })
                    .await
                {
                    tracing::warn!(error = %e, "failed to cache contact");
                }
            }
            0x05 => {
                let blob = payload.to_vec();
                let _ = self.state.kv_set("self_info_raw", &blob).await;
                self.extract_radio_identity(&blob);
                self.self_info_raw = Some(blob);
            }
            0x0C => {
                let blob = payload.to_vec();
                let _ = self.state.kv_set("battery_info_raw", &blob).await;
                self.battery_info_raw = Some(blob);
            }
            0x0D => {
                let blob = payload.to_vec();
                let _ = self.state.kv_set("device_info_raw", &blob).await;
                self.extract_max_channels(&blob);
                self.device_info_raw = Some(blob);
            }
            0x07 => {
                if let Some(decoded) = decode_response_payload(code, payload) {
                    let text = decoded
                        .get("text")
                        .map(|v| match v {
                            DecodedValue::String(s) => s.clone(),
                            _ => String::new(),
                        })
                        .unwrap_or_default();
                    let from_key = if payload.len() >= 7 {
                        Some(payload[1..7].to_vec())
                    } else {
                        None
                    };
                    let ts = decoded
                        .get("timestamp")
                        .map(|v| match v {
                            DecodedValue::Integer(i) => *i,
                            _ => chrono::Utc::now().timestamp(),
                        })
                        .unwrap_or_else(|| chrono::Utc::now().timestamp());

                    if let Err(e) = self
                        .state
                        .insert_message("contact", from_key.as_deref(), None, &text, ts)
                        .await
                    {
                        tracing::warn!(error = %e, "failed to cache message");
                    }
                }
            }
            0x08 => {
                if let Some(decoded) = decode_response_payload(code, payload) {
                    let text = decoded
                        .get("text")
                        .map(|v| match v {
                            DecodedValue::String(s) => s.clone(),
                            _ => String::new(),
                        })
                        .unwrap_or_default();
                    let channel_idx = decoded.get("channel").map(|v| match v {
                        DecodedValue::Integer(i) => *i,
                        _ => 0,
                    });
                    let ts = decoded
                        .get("timestamp")
                        .map(|v| match v {
                            DecodedValue::Integer(i) => *i,
                            _ => chrono::Utc::now().timestamp(),
                        })
                        .unwrap_or_else(|| chrono::Utc::now().timestamp());

                    if let Err(e) = self
                        .state
                        .insert_message("channel", None, channel_idx, &text, ts)
                        .await
                    {
                        tracing::warn!(error = %e, "failed to cache channel message");
                    }
                }
            }
            0x10 if payload.len() >= 13 => {
                let txt_type = payload[11];
                let from_key = Some(payload[4..10].to_vec());
                let ts = u32::from_le_bytes(payload[12..16].try_into().unwrap_or([0; 4])) as i64;
                let text_offset = if txt_type == 2 { 20 } else { 16 };
                let text = if payload.len() > text_offset {
                    String::from_utf8_lossy(&payload[text_offset..]).to_string()
                } else {
                    String::new()
                };
                if let Err(e) = self
                    .state
                    .insert_message("contact", from_key.as_deref(), None, &text, ts)
                    .await
                {
                    tracing::warn!(error = %e, "failed to cache V3 contact message");
                }
            }
            0x11 if payload.len() >= 11 => {
                let channel_idx = payload[4] as i64;
                let ts = u32::from_le_bytes(payload[7..11].try_into().unwrap_or([0; 4])) as i64;
                let text = if payload.len() > 11 {
                    String::from_utf8_lossy(&payload[11..]).to_string()
                } else {
                    String::new()
                };
                if let Err(e) = self
                    .state
                    .insert_message("channel", None, Some(channel_idx), &text, ts)
                    .await
                {
                    tracing::warn!(error = %e, "failed to cache V3 channel message");
                }
            }
            0x12 if payload.len() >= 50 => {
                let idx = payload[1] as i64;
                let name_end = payload[2..34].iter().position(|&b| b == 0).unwrap_or(32);
                let name = String::from_utf8_lossy(&payload[2..2 + name_end]).to_string();
                let secret = payload[34..50].to_vec();
                if let Err(e) = self
                    .state
                    .upsert_channel(&CachedChannel {
                        idx,
                        name,
                        config: Some(secret),
                    })
                    .await
                {
                    tracing::warn!(error = %e, "failed to cache channel info");
                }
            }
            _ => {}
        }
    }

    fn log_event(&self, arrow: &str, code: u8, payload: &[u8]) {
        if self.config.event_log_level == LogLevel::Off {
            return;
        }

        let (type_name, decoded) = if arrow == "->" {
            let cmd = crate::protocol::types::CommandCode::from_byte(code)
                .map(|c| c.to_string())
                .unwrap_or_else(|| format!("CMD_UNKNOWN(0x{code:02x})"));
            let decoded = decode_command_payload(code, payload);
            (cmd, decoded)
        } else {
            let resp = crate::protocol::types::ResponseCode::from_byte(code)
                .map(|r| r.to_string())
                .unwrap_or_else(|| format!("RESP_UNKNOWN(0x{code:02x})"));
            let decoded = decode_response_payload(code, payload);
            (resp, decoded)
        };

        if self.config.event_log_json {
            use serde::Serialize;
            #[derive(Serialize)]
            struct Event {
                direction: String,
                packet_type: String,
                #[serde(skip_serializing_if = "Option::is_none")]
                decoded: Option<HashMap<String, String>>,
                #[serde(skip_serializing_if = "Option::is_none")]
                payload_hex: Option<String>,
                #[serde(skip_serializing_if = "Option::is_none")]
                payload_len: Option<usize>,
            }

            let decoded_str = decoded.as_ref().map(|d| {
                d.iter()
                    .map(|(k, v)| {
                        let val = match v {
                            DecodedValue::String(s) => s.clone(),
                            DecodedValue::Integer(i) => i.to_string(),
                            DecodedValue::Float(f) => format!("{f:.2}"),
                            DecodedValue::Bool(b) => b.to_string(),
                        };
                        (k.clone(), val)
                    })
                    .collect::<HashMap<_, _>>()
            });

            let event = Event {
                direction: if arrow == "->" {
                    "TO_RADIO".into()
                } else {
                    "FROM_RADIO".into()
                },
                packet_type: type_name,
                decoded: decoded_str,
                payload_hex: if self.config.event_log_level == LogLevel::Verbose {
                    Some(hex::encode(payload))
                } else {
                    None
                },
                payload_len: if self.config.event_log_level == LogLevel::Verbose {
                    Some(payload.len())
                } else {
                    None
                },
            };

            if let Ok(json) = serde_json::to_string(&event) {
                tracing::info!("{json}");
            }
        } else {
            let msg = build_event_msg(
                arrow,
                &type_name,
                decoded.as_ref(),
                payload,
                self.config.event_log_level,
            );
            tracing::info!("{msg}");
        }
    }
}

fn sanitize(msg: &str) -> String {
    msg.chars()
        .filter(|&c| c.is_ascii_graphic() || c == ' ')
        .collect()
}

fn build_event_msg(
    arrow: &str,
    type_name: &str,
    decoded: Option<&HashMap<String, DecodedValue>>,
    payload: &[u8],
    level: LogLevel,
) -> String {
    let mut msg = String::with_capacity(64 + payload.len() * 2);
    msg.push_str(arrow);
    msg.push(' ');
    msg.push_str(type_name);
    if let Some(d) = decoded {
        let formatted = format_decoded(d);
        if !formatted.is_empty() {
            msg.push_str(": ");
            msg.push_str(&formatted);
        }
    }
    if level == LogLevel::Verbose {
        use std::fmt::Write;
        let _ = write!(msg, " [{} bytes]: {}", payload.len(), hex::encode(payload));
    }
    sanitize(&msg)
}

fn build_appstart() -> Vec<u8> {
    let app_name = b"coresplitter";
    let mut payload = Vec::with_capacity(3 + app_name.len());
    payload.push(0x01);
    payload.push(1);
    payload.extend_from_slice(app_name);
    payload
}
