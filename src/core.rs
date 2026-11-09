use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{self, Duration};

use crate::backend;
use crate::frontend::tcp::{ClientCommand, ClientDirectMap, ClientId, TcpFrontend};
use crate::node::state::{Backlog, CachedChannel, CachedContact, NewMessage, NodeState, Retention};
use crate::protocol::decode::{
    DecodedValue, decode_command_payload, decode_response_payload, format_decoded,
};

/// How long the radio may stay silent on an in-flight request. Measured
/// from the last frame of the reply, so a long GET_CONTACTS stream over a
/// slow link (BLE, serial) isn't cut off as long as frames keep coming.
const IN_FLIGHT_TIMEOUT: Duration = Duration::from_secs(5);
/// CONTACT frame: code + pubkey(32) + type + flags + out_path_len +
/// out_path(64) + name(32) + last_advert + lat + lon + lastmod (4 each).
const CONTACT_FRAME_LEN: usize = 148;
const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// How often the database is purged according to the retention settings.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(24 * 3600);
/// Maximum age of a pending_acks entry before it is considered abandoned.
/// LoRa retransmissions inside the radio top out around ~30 s; five minutes
/// is comfortably above any realistic real ACK and short enough to bound
/// memory growth from channel sends (which never produce an ACK).
const PENDING_ACK_TTL: Duration = Duration::from_secs(300);
/// Companion protocol version the proxy negotiates with the radio. The
/// radio picks the layout of a received message when it queues it, from
/// the last DEVICE_QUERY it got, so it must not follow each client's: v3
/// keeps the SNR, and older clients get the frame converted back.
const RADIO_PROTOCOL_VERSION: u8 = 3;
/// Longest text the firmware puts in a message (MAX_TEXT_LEN).
const MAX_TEXT_LEN: usize = 160;

/// The firmware reserves every code with the high bit set (PUSH_CODE_*,
/// 0x80..) for spontaneous push events: ADVERT, MSG_WAITING, LOGIN_*,
/// STATUS_RESPONSE, TRACE_DATA, CONTACT_DELETED, ... They never complete
/// an in-flight request and are broadcast to every client, except ACK
/// (0x82), which has its own routing via `pending_acks`.
fn is_push_event(code: u8) -> bool {
    code >= 0x80
}

/// A channel message as the rest of the mesh receives it: the firmware
/// sends "<node name>: <text>", cut at MAX_TEXT_LEN bytes
/// (BaseChatMesh::sendGroupMessage).
fn channel_text_on_air(node_name: Option<&str>, text: &[u8]) -> Vec<u8> {
    let mut out = node_name
        .map(|name| format!("{name}: ").into_bytes())
        .unwrap_or_default();
    let room = MAX_TEXT_LEN.saturating_sub(out.len());
    out.extend_from_slice(&text[..text.len().min(room)]);
    out
}

/// From v3 on, CONTACT_MSG_RECV and CHANNEL_MSG_RECV (0x07/0x08) become
/// 0x10/0x11 and carry SNR plus two reserved bytes right after the code.
/// Hand a message frame over in the layout the client negotiated; any
/// other frame goes through untouched. A v1 frame upgraded to v3 has no
/// SNR to report and gets 0.
fn message_frame_for(version: u8, frame: &[u8]) -> Cow<'_, [u8]> {
    let converted = match (frame.first(), version >= 3) {
        (Some(&code @ (0x10 | 0x11)), false) if frame.len() >= 4 => {
            [&[code - 0x09][..], &frame[4..]].concat()
        }
        (Some(&code @ (0x07 | 0x08)), true) => [&[code + 0x09, 0, 0, 0][..], &frame[1..]].concat(),
        _ => return Cow::Borrowed(frame),
    };
    Cow::Owned(converted)
}

/// What a request is waiting for, to know when to free the slot.
#[derive(Debug, Clone)]
enum Awaiting {
    /// Any single non-push frame completes the request. Fallback for
    /// commands not listed in `awaiting_for`, so new firmware commands
    /// keep working without a per-command entry.
    Any,
    /// A single frame whose code is one of these (or ERROR, which every
    /// command may return). Pinning the expected codes keeps a late reply
    /// to a timed-out request from being taken as the answer to the next
    /// one. May list a push code when the firmware replies with one.
    OneOf(&'static [u8]),
    /// A multi-frame stream: any frame whose code is in `frame_codes`
    /// belongs to the response; the request completes with `terminator_code`.
    Stream {
        frame_codes: &'static [u8],
        terminator_code: u8,
    },
}

impl Awaiting {
    /// Returns `(belongs, completes)` for a frame with `code`: whether it
    /// is part of the reply and whether it terminates the request.
    /// Push events never belong unless the command's reply is explicitly
    /// listed as one (see awaiting_for). ERROR answers any single-frame
    /// command.
    fn classify(&self, code: u8) -> (bool, bool) {
        match self {
            Awaiting::Any => (!is_push_event(code), true),
            Awaiting::OneOf(codes) => (code == 0x01 || codes.contains(&code), true),
            // The firmware may refuse to start the stream (GET_CONTACTS
            // answers ERR/BAD_STATE while an iteration is running); that
            // ERROR ends the request like the terminator would.
            Awaiting::Stream {
                frame_codes,
                terminator_code,
            } => {
                let ends = code == *terminator_code || code == 0x01;
                (ends || frame_codes.contains(&code), ends)
            }
        }
    }
}

#[derive(Debug)]
struct InFlight {
    /// `None` for internal sync commands (sync_from_radio); `Some` for client requests.
    client_id: Option<ClientId>,
    /// The command as sent, for side effects that must wait until the
    /// radio accepts it (see on_command_accepted).
    payload: Vec<u8>,
    /// When the command was sent or, for streams, the last frame arrived.
    last_activity: Instant,
    /// Wall-clock dispatch time (unix secs), comparable with DB timestamps.
    sent_unix: i64,
    awaiting: Awaiting,
    /// (port, app name) of the requesting client, captured at dispatch so
    /// it is still known if the client disconnects before the reply.
    origin: Option<(u16, String)>,
}

impl InFlight {
    fn cmd_code(&self) -> u8 {
        self.payload[0]
    }
}

/// A connected client that has identified itself with APP_START.
#[derive(Debug)]
struct Session {
    port: u16,
    app_name: String,
    /// Stored messages it missed while offline, served one per GET_MSG
    /// before the radio is asked. While non-empty its cursor advances
    /// only as these are delivered.
    pending: VecDeque<i64>,
}

#[derive(Debug)]
struct Queued {
    /// `None` for internal sync commands; `Some` for client requests.
    client_id: Option<ClientId>,
    payload: Vec<u8>,
}

/// Map a command to the shape of the radio's reply, as written by the
/// companion firmware (examples/companion_radio/MyMesh.cpp).
fn awaiting_for(payload: &[u8]) -> Awaiting {
    match payload[0] {
        // GET_CONTACTS → CONTACTS_START + N x CONTACT + END_OF_CONTACTS
        0x04 => Awaiting::Stream {
            frame_codes: &[0x02, 0x03],
            terminator_code: 0x04,
        },
        0x01 => Awaiting::OneOf(&[0x05]), // APP_START → SELF_INFO
        // SEND_TXT_MSG, SEND_LOGIN, SEND_STATUS_REQ, SEND_TRACE_PATH,
        // SEND_BINARY_REQ, SEND_PATH_DISCOVERY_REQ, SEND_ANON_REQ → SENT
        0x02 | 0x1A | 0x1B | 0x24 | 0x32 | 0x34 | 0x39 => Awaiting::OneOf(&[0x06]),
        // SEND_TELEMETRY_REQ: about ourselves (len 4) the firmware answers
        // straight away with the TELEMETRY_RESPONSE push; for a remote
        // node it replies SENT and the push comes later.
        0x27 if payload.len() == 4 => Awaiting::OneOf(&[0x8B]),
        0x27 => Awaiting::OneOf(&[0x06]),
        0x05 => Awaiting::OneOf(&[0x09]), // GET_DEVICE_TIME → CURR_TIME
        // SYNC_NEXT_MESSAGE → a queued message or NO_MORE_MESSAGES
        0x0A => Awaiting::OneOf(&[0x07, 0x08, 0x10, 0x11, 0x1B, 0x0A]),
        0x11 => Awaiting::OneOf(&[0x0B]),       // EXPORT_CONTACT
        0x14 => Awaiting::OneOf(&[0x0C]),       // GET_BATT_AND_STORAGE
        0x16 => Awaiting::OneOf(&[0x0D]),       // DEVICE_QUERY → DEVICE_INFO
        0x17 => Awaiting::OneOf(&[0x0E, 0x0F]), // EXPORT_PRIVATE_KEY
        0x18 => Awaiting::OneOf(&[0x00, 0x0F]), // IMPORT_PRIVATE_KEY
        0x1E => Awaiting::OneOf(&[0x03]),       // GET_CONTACT_BY_KEY → CONTACT
        0x1F => Awaiting::OneOf(&[0x12]),       // GET_CHANNEL → CHANNEL_INFO
        0x21 => Awaiting::OneOf(&[0x13]),       // SIGN_START
        0x23 => Awaiting::OneOf(&[0x14]),       // SIGN_FINISH → SIGNATURE
        0x28 => Awaiting::OneOf(&[0x15]),       // GET_CUSTOM_VARS
        0x2A => Awaiting::OneOf(&[0x16]),       // GET_ADVERT_PATH
        0x2B => Awaiting::OneOf(&[0x17]),       // GET_TUNING_PARAMS
        0x38 => Awaiting::OneOf(&[0x18]),       // GET_STATS
        0x3B => Awaiting::OneOf(&[0x19]),       // GET_AUTOADD_CONFIG
        0x3C => Awaiting::OneOf(&[0x1A]),       // GET_ALLOWED_REPEAT_FREQ
        0x40 => Awaiting::OneOf(&[0x1C]),       // GET_DEFAULT_FLOOD_SCOPE
        0x42 => Awaiting::OneOf(&[0x1D]),       // RUN_CLI_COMMAND → CLI_REPLY
        // Setters and actions that only ever answer OK / ERROR.
        0x03 | 0x06 | 0x07 | 0x08 | 0x09 | 0x0B | 0x0C | 0x0D | 0x0E | 0x0F | 0x10 | 0x12
        | 0x15 | 0x19 | 0x1C | 0x1D | 0x20 | 0x22 | 0x25 | 0x26 | 0x29 | 0x33 | 0x36 | 0x37
        | 0x3A | 0x3D | 0x3E | 0x3F | 0x41 => Awaiting::OneOf(&[0x00]),
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
    pub tcp_frontend_ports: Vec<u16>,
    pub data_dir: PathBuf,
    pub event_log_level: LogLevel,
    pub event_log_json: bool,
    pub record_radio_rx: bool,
    pub retention: Retention,
    pub new_client_backlog: Backlog,
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
    // true while the radio is connected and initialized. The frontend
    // only listens (and keeps clients connected) while this is true.
    radio_up_tx: watch::Sender<bool>,
    // The radio handles one command at a time. We serialize: a single
    // request is in flight; further client commands wait in pending_queue.
    in_flight: Option<InFlight>,
    pending_queue: VecDeque<Queued>,
    // Clients waiting for the (possibly much later) LoRa-delivery ACK
    // (0x82), keyed by the 4-byte expected_ack the radio handed back in
    // the corresponding MSG_SENT. The Instant is the insertion time,
    // used to evict stale entries when a new ack is recorded. SENT also
    // closes login/status/telemetry requests, which never produce an
    // ACK, so without eviction those entries would accumulate forever.
    pending_acks: HashMap<[u8; 4], (ClientId, Instant)>,
    // Identified clients, keyed by connection. Entries of disconnected
    // clients are dropped lazily (see follow_head).
    sessions: HashMap<ClientId, Session>,
    // Protocol version each client asked for in DEVICE_QUERY; clients that
    // never sent one are taken as v0, like the firmware does. Dropped
    // lazily along with `sessions`.
    protocol_versions: HashMap<ClientId, u8>,
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
            radio_up_tx: watch::Sender::new(false),
            in_flight: None,
            pending_queue: VecDeque::new(),
            pending_acks: HashMap::new(),
            sessions: HashMap::new(),
            protocol_versions: HashMap::new(),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        tracing::info!("starting coresplitter (MeshCore client multiplexer + cache)");

        if let Err(e) = self.connect_backend().await {
            tracing::warn!(
                error = format_args!("{e:#}"),
                "initial radio connection failed, will retry"
            );
            self.reconnect_radio().await;
        } else if let Err(e) = self.initialize_radio().await {
            tracing::warn!(
                error = format_args!("{e:#}"),
                "initial radio initialization failed, will retry"
            );
            self.radio_send_tx = None;
            self.reconnect_radio().await;
        } else {
            // Queue sync commands so they serialize with any early client
            // requests. reconnect_radio() already does this on its own
            // success path, so only the direct-connect branch needs it.
            self.sync_from_radio();
            self.radio_up_tx.send_replace(true);
        }

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

        let frontend_addrs = self
            .config
            .tcp_frontend_ports
            .iter()
            .map(|port| format!("{}:{port}", self.config.tcp_frontend_host))
            .collect();
        let shutdown_rx = self.shutdown_rx.clone();
        let mut frontend = TcpFrontend::new(
            frontend_addrs,
            self.command_tx.clone(),
            self.broadcast_tx.clone(),
            self.client_channels.clone(),
            self.radio_up_tx.subscribe(),
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
        // First tick fires immediately: maintenance also runs at startup.
        let mut maintenance_tick = tokio::time::interval(MAINTENANCE_INTERVAL);
        maintenance_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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
                _ = maintenance_tick.tick() => {
                    self.run_maintenance().await;
                }
            }
        }

        tracing::info!("performing graceful shutdown");
        self.shutdown().await;

        Ok(())
    }

    async fn run_maintenance(&mut self) {
        let retention = self.config.retention;
        // Contact purging relies on the last full sync to tell which
        // contacts the radio no longer has; keep that sync fresh even if
        // the proxy runs for weeks without reconnecting.
        let full_sync_pending = self.in_flight.as_ref().is_some_and(|f| f.payload == [0x04])
            || self.pending_queue.iter().any(|q| q.payload == [0x04]);
        if retention.contacts_days > 0 && !full_sync_pending {
            self.pending_queue.push_back(Queued {
                client_id: None,
                payload: vec![0x04],
            });
            if self.in_flight.is_none() {
                self.advance_queue().await;
            }
        }
        match self.state.maintain(&retention).await {
            Ok(stats) => tracing::info!(
                messages = stats.messages,
                raw_rx = stats.raw_rx,
                contacts = stats.contacts,
                clients = stats.clients,
                "database maintenance done"
            ),
            Err(e) => tracing::warn!(error = %e, "database maintenance failed"),
        }
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
        // Close the frontend: without a radio we have nothing honest to
        // offer, and MeshCore has no "unavailable" frame to say so.
        self.radio_up_tx.send_replace(false);

        // Fail any request that was in flight when the radio dropped.
        if let Some(f) = self.in_flight.take()
            && let Some(cid) = f.client_id
        {
            self.send_to_client(&cid, vec![0x01, 0x01]);
        }
        // Fail all queued client requests; discard internal sync entries.
        while let Some(q) = self.pending_queue.pop_front() {
            if let Some(cid) = q.client_id {
                self.send_to_client(&cid, vec![0x01, 0x01]);
            }
        }

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
                            self.sync_from_radio();
                            self.radio_up_tx.send_replace(true);
                            return;
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = format_args!("{e:#}"),
                                "re-initialization failed, will retry"
                            );
                            self.radio_send_tx = None;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = format_args!("{e:#}"), "reconnection failed");
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
        let device_query = vec![0x16, RADIO_PROTOCOL_VERSION];
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

    fn sync_from_radio(&mut self) {
        tracing::info!("queuing contacts and channels sync from physical radio");

        // Enqueue through the normal serialization path so sync responses
        // can never interleave with in-flight client requests.
        // client_id=None: responses are cached but not forwarded to any client.
        self.pending_queue.push_back(Queued {
            client_id: None,
            payload: vec![0x04],
        });
        for idx in 0..self.max_channels {
            self.pending_queue.push_back(Queued {
                client_id: None,
                payload: vec![0x1F, idx],
            });
        }
        // Kick off the first entry if the radio is idle.
        if self.in_flight.is_none() {
            let _ = self.advance_queue_sync();
        }
    }

    /// Synchronous (non-async) queue advance used during sync_from_radio
    /// before the main loop is running. Pops one entry and sends it directly.
    fn advance_queue_sync(&mut self) -> Option<()> {
        while let Some(next) = self.pending_queue.pop_front() {
            if let Some(cid) = next.client_id
                && !self.client_is_alive(&cid)
            {
                continue;
            }
            let tx = self.radio_send_tx.as_ref()?;
            if tx.send(next.payload.clone()).is_err() {
                if let Some(cid) = next.client_id {
                    self.send_to_client(&cid, vec![0x01, 0x01]);
                }
                return None;
            }
            self.in_flight = Some(InFlight {
                client_id: next.client_id,
                last_activity: Instant::now(),
                sent_unix: chrono::Utc::now().timestamp(),
                awaiting: awaiting_for(&next.payload),
                origin: next.client_id.and_then(|cid| self.identity_of(&cid)),
                payload: next.payload,
            });
            return Some(());
        }
        None
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

        // Commands may sit in command_rx while the radio reconnects; by
        // then their client has been disconnected. Don't act on them.
        if !self.client_is_alive(&cmd.client_id) {
            return;
        }

        let cmd_code = cmd.payload[0];
        self.log_event("->", cmd_code, &cmd.payload);

        if cmd_code == 0x01 {
            self.register_session(&cmd).await;
        }
        if cmd_code == 0x16
            && let Some(&version) = cmd.payload.get(1)
        {
            self.protocol_versions.insert(cmd.client_id, version);
        }
        // Messages missed while offline are served before asking the radio.
        if cmd_code == 0x0A && self.replay_next_missed(&cmd.client_id).await {
            return;
        }

        // 1. Cache-only paths: serve from RAM / SQLite, never touch the radio.
        if self.try_serve_from_cache(cmd_code, &cmd).await {
            if cmd_code == 0x01 {
                self.notify_missed(&cmd.client_id);
            }
            return;
        }

        // 2. Anything else goes through the serialized radio queue. A
        //    DEVICE_QUERY that reaches the radio keeps the proxy's version.
        let payload = if cmd_code == 0x16 {
            vec![0x16, RADIO_PROTOCOL_VERSION]
        } else {
            cmd.payload
        };
        self.enqueue_or_dispatch(Some(cmd.client_id), payload).await;
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
                    // Optional 'since' filter: only contacts modified after it.
                    let since = cmd
                        .payload
                        .get(1..5)
                        .map(|b| u32::from_le_bytes(b.try_into().unwrap()));
                    self.respond_contacts(&cmd.client_id, &contacts, since)
                        .await;
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
    async fn enqueue_or_dispatch(&mut self, client_id: Option<ClientId>, payload: Vec<u8>) {
        if self.in_flight.is_none() {
            self.dispatch_to_radio(client_id, payload).await;
        } else {
            self.pending_queue.push_back(Queued { client_id, payload });
        }
    }

    /// Forward a command to the radio and set up the in_flight slot.
    /// On forward failure replies ERROR to the originating client (if any)
    /// and leaves the slot free. On success performs post-forward side
    /// effects (echo synthesis for SEND_*, cache invalidation for setters).
    async fn dispatch_to_radio(&mut self, client_id: Option<ClientId>, payload: Vec<u8>) {
        let cmd_code = payload[0];

        if let Err(e) = self.send_to_radio(&payload).await {
            tracing::warn!(error = %e, cmd = format_args!("0x{cmd_code:02x}"), "forward failed");
            if let Some(cid) = client_id {
                self.send_to_client(&cid, vec![0x01, 0x01]);
            }
            return;
        }

        match cmd_code {
            // REMOVE_CONTACT: drop it from the cache too, otherwise the
            // next cached GET_CONTACTS would resurrect it.
            0x0F if payload.len() >= 33 => {
                if let Err(e) = self.state.delete_contact(&payload[1..33]).await {
                    tracing::warn!(error = %e, "failed to delete cached contact");
                }
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
            last_activity: Instant::now(),
            sent_unix: chrono::Utc::now().timestamp(),
            awaiting: awaiting_for(&payload),
            origin: client_id.and_then(|cid| self.identity_of(&cid)),
            payload,
        });
    }

    fn identity_of(&self, client_id: &ClientId) -> Option<(u16, String)> {
        self.sessions
            .get(client_id)
            .map(|s| (s.port, s.app_name.clone()))
    }

    /// Side effects of a command that only make sense once the radio has
    /// accepted it, i.e. its reply was anything but ERROR.
    async fn on_command_accepted(&mut self, done: &InFlight) {
        let (client_id, payload) = (done.client_id, done.payload.as_slice());

        // APP_START answered by the radio itself (no cached SELF_INFO yet).
        if payload[0] == 0x01
            && let Some(cid) = client_id
        {
            self.notify_missed(&cid);
        }
        // Contact table changes: re-read what the radio now holds so the
        // cache serves the same frame (path, flags, lastmod) it would.
        match payload[0] {
            // A complete, unfiltered GET_CONTACTS: every contact the radio
            // holds was just re-cached with seen_at >= the dispatch time.
            0x04 if payload.len() == 1 => {
                if let Err(e) = self.state.set_contacts_synced_at(done.sent_unix).await {
                    tracing::warn!(error = %e, "failed to record contacts sync time");
                }
            }
            // ADD_UPDATE_CONTACT, RESET_PATH: [cmd, pubkey(32), ...]
            0x09 | 0x0D if payload.len() >= 33 => self.queue_contact_refresh(&payload[1..33]),
            // IMPORT_CONTACT takes an advert blob; the key isn't at a fixed
            // offset, so re-read the whole list.
            0x12 => self.pending_queue.push_back(Queued {
                client_id: None,
                payload: vec![0x04],
            }),
            // SET_ADVERT_NAME: [cmd, name]; the firmware keeps 31 bytes.
            // Channel echoes carry the name, so keep it current.
            0x08 => {
                let name = &payload[1..payload.len().min(32)];
                let name = String::from_utf8_lossy(name);
                self.radio_name = Some(name.trim_end_matches('\0').to_string());
            }
            _ => {}
        }

        if let Some(cid) = client_id {
            match payload[0] {
                0x02 if payload.len() >= 14 => {
                    self.synthesize_contact_echo(&cid, done.origin.as_ref(), payload)
                        .await;
                }
                0x03 if payload.len() >= 8 => {
                    self.synthesize_channel_echo(&cid, done.origin.as_ref(), payload)
                        .await;
                }
                _ => {}
            }
        }
    }

    /// Queue an internal GET_CONTACT_BY_KEY; its CONTACT reply is cached
    /// by cache_response like any other. Goes to the back of the queue so
    /// it never jumps ahead of client requests already waiting.
    fn queue_contact_refresh(&mut self, pubkey: &[u8]) {
        let mut payload = Vec::with_capacity(33);
        payload.push(0x1E);
        payload.extend_from_slice(pubkey);
        self.pending_queue.push_back(Queued {
            client_id: None,
            payload,
        });
    }

    /// Pop and dispatch the next queued command for which the originating
    /// client is still connected. Internal sync entries (client_id=None)
    /// are always eligible. Stops at the first one successfully dispatched,
    /// or when the queue empties.
    async fn advance_queue(&mut self) {
        while let Some(next) = self.pending_queue.pop_front() {
            if let Some(cid) = next.client_id
                && !self.client_is_alive(&cid)
            {
                tracing::debug!(client = %cid, "dropping queued request: client gone");
                continue;
            }
            self.dispatch_to_radio(next.client_id, next.payload).await;
            if self.in_flight.is_some() {
                return;
            }
            // dispatch failed; ERROR already sent. Try the next.
        }
    }

    /// Identify the client from APP_START as (local port, app name) and
    /// work out which stored messages it hasn't seen. app_ver is left out
    /// so an updated app keeps its history.
    async fn register_session(&mut self, cmd: &ClientCommand) {
        // APP_START: [0x01, app_ver, reserved(6), app_name...]
        let app_name = cmd
            .payload
            .get(8..)
            .map(|b| {
                String::from_utf8_lossy(b)
                    .trim_end_matches('\0')
                    .to_string()
            })
            .unwrap_or_default();
        let port = cmd.local_port;

        let pending = match self.state.client_cursor(port, &app_name).await {
            Ok(Some(cursor)) => self.state.message_ids_after(cursor, port, &app_name).await,
            Ok(None) => {
                self.state
                    .backlog_message_ids(self.config.new_client_backlog, port, &app_name)
                    .await
            }
            Err(e) => Err(e),
        };
        let pending: VecDeque<i64> = match pending {
            Ok(ids) => ids.into(),
            Err(e) => {
                tracing::warn!(error = %e, "failed to load missed messages");
                VecDeque::new()
            }
        };

        // Nothing pending: the client is up to date from here on. Otherwise
        // leave the cursor just below the first pending message (this also
        // records a client seen for the first time).
        let cursor = match pending.front() {
            None => self.state.message_head().await.unwrap_or(0),
            Some(first) => first - 1,
        };
        if let Err(e) = self
            .state
            .advance_client_cursor(port, &app_name, cursor)
            .await
        {
            tracing::warn!(error = %e, "failed to record client");
        }

        tracing::info!(
            client = %cmd.client_id,
            port,
            app = %app_name,
            missed = pending.len(),
            "client identified"
        );
        self.sessions.insert(
            cmd.client_id,
            Session {
                port,
                app_name,
                pending,
            },
        );
    }

    /// Tell a freshly identified client it has messages waiting, the same
    /// way the radio does, so it starts polling with GET_MSG.
    fn notify_missed(&self, client_id: &ClientId) {
        if self
            .sessions
            .get(client_id)
            .is_some_and(|s| !s.pending.is_empty())
        {
            self.send_to_client(client_id, vec![0x83]);
        }
    }

    /// Answer a GET_MSG with the next message the client missed. Returns
    /// false when there is none left, so the radio is asked instead.
    async fn replay_next_missed(&mut self, client_id: &ClientId) -> bool {
        let Some(session) = self.sessions.get(client_id) else {
            return false;
        };
        let (port, app_name) = (session.port, session.app_name.clone());
        let mut served = false;
        while let Some(id) = self
            .sessions
            .get_mut(client_id)
            .and_then(|s| s.pending.pop_front())
        {
            // May have been purged by retention since; skip it.
            let Ok(Some(frame)) = self.state.message_frame(id).await else {
                continue;
            };
            self.send_to_client(client_id, frame);
            let _ = self.state.advance_client_cursor(port, &app_name, id).await;
            served = true;
            break;
        }
        // Caught up: from now on the cursor follows live traffic.
        if self
            .sessions
            .get(client_id)
            .is_some_and(|s| s.pending.is_empty())
            && let Ok(head) = self.state.message_head().await
        {
            let _ = self
                .state
                .advance_client_cursor(port, &app_name, head)
                .await;
        }
        served
    }

    /// Persist a message as clients saw it and advance the cursor of every
    /// connected, caught-up client: they got it live, or sent it.
    /// `msg.origin` is the client whose SEND_* produced it (None for radio
    /// traffic), so the message is never replayed back to that client.
    async fn store_message(&mut self, msg: NewMessage<'_>) -> Result<()> {
        let id = self.state.insert_message(&msg).await?;
        self.follow_head(id).await;
        Ok(())
    }

    async fn follow_head(&mut self, id: i64) {
        let mut identities: Vec<(u16, String)> = {
            let channels = self.client_channels.lock().unwrap();
            self.sessions.retain(|cid, _| channels.contains_key(cid));
            self.protocol_versions
                .retain(|cid, _| channels.contains_key(cid));
            self.sessions
                .values()
                .filter(|s| s.pending.is_empty())
                .map(|s| (s.port, s.app_name.clone()))
                .collect()
        };
        identities.sort();
        identities.dedup();
        for (port, app_name) in identities {
            if let Err(e) = self.state.advance_client_cursor(port, &app_name, id).await {
                tracing::warn!(error = %e, "failed to advance client cursor");
            }
        }
    }

    fn client_is_alive(&self, client_id: &ClientId) -> bool {
        self.client_channels.lock().unwrap().contains_key(client_id)
    }

    /// Synthesize a CONTACT_MSG_RECV (0x07) for the rest of the clients so
    /// they see this message as if it had arrived over LoRa from the radio.
    async fn synthesize_contact_echo(
        &mut self,
        sender: &ClientId,
        origin: Option<&(u16, String)>,
        payload: &[u8],
    ) {
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
            .store_message(NewMessage {
                msg_type: "contact",
                from_key: Some(from_key),
                channel_idx: None,
                text: &text,
                timestamp: ts,
                frame: &fake,
                origin: origin.map(|(port, app)| (*port, app.as_str())),
            })
            .await;
    }

    /// Synthesize a CHANNEL_MSG_RECV (0x08) for the rest of the clients.
    async fn synthesize_channel_echo(
        &mut self,
        sender: &ClientId,
        origin: Option<&(u16, String)>,
        payload: &[u8],
    ) {
        let Some(pk) = self.radio_pubkey else {
            tracing::warn!("no radio pubkey known, skipping synthetic broadcast for SEND_CHAN_MSG");
            return;
        };
        let from_key = &pk[..6];
        let channel = payload[2] as i64;
        let ts = u32::from_le_bytes(payload[3..7].try_into().unwrap_or([0; 4])) as i64;
        let on_air = channel_text_on_air(self.radio_name.as_deref(), &payload[7..]);
        let text = String::from_utf8_lossy(&on_air).to_string();

        let mut fake = Vec::with_capacity(8 + on_air.len());
        fake.push(0x08);
        fake.push(payload[2]); // channel
        fake.push(0); // path_len
        fake.push(0); // txt_type = text
        fake.extend_from_slice(&payload[3..7]); // timestamp
        fake.extend_from_slice(&on_air);
        self.broadcast_to_others(Some(sender), &fake);

        let _ = self
            .store_message(NewMessage {
                msg_type: "channel",
                from_key: Some(from_key),
                channel_idx: Some(channel),
                text: &text,
                timestamp: ts,
                frame: &fake,
                origin: origin.map(|(port, app)| (*port, app.as_str())),
            })
            .await;
    }

    /// Called on every timeout tick. If the current in_flight has been
    /// silent longer than IN_FLIGHT_TIMEOUT, fail it with ERROR and
    /// advance the queue.
    async fn check_in_flight_timeout(&mut self) {
        let timed_out = self
            .in_flight
            .as_ref()
            .is_some_and(|f| f.last_activity.elapsed() >= IN_FLIGHT_TIMEOUT);
        if !timed_out {
            return;
        }
        let f = self.in_flight.take().expect("checked Some above");
        if let Some(cid) = f.client_id {
            tracing::warn!(
                client = %cid,
                cmd = format_args!("0x{:02x}", f.cmd_code()),
                "in_flight timeout, replying ERROR"
            );
            self.send_to_client(&cid, vec![0x01, 0x01]);
        } else {
            tracing::warn!(
                cmd = format_args!("0x{:02x}", f.cmd_code()),
                "in_flight sync timeout, discarding"
            );
        }
        self.advance_queue().await;
    }

    fn protocol_version(&self, client_id: &ClientId) -> u8 {
        self.protocol_versions.get(client_id).copied().unwrap_or(0)
    }

    fn send_to_client(&self, client_id: &ClientId, payload: Vec<u8>) {
        let client_tx = self.client_channels.lock().unwrap().get(client_id).cloned();
        if let Some(tx) = client_tx {
            let payload = match message_frame_for(self.protocol_version(client_id), &payload) {
                Cow::Borrowed(_) => payload,
                Cow::Owned(converted) => converted,
            };
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
            let _ = tx.send(message_frame_for(self.protocol_version(id), payload).into_owned());
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

    /// Replay GET_CONTACTS from the cache the way the firmware does it:
    /// CONTACTS_START carries the total count (not the filtered one),
    /// only contacts with lastmod > `since` are sent, and END_OF_CONTACTS
    /// carries the most recent lastmod among those sent.
    async fn respond_contacts(
        &self,
        client_id: &ClientId,
        contacts: &[CachedContact],
        since: Option<u32>,
    ) {
        let count = contacts.len() as u32;

        let mut start = vec![0x02];
        start.extend_from_slice(&u32::to_le_bytes(count));
        self.send_to_client(client_id, start);

        let mut most_recent_lastmod = 0u32;
        let mut sent = 0usize;
        for contact in contacts {
            let frame = match &contact.raw {
                Some(raw) if raw.len() >= CONTACT_FRAME_LEN => {
                    let mut f = raw[..CONTACT_FRAME_LEN].to_vec();
                    f[0] = 0x03;
                    f
                }
                _ => synthesize_contact_frame(contact),
            };
            let lastmod = u32::from_le_bytes(frame[144..148].try_into().unwrap());
            if let Some(since) = since
                && lastmod <= since
            {
                continue;
            }
            most_recent_lastmod = most_recent_lastmod.max(lastmod);
            sent += 1;
            self.send_to_client(client_id, frame);
        }

        let mut end = vec![0x04];
        end.extend_from_slice(&u32::to_le_bytes(most_recent_lastmod));
        self.send_to_client(client_id, end);

        tracing::debug!(total = count, sent, "responded with cached contacts");
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
        // 3. A push event from the radio (MESSAGES_WAITING, ADVERTISEMENT,
        //    NEW_ADVERT, ...) goes to everybody.
        if is_push_event(resp_code) {
            // ADVERT / PATH_UPDATE: [code, pubkey(32)] for a contact the
            // radio just updated (last_advert, path). Refresh our copy.
            if matches!(resp_code, 0x80 | 0x81) && payload.len() >= 33 {
                self.queue_contact_refresh(&payload[1..33]);
                if self.in_flight.is_none() {
                    self.advance_queue().await;
                }
            }
            let _ = self.broadcast_tx.send(payload);
            return;
        }
        // 4. A reply nobody is waiting for: typically the late answer to a
        //    request that already timed out. Handing it to the clients would
        //    only make them take it as the reply to something else.
        tracing::warn!(
            code = format_args!("0x{resp_code:02x}"),
            "dropping unsolicited radio response"
        );
    }

    /// Match the response against the current in_flight request. Returns
    /// true if it was routed (whether or not it terminated the request).
    async fn try_route_to_in_flight(&mut self, code: u8, payload: &[u8]) -> bool {
        let Some(in_flight) = self.in_flight.as_ref() else {
            return false;
        };

        let (matched, is_terminator) = in_flight.awaiting.classify(code);

        if !matched {
            return false;
        }

        let client_id = in_flight.client_id;
        let cmd_code = in_flight.cmd_code();

        if let Some(cid) = client_id {
            // If this is the MSG_SENT closing a SEND_*, stash the expected_ack
            // so the later 0x82 finds its way back. Lazy GC: purge stale
            // entries first so the map stays bounded (SENT also closes
            // login/status/telemetry requests, which never get an 0x82, so
            // without this they'd accumulate forever).
            if code == 0x06
                && payload.len() >= 6
                && let Ok(ack) = <[u8; 4]>::try_from(&payload[2..6])
            {
                let now = Instant::now();
                self.pending_acks.retain(|_, (_, inserted_at)| {
                    now.duration_since(*inserted_at) < PENDING_ACK_TTL
                });
                self.pending_acks.insert(ack, (cid, now));
            }

            self.send_to_client(&cid, payload.to_vec());
        }

        // An incoming LoRa message delivered as the reply to a GET_MSG is
        // a global event from the rest of the network's point of view:
        // every connected client should see it. Fan out to the other
        // clients (the originator just got it through send_to_client).
        if cmd_code == 0x0A && matches!(code, 0x07 | 0x08 | 0x10 | 0x11) {
            self.broadcast_to_others(client_id.as_ref(), payload);
        }

        if is_terminator {
            let done = self.in_flight.take().expect("checked Some above");
            if code != 0x01 {
                self.on_command_accepted(&done).await;
            }
            self.advance_queue().await;
        } else if let Some(f) = self.in_flight.as_mut() {
            f.last_activity = Instant::now();
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
            0x03 if payload.len() >= CONTACT_FRAME_LEN => {
                let pk = payload[1..33].to_vec();
                let contact_type = payload[33] as i64;
                let name_field = &payload[100..132];
                let name_end = name_field.iter().position(|&b| b == 0).unwrap_or(32);
                let name = String::from_utf8_lossy(&name_field[..name_end]).to_string();
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
                        raw: Some(payload[..CONTACT_FRAME_LEN].to_vec()),
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
                        .store_message(NewMessage {
                            msg_type: "contact",
                            from_key: from_key.as_deref(),
                            channel_idx: None,
                            text: &text,
                            timestamp: ts,
                            frame: payload,
                            origin: None,
                        })
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
                        .store_message(NewMessage {
                            msg_type: "channel",
                            from_key: None,
                            channel_idx,
                            text: &text,
                            timestamp: ts,
                            frame: payload,
                            origin: None,
                        })
                        .await
                    {
                        tracing::warn!(error = %e, "failed to cache channel message");
                    }
                }
            }
            0x10 if payload.len() >= 16 => {
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
                    .store_message(NewMessage {
                        msg_type: "contact",
                        from_key: from_key.as_deref(),
                        channel_idx: None,
                        text: &text,
                        timestamp: ts,
                        frame: payload,
                        origin: None,
                    })
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
                    .store_message(NewMessage {
                        msg_type: "channel",
                        from_key: None,
                        channel_idx: Some(channel_idx),
                        text: &text,
                        timestamp: ts,
                        frame: payload,
                        origin: None,
                    })
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

/// Rebuild a CONTACT frame for rows cached before raw frames were kept.
/// flags, path and lastmod were not stored, so they go out as zero.
fn synthesize_contact_frame(contact: &CachedContact) -> Vec<u8> {
    let mut c = Vec::with_capacity(CONTACT_FRAME_LEN);
    c.push(0x03);
    let mut pk_b = [0u8; 32];
    let pklen = contact.public_key.len().min(32);
    pk_b[..pklen].copy_from_slice(&contact.public_key[..pklen]);
    c.extend_from_slice(&pk_b);
    c.push(contact.contact_type as u8);
    c.push(0); // flags
    c.push(0); // out_path_len
    c.extend_from_slice(&[0u8; 64]); // out_path
    let mut name_b = [0u8; 32];
    let nlen = contact.name.len().min(32);
    name_b[..nlen].copy_from_slice(&contact.name.as_bytes()[..nlen]);
    c.extend_from_slice(&name_b);
    let last_advert = contact.last_advert.unwrap_or(0) as u32;
    c.extend_from_slice(&u32::to_le_bytes(last_advert));
    let lat_i = (contact.lat.unwrap_or(0.0) * 1_000_000.0) as i32;
    let lon_i = (contact.lon.unwrap_or(0.0) * 1_000_000.0) as i32;
    c.extend_from_slice(&i32::to_le_bytes(lat_i));
    c.extend_from_slice(&i32::to_le_bytes(lon_i));
    c.extend_from_slice(&[0u8; 4]); // lastmod
    c
}

/// APP_START: [0x01, app_ver, reserved(6), app_name]; the firmware reads
/// the name from offset 8.
fn build_appstart() -> Vec<u8> {
    let app_name = b"coresplitter";
    let mut payload = Vec::with_capacity(8 + app_name.len());
    payload.push(0x01);
    payload.push(1);
    payload.extend_from_slice(&[0; 6]);
    payload.extend_from_slice(app_name);
    payload
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_codes_are_the_high_bit_range() {
        assert!(is_push_event(0x80));
        assert!(is_push_event(0x82));
        assert!(is_push_event(0x8F));
        assert!(!is_push_event(0x00));
        assert!(!is_push_event(0x12));
    }

    #[test]
    fn late_reply_does_not_complete_other_command() {
        // A SELF_INFO that arrives late must not be taken as the
        // CHANNEL_INFO a GET_CHANNEL is waiting for.
        let get_channel = awaiting_for(&[0x1F, 0]);
        assert_eq!(get_channel.classify(0x05), (false, true));
        assert_eq!(get_channel.classify(0x12), (true, true));
        assert_eq!(get_channel.classify(0x01), (true, true));
    }

    #[test]
    fn channel_send_expects_ok_not_sent() {
        let send_chan = awaiting_for(&[0x03, 0, 0, 0, 0, 0, 0, b'x']);
        assert!(send_chan.classify(0x00).0);
        assert!(!send_chan.classify(0x06).0);
    }

    #[test]
    fn self_telemetry_reply_is_a_push() {
        let self_telemetry = awaiting_for(&[0x27, 0, 0, 0]);
        assert!(self_telemetry.classify(0x8B).0);
        let remote_telemetry = awaiting_for(&[0x27; 36]);
        assert!(!remote_telemetry.classify(0x8B).0);
        assert!(remote_telemetry.classify(0x06).0);
    }

    #[test]
    fn contacts_stream_ends_on_terminator_or_error() {
        let contacts = awaiting_for(&[0x04]);
        assert_eq!(contacts.classify(0x02), (true, false));
        assert_eq!(contacts.classify(0x03), (true, false));
        assert_eq!(contacts.classify(0x04), (true, true));
        assert_eq!(contacts.classify(0x01), (true, true));
        assert!(!contacts.classify(0x83).0);
    }

    #[test]
    fn unknown_command_takes_any_non_push() {
        let unknown = awaiting_for(&[0x7E]);
        assert!(unknown.classify(0x2A).0);
        assert!(!unknown.classify(0x83).0);
    }

    #[test]
    fn channel_echo_carries_node_name() {
        assert_eq!(channel_text_on_air(Some("Node"), b"hola"), b"Node: hola");
        assert_eq!(channel_text_on_air(None, b"hola"), b"hola");
        // Cut like the firmware: prefix plus text never exceed MAX_TEXT_LEN.
        let long = [b'x'; 200];
        let out = channel_text_on_air(Some("Node"), &long);
        assert_eq!(out.len(), MAX_TEXT_LEN);
        assert!(out.starts_with(b"Node: x"));
    }

    #[test]
    fn message_frames_follow_client_version() {
        // CHANNEL_MSG_RECV_V3: snr, 2 reserved, channel, path_len, txt_type, ts, text
        let v3 = [0x11, 0x2F, 0, 0, 1, 6, 0, 1, 2, 3, 4, b'h', b'i'];
        let v1 = [0x08, 1, 6, 0, 1, 2, 3, 4, b'h', b'i'];
        assert_eq!(&*message_frame_for(0, &v3), &v1);
        assert_eq!(&*message_frame_for(3, &v3), &v3);
        assert_eq!(&*message_frame_for(1, &v1), &v1);
        let upgraded = message_frame_for(3, &v1);
        assert_eq!(upgraded[..4], [0x11, 0, 0, 0]);
        assert_eq!(upgraded[4..], v3[4..]);

        // CONTACT_MSG_RECV_V3 → CONTACT_MSG_RECV
        let v3 = [
            0x10, 0x10, 0, 0, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0, 0, 1, 2, 3, 4,
        ];
        assert_eq!(message_frame_for(0, &v3)[..2], [0x07, 0xAA]);

        // Anything else is left alone.
        assert_eq!(&*message_frame_for(0, &[0x12, 1, 2]), &[0x12, 1, 2]);
        assert_eq!(&*message_frame_for(3, &[0x83]), &[0x83]);
    }
}
