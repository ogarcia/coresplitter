use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{self, Duration};

use crate::backend;
use crate::frontend::tcp::{ClientCommand, ClientDirectMap, ClientId, TcpFrontend};
use crate::node::identity::Identity;
use crate::node::state::{CachedChannel, CachedContact, NodeState};
use crate::protocol::decode::{
    DecodedValue, decode_command_payload, decode_response_payload, format_decoded,
};

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
    pub ble_pin: String,
    pub tcp_backend_host: Option<String>,
    pub tcp_backend_port: Option<u16>,
    pub tcp_frontend_host: String,
    pub tcp_frontend_port: u16,
    pub data_dir: PathBuf,
    pub node_name: String,
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
    identity: Identity,
    state: Arc<NodeState>,
    self_info: Option<HashMap<String, String>>,
    device_info: Option<HashMap<String, String>>,
    max_channels: u8,
    radio_send_tx: Option<mpsc::UnboundedSender<Vec<u8>>>,
    radio_recv_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    shutdown_rx: watch::Receiver<bool>,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    command_rx: mpsc::UnboundedReceiver<ClientCommand>,
    broadcast_tx: broadcast::Sender<Vec<u8>>,
    client_channels: ClientDirectMap,
}

impl Core {
    pub async fn new(config: CoreConfig, shutdown_rx: watch::Receiver<bool>) -> Result<Self> {
        let state = NodeState::open(config.data_dir.join("state.db")).await?;

        let mut identity = Identity::new(config.node_name.clone());
        if !identity.load_from_db(&state).await? {
            identity.generate();
            identity.save_to_db(&state).await?;
        }
        if identity.name.is_empty() {
            identity.name = "Core Splitter".into();
        }

        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(256);

        let (_, dummy_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        Ok(Self {
            config,
            identity,
            state: Arc::new(state),
            self_info: None,
            device_info: None,
            max_channels: 40,
            radio_send_tx: None,
            radio_recv_rx: dummy_rx,
            shutdown_rx,
            command_tx,
            command_rx,
            broadcast_tx,
            client_channels: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        tracing::info!(
            node_name = %self.identity.name,
            pubkey = %hex::encode(&self.identity.public_key[..8]),
            "starting coresplitter virtual node"
        );

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

        tracing::info!("virtual node ready, accepting clients");

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
            }
        }

        tracing::info!("performing graceful shutdown");
        self.shutdown().await;

        Ok(())
    }

    async fn shutdown(&mut self) {
        tracing::info!("shutting down virtual node");

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
                let port = self
                    .config
                    .tcp_backend_port
                    .context("TCP backend port not specified")?;
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
                            if let Some(decoded) = decode_response_payload(payload[0], &payload) {
                                let mut info = HashMap::new();
                                if let Some(DecodedValue::String(name)) = decoded.get("name") {
                                    info.insert("name".into(), name.clone());
                                    if self.config.node_name.is_empty() {
                                        self.identity.name = name.clone();
                                        tracing::info!(name = %name, "adopted real radio node name");
                                    }
                                }
                                if let Some(DecodedValue::String(pk)) = decoded.get("public_key") {
                                    info.insert("public_key".into(), pk.clone());
                                }
                                if let Some(DecodedValue::String(ntype)) = decoded.get("type") {
                                    info.insert("node_type".into(), ntype.clone());
                                }
                                self.self_info = Some(info);
                                tracing::info!("radio initialized, received SELF_INFO");
                                return Ok(());
                            } else {
                                anyhow::bail!("failed to decode SELF_INFO");
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
                            if let Some(decoded) = decode_response_payload(0x0D, &payload) {
                                if let Some(DecodedValue::Integer(n)) = decoded.get("max_channels") {
                                    let max = (*n).min(255) as u8;
                                    if max > 0 {
                                        self.max_channels = max;
                                    }
                                    tracing::info!(max_channels = self.max_channels, "radio reports max channels");
                                }
                                let mut info = HashMap::new();
                                for (k, v) in &decoded {
                                    match v {
                                        DecodedValue::String(s) => { info.insert(k.clone(), s.clone()); }
                                        DecodedValue::Integer(i) => { info.insert(k.clone(), i.to_string()); }
                                        DecodedValue::Float(f) => { info.insert(k.clone(), f.to_string()); }
                                        _ => {}
                                    }
                                }
                                self.device_info = Some(info);
                                return Ok(());
                            } else {
                                anyhow::bail!("failed to decode DEVICE_INFO");
                            }
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
        tracing::info!("syncing contacts and channels from physical radio");

        // Query contacts
        let get_contacts = vec![0x04];
        let _ = self.send_to_radio(&get_contacts).await;

        // Give the radio a moment to respond before querying channels
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Query channels 0..max_channels. The radio responds with CHANNEL_INFO
        // for configured channels and nothing for unconfigured ones.
        for idx in 0..self.max_channels {
            let get_channel = vec![0x1F, idx];
            if self.send_to_radio(&get_channel).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tracing::info!("sync complete");
    }

    async fn send_to_radio(&self, data: &[u8]) -> Result<()> {
        if let Some(ref tx) = self.radio_send_tx {
            tx.send(data.to_vec()).context("failed to send to radio")?;
        }
        Ok(())
    }

    async fn handle_client_command(&mut self, cmd: ClientCommand) {
        let payload = &cmd.payload;
        if payload.is_empty() {
            return;
        }

        let cmd_code = payload[0];
        self.log_event("->", cmd_code, payload);

        match cmd_code {
            0x01 => self.respond_self_info(&cmd.client_id).await,
            0x16 => {
                if let Some(ref info) = self.device_info.clone() {
                    self.respond_device_info(&cmd.client_id, info).await;
                } else {
                    let _ = self.send_to_radio(payload).await;
                }
            }
            0x04 => {
                if let Ok(contacts) = self.state.get_contacts().await
                    && !contacts.is_empty()
                {
                    self.respond_contacts(&cmd.client_id, &contacts).await;
                    return;
                }
                let _ = self.send_to_radio(payload).await;
            }
            0x1F => {
                let requested_idx = if payload.len() > 1 {
                    payload[1] as i64
                } else {
                    -1
                };
                if let Ok(channels) = self.state.get_channels().await {
                    tracing::info!(n = channels.len(), "GET_CHANNEL: channels in DB");
                    if let Some(ch) = channels.iter().find(|c| c.idx == requested_idx) {
                        self.respond_channel_info(&cmd.client_id, ch).await;
                        return;
                    }
                }
                tracing::info!("GET_CHANNEL: forwarding to radio");
                let _ = self.send_to_radio(payload).await;
            }
            0x02 | 0x03 => {
                // Broadcast outgoing message to all other clients,
                // then forward to the physical radio.
                self.broadcast_to_others(Some(&cmd.client_id), payload);
                let _ = self.send_to_radio(payload).await;

                // Persist outgoing message so it survives restart even
                // if the radio does not echo it back.
                let from_key = &self.identity.public_key[..6];
                if payload[0] == 0x02 && payload.len() >= 14 {
                    let ts = u32::from_le_bytes(payload[3..7].try_into().unwrap_or([0; 4])) as i64;
                    let text = String::from_utf8_lossy(&payload[13..]).to_string();
                    let _ = self
                        .state
                        .insert_message("contact", Some(from_key), None, &text, ts)
                        .await;
                } else if payload[0] == 0x03 && payload.len() >= 8 {
                    let channel = payload[2] as i64;
                    let ts = u32::from_le_bytes(payload[3..7].try_into().unwrap_or([0; 4])) as i64;
                    let text = String::from_utf8_lossy(&payload[7..]).to_string();
                    let _ = self
                        .state
                        .insert_message("channel", Some(from_key), Some(channel), &text, ts)
                        .await;
                }
            }
            0x20 if payload.len() > 1 => {
                // Invalidate cached channel so next GET_CHANNEL hits the radio
                let idx = payload[1] as i64;
                let _ = self.state.delete_channel(idx).await;
                let _ = self.send_to_radio(payload).await;
            }
            _ => {
                let _ = self.send_to_radio(payload).await;
            }
        }
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

    async fn respond_self_info(&self, client_id: &ClientId) {
        let mut payload = vec![0x05];
        payload.push(1);
        payload.push(0);
        payload.push(20);
        payload.extend_from_slice(&self.identity.public_key);
        payload.extend_from_slice(&[0u8; 4]);
        payload.extend_from_slice(&[0u8; 4]);
        payload.push(0); // multi_acks
        payload.push(0); // adv_loc_policy
        payload.push(0); // telemetry_mode
        payload.push(0); // manual_add_contacts
        payload.extend_from_slice(&u32::to_le_bytes(868_000));
        payload.extend_from_slice(&u32::to_le_bytes(125_000));
        payload.push(12);
        payload.push(5);
        payload.extend_from_slice(self.identity.name.as_bytes());

        self.send_to_client(client_id, payload);
        tracing::debug!("responded with SELF_INFO");
    }

    async fn respond_device_info(&self, client_id: &ClientId, info: &HashMap<String, String>) {
        let fw_ver: u8 = info
            .get("fw_version")
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);
        let mut payload = vec![0x0D, fw_ver];

        if fw_ver >= 3 {
            let max_contacts: u8 = info
                .get("max_contacts")
                .and_then(|v| v.parse().ok())
                .unwrap_or(100);
            let max_channels: u8 = info
                .get("max_channels")
                .and_then(|v| v.parse().ok())
                .unwrap_or(40);
            payload.push(max_contacts / 2);
            payload.push(max_channels);
            payload.extend_from_slice(&u32::to_le_bytes(123456));

            let build = info
                .get("fw_build")
                .map(|s| s.as_bytes())
                .unwrap_or(b"coresplitter");
            let mut build_b = [0u8; 12];
            let blen = build.len().min(12);
            build_b[..blen].copy_from_slice(&build[..blen]);
            payload.extend_from_slice(&build_b);

            let model = info
                .get("model")
                .map(|s| s.as_bytes())
                .unwrap_or(b"Core Splitter Virtual Node");
            let mut model_b = [0u8; 40];
            let mlen = model.len().min(40);
            model_b[..mlen].copy_from_slice(&model[..mlen]);
            payload.extend_from_slice(&model_b);

            let version = b"0.1.0";
            let mut ver_b = [0u8; 20];
            let vlen = version.len().min(20);
            ver_b[..vlen].copy_from_slice(&version[..vlen]);
            payload.extend_from_slice(&ver_b);
        }

        self.send_to_client(client_id, payload);
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

        let _ = self.broadcast_tx.send(payload);
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
                if let Some(decoded) = decode_response_payload(code, payload) {
                    let mut info = HashMap::new();
                    if let Some(DecodedValue::String(name)) = decoded.get("name") {
                        info.insert("name".into(), name.clone());
                    }
                    if let Some(DecodedValue::String(pk)) = decoded.get("public_key") {
                        info.insert("public_key".into(), pk.clone());
                    }
                    self.self_info = Some(info);
                }
            }
            0x0D => {
                if let Some(decoded) = decode_response_payload(code, payload) {
                    let mut info = HashMap::new();
                    for (k, v) in &decoded {
                        match v {
                            DecodedValue::String(s) => {
                                info.insert(k.clone(), s.clone());
                            }
                            DecodedValue::Integer(i) => {
                                info.insert(k.clone(), i.to_string());
                            }
                            DecodedValue::Float(f) => {
                                info.insert(k.clone(), f.to_string());
                            }
                            _ => {}
                        }
                    }
                    self.device_info = Some(info);
                }
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
                            DecodedValue::Bytes(b) => hex::encode(b),
                            DecodedValue::Map(m) => format!("{m:?}"),
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
