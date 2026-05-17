use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};

use crate::protocol::frame::{Frame, FrameParser};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct ClientId(u64);

impl ClientId {
    pub fn new() -> Self {
        Self(NEXT_CLIENT_ID.fetch_add(1, Ordering::SeqCst))
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct ClientCommand {
    pub client_id: ClientId,
    pub payload: Vec<u8>,
}

pub type ClientDirectMap = Arc<Mutex<HashMap<ClientId, mpsc::UnboundedSender<Vec<u8>>>>>;

pub struct TcpFrontend {
    bind_addr: String,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    broadcast_tx: broadcast::Sender<Vec<u8>>,
    direct_map: ClientDirectMap,
    shutdown_rx: watch::Receiver<bool>,
}

impl TcpFrontend {
    pub fn new(
        bind_addr: String,
        command_tx: mpsc::UnboundedSender<ClientCommand>,
        broadcast_tx: broadcast::Sender<Vec<u8>>,
        direct_map: ClientDirectMap,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            bind_addr,
            command_tx,
            broadcast_tx,
            direct_map,
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.bind_addr).await?;
        tracing::info!(addr = %self.bind_addr, "TCP frontend listening");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    let command_tx = self.command_tx.clone();
                    let broadcast_rx = self.broadcast_tx.subscribe();
                    let direct_map = self.direct_map.clone();
                    tracing::info!(addr = %addr, "spawning client handler");
                    tokio::spawn(handle_client(stream, addr, command_tx, broadcast_rx, direct_map));
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("TCP frontend shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

async fn handle_client(
    stream: TcpStream,
    addr: SocketAddr,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    mut broadcast_rx: broadcast::Receiver<Vec<u8>>,
    direct_map: ClientDirectMap,
) {
    let client_id = ClientId::new();
    tracing::info!(client = %client_id, addr = %addr, "client connected");

    let (mut reader, mut writer) = stream.into_split();
    let mut parser = FrameParser::new();

    let (direct_tx, mut direct_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    direct_map.lock().unwrap().insert(client_id, direct_tx);

    let read_handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];

        loop {
            match tokio::io::AsyncReadExt::read(&mut reader, &mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let frames = parser.feed(&buf[..n]);
                    for payload in frames {
                        tracing::trace!(
                            client = %client_id,
                            len = payload.len(),
                            "received command from client"
                        );
                        let cmd = ClientCommand { client_id, payload };
                        if command_tx.send(cmd).is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(client = %client_id, error = %e, "client read error");
                    break;
                }
            }
        }
    });

    let write_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = broadcast_rx.recv() => {
                    match result {
                        Ok(payload) => {
                            let framed = Frame::encode_response(&payload);
                            if tokio::io::AsyncWriteExt::write_all(&mut writer, &framed).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    }
                }
                Some(payload) = direct_rx.recv() => {
                    let framed = Frame::encode_response(&payload);
                    if tokio::io::AsyncWriteExt::write_all(&mut writer, &framed).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = read_handle => {},
        _ = write_handle => {},
    }

    direct_map.lock().unwrap().remove(&client_id);
    tracing::info!(client = %client_id, addr = %addr, "client disconnected");
}
