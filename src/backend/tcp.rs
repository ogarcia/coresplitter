use anyhow::{Context as _, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::protocol::frame;

use super::RadioIo;

pub async fn connect(host: &str, port: u16) -> Result<RadioIo> {
    let addr = format!("{host}:{port}");
    let stream = TcpStream::connect(&addr)
        .await
        .context(format!("failed to connect to TCP backend at {addr}"))?;

    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (recv_tx, recv_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel::<()>();
    let mut disconnect_tx = Some(disconnect_tx);

    let handle: JoinHandle<()> = tokio::spawn(async move {
        let (mut reader, mut writer) = stream.into_split();

        let read_handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let mut parser = crate::protocol::frame::FrameParser::new();

            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        tracing::warn!("tcp backend: connection closed");
                        break;
                    }
                    Ok(n) => {
                        let frames = parser.feed(&buf[..n]);
                        for payload in frames {
                            tracing::trace!(len = payload.len(), "tcp backend: received frame");
                            let _ = recv_tx.send(payload);
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "tcp backend: read error");
                        break;
                    }
                }
            }
        });

        let write_handle = tokio::spawn(async move {
            while let Some(data) = send_rx.recv().await {
                let framed = frame::encode_command(&data);
                if let Err(e) = writer.write_all(&framed).await {
                    tracing::error!(error = %e, "tcp backend: write error");
                    break;
                }
                if let Err(e) = writer.flush().await {
                    tracing::error!(error = %e, "tcp backend: flush error");
                    break;
                }
                tracing::trace!(len = data.len(), "tcp backend: sent data");
            }
        });

        tokio::select! {
            _ = read_handle => {},
            _ = write_handle => {},
        }

        if let Some(tx) = disconnect_tx.take() {
            let _ = tx.send(());
        }
    });

    Ok(RadioIo {
        send_tx,
        recv_rx,
        disconnect_rx,
        _handle: handle,
    })
}
