use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_serial::SerialPortBuilderExt;

use super::RadioIo;

pub async fn connect(path: &str, baud: u32) -> Result<RadioIo> {
    let mut serial = tokio_serial::new(path, baud)
        .open_native_async()
        .context("failed to open serial port")?;

    serial
        .set_exclusive(false)
        .context("failed to set serial exclusive mode")?;

    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (recv_tx, recv_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel::<()>();
    let mut disconnect_tx = Some(disconnect_tx);

    let handle: JoinHandle<()> = tokio::spawn(async move {
        let (mut reader, mut writer) = tokio::io::split(serial);

        let read_handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        tracing::warn!("serial: end of stream");
                        break;
                    }
                    Ok(n) => {
                        let data = buf[..n].to_vec();
                        tracing::trace!(len = n, "serial: received data");
                        if recv_tx.send(data).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "serial: read error");
                        break;
                    }
                }
            }
        });

        let write_handle = tokio::spawn(async move {
            while let Some(data) = send_rx.recv().await {
                if let Err(e) = writer.write_all(&data).await {
                    tracing::error!(error = %e, "serial: write error");
                    break;
                }
                if let Err(e) = writer.flush().await {
                    tracing::error!(error = %e, "serial: flush error");
                    break;
                }
                tracing::trace!(len = data.len(), "serial: sent data");
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
