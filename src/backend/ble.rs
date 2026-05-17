use anyhow::{Context, Result};
use btleplug::api::{
    Central, CentralEvent, Manager as _, Peripheral as PeripheralTrait, ScanFilter, WriteType,
};
use btleplug::platform::{Adapter, Manager, PeripheralId};
use futures::StreamExt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::RadioIo;

const MESHCORE_TX_UUID: &str = "49535343-1E4D-4BD9-BA61-23C647249616";
const MESHCORE_RX_UUID: &str = "49535343-8841-43F4-A8D4-ECBE34729BB3";

pub async fn connect(address: &str, _pin: &str) -> Result<RadioIo> {
    let manager = Manager::new()
        .await
        .context("failed to create BLE manager")?;
    let adapters = manager.adapters().await.context("no BLE adapters found")?;
    let central = adapters
        .into_iter()
        .next()
        .context("no BLE adapter available")?;

    let peripheral_id = if address.contains('-') && address.len() > 20 {
        find_by_uuid(&central, address).await?
    } else {
        find_by_mac(&central, address).await?
    };

    let peripheral = central
        .peripheral(&peripheral_id)
        .await
        .context("peripheral not found")?;

    peripheral
        .connect()
        .await
        .context("failed to connect to BLE peripheral")?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    peripheral
        .discover_services()
        .await
        .context("failed to discover services")?;

    let chars = peripheral.characteristics();
    let tx_char = chars
        .iter()
        .find(|c| c.uuid.to_string() == MESHCORE_TX_UUID)
        .context("TX characteristic not found")?
        .clone();
    let rx_char = chars
        .iter()
        .find(|c| c.uuid.to_string() == MESHCORE_RX_UUID)
        .context("RX characteristic not found")?
        .clone();

    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (recv_tx, recv_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel::<()>();
    let mut disconnect_tx = Some(disconnect_tx);

    let handle: JoinHandle<()> = tokio::spawn(async move {
        if let Err(e) = peripheral.subscribe(&rx_char).await {
            tracing::error!(error = %e, "ble: failed to subscribe");
            return;
        }

        let mut notification_stream = match peripheral.notifications().await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "ble: failed to get notification stream");
                return;
            }
        };

        let notify_handle = tokio::spawn(async move {
            while let Some(notification) = notification_stream.next().await {
                if notification.uuid.to_string() == MESHCORE_RX_UUID {
                    tracing::trace!(len = notification.value.len(), "ble: received notification");
                    let _ = recv_tx.send(notification.value);
                }
            }
        });

        let write_handle = tokio::spawn(async move {
            while let Some(data) = send_rx.recv().await {
                if let Err(e) = peripheral
                    .write(&tx_char, &data, WriteType::WithoutResponse)
                    .await
                {
                    tracing::error!(error = %e, "ble: write error");
                    break;
                }
                tracing::trace!(len = data.len(), "ble: sent data");
            }
        });

        tokio::select! {
            _ = notify_handle => {},
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

async fn find_by_mac(central: &Adapter, mac: &str) -> Result<PeripheralId> {
    central
        .start_scan(ScanFilter::default())
        .await
        .context("failed to start BLE scan")?;

    let mut events = central
        .events()
        .await
        .context("failed to get BLE event stream")?;

    let mac_upper = mac.to_uppercase();
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            event = events.next() => {
                if let Some(CentralEvent::DeviceDiscovered(id)) = event
                    && let Ok(p) = central.peripheral(&id).await
                    && let Ok(Some(props)) = p.properties().await
                    && let Some(ref name) = props.local_name
                    && name.to_uppercase() == mac_upper
                {
                    let _ = central.stop_scan().await;
                    return Ok(id);
                }
            }
            _ = &mut timeout => {
                let _ = central.stop_scan().await;
                anyhow::bail!("BLE device with MAC {mac} not found (timeout)");
            }
        }
    }
}

async fn find_by_uuid(central: &Adapter, uuid: &str) -> Result<PeripheralId> {
    central
        .start_scan(ScanFilter::default())
        .await
        .context("failed to start BLE scan")?;

    let mut events = central
        .events()
        .await
        .context("failed to get BLE event stream")?;

    let uuid_upper = uuid.to_uppercase();
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            event = events.next() => {
                if let Some(CentralEvent::DeviceDiscovered(id)) = event
                    && id.to_string().to_uppercase() == uuid_upper
                {
                    let _ = central.stop_scan().await;
                    return Ok(id);
                }
            }
            _ = &mut timeout => {
                let _ = central.stop_scan().await;
                anyhow::bail!("BLE device with UUID {uuid} not found (timeout)");
            }
        }
    }
}
