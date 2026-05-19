#[cfg(feature = "ble")]
pub mod ble;
pub mod serial;
pub mod tcp;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub struct RadioIo {
    pub send_tx: mpsc::UnboundedSender<Vec<u8>>,
    pub recv_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    #[allow(dead_code)]
    pub disconnect_rx: tokio::sync::oneshot::Receiver<()>,
    pub _handle: JoinHandle<()>,
}
