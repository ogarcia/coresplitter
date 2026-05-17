use std::path::PathBuf;

use clap::Parser;

use crate::core::{BackendType, CoreConfig, LogLevel};

#[derive(Parser, Debug)]
#[command(name = "coresplitter", version, about = "MeshCore virtual node proxy")]
pub struct Cli {
    /// Serial port path (e.g., /dev/ttyUSB0)
    #[arg(long, short = 's', env = "SERIAL_PORT")]
    pub serial: Option<String>,

    /// Serial baud rate
    #[arg(long, default_value = "115200", env = "BAUD_RATE")]
    pub baud: u32,

    /// BLE device address (MAC or UUID)
    #[arg(long, short = 'b', env = "BLE_ADDRESS")]
    pub ble: Option<String>,

    /// BLE pairing PIN
    #[arg(long, default_value = "123456", env = "BLE_PIN")]
    pub ble_pin: String,

    /// TCP backend host (connect to remote radio/proxy)
    #[arg(long, short = 't', env = "TCP_BACKEND_HOST")]
    pub tcp_backend_host: Option<String>,

    /// TCP backend port
    #[arg(long, short = 'p', default_value = "5000", env = "TCP_BACKEND_PORT")]
    pub tcp_backend_port: Option<u16>,

    /// TCP frontend bind address
    #[arg(long, default_value = "0.0.0.0", env = "TCP_FRONTEND_HOST")]
    pub tcp_frontend_host: String,

    /// TCP frontend port
    #[arg(long, default_value_t = 5000, env = "TCP_FRONTEND_PORT")]
    pub tcp_frontend_port: u16,

    /// Node name (sent in SELF_INFO; defaults to the real radio's name)
    #[arg(long, default_value = "", env = "NODE_NAME")]
    pub node_name: String,

    /// Data directory for identity keys and state database
    #[arg(long, default_value = "./data", env = "DATA_DIR")]
    pub data_dir: PathBuf,

    /// Log level: off, error, warn, info, debug, verbose
    #[arg(long, default_value = "info", env = "LOG_LEVEL")]
    pub log_level: String,

    /// JSON event logging
    #[arg(long, env = "LOG_JSON")]
    pub json: bool,
}

impl Cli {
    pub fn into_config(self) -> CoreConfig {
        let backend_type = if self.serial.is_some() {
            BackendType::Serial
        } else if self.ble.is_some() {
            BackendType::Ble
        } else if self.tcp_backend_host.is_some() {
            BackendType::Tcp
        } else {
            // Default to TCP backend localhost:5000 for development
            BackendType::Serial
        };

        let event_log_level = match self.log_level.as_str() {
            "off" => LogLevel::Off,
            "verbose" => LogLevel::Verbose,
            _ => LogLevel::Summary,
        };

        CoreConfig {
            backend_type,
            serial_port: self.serial,
            serial_baud: self.baud,
            ble_address: self.ble,
            ble_pin: self.ble_pin,
            tcp_backend_host: self.tcp_backend_host,
            tcp_backend_port: self.tcp_backend_port,
            tcp_frontend_host: self.tcp_frontend_host,
            tcp_frontend_port: self.tcp_frontend_port,
            data_dir: self.data_dir,
            node_name: self.node_name,
            event_log_level,
            event_log_json: self.json,
        }
    }
}
