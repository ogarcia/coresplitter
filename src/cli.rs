use std::path::PathBuf;

use clap::Parser;

use crate::core::{BackendType, CoreConfig, LogLevel};

#[derive(Parser, Debug)]
#[command(
    name = "coresplitter",
    version,
    about = "MeshCore client multiplexer with cache"
)]
pub struct Cli {
    /// Serial port path (e.g., /dev/ttyUSB0)
    #[arg(long, short = 's', env = "CORESPLITTER_SERIAL_PORT")]
    pub serial: Option<String>,

    /// Serial baud rate
    #[arg(long, default_value = "115200", env = "CORESPLITTER_BAUD_RATE")]
    pub baud: u32,

    /// BLE device address (MAC or UUID)
    #[arg(long, short = 'b', env = "CORESPLITTER_BLE_ADDRESS")]
    pub ble: Option<String>,

    /// BLE pairing PIN
    #[arg(long, default_value = "123456", env = "CORESPLITTER_BLE_PIN")]
    pub ble_pin: String,

    /// TCP backend host (connect to remote radio/proxy)
    #[arg(long, short = 't', env = "CORESPLITTER_TCP_BACKEND_HOST")]
    pub tcp_backend_host: Option<String>,

    /// TCP backend port
    #[arg(
        long,
        short = 'p',
        default_value_t = 5000,
        env = "CORESPLITTER_TCP_BACKEND_PORT"
    )]
    pub tcp_backend_port: u16,

    /// TCP frontend bind address
    #[arg(
        long,
        default_value = "0.0.0.0",
        env = "CORESPLITTER_TCP_FRONTEND_HOST"
    )]
    pub tcp_frontend_host: String,

    /// TCP frontend port
    #[arg(long, default_value_t = 5000, env = "CORESPLITTER_TCP_FRONTEND_PORT")]
    pub tcp_frontend_port: u16,

    /// Data directory for the state database
    #[arg(long, default_value = "./data", env = "CORESPLITTER_DATA_DIR")]
    pub data_dir: PathBuf,

    /// Log level: off, error, warn, info, debug, verbose
    #[arg(long, default_value = "info", env = "CORESPLITTER_LOG_LEVEL")]
    pub log_level: String,

    /// JSON event logging
    #[arg(long, env = "CORESPLITTER_LOG_JSON")]
    pub json: bool,

    /// Record all raw radio RX payloads into the raw_rx database table
    #[arg(long, env = "CORESPLITTER_RECORD_RADIO_RX")]
    pub record_radio_rx: bool,
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
            event_log_level,
            event_log_json: self.json,
            record_radio_rx: self.record_radio_rx,
        }
    }
}
