use std::path::PathBuf;

use clap::Parser;

use crate::core::{BackendType, CoreConfig, LogLevel};
use crate::node::state::{Backlog, Retention};

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

    /// TCP frontend port(s), comma separated. Clients are told apart by
    /// port and app name, so a second port lets two copies of the same
    /// app keep separate message histories.
    #[arg(
        long,
        default_value = "5000",
        value_delimiter = ',',
        num_args = 1..,
        env = "CORESPLITTER_TCP_FRONTEND_PORT"
    )]
    pub tcp_frontend_port: Vec<u16>,

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

    /// Don't fetch the radio's queued messages while no client is connected
    #[arg(
        long = "no-drain-offline",
        action = clap::ArgAction::SetFalse,
        env = "CORESPLITTER_DRAIN_OFFLINE"
    )]
    pub drain_offline: bool,

    /// Days to keep stored messages (0 = forever)
    #[arg(
        long,
        default_value_t = 0,
        env = "CORESPLITTER_MESSAGES_RETENTION_DAYS"
    )]
    pub messages_retention_days: u32,

    /// Days to keep raw_rx recordings (0 = forever)
    #[arg(long, default_value_t = 7, env = "CORESPLITTER_RAW_RX_RETENTION_DAYS")]
    pub raw_rx_retention_days: u32,

    /// Days a contact may be missing from the radio before it is dropped
    /// from the cache (0 = never)
    #[arg(
        long,
        default_value_t = 0,
        env = "CORESPLITTER_CONTACTS_RETENTION_DAYS"
    )]
    pub contacts_retention_days: u32,

    /// Days a client may stay away before it is forgotten; if it comes
    /// back it is treated as new (0 = never)
    #[arg(
        long,
        default_value_t = 180,
        env = "CORESPLITTER_CLIENTS_RETENTION_DAYS"
    )]
    pub clients_retention_days: u32,

    /// Messages replayed to a client seen for the first time: 0 = none,
    /// N = the last N messages, Nh / Nd = those of the last N hours / days
    #[arg(long, default_value = "0", env = "CORESPLITTER_NEW_CLIENT_BACKLOG")]
    pub new_client_backlog: Backlog,
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
            tcp_frontend_ports: self.tcp_frontend_port,
            data_dir: self.data_dir,
            event_log_level,
            event_log_json: self.json,
            record_radio_rx: self.record_radio_rx,
            drain_offline: self.drain_offline,
            retention: Retention {
                messages_days: self.messages_retention_days,
                raw_rx_days: self.raw_rx_retention_days,
                contacts_days: self.contacts_retention_days,
                clients_days: self.clients_retention_days,
            },
            new_client_backlog: self.new_client_backlog,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn drain_offline_is_on_unless_disabled() {
        let parse = |args: &[&str]| Cli::try_parse_from(args).unwrap().drain_offline;
        // SAFETY: the only test touching this variable.
        unsafe { std::env::remove_var("CORESPLITTER_DRAIN_OFFLINE") };
        assert!(parse(&["coresplitter"]));
        assert!(!parse(&["coresplitter", "--no-drain-offline"]));
        unsafe { std::env::set_var("CORESPLITTER_DRAIN_OFFLINE", "false") };
        assert!(!parse(&["coresplitter"]));
        unsafe { std::env::set_var("CORESPLITTER_DRAIN_OFFLINE", "true") };
        assert!(parse(&["coresplitter"]));
        unsafe { std::env::remove_var("CORESPLITTER_DRAIN_OFFLINE") };
    }
}
