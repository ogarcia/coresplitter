use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

pub fn init(log_level: &str, json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let level = match log_level {
            "off" => "error",
            "error" => "error",
            "warn" => "warn",
            "info" => "info",
            "debug" => "debug",
            "verbose" => "debug",
            _ => "info",
        };
        EnvFilter::new(format!("coresplitter={level}"))
    });

    if json {
        let fmt_layer = fmt::layer().json().with_target(true).with_level(true);
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
    } else {
        let fmt_layer = fmt::layer().with_target(false).with_level(true).compact();
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
    }
}
