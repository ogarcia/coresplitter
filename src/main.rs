#![allow(dead_code)]

mod backend;
mod cli;
mod core;
mod frontend;
mod logging;
mod node;
mod protocol;

use clap::Parser;
use tokio::signal;
use tokio::sync::watch;

async fn wait_for_shutdown() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        let mut sig = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        sig.recv().await;
    };

    tokio::select! {
        _ = ctrl_c => tracing::info!("received SIGINT"),
        _ = terminate => tracing::info!("received SIGTERM"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();
    logging::init(&cli.log_level, cli.json);

    std::fs::create_dir_all(&cli.data_dir)?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        wait_for_shutdown().await;
        let _ = shutdown_tx.send(true);
    });

    let config = cli.into_config();
    let mut core = core::Core::new(config, shutdown_rx).await?;
    core.run().await
}
