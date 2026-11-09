//! Best-effort self-ping of the public /health URL. This only generates inbound
//! traffic while the instance is already running; it cannot wake a spun-down service.
use reqwest::Client;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub async fn run(client: Client, base_url: String, every: Duration, token: CancellationToken) {
    let url = format!("{base_url}/health");
    let mut tick = interval(every);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick.tick().await; // first tick fires immediately; skip it
    info!(%url, secs = every.as_secs(), "self-ping enabled");
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = tick.tick() => {}
        }
        match client.get(&url).timeout(Duration::from_secs(15)).send().await {
            Ok(r) if r.status().is_success() => info!(status = r.status().as_u16(), "self-ping ok"),
            Ok(r) => warn!(status = r.status().as_u16(), "self-ping got non-success status"),
            Err(e) => warn!("self-ping failed: {}", e.without_url()),
        }
    }
}
