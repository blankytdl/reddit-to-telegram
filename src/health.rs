use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

/// Extra time allowed beyond the planned wait before we call the scheduler stalled.
const STALL_MARGIN_SECS: u64 = 25 * 60;

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

pub struct Health {
    last_beat: AtomicU64,
    deadline: AtomicU64,
    cycles: AtomicU64,
    stage: Mutex<String>,
}

pub fn is_stalled(now: u64, deadline: u64) -> bool {
    now > deadline
}

impl Health {
    pub fn new() -> Self {
        let n = now();
        Self { last_beat: AtomicU64::new(n), deadline: AtomicU64::new(n + 20 * 60), cycles: AtomicU64::new(0), stage: Mutex::new("starting".into()) }
    }
    /// Progress marker exposed by /health. `planned_wait` is the intentional idle period.
    pub fn set_stage(&self, stage: &str, planned_wait_secs: u64) {
        let n = now();
        self.last_beat.store(n, Ordering::Relaxed);
        self.deadline.store(n + planned_wait_secs + STALL_MARGIN_SECS, Ordering::Relaxed);
        if let Ok(mut current) = self.stage.lock() {
            *current = stage.chars().take(160).collect();
        }
    }

    pub fn beat(&self, planned_wait_secs: u64) {
        self.set_stage("scheduler", planned_wait_secs);
    }
    pub fn cycle_done(&self) {
        self.cycles.fetch_add(1, Ordering::Relaxed);
    }
}

async fn handler(State(h): State<Arc<Health>>) -> (StatusCode, Json<Value>) {
    let n = now();
    let stalled = is_stalled(n, h.deadline.load(Ordering::Relaxed));
    let stage = h.stage.lock().map(|s| s.clone()).unwrap_or_else(|_| "unavailable".into());
    let body = json!({
        "status": if stalled { "stalled" } else { "ok" },
        "seconds_since_progress": n.saturating_sub(h.last_beat.load(Ordering::Relaxed)),
        "completed_cycles": h.cycles.load(Ordering::Relaxed),
        "stage": stage,
    });
    (if stalled { StatusCode::SERVICE_UNAVAILABLE } else { StatusCode::OK }, Json(body))
}

pub async fn serve(port: u16, health: Arc<Health>, token: CancellationToken) -> anyhow::Result<()> {
    let app = Router::new().route("/health", get(handler)).route("/", get(|| async { "ok" })).with_state(health);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "health server listening");
    axum::serve(listener, app).with_graceful_shutdown(async move { token.cancelled().await }).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stall_detection() {
        assert!(!is_stalled(100, 100));
        assert!(is_stalled(101, 100));
        let h = Health::new();
        h.beat(0);
        assert!(!is_stalled(now(), h.deadline.load(Ordering::Relaxed)));
    }
}
