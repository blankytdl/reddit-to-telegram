//! One persistent Lightpanda process + a minimal CDP client over WebSocket.
//! One page at a time; the page (target) is closed after every fetch.
use crate::config::BrowserSettings;
use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{process::Stdio, time::Duration};
use tokio::{
    net::TcpStream,
    process::{Child, Command},
    time::{sleep, timeout, Instant},
};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Runs inside the page (old.reddit.com listing markup). Returns a JSON string.
pub const EXTRACT_JS: &str = r#"(() => {
  const posts = [];
  document.querySelectorAll('#siteTable > div.thing').forEach(el => {
    const a = el.querySelector('a.title');
    const flair = el.querySelector('.entry .linkflairlabel');
    const spoiler = el.getAttribute('data-spoiler') === 'true'
      || !!el.querySelector('.entry .spoiler-stamp')
      || (!!flair && /spoiler/i.test(flair.textContent || ''));
    posts.push({
      subreddit: el.getAttribute('data-subreddit') || '',
      spoiler: spoiler,
      fullname: el.getAttribute('data-fullname') || '',
      author: el.getAttribute('data-author') || '',
      permalink: el.getAttribute('data-permalink') || '',
      url: el.getAttribute('data-url') || '',
      title: a ? (a.textContent || '').trim() : '',
      stickied: el.classList.contains('stickied'),
      promoted: el.classList.contains('promoted') || el.getAttribute('data-promoted') === 'true'
    });
  });
  const body = (document.body && document.body.textContent) || '';
  return JSON.stringify({ title: document.title || '', url: location.href || '', text: body.slice(0, 400), posts: posts });
})()"#;

/// Returns the raw page text (used to read a post's JSON document in the browser).
pub const TEXT_JS: &str = "((document.body && document.body.textContent) || '')";

struct Cdp {
    ws: Ws,
    next_id: u64,
    events: Vec<String>,
}

impl Cdp {
    async fn call(&mut self, method: &str, params: Value, session: Option<&str>, to: Duration) -> Result<Value> {
        self.next_id += 1;
        let id = self.next_id;
        let mut msg = json!({ "id": id, "method": method, "params": params });
        if let Some(s) = session {
            msg["sessionId"] = json!(s);
        }
        self.ws.send(Message::Text(msg.to_string())).await?;
        let deadline = Instant::now() + to;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let m = timeout(left, self.ws.next())
                .await
                .map_err(|_| anyhow!("CDP timeout waiting for {method}"))?
                .ok_or_else(|| anyhow!("CDP socket closed"))??;
            if let Message::Text(t) = m {
                let v: Value = serde_json::from_str(&t)?;
                if v.get("id").and_then(Value::as_u64) == Some(id) {
                    if let Some(e) = v.get("error") {
                        bail!("CDP {method} error: {e}");
                    }
                    return Ok(v.get("result").cloned().unwrap_or(Value::Null));
                }
                if let Some(ev) = v.get("method").and_then(Value::as_str) {
                    if self.events.len() < 64 {
                        self.events.push(ev.to_string());
                    }
                }
            }
        }
    }

    /// Returns true if the event was seen before the timeout.
    async fn wait_event(&mut self, names: &[&str], to: Duration) -> bool {
        if self.events.iter().any(|e| names.contains(&e.as_str())) {
            return true;
        }
        let deadline = Instant::now() + to;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let Ok(Some(Ok(m))) = timeout(left, self.ws.next()).await else { return false };
            if let Message::Text(t) = m {
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    if v.get("method").and_then(Value::as_str).is_some_and(|m| names.contains(&m)) {
                        return true;
                    }
                }
            }
        }
    }
}

pub struct Browser {
    s: BrowserSettings,
    child: Option<Child>,
    cdp: Option<Cdp>,
}

impl Browser {
    pub fn new(s: BrowserSettings) -> Self {
        Self { s, child: None, cdp: None }
    }

    async fn ensure(&mut self) -> Result<()> {
        if let Some(c) = self.child.as_mut() {
            if c.try_wait()?.is_some() {
                warn!("browser process exited; restarting");
                self.child = None;
                self.cdp = None;
            }
        }
        if self.child.is_none() {
            let mut cmd = Command::new(&self.s.bin);
            cmd.arg("serve")
                .arg("--host")
                .arg("127.0.0.1")
                .arg("--port")
                .arg(self.s.port.to_string())
                .args(&self.s.extra_args)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .kill_on_drop(true);
            let child = cmd.spawn().map_err(|e| anyhow!("cannot start browser {}: {e}", self.s.bin))?;
            info!(pid = child.id(), "browser started");
            self.child = Some(child);
        }
        if self.cdp.is_none() {
            let url = format!("ws://127.0.0.1:{}", self.s.port);
            let mut last = None;
            for _ in 0..60 {
                match connect_async(url.clone()).await {
                    Ok((ws, _)) => {
                        self.cdp = Some(Cdp { ws, next_id: 0, events: Vec::new() });
                        return Ok(());
                    }
                    Err(e) => {
                        last = Some(e);
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            }
            self.shutdown().await;
            bail!("could not connect to browser CDP: {last:?}");
        }
        Ok(())
    }

    /// Navigates to `url` and returns the JSON string produced by EXTRACT_JS.
    pub async fn fetch(&mut self, url: &str) -> Result<String> {
        self.fetch_script(url, EXTRACT_JS).await
    }

    /// Navigates to `url` and returns the page's text content.
    pub async fn fetch_text(&mut self, url: &str) -> Result<String> {
        self.fetch_script(url, TEXT_JS).await
    }

    async fn fetch_script(&mut self, url: &str, script: &'static str) -> Result<String> {
        self.ensure().await?;
        let pt = self.s.page_timeout;
        let cdp = self.cdp.as_mut().ok_or_else(|| anyhow!("no CDP connection"))?;
        let r = timeout(pt, fetch_inner(cdp, url, pt, script)).await;
        match r {
            Ok(Ok(s)) => Ok(s),
            Ok(Err(e)) => {
                self.shutdown().await;
                Err(e)
            }
            Err(_) => {
                self.shutdown().await;
                bail!("page load timed out after {}s", pt.as_secs())
            }
        }
    }

    pub async fn shutdown(&mut self) {
        self.cdp = None;
        if let Some(mut c) = self.child.take() {
            let _ = c.start_kill();
            let _ = timeout(Duration::from_secs(5), c.wait()).await;
            info!("browser stopped");
        }
    }
}

async fn fetch_inner(cdp: &mut Cdp, url: &str, pt: Duration, script: &str) -> Result<String> {
    let step = Duration::from_secs(30).min(pt);
    let r = cdp.call("Target.createTarget", json!({ "url": "about:blank" }), None, step).await?;
    let target = r["targetId"].as_str().ok_or_else(|| anyhow!("no targetId"))?.to_string();
    let res = navigate_and_extract(cdp, &target, url, step, pt, script).await;
    let _ = cdp.call("Target.closeTarget", json!({ "targetId": target }), None, Duration::from_secs(5)).await;
    cdp.events.clear();
    res
}

async fn navigate_and_extract(cdp: &mut Cdp, target: &str, url: &str, step: Duration, pt: Duration, script: &str) -> Result<String> {
    let r = cdp.call("Target.attachToTarget", json!({ "targetId": target, "flatten": true }), None, step).await?;
    let sid = r["sessionId"].as_str().ok_or_else(|| anyhow!("no sessionId"))?.to_string();
    let s = Some(sid.as_str());
    cdp.call("Page.enable", json!({}), s, step).await?;
    cdp.events.clear();
    let nav = cdp.call("Page.navigate", json!({ "url": url }), s, step).await?;
    if let Some(err) = nav.get("errorText").and_then(Value::as_str) {
        bail!("navigation failed: {err}");
    }
    if !cdp.wait_event(&["Page.loadEventFired", "Page.frameStoppedLoading"], pt.min(Duration::from_secs(45))).await {
        warn!("no load event seen; extracting anyway");
    }
    let r = cdp
        .call("Runtime.evaluate", json!({ "expression": script, "returnByValue": true }), s, step)
        .await?;
    if let Some(ex) = r.get("exceptionDetails") {
        bail!("extraction script threw: {ex}");
    }
    r["result"]["value"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| anyhow!("extraction returned no string value"))
}
