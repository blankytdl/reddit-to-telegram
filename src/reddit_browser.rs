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
    const nsfw = el.classList.contains('over18')
      || el.classList.contains('nsfw')
      || el.getAttribute('data-nsfw') === 'true';
    let created = Math.floor(Number(el.getAttribute('data-timestamp'))) || 0;
    if (!created) {
      const t = el.querySelector('time[datetime]');
      const parsed = t ? Date.parse(t.getAttribute('datetime') || '') : NaN;
      created = Number.isFinite(parsed) ? parsed : 0;
    }
    posts.push({
      created_ms: created,
      subreddit: el.getAttribute('data-subreddit') || '',
      spoiler: spoiler,
      nsfw: nsfw,
      fullname: el.getAttribute('data-fullname') || '',
      author: el.getAttribute('data-author') || '',
      permalink: el.getAttribute('data-permalink') || '',
      url: el.getAttribute('data-url') || '',
      title: a ? (a.textContent || '').trim() : '',
      stickied: el.classList.contains('stickied'),
      promoted: el.classList.contains('promoted') || el.getAttribute('data-promoted') === 'true'
    });
  });
  let after = '';
  const nextLink = document.querySelector('.nav-buttons .next-button a, span.next-button a');
  if (nextLink) {
    const m = /[?&]after=([^&#]+)/.exec(nextLink.getAttribute('href') || '');
    if (m) { try { after = decodeURIComponent(m[1]); } catch (_) { after = m[1]; } }
  }
  const body = (document.body && document.body.textContent) || '';
  return JSON.stringify({ title: document.title || '', url: location.href || '', text: body.slice(0, 400), after: after, posts: posts });
})()"#;

/// Returns the raw page text (used to read a post's JSON document in the browser).
pub const TEXT_JS: &str = "((document.body && document.body.textContent) || '')";

/// Extract all media from Reddit's normal HTML gallery page. This is a DOM
/// fallback for networks where Reddit's comments .json document is unavailable.
pub const GALLERY_JS: &str = r#"(() => {
  // Reddit inserts gallery-carousel asynchronously through
  // shreddit-async-loader[bundlename="post_detail_gallery"].
  // Wait for that loader and traverse open shadow roots where present.
  const deepFind = (root, selector) => {
    if (!root) return null;
    try {
      const direct = root.querySelector && root.querySelector(selector);
      if (direct) return direct;
      if (root.shadowRoot) {
        const nested = deepFind(root.shadowRoot, selector);
        if (nested) return nested;
      }
      const elements = root.querySelectorAll ? root.querySelectorAll('*') : [];
      for (const el of elements) {
        if (el.shadowRoot) {
          const nested = deepFind(el.shadowRoot, selector);
          if (nested) return nested;
        }
      }
    } catch (_) {}
    return null;
  };
  const deepAll = (root, selector) => {
    const out = [];
    const seen = new Set();
    const visit = scope => {
      if (!scope || seen.has(scope)) return;
      seen.add(scope);
      try {
        if (scope.querySelectorAll) {
          for (const node of scope.querySelectorAll(selector)) out.push(node);
          const elements = scope.querySelectorAll('*');
          for (const el of elements) if (el.shadowRoot) visit(el.shadowRoot);
        }
        if (scope.shadowRoot) visit(scope.shadowRoot);
      } catch (_) {}
    };
    visit(root);
    return out;
  };

  const allowedImage = u => /^https:\/\/(?:i|preview|external-preview)\.redd\.it\//i.test(u);
  const allowedVideo = u => /^https:\/\/(?:v|preview|external-preview)\.redd\.it\//i.test(u);
  const cleanImage = raw => {
    if (!raw) return '';
    let u = String(raw).replace(/&amp;/g, '&');
    if (u.startsWith('//')) u = 'https:' + u;
    // Reddit's original image is the filename after the optional v0 marker.
    try {
      const parsed = new URL(u);
      if (/^(preview|external-preview)\.redd\.it$/i.test(parsed.hostname)) {
        const parts = parsed.pathname.split('/').filter(Boolean);
        let file = parts[parts.length - 1] || '';
        const marker = file.lastIndexOf('-v0-');
        if (marker >= 0) file = file.slice(marker + 4);
        if (/\.(jpe?g|png|gif|webp)$/i.test(file)) return 'https://i.redd.it/' + file;
      }
    } catch (_) {}
    return u;
  };

  const extract = () => {
    const posts = deepAll(document, 'shreddit-post');
    const post = posts.find(el => (el.getAttribute('post-type') || '').toLowerCase() === 'gallery')
      || posts.find(el => el.getAttribute('post-id') || el.getAttribute('id'));
    const loaderSelector = 'shreddit-async-loader[bundlename="post_detail_gallery"], shreddit-async-loader[bundlename="gallery_carousel"]';
    const loader = (post && deepFind(post, loaderSelector)) || deepFind(document, loaderSelector);
    const carousel = (loader && deepFind(loader, 'gallery-carousel, shreddit-gallery-carousel'))
      || (post && deepFind(post, 'gallery-carousel, shreddit-gallery-carousel'))
      || deepFind(document, 'gallery-carousel, shreddit-gallery-carousel');

    const out = [];
    const seen = new Set();
    const add = (raw, kind) => {
      if (!raw) return;
      const url = String(raw).replace(/&amp;/g, '&');
      const valid = kind === 'photo' || kind === 'animation' ? allowedImage(url) : allowedVideo(url);
      if (!valid || seen.has(url)) return;
      seen.add(url);
      out.push({ url, kind });
    };

    let slides = carousel ? deepAll(carousel, 'ul > li[slot^="page-"], ul > li, li[slot^="page-"]') : [];
    slides = Array.from(new Set(slides));
    let imageCount = 0;
    let playerCount = 0;

    for (const slide of slides) {
      const players = deepAll(slide, 'shreddit-player, shreddit-player-2, shreddit-player-static, shreddit-player-static-hlsjs');
      const videos = deepAll(slide, 'video');
      const player = players[0];
      if (player) playerCount++;

      let videoUrl = '';
      const packaged = player && player.getAttribute('packaged-media-json');
      if (packaged) {
        try {
          const parsed = JSON.parse(packaged);
          const playback = parsed && parsed.playbackMp4s;
          const permutations = playback && Array.isArray(playback.permutations) ? playback.permutations.slice() : [];
          permutations.sort((a, b) =>
            ((b.source && b.source.dimensions && b.source.dimensions.height) || 0) -
            ((a.source && a.source.dimensions && a.source.dimensions.height) || 0));
          for (const candidate of permutations) {
            const candidateUrl = candidate && candidate.source && candidate.source.url;
            if (candidateUrl && allowedVideo(candidateUrl)) {
              videoUrl = candidateUrl;
              break;
            }
          }
        } catch (_) {}
      }
      if (!videoUrl && player) {
        const source = deepFind(player, 'video source[src], source[src], video[src]');
        videoUrl = (source && (source.getAttribute('src') || source.src))
          || player.getAttribute('src')
          || player.getAttribute('preview')
          || '';
      }
      if (!videoUrl && videos.length) {
        const video = videos[0];
        const source = deepFind(video, 'source[src]');
        videoUrl = (source && (source.getAttribute('src') || source.src))
          || video.getAttribute('src') || video.src || '';
      }
      if (videoUrl && allowedVideo(videoUrl)) {
        add(videoUrl, /\.mpd(?:\?|$)/i.test(videoUrl) || /\.m3u8(?:\?|$)/i.test(videoUrl) ? 'dash_video' : 'video');
        continue;
      }

      const imgs = deepAll(slide, 'figure img, img');
      const img = imgs.find(el => {
        const candidate = cleanImage(el.currentSrc || el.getAttribute('data-lazy-src') || el.getAttribute('src') || '');
        return allowedImage(candidate);
      });
      if (img) {
        imageCount++;
        const url = cleanImage(img.currentSrc || img.getAttribute('data-lazy-src') || img.getAttribute('src') || '');
        add(url, /\.gif(?:[?#]|$)/i.test(url) ? 'animation' : 'photo');
      }
    }

    // old.reddit post pages render galleries as plain tiles (no custom elements).
    let oldRedditCount = 0;
    if (out.length === 0) {
      const tiles = deepAll(document, '.gallery-preview a[href], .gallery-tile a[href], a.gallery-item-thumbnail-link[href], .gallery-preview img, .gallery-tile img');
      for (const node of tiles) {
        const raw = node.getAttribute('href') || node.currentSrc || node.getAttribute('data-lazy-src') || node.getAttribute('src') || '';
        const url = cleanImage(raw);
        if (allowedImage(url)) {
          oldRedditCount++;
          add(url, /\.gif(?:[?#]|$)/i.test(url) ? 'animation' : 'photo');
        }
      }
    }

    return {
      items: out,
      diagnostics: {
        oldRedditCount,
        pageUrl: location.href || '',
        postCount: posts.length,
        galleryPostFound: !!post,
        loaderFound: !!loader,
        carouselFound: !!carousel,
        slideCount: slides.length,
        imageCount,
        playerCount
      }
    };
  };

  return new Promise(resolve => {
    const started = Date.now();
    const deadlineMs = 7000;
    const tick = () => {
      let result;
      try { result = extract(); }
      catch (error) {
        result = { items: [], diagnostics: { pageUrl: location.href || '', extractionError: String(error) } };
      }
      if ((result.items && result.items.length > 0) || Date.now() - started >= deadlineMs) {
        resolve(JSON.stringify(result));
      } else {
        setTimeout(tick, 200);
      }
    };
    tick();
  });
})()"#;

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
                .env("LIGHTPANDA_DISABLE_TELEMETRY", "true")
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
                // A WebSocket handshake can otherwise remain pending forever. The
                // outer startup deadline below also caps the complete retry loop.
                match timeout(Duration::from_secs(3), connect_async(url.clone())).await {
                    Ok(Ok((ws, _))) => {
                        self.cdp = Some(Cdp { ws, next_id: 0, events: Vec::new() });
                        return Ok(());
                    }
                    Ok(Err(e)) => last = Some(anyhow!("CDP WebSocket handshake failed: {e}")),
                    Err(_) => last = Some(anyhow!("CDP WebSocket handshake timed out")),
                }
                sleep(Duration::from_millis(500)).await;
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
        let page_timeout = self.s.page_timeout;
        self.fetch_script_with_timeout(url, script, page_timeout).await
    }

    /// Fetches a page using a shorter caller-selected page deadline. The timeout
    /// is enforced here, not by canceling this future externally, so a timeout
    /// always closes the Lightpanda process and its in-flight CDP target.
    pub async fn fetch_text_with_timeout(&mut self, url: &str, page_timeout: Duration) -> Result<String> {
        self.fetch_script_with_timeout(url, TEXT_JS, page_timeout).await
    }

    /// Reads gallery media from the normal HTML page DOM when the JSON route is blocked.
    pub async fn fetch_gallery_with_timeout(&mut self, url: &str, page_timeout: Duration) -> Result<String> {
        self.fetch_script_with_timeout(url, GALLERY_JS, page_timeout).await
    }

    async fn fetch_script_with_timeout(&mut self, url: &str, script: &'static str, pt: Duration) -> Result<String> {
        // `ensure()` includes spawning Lightpanda and completing the CDP WebSocket
        // handshake. Keep it inside its own deadline; the page timeout must not
        // leave startup/connection waits unbounded.
        match timeout(Duration::from_secs(30), self.ensure()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.shutdown().await;
                return Err(e);
            }
            Err(_) => {
                self.shutdown().await;
                bail!("Lightpanda startup/CDP connection timed out after 30s");
            }
        }
        let session_cookie = self.s.reddit_session_cookie.clone();
        let cdp = self.cdp.as_mut().ok_or_else(|| anyhow!("no CDP connection"))?;
        let r = timeout(pt, fetch_inner(cdp, url, pt, script, session_cookie.as_deref())).await;
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

async fn fetch_inner(cdp: &mut Cdp, url: &str, pt: Duration, script: &str, reddit_session_cookie: Option<&str>) -> Result<String> {
    let step = Duration::from_secs(30).min(pt);
    let r = cdp.call("Target.createTarget", json!({ "url": "about:blank" }), None, step).await?;
    let target = r["targetId"].as_str().ok_or_else(|| anyhow!("no targetId"))?.to_string();
    let res = navigate_and_extract(cdp, &target, url, step, pt, script, reddit_session_cookie).await;
    let _ = cdp.call("Target.closeTarget", json!({ "targetId": target }), None, Duration::from_secs(5)).await;
    cdp.events.clear();
    res
}

async fn navigate_and_extract(cdp: &mut Cdp, target: &str, url: &str, step: Duration, pt: Duration, script: &str, reddit_session_cookie: Option<&str>) -> Result<String> {
    let r = cdp.call("Target.attachToTarget", json!({ "targetId": target, "flatten": true }), None, step).await?;
    let sid = r["sessionId"].as_str().ok_or_else(|| anyhow!("no sessionId"))?.to_string();
    let s = Some(sid.as_str());
    cdp.call("Page.enable", json!({}), s, step).await?;
    if let Some(cookie) = reddit_session_cookie {
        cdp.call("Network.enable", json!({}), s, step).await?;
        let result = cdp.call(
            "Network.setCookie",
            json!({
                "name": "reddit_session",
                "value": cookie,
                "domain": ".reddit.com",
                "path": "/",
                "secure": true,
                "httpOnly": true,
                "sameSite": "Lax"
            }),
            s,
            step,
        ).await?;
        if result.get("success").and_then(Value::as_bool) == Some(false) {
            bail!("Lightpanda rejected REDDIT_SESSION_COOKIE");
        }
    }
    cdp.events.clear();
    let nav = cdp.call("Page.navigate", json!({ "url": url }), s, step).await?;
    if let Some(err) = nav.get("errorText").and_then(Value::as_str) {
        bail!("navigation failed: {err}");
    }
    if !cdp.wait_event(&["Page.loadEventFired", "Page.frameStoppedLoading"], pt.min(Duration::from_secs(45))).await {
        warn!("no load event seen; extracting anyway");
    }
    let r = cdp
        .call("Runtime.evaluate", json!({ "expression": script, "returnByValue": true, "awaitPromise": true }), s, step)
        .await?;
    if let Some(ex) = r.get("exceptionDetails") {
        bail!("extraction script threw: {ex}");
    }
    r["result"]["value"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| anyhow!("extraction returned no string value"))
}
