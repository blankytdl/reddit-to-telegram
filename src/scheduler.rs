use crate::{
    config::{CaptionOpts, Config, Group, Sort, SpoilerMode},
    health::Health,
    media_downloader as md,
    reddit_browser::Browser,
    reddit_parser::{parse_gallery, parse_page, GalleryItem, Media, PageStatus, Post},
    seen_store::SeenStore,
    telegram::{FileKind, Telegram, TgError},
};
use anyhow::{bail, Result};
use reqwest::Client;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{sleep, timeout, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

// ---------- small pure helpers ----------

pub fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn trunc(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        s.chars().take(n.saturating_sub(1)).collect::<String>() + "…"
    }
}

/// Same layout as the Python bot: title / "r/sub • u/author" / "Open on Reddit" link, blank-line
/// separated. Telegram's caption limit is 1024 chars after parsing; we stay under 1000 raw chars.
pub fn caption(p: &Post, spoiler: bool, o: CaptionOpts) -> String {
    let mut tail: Vec<String> = Vec::new();
    if o.footer {
        let mut f = String::new();
        if !p.subreddit.is_empty() {
            f.push_str(&format!("r/{} • ", esc(&p.subreddit)));
        }
        f.push_str(&format!("u/{}", esc(&p.author)));
        tail.push(f);
    }
    if o.link {
        tail.push(format!("<a href=\"{}\">Open on Reddit</a>", esc(&p.permalink).replace('"', "&quot;")));
    }
    let mut out = String::new();
    for lim in [300usize, 150, 60, 20] {
        let mut parts: Vec<String> = Vec::new();
        if o.title && !p.title.trim().is_empty() {
            let t = esc(&trunc(p.title.trim(), lim));
            parts.push(if spoiler { format!("<tg-spoiler>{t}</tg-spoiler>") } else { t });
        }
        parts.extend(tail.iter().cloned());
        out = parts.join("\n\n");
        if out.chars().count() <= 1000 {
            break;
        }
    }
    out
}

pub fn next_penalty(prev: Duration) -> Duration {
    if prev.is_zero() {
        Duration::from_secs(300)
    } else {
        (prev * 2).min(Duration::from_secs(3600))
    }
}

fn fnv64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Per-channel identity of a piece of media (cross-posts to the same channel are sent once).
pub fn media_key(chat: &str, media: &str) -> String {
    format!("m:{:016x}", fnv64(&format!("{chat}\n{media}")))
}

/// Per-channel + subreddit key used to remember that a sub has been "seeded".
pub fn sub_key(chat: &str, sub: &str) -> String {
    format!("{chat}|{sub}").to_ascii_lowercase()
}

fn retry_as_document(e: &TgError, kind: FileKind) -> bool {
    if e.code != 400 || kind == FileKind::Document {
        return false;
    }
    let d = e.desc.to_ascii_lowercase();
    !["chat", "rights", "forbidden", "caption", "blocked", "kicked", "entities", "parse"].iter().any(|w| d.contains(w))
}

// ---------- delivery ----------

/// Per-image download cap for gallery items (an album holds up to 10 temp files at once).
const GALLERY_ITEM_MAX: u64 = 25 * 1024 * 1024;

struct Ctx<'a> {
    chat: &'a str,
    p: &'a Post,
    cap: String,
    spoiler: bool,
    media_only: bool,
}

pub struct Deliverer {
    cfg: Arc<Config>,
    http: Client,
    tg: Telegram,
    last_send: Mutex<HashMap<String, Instant>>,
}

impl Deliverer {
    pub fn new(cfg: Arc<Config>, http: Client, tg: Telegram) -> Self {
        Self { cfg, http, tg, last_send: Mutex::new(HashMap::new()) }
    }

    /// Keeps us under Telegram's per-chat rate limits (~20 msgs/min to a channel).
    async fn pace(&self, chat: &str) {
        self.pace_n(chat, 1).await
    }

    async fn pace_n(&self, chat: &str, n: u32) {
        let wait = {
            let mut m = self.last_send.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            let start = match m.get(chat) {
                Some(t) => (*t).max(now),
                None => now,
            };
            m.insert(chat.to_string(), start + self.cfg.send_gap * n);
            start - now
        };
        if !wait.is_zero() {
            sleep(wait).await;
        }
    }

    async fn msg(&self, chat: &str, text: &str) -> Result<()> {
        self.pace(chat).await;
        self.tg.send_message(chat, text).await?;
        Ok(())
    }

    async fn file(&self, chat: &str, kind: FileKind, path: &Path, cap: &str, spoiler: bool) -> Result<(), TgError> {
        self.pace(chat).await;
        self.tg.send_file(chat, kind, path, cap, spoiler).await
    }

    pub async fn deliver(&self, chat: &str, p: &Post, media_only: bool) -> Result<()> {
        let spoiler = p.spoiler && self.cfg.spoiler_mode == SpoilerMode::Mark;
        let c = Ctx { chat, p, cap: caption(p, spoiler, self.cfg.caption), spoiler, media_only };
        match &p.media {
            Media::Image(u) => self.media(&c, u, FileKind::Photo).await,
            Media::Gif(u) => self.media(&c, u, FileKind::Animation).await,
            Media::Mp4(u) => self.media(&c, u, FileKind::Video).await,
            Media::RedditVideo(id) => self.reddit_video(&c, id).await,
            Media::Gallery(_) => self.link(&c, Some("Gallery post; see original.")).await,
            Media::None => self.link(&c, None).await,
        }
    }

    /// Text-only fallback used when a gallery could not be resolved.
    pub async fn deliver_link(&self, chat: &str, p: &Post, note: &str) -> Result<()> {
        let spoiler = p.spoiler && self.cfg.spoiler_mode == SpoilerMode::Mark;
        let c = Ctx { chat, p, cap: caption(p, spoiler, self.cfg.caption), spoiler, media_only: false };
        self.link(&c, Some(note)).await
    }

    async fn fetch_photo(&self, url: &str) -> Option<(md::TempFile, u64)> {
        let tf = match md::download(&self.http, url, GALLERY_ITEM_MAX, &md::ext_of(url)).await {
            Ok(t) => t,
            Err(e) => {
                warn!(reason = %e, "gallery image skipped");
                return None;
            }
        };
        let size = tf.size().await;
        if size <= self.cfg.max_photo_bytes {
            return Some((tf, size));
        }
        match self.shrink(tf.path()).await {
            Some(s) => {
                let sz = s.size().await;
                Some((s, sz))
            }
            None => {
                warn!("gallery image too large and cannot be resized; skipped");
                None
            }
        }
    }

    async fn send_album(&self, c: &Ctx<'_>, files: &[(md::TempFile, u64)], cap: &str) -> Result<()> {
        let one = |cap: &str| Ctx { chat: c.chat, p: c.p, cap: cap.to_string(), spoiler: c.spoiler, media_only: c.media_only };
        if files.len() == 1 {
            return self.upload_photo(&one(cap), files[0].0.path(), files[0].1).await;
        }
        let paths: Vec<&Path> = files.iter().map(|(t, _)| t.path()).collect();
        self.pace_n(c.chat, paths.len() as u32).await;
        match self.tg.send_media_group(c.chat, &paths, cap, c.spoiler).await {
            Ok(()) => Ok(()),
            Err(e) if retry_as_document(&e, FileKind::Photo) => {
                warn!("telegram rejected album ({}); sending images one by one", e.desc);
                for (i, (t, s)) in files.iter().enumerate() {
                    self.upload_photo(&one(if i == 0 { cap } else { "" }), t.path(), *s).await?;
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Sends a whole Reddit gallery as photo albums (10 per album, in gallery order).
    /// Animated items go afterwards as separate animations (Telegram albums can't hold GIFs).
    pub async fn deliver_gallery(&self, chat: &str, p: &Post, items: &[GalleryItem]) -> Result<()> {
        let spoiler = p.spoiler && self.cfg.spoiler_mode == SpoilerMode::Mark;
        let cap = caption(p, spoiler, self.cfg.caption);
        let c = Ctx { chat, p, cap: cap.clone(), spoiler, media_only: true };
        let statics: Vec<&GalleryItem> = items.iter().filter(|i| !i.animated).collect();
        let mut sent_any = false;
        let mut cap_used = false;
        for chunk in statics.chunks(10) {
            let mut files = Vec::with_capacity(chunk.len());
            for it in chunk {
                if let Some(f) = self.fetch_photo(&it.url).await {
                    files.push(f);
                }
            }
            if files.is_empty() {
                continue;
            }
            let chunk_cap = if cap_used { "" } else { cap.as_str() };
            cap_used = true;
            match self.send_album(&c, &files, chunk_cap).await {
                Ok(()) => sent_any = true,
                Err(e) if sent_any => {
                    // Part of the gallery is already out; don't re-send it next cycle.
                    warn!(post = %p.id, "later album failed after earlier one was sent: {e:#}");
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }
        for it in items.iter().filter(|i| i.animated) {
            let ac = Ctx { chat, p, cap: if sent_any { String::new() } else { cap.clone() }, spoiler, media_only: true };
            match self.media(&ac, &it.url, FileKind::Animation).await {
                Ok(()) => sent_any = true,
                Err(e) => warn!(post = %p.id, "gallery animation skipped: {e:#}"),
            }
        }
        if sent_any {
            Ok(())
        } else {
            bail!("no gallery image could be delivered")
        }
    }

    async fn link(&self, c: &Ctx<'_>, note: Option<&str>) -> Result<()> {
        let mut text = c.cap.clone();
        if text.trim().is_empty() {
            text = esc(&c.p.title); // captions switched off: a text message still needs some text
        }
        if let Some(n) = note {
            text.push_str(&format!("\n<i>{}</i>", esc(n)));
        }
        if !c.p.source_url.is_empty() && !c.spoiler {
            text.push_str(&format!("\n{}", esc(&c.p.source_url)));
        }
        self.msg(c.chat, &text).await
    }

    /// Shrinks an image (ffmpeg, one at a time) until it fits Telegram's photo limits.
    async fn shrink(&self, path: &Path) -> Option<md::TempFile> {
        let dim = self.cfg.photo_max_dim;
        for (d, q) in [(dim, 3u8), (dim, 8), (dim.min(1600), 8)] {
            match md::shrink_image(&self.cfg.ffmpeg_bin, &self.cfg.ffprobe_bin, path, d, q).await {
                Ok(t) => {
                    if t.size().await <= self.cfg.max_photo_bytes {
                        return Some(t);
                    }
                }
                Err(e) => {
                    warn!("cannot resize image: {e:#}");
                    return None;
                }
            }
        }
        None
    }

    /// Always tries to deliver a NATIVE photo; a document is only the last resort
    /// (undecodable file, or too many pixels to resize safely within the memory budget).
    async fn upload_photo(&self, c: &Ctx<'_>, path: &Path, size: u64) -> Result<()> {
        if size <= self.cfg.max_photo_bytes {
            match self.file(c.chat, FileKind::Photo, path, &c.cap, c.spoiler).await {
                Ok(()) => return Ok(()),
                Err(e) if !retry_as_document(&e, FileKind::Photo) => return Err(e.into()),
                Err(e) => warn!("telegram rejected photo ({}); resizing and retrying as photo", e.desc),
            }
        }
        if let Some(t) = self.shrink(path).await {
            match self.file(c.chat, FileKind::Photo, t.path(), &c.cap, c.spoiler).await {
                Ok(()) => return Ok(()),
                Err(e) if !retry_as_document(&e, FileKind::Photo) => return Err(e.into()),
                Err(e) => warn!("telegram rejected resized photo ({})", e.desc),
            }
        }
        warn!(post = %c.p.id, "cannot send as a photo; sending original as document");
        self.file(c.chat, FileKind::Document, path, &c.cap, false).await?;
        Ok(())
    }

    async fn upload(&self, c: &Ctx<'_>, path: &Path, size: u64, kind: FileKind) -> Result<()> {
        if kind == FileKind::Photo {
            return self.upload_photo(c, path, size).await;
        }
        match self.file(c.chat, kind, path, &c.cap, c.spoiler).await {
            Ok(()) => Ok(()),
            Err(e) if retry_as_document(&e, kind) => {
                warn!("telegram rejected {:?} ({}); retrying as document", kind, e.desc);
                self.file(c.chat, FileKind::Document, path, &c.cap, false).await?;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn media(&self, c: &Ctx<'_>, url: &str, kind: FileKind) -> Result<()> {
        match md::download(&self.http, url, self.cfg.max_upload_bytes, &md::ext_of(url)).await {
            Ok(tf) => {
                let size = tf.size().await;
                self.upload(c, tf.path(), size, kind).await // tf dropped -> temp file removed
            }
            Err(e) => {
                let big = md::is_too_large(&e);
                warn!(post = %c.p.id, reason = %e, "media could not be sent as a file");
                if c.media_only && !big {
                    return Err(e); // retried next cycle, dropped after 3 failures
                }
                let note = if big { "Media too large for Telegram upload; see original post." } else { "Media could not be downloaded; see original post." };
                self.link(c, Some(note)).await
            }
        }
    }

    async fn reddit_video(&self, c: &Ctx<'_>, id: &str) -> Result<()> {
        match md::fetch_reddit_video(&self.http, &self.cfg.ffmpeg_bin, id, self.cfg.max_upload_bytes).await {
            Ok(tf) => {
                let size = tf.size().await;
                if size > self.cfg.max_upload_bytes {
                    warn!(post = %c.p.id, size, "video exceeds upload limit; sending link");
                    return self.link(c, Some("Video too large for Telegram upload; see original post.")).await;
                }
                self.upload(c, tf.path(), size, FileKind::Video).await
            }
            Err(e) => {
                warn!(post = %c.p.id, reason = %e, "reddit video could not be fetched");
                if c.media_only {
                    return Err(e);
                }
                self.link(c, Some("Video could not be fetched; see original post.")).await
            }
        }
    }
}

// ---------- scheduler ----------

pub struct Scheduler {
    cfg: Arc<Config>,
    browser: Browser,
    store: SeenStore,
    deliverer: Deliverer,
    health: Arc<Health>,
    token: CancellationToken,
    fail_counts: HashMap<String, u8>,
    cycles: u64,
    penalty: Duration,
    next_group: usize,
}

impl Scheduler {
    pub fn new(cfg: Arc<Config>, browser: Browser, store: SeenStore, deliverer: Deliverer, health: Arc<Health>, token: CancellationToken) -> Self {
        Self { cfg, browser, store, deliverer, health, token, fail_counts: HashMap::new(), cycles: 0, penalty: Duration::ZERO, next_group: 0 }
    }

    pub async fn run(mut self) {
        loop {
            if self.token.is_cancelled() {
                break;
            }
            let started = Instant::now();
            // On SIGTERM give the cycle 20 s to wrap up (Render kills after ~30 s), then
            // abandon it so the final state flush still happens.
            let tok = self.token.clone();
            let hard = self.cfg.cycle_budget + Duration::from_secs(600);
            let outcome = tokio::select! {
                r = timeout(hard, self.cycle()) => Some(r),
                _ = async { tok.cancelled().await; sleep(Duration::from_secs(20)).await; } => None,
            };
            match outcome {
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(e))) => warn!("cycle error: {e:#}"),
                Some(Err(_)) => {
                    warn!("cycle exceeded hard timeout; restarting browser");
                    self.browser.shutdown().await;
                }
                None => warn!("shutdown grace period elapsed; abandoning current cycle"),
            }
            if self.token.is_cancelled() {
                break;
            }
            self.cycles += 1;
            self.health.cycle_done();
            if self.cycles % self.cfg.recycle_cycles == 0 {
                info!("recycling browser");
                self.browser.shutdown().await;
            }
            let wait = self.cfg.interval.saturating_sub(started.elapsed()).max(Duration::from_secs(30)) + self.penalty;
            self.health.beat(wait.as_secs());
            info!(next_in_secs = wait.as_secs(), "cycle finished");
            tokio::select! {
                _ = self.token.cancelled() => break,
                _ = sleep(wait) => {}
            }
        }
        info!("shutting down: flushing state");
        self.store.flush().await;
        self.browser.shutdown().await;
    }

    async fn cycle(&mut self) -> Result<()> {
        let cfg = self.cfg.clone();
        let n = cfg.groups.len();
        if n == 0 {
            return Ok(());
        }
        let started = Instant::now();
        let (mut limited, mut done, mut longest) = (false, 0usize, Duration::ZERO);
        for k in 0..n {
            if self.token.is_cancelled() {
                break;
            }
            // Time budget: stop starting page loads we can't finish; resume round-robin next cycle.
            if k > 0 && started.elapsed() + longest > cfg.cycle_budget {
                warn!(done, total = n, "cycle time budget reached; remaining groups run next cycle (raise POLL_INTERVAL_SECS or reduce subs)");
                break;
            }
            let gi = (self.next_group + k) % n;
            let g = &cfg.groups[gi];
            if k > 0 {
                sleep(cfg.request_gap).await;
            }
            let t0 = Instant::now();
            self.health.beat(0);
            match self.fetch_group(g).await {
                Ok(Some(posts)) => {
                    self.process_group(g, posts).await;
                    self.store.flush_if_due(cfg.flush_min).await;
                    self.next_group = (gi + 1) % n;
                    done += 1;
                }
                Ok(None) => {
                    limited = true;
                    self.next_group = gi; // retry this group first next cycle
                    break;
                }
                Err(e) => {
                    warn!(subs = %g.names.join("+"), "fetch failed: {e:#}");
                    self.next_group = (gi + 1) % n;
                    done += 1;
                }
            }
            longest = longest.max(t0.elapsed());
        }
        self.store.flush().await;
        self.penalty = if limited { next_penalty(self.penalty) } else { Duration::ZERO };
        info!(done, total = n, secs = started.elapsed().as_secs(), "cycle summary");
        Ok(())
    }

    /// One multi-subreddit page load; falls back to per-sub loads if the combined page is empty.
    async fn fetch_group(&mut self, g: &Group) -> Result<Option<Vec<Post>>> {
        let limit = g.total_limit();
        let posts = match self.fetch_listing(&g.names, g.sort, limit).await? {
            Some(p) => p,
            None => return Ok(None),
        };
        if !posts.is_empty() || g.names.len() == 1 {
            return Ok(Some(posts));
        }
        warn!(subs = %g.names.join("+"), "combined listing was empty; retrying each subreddit separately");
        let mut all = Vec::new();
        for (name, lim) in g.names.iter().zip(&g.limits) {
            if self.token.is_cancelled() {
                break;
            }
            sleep(self.cfg.request_gap).await;
            match self.fetch_listing(std::slice::from_ref(name), g.sort, *lim).await {
                Ok(Some(p)) => all.extend(p),
                Ok(None) => return Ok(None),
                Err(e) => warn!(sub = %name, "fetch failed: {e:#}"),
            }
        }
        Ok(Some(all))
    }

    /// Ok(None) = rate limited / blocked even after backoff.
    async fn fetch_listing(&mut self, names: &[String], sort: Sort, limit: usize) -> Result<Option<Vec<Post>>> {
        let url = format!("{}/r/{}/{}/?limit={}", self.cfg.reddit_base, names.join("+"), sort.path(), limit);
        let mut delay = Duration::from_secs(30);
        for attempt in 1..=3 {
            let json = self.browser.fetch(&url).await?;
            match parse_page(&json, limit)? {
                PageStatus::Ok(p) => return Ok(Some(p)),
                PageStatus::Blocked(r) => {
                    // No bypass attempt: treat like a rate limit and back off the whole cycle.
                    warn!(reason = %r, "Reddit served a block/CAPTCHA/login page; backing off");
                    return Ok(None);
                }
                PageStatus::RateLimited => {
                    warn!(attempt, wait_secs = delay.as_secs(), "rate limited by Reddit; backing off");
                    tokio::select! {
                        _ = self.token.cancelled() => return Ok(None),
                        _ = sleep(delay) => {}
                    }
                    delay *= 2;
                }
            }
        }
        Ok(None)
    }

    /// Loads the post's JSON in the browser (one extra page load) to learn the gallery's images.
    async fn deliver_gallery_post(&mut self, chat: &str, p: &Post, id: &str, media_only: bool) -> Result<()> {
        sleep(self.cfg.request_gap).await;
        let url = format!("{}/comments/{}.json?raw_json=1", self.cfg.reddit_base, id);
        let items = match self.browser.fetch_text(&url).await {
            Ok(t) if !t.trim_start().starts_with('[') && t.to_ascii_lowercase().contains("too many requests") => Err(anyhow::anyhow!("rate limited while loading gallery data")),
            Ok(t) => parse_gallery(&t),
            Err(e) => Err(e),
        };
        match items {
            Ok(items) => self.deliverer.deliver_gallery(chat, p, &items).await,
            Err(e) if media_only => Err(e),
            Err(e) => {
                warn!(post = %p.id, "gallery could not be resolved: {e:#}");
                self.deliverer.deliver_link(chat, p, "Gallery could not be loaded; see original post.").await
            }
        }
    }

    /// Seen is tracked per destination channel (like the Python bot's `channel_seen`), so the same
    /// subreddit can feed two channels. Ids imported from the Python bot are bare and apply everywhere.
    fn is_seen(&self, chat: &str, id: &str) -> bool {
        self.store.contains(id) || self.store.contains(&format!("{chat}|{id}"))
    }

    fn mark_seen(&mut self, chat: &str, id: &str) {
        self.store.insert(&format!("{chat}|{id}"));
    }

    async fn process_group(&mut self, g: &Group, posts: Vec<Post>) {
        let chat = g.chat_id.as_str();
        let page_ok = !posts.is_empty();
        let mut fresh: Vec<Post> = posts.into_iter().filter(|p| !self.is_seen(chat, &p.id)).collect();
        fresh.reverse(); // oldest first
        if !fresh.is_empty() {
            info!(subs = %g.names.join("+"), new = fresh.len(), "processing");
        }
        let (mut sent, mut seeded, mut skipped) = (0u32, 0u32, 0u32);

        for p in fresh {
            if self.token.is_cancelled() {
                return; // subs stay un-marked; nothing is lost, unsent posts are retried next run
            }
            let sub = if p.subreddit.is_empty() { g.names[0].as_str() } else { p.subreddit.as_str() };
            // First run for this sub+channel: remember what exists, don't flood the channel.
            if !self.cfg.send_backlog && !self.store.sub_known(chat, sub) {
                self.mark_seen(chat, &p.id);
                seeded += 1;
                continue;
            }
            if (g.media_only && p.media == Media::None) || (p.spoiler && self.cfg.spoiler_mode == SpoilerMode::Skip) {
                self.mark_seen(chat, &p.id);
                skipped += 1;
                continue;
            }
            let mkey = if self.cfg.dedupe_media { p.media.key().map(|m| media_key(chat, m)) } else { None };
            if let Some(k) = &mkey {
                if self.store.contains(k) {
                    debug!(post = %p.id, "same media already sent to this channel; skipping");
                    self.mark_seen(chat, &p.id);
                    skipped += 1;
                    continue;
                }
            }
            let result = match &p.media {
                Media::Gallery(id) => self.deliver_gallery_post(chat, &p, id, g.media_only).await,
                _ => self.deliverer.deliver(chat, &p, g.media_only).await,
            };
            match result {
                Ok(()) => {
                    info!(sub = %sub, post = %p.id, "delivered post");
                    self.mark_seen(chat, &p.id);
                    if let Some(k) = mkey {
                        self.store.insert(&k);
                    }
                    self.fail_counts.remove(&format!("{chat}|{}", p.id));
                    sent += 1;
                }
                Err(e) => {
                    if self.fail_counts.len() > 1000 {
                        self.fail_counts.clear();
                    }
                    let n = self.fail_counts.entry(format!("{chat}|{}", p.id)).or_insert(0);
                    *n += 1;
                    warn!(sub = %sub, post = %p.id, attempt = *n, "delivery failed: {e:#}");
                    if *n >= 3 {
                        warn!(post = %p.id, "giving up on post after 3 failed cycles");
                        self.mark_seen(chat, &p.id);
                    }
                }
            }
        }
        if page_ok {
            for name in &g.names {
                self.store.mark_sub(&sub_key(chat, name));
            }
        }
        if sent + seeded + skipped > 0 {
            info!(subs = %g.names.join("+"), sent, seeded, skipped, "group done");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn post(title: &str, spoiler: bool) -> Post {
        Post {
            id: "t3_a".into(),
            title: title.into(),
            author: "bob".into(),
            permalink: "https://www.reddit.com/r/x/comments/a/".into(),
            source_url: String::new(),
            media: Media::None,
            subreddit: "anime".into(),
            spoiler,
        }
    }

    #[test]
    fn escapes_html() {
        assert_eq!(esc("a<b>&c"), "a&lt;b&gt;&amp;c");
    }

    #[test]
    fn caption_matches_python_layout_and_marks_spoilers() {
        let o = CaptionOpts { title: true, footer: true, link: true };
        let c = caption(&post("Hello", false), false, o);
        assert_eq!(c, "Hello\n\nr/anime • u/bob\n\n<a href=\"https://www.reddit.com/r/x/comments/a/\">Open on Reddit</a>");
        assert!(caption(&post(&"<".repeat(2000), false), false, o).chars().count() <= 1024);
        let s = caption(&post("Big twist", true), true, o);
        assert!(s.starts_with("<tg-spoiler>Big twist</tg-spoiler>"));
        assert_eq!(caption(&post("T", false), false, CaptionOpts { title: false, footer: false, link: false }), "");
    }

    #[test]
    fn caption_escapes_permalink() {
        let mut p = post("T", false);
        p.permalink = "https://x/\"><b>".into();
        let c = caption(&p, false, CaptionOpts { title: false, footer: false, link: true });
        assert!(!c.contains("\"><b>"));
    }

    #[test]
    fn entity_errors_do_not_retry_as_document() {
        let e = TgError { code: 400, desc: "Bad Request: can't parse entities".into(), retry_after: None };
        assert!(!retry_as_document(&e, FileKind::Photo));
    }

    #[test]
    fn media_key_is_stable_and_channel_scoped() {
        assert_eq!(media_key("@a", "https://i.redd.it/x.png"), media_key("@a", "https://i.redd.it/x.png"));
        assert_ne!(media_key("@a", "https://i.redd.it/x.png"), media_key("@b", "https://i.redd.it/x.png"));
        assert_eq!(sub_key("@a", "Anime"), "@a|anime");
        assert_eq!(sub_key("@AnimeChannel", "Anime"), "@animechannel|anime");
    }

    #[test]
    fn document_fallback_only_for_file_problems() {
        let e = |d: &str| TgError { code: 400, desc: d.into(), retry_after: None };
        assert!(retry_as_document(&e("Bad Request: PHOTO_INVALID_DIMENSIONS"), FileKind::Photo));
        assert!(!retry_as_document(&e("Bad Request: chat not found"), FileKind::Photo));
        assert!(!retry_as_document(&e("Bad Request: PHOTO_INVALID_DIMENSIONS"), FileKind::Document));
    }

    #[test]
    fn penalty_backs_off_exponentially_with_cap() {
        let mut p = next_penalty(Duration::ZERO);
        assert_eq!(p.as_secs(), 300);
        p = next_penalty(p);
        assert_eq!(p.as_secs(), 600);
        for _ in 0..10 {
            p = next_penalty(p);
        }
        assert_eq!(p.as_secs(), 3600);
    }
}
