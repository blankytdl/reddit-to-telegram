use crate::{
    config::{CaptionOpts, Config, Group, Sort, SpoilerMode},
    health::Health,
    media_downloader as md,
    reddit_browser::Browser,
    reddit_parser::{parse_gallery, parse_page, GalleryItem, GalleryKind, Media, PageStatus, Post},
    seen_store::{PendingDelivery, SeenStore},
    telegram::{FileKind, Telegram, TgError},
};
use anyhow::{bail, Result};
use reqwest::Client;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
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

/// Keep failed posts eligible for retry, with per-post exponential backoff.
fn delivery_retry_delay(interval: Duration, attempts: u8) -> Duration {
    let shift = attempts.saturating_sub(1).min(5);
    interval
        .saturating_mul(1u32 << shift)
        .min(Duration::from_secs(6 * 60 * 60))
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
pub fn sub_key(chat: &str, sub: &str, sort: Sort) -> String {
    format!("{chat}|{sub}|{}", sort.path()).to_ascii_lowercase()
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
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
            Media::Gallery(_) => bail!("gallery post reached media delivery without being resolved"),
            Media::None => self.link(&c, None).await,
        }
    }

    async fn fetch_photo(&self, url: &str) -> Result<(md::TempFile, u64)> {
        let tf = md::download(&self.http, url, GALLERY_ITEM_MAX, &md::ext_of(url)).await?;
        let size = tf.size().await;
        if size <= self.cfg.max_photo_bytes {
            return Ok((tf, size));
        }
        let resized = self.shrink(tf.path()).await
            .ok_or_else(|| anyhow::anyhow!("gallery image too large and could not be resized"))?;
        let resized_size = resized.size().await;
        Ok((resized, resized_size))
    }

    /// Returns how many leading photos were confirmed sent, so a failed gallery
    /// can resume without repeating already delivered photos.
    async fn send_album(&self, c: &Ctx<'_>, files: &[(md::TempFile, u64)], cap: &str) -> (usize, Result<()>) {
        let one = |cap: &str| Ctx { chat: c.chat, p: c.p, cap: cap.to_string(), spoiler: c.spoiler, media_only: c.media_only };
        if files.len() == 1 {
            return match self.upload_photo(&one(cap), files[0].0.path(), files[0].1).await {
                Ok(()) => (1, Ok(())),
                Err(e) => (0, Err(e)),
            };
        }
        let paths: Vec<&Path> = files.iter().map(|(t, _)| t.path()).collect();
        self.pace_n(c.chat, paths.len() as u32).await;
        match self.tg.send_media_group(c.chat, &paths, cap, c.spoiler).await {
            Ok(()) => (files.len(), Ok(())),
            Err(e) if retry_as_document(&e, FileKind::Photo) => {
                warn!("telegram rejected album ({}); sending images one by one", e.desc);
                for (i, (t, size)) in files.iter().enumerate() {
                    if let Err(err) = self.upload_photo(&one(if i == 0 { cap } else { "" }), t.path(), *size).await {
                        return (i, Err(err));
                    }
                }
                (files.len(), Ok(()))
            }
            Err(e) => (0, Err(e.into())),
        }
    }

    /// Sends every item in a Reddit gallery in its original order:
    /// photos in albums, GIFs as animations, and Reddit videos as videos.
    /// Sends gallery items in order. On failure, the returned index points to the
    /// first item not confirmed sent; the scheduler persists it for restart-safe resume.
    pub async fn deliver_gallery(
        &self,
        chat: &str,
        p: &Post,
        items: &[GalleryItem],
        start_index: usize,
    ) -> (usize, Result<()>) {
        let spoiler = p.spoiler && self.cfg.spoiler_mode == SpoilerMode::Mark;
        let cap = caption(p, spoiler, self.cfg.caption);
        let c = Ctx { chat, p, cap: cap.clone(), spoiler, media_only: true };
        let mut index = start_index.min(items.len());
        let mut cap_used = index > 0;

        while index < items.len() {
            if items[index].kind == GalleryKind::Photo {
                let start = index;
                let mut end = start;
                while end < items.len() && end - start < 10 && items[end].kind == GalleryKind::Photo {
                    end += 1;
                }

                let mut files = Vec::with_capacity(end - start);
                for item in &items[start..end] {
                    match self.fetch_photo(&item.url).await {
                        Ok(file) => files.push(file),
                        Err(e) => return (start, Err(e)),
                    }
                }

                let chunk_cap = if cap_used { "" } else { cap.as_str() };
                let (sent, result) = self.send_album(&c, &files, chunk_cap).await;
                if let Err(e) = result {
                    return (start + sent, Err(e));
                }
                index = end;
                cap_used = true;
                continue;
            }

            let item_index = index;
            let item = &items[item_index];
            let item_ctx = Ctx {
                chat,
                p,
                cap: if cap_used { String::new() } else { cap.clone() },
                spoiler,
                media_only: true,
            };
            let result = match item.kind {
                GalleryKind::Animation => self.media(&item_ctx, &item.url, FileKind::Animation).await,
                GalleryKind::Video => self.media(&item_ctx, &item.url, FileKind::Video).await,
                GalleryKind::DashVideo => self.gallery_video_manifest(&item_ctx, &item.url).await,
                GalleryKind::Photo => unreachable!("photo items are sent in albums"),
            };
            if let Err(e) = result {
                return (item_index, Err(e));
            }
            index += 1;
            cap_used = true;
        }

        (index, Ok(()))
    }

    async fn gallery_video_manifest(&self, c: &Ctx<'_>, url: &str) -> Result<()> {
        let file = md::fetch_video_manifest(&self.cfg.ffmpeg_bin, url, self.cfg.max_upload_bytes).await?;
        let size = file.size().await;
        self.upload(c, file.path(), size, FileKind::Video).await
    }

    /// Link fallback is used only when Reddit's gallery metadata itself cannot be read.
    pub async fn deliver_link(&self, chat: &str, p: &Post, note: &str) -> Result<()> {
        let spoiler = p.spoiler && self.cfg.spoiler_mode == SpoilerMode::Mark;
        let c = Ctx {
            chat,
            p,
            cap: caption(p, spoiler, self.cfg.caption),
            spoiler,
            media_only: false,
        };
        self.link(&c, Some(note)).await
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
                // Never replace failed media with a Reddit link. Return the failure so the
                // scheduler can retry it and then skip it without blocking later posts.
                warn!(post = %c.p.id, reason = %e, "media could not be sent; no link fallback");
                Err(e)
            }
        }
    }

    async fn reddit_video(&self, c: &Ctx<'_>, id: &str) -> Result<()> {
        match md::fetch_reddit_video(&self.http, &self.cfg.ffmpeg_bin, id, self.cfg.max_upload_bytes).await {
            Ok(tf) => {
                let size = tf.size().await;
                if size > self.cfg.max_upload_bytes {
                    warn!(post = %c.p.id, size, "video exceeds upload limit; skipping media");
                    bail!("video exceeds Telegram upload limit");
                }
                self.upload(c, tf.path(), size, FileKind::Video).await
            }
            Err(e) => {
                warn!(post = %c.p.id, reason = %e, "reddit video could not be fetched");
                Err(e)
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
    cycles: u64,
    penalty: Duration,
    next_group: usize,
}

impl Scheduler {
    pub fn new(cfg: Arc<Config>, browser: Browser, store: SeenStore, deliverer: Deliverer, health: Arc<Health>, token: CancellationToken) -> Self {
        Self { cfg, browser, store, deliverer, health, token, cycles: 0, penalty: Duration::ZERO, next_group: 0 }
    }

    pub async fn run(mut self) {
        loop {
            if self.token.is_cancelled() {
                break;
            }
            let started = Instant::now();
            self.health.set_stage("cycle starting", 0);
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
            self.health.set_stage("idle until next poll", wait.as_secs());
            info!(next_in_secs = wait.as_secs(), "cycle finished");
            // Keep the host console visibly alive during the poll interval.
            // This is only a log heartbeat; it does not trigger Reddit requests or state writes.
            let mut remaining = wait;
            while !remaining.is_zero() {
                let tick = remaining.min(Duration::from_secs(60));
                tokio::select! {
                    _ = self.token.cancelled() => break,
                    _ = sleep(tick) => {
                        remaining = remaining.saturating_sub(tick);
                        if !remaining.is_zero() {
                            info!(next_in_secs = remaining.as_secs(), "waiting for next poll");
                        }
                    }
                }
            }
            if self.token.is_cancelled() {
                break;
            }
        }
        info!("shutting down: flushing state");
        self.store.flush().await;
        self.browser.shutdown().await;
    }

    async fn cycle(&mut self) -> Result<()> {
        self.retry_pending().await;
        let cfg = self.cfg.clone();
        let n = cfg.groups.len();
        if n == 0 {
            return Ok(());
        }
        let started = Instant::now();
        let (mut limited, mut done) = (false, 0usize);
        for k in 0..n {
            if self.token.is_cancelled() {
                break;
            }
            // Time budget: stop starting page loads we can't finish; resume round-robin next cycle.
            if k > 0 && started.elapsed() > cfg.cycle_budget {
                warn!(done, total = n, "cycle time budget reached; remaining groups run next cycle (raise POLL_INTERVAL_SECS or reduce subs)");
                break;
            }
            let gi = (self.next_group + k) % n;
            let g = &cfg.groups[gi];
            if k > 0 {
                sleep(cfg.request_gap).await;
            }
            self.health.set_stage(&format!("fetching r/{}", g.names.join("+")), 0);
            match self.fetch_group(g).await {
                Ok(Some(posts)) => {
                    self.process_group(g, posts).await;
                    self.health.set_stage("checking state sync", 0);
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
        }
        self.health.set_stage("flushing cycle state", 0);
        // Respect the configured sync interval. A forced flush is still done on shutdown.
        self.store.flush_if_due(cfg.flush_min).await;
        self.penalty = if limited { next_penalty(self.penalty) } else { Duration::ZERO };
        info!(done, total = n, secs = started.elapsed().as_secs(), "cycle summary");
        Ok(())
    }

    /// Fetch each subreddit separately so a busy subreddit cannot consume another
    /// subreddit's configured post limit.
    async fn fetch_group(&mut self, g: &Group) -> Result<Option<Vec<Post>>> {
        let mut all = Vec::new();
        for (index, (name, limit)) in g.names.iter().zip(&g.limits).enumerate() {
            if self.token.is_cancelled() {
                return Ok(None);
            }
            if index > 0 {
                sleep(self.cfg.request_gap).await;
            }
            let mut posts = match self.fetch_listing(std::slice::from_ref(name), g.sort, *limit).await? {
                Some(posts) => posts,
                None => return Ok(None),
            };
            // A single-subreddit request makes the source unambiguous even when
            // Reddit omits the card's data-subreddit field.
            for post in &mut posts {
                if post.subreddit.is_empty() {
                    post.subreddit = name.clone();
                }
            }
            all.extend(posts);
        }
        Ok(Some(all))
    }

    /// Ok(None) = rate limited / blocked even after backoff.
    async fn fetch_listing(&mut self, names: &[String], sort: Sort, limit: usize) -> Result<Option<Vec<Post>>> {
        let url = format!("{}/r/{}/{}/?limit={}", self.cfg.reddit_base, names.join("+"), sort.path(), limit);
        let mut delay = Duration::from_secs(30);
        for attempt in 1..=3 {
            self.health.set_stage(&format!("Reddit fetch r/{} attempt {attempt}", names.join("+")), 0);
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

    /// Read gallery metadata from Reddit's JSON document. If the configured host
    /// returns an HTML shell/error page, try the other canonical Reddit host.
    async fn deliver_gallery_post(
        &mut self,
        chat: &str,
        p: &Post,
        id: &str,
        media_only: bool,
        start_index: usize,
    ) -> (usize, Result<()>) {
        sleep(self.cfg.request_gap).await;

        let primary_base = self.cfg.reddit_base.trim_end_matches('/');
        let alternate_base = if primary_base.contains("old.reddit.com") {
            "https://www.reddit.com"
        } else {
            "https://old.reddit.com"
        };
        let urls = [
            format!("{primary_base}/comments/{id}.json?raw_json=1"),
            format!("{alternate_base}/comments/{id}.json?raw_json=1"),
        ];

        let mut last_error = None;
        for (index, url) in urls.iter().enumerate() {
            if index > 0 {
                sleep(self.cfg.request_gap).await;
            }

            let body = match self.browser.fetch_text(url).await {
                Ok(body) => body,
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            };

            if body.to_ascii_lowercase().contains("too many requests") {
                last_error = Some(anyhow::anyhow!("Reddit rate-limited the gallery metadata request"));
                break;
            }

            match parse_gallery(&body) {
                Ok(items) => return self.deliverer.deliver_gallery(chat, p, &items, start_index).await,
                Err(e) => {
                    let prefix = body.trim().chars().take(120).collect::<String>().replace('\n', " ");
                    last_error = Some(anyhow::anyhow!("{e:#}; response prefix: {prefix:?}"));
                }
            }
        }

        let error = last_error.unwrap_or_else(|| anyhow::anyhow!("Reddit returned no gallery metadata"));
        warn!(post = %p.id, media_only, reason = %error, "gallery media could not be resolved");

        if media_only {
            (start_index, Err(anyhow::anyhow!("gallery extraction failed; post remains unseen for retry: {error:#}")))
        } else {
            (start_index, self.deliverer
                .deliver_link(chat, p, "Gallery media could not be loaded; open the original post.")
                .await)
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
        self.health.set_stage(&format!("processing r/{}", g.names.join("+")), 0);
        let chat = g.chat_id.as_str();
        let fetched = posts.len();
        let page_ok = fetched > 0;
        let mut per_sub = HashMap::<String, usize>::new();
        for p in &posts {
            let name = if p.subreddit.is_empty() { "(missing subreddit field)" } else { &p.subreddit };
            *per_sub.entry(name.to_string()).or_insert(0) += 1;
        }
        let per_sub = per_sub
            .into_iter()
            .map(|(name, count)| format!("{name}:{count}"))
            .collect::<Vec<_>>()
            .join(",");

        // Remove duplicate cards returned within the same Reddit page as well as IDs
        // already delivered in earlier cycles. Count both cases so the logs explain
        // why a configured subreddit may not produce a Telegram message.
        let mut batch_ids = HashSet::new();
        let (mut duplicate_in_batch, mut already_seen) = (0u32, 0u32);
        let mut fresh = Vec::with_capacity(fetched);
        for p in posts {
            if !batch_ids.insert(p.id.clone()) {
                duplicate_in_batch += 1;
            } else if self.is_seen(chat, &p.id) {
                already_seen += 1;
            } else {
                fresh.push(p);
            }
        }
        fresh.reverse(); // oldest first
        let fresh_count = fresh.len() as u32;
        if fresh_count > 0 {
            info!(subs = %g.names.join("+"), new = fresh_count, "processing");
        }
        let (mut sent, mut seeded, mut skipped, mut retry_deferred) = (0u32, 0u32, 0u32, 0u32);
        let (mut filtered_nsfw, mut filtered_media_only, mut filtered_spoiler, mut duplicate_media, mut failed) =
            (0u32, 0u32, 0u32, 0u32, 0u32);

        for p in fresh {
            if self.token.is_cancelled() {
                return; // subs stay un-marked; nothing is lost, unsent posts are retried next run
            }
            let sub = if p.subreddit.is_empty() { g.names[0].as_str() } else { p.subreddit.as_str() };
            self.health.set_stage(&format!("post {} from r/{sub}", p.id), 0);
            // First run for this sub+channel: remember what exists, don't flood the channel.
            if !self.cfg.send_backlog && !self.store.sub_known(chat, sub) {
                self.mark_seen(chat, &p.id);
                seeded += 1;
                continue;
            }
            if p.nsfw && !self.cfg.include_nsfw {
                self.mark_seen(chat, &p.id);
                filtered_nsfw += 1;
                skipped += 1;
                continue;
            }
            // When NSFW is enabled, keep flagged posts even if MEDIA_ONLY is on.
            // Unsupported external media is still sent as its Reddit post link.
            if g.media_only && p.media == Media::None && !(p.nsfw && self.cfg.include_nsfw) {
                self.mark_seen(chat, &p.id);
                filtered_media_only += 1;
                skipped += 1;
                continue;
            }
            if p.spoiler && self.cfg.spoiler_mode == SpoilerMode::Skip {
                self.mark_seen(chat, &p.id);
                filtered_spoiler += 1;
                skipped += 1;
                continue;
            }
            let mkey = if self.cfg.dedupe_media { p.media.key().map(|m| media_key(chat, m)) } else { None };
            if let Some(k) = &mkey {
                if self.store.contains(k) {
                    debug!(post = %p.id, "same media already sent to this channel; skipping");
                    self.mark_seen(chat, &p.id);
                    duplicate_media += 1;
                    skipped += 1;
                    continue;
                }
            }
            let retry_key = format!("{chat}|{}", p.id);
            if let Some((attempts, retry_at)) = self.fail_counts.get(&retry_key) {
                let now = Instant::now();
                if *retry_at > now {
                    retry_deferred += 1;
                    debug!(
                        post = %p.id,
                        previous_attempts = *attempts,
                        retry_in_secs = (*retry_at - now).as_secs(),
                        "delivery retry deferred"
                    );
                    continue;
                }
            }

            info!(sub = %sub, post = %p.id, "delivery started");
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
                    self.fail_counts.remove(&retry_key);
                    sent += 1;
                }
                Err(e) => {
                    failed += 1;
                    if self.fail_counts.len() > 1000 {
                        self.fail_counts.clear();
                    }
                    let (attempt, delay) = {
                        let state = self.fail_counts
                            .entry(retry_key.clone())
                            .or_insert((0, Instant::now()));
                        state.0 = state.0.saturating_add(1);
                        let delay = delivery_retry_delay(self.cfg.interval, state.0);
                        state.1 = Instant::now() + delay;
                        (state.0, delay)
                    };
                    warn!(
                        sub = %sub,
                        post = %p.id,
                        attempt,
                        retry_in_secs = delay.as_secs(),
                        "delivery failed; post remains unseen and will be retried"
                    );
                }
            }
        }
        if page_ok {
            for name in &g.names {
                self.store.mark_sub(&sub_key(chat, name));
            }
        }
        info!(
            subs = %g.names.join("+"),
            fetched,
            per_sub = %per_sub,
            fresh = fresh_count,
            already_seen,
            duplicate_in_batch,
            seeded,
            filtered_nsfw,
            filtered_media_only,
            filtered_spoiler,
            duplicate_media,
            sent,
            failed,
            skipped,
            retry_deferred,
            "group processing summary"
        );
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
            nsfw: false,
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

    #[test]
    fn failed_delivery_retry_uses_capped_exponential_backoff() {
        let interval = Duration::from_secs(600);
        assert_eq!(delivery_retry_delay(interval, 1).as_secs(), 600);
        assert_eq!(delivery_retry_delay(interval, 2).as_secs(), 1200);
        assert_eq!(delivery_retry_delay(interval, 3).as_secs(), 2400);
        assert_eq!(delivery_retry_delay(interval, 99).as_secs(), 6 * 60 * 60);
    }
}
