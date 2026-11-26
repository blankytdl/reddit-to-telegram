use anyhow::{anyhow, bail, Result};
use std::{collections::HashMap, path::PathBuf, sync::OnceLock, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sort {
    New,
    Hot,
}

impl Sort {
    pub fn path(self) -> &'static str {
        match self {
            Sort::New => "new",
            Sort::Hot => "hot",
        }
    }
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "new" => Ok(Sort::New),
            "hot" => Ok(Sort::Hot),
            o => bail!("invalid sort {o:?} (use new or hot)"),
        }
    }
}

/// What to do with posts Reddit marks as spoilers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpoilerMode {
    /// Send, hiding media behind Telegram's spoiler overlay and the title in a spoiler tag.
    Mark,
    /// Do not send spoiler posts.
    Skip,
    /// Send them like any other post.
    Ignore,
}

impl SpoilerMode {
    fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "mark" => Ok(Self::Mark),
            "skip" => Ok(Self::Skip),
            "ignore" => Ok(Self::Ignore),
            o => bail!("invalid SPOILER_MODE {o:?} (use mark, skip or ignore)"),
        }
    }
}

/// Mirrors the Python bot's INCLUDE_TITLE / INCLUDE_FOOTER / INCLUDE_REDDIT_LINK.
#[derive(Clone, Copy, Debug)]
pub struct CaptionOpts {
    pub title: bool,
    pub footer: bool,
    pub link: bool,
}

#[derive(Clone, Debug)]
pub struct SubConfig {
    pub name: String,
    pub chat_id: String,
    pub sort: Sort,
    pub media_only: bool,
    pub limit: usize,
}

/// Subreddits sharing chat/sort/media-only settings for scheduler processing.
/// Each subreddit is still fetched independently to enforce its own post limit.
#[derive(Clone, Debug)]
pub struct Group {
    pub names: Vec<String>,
    pub limits: Vec<usize>,
    pub chat_id: String,
    pub sort: Sort,
    pub media_only: bool,
}

pub fn build_groups(subs: &[SubConfig], enabled: bool, max: usize) -> Vec<Group> {
    let mut groups: Vec<Group> = Vec::new();
    for s in subs {
        if enabled {
            if let Some(g) = groups
                .iter_mut()
                .find(|g| g.chat_id == s.chat_id && g.sort == s.sort && g.media_only == s.media_only && g.names.len() < max)
            {
                g.names.push(s.name.clone());
                g.limits.push(s.limit);
                continue;
            }
        }
        groups.push(Group { names: vec![s.name.clone()], limits: vec![s.limit], chat_id: s.chat_id.clone(), sort: s.sort, media_only: s.media_only });
    }
    groups
}

#[derive(Clone)]
pub struct BrowserSettings {
    pub bin: String,
    pub port: u16,
    pub extra_args: Vec<String>,
    pub page_timeout: Duration,
    /// Optional Reddit login cookie. Treat like a password; never log or commit it.
    pub reddit_session_cookie: Option<String>,
}

/// Deliberately no `Debug` derive: holds secrets.
#[derive(Clone)]
pub struct Config {
    pub subs: Vec<SubConfig>,
    pub groups: Vec<Group>,
    pub interval: Duration,
    pub cycle_budget: Duration,
    /// Maximum wall-clock time allowed for one post, including network and upload work.
    pub post_delivery_timeout: Duration,
    /// Total deadline for one Telegram API operation, including fast retries.
    pub telegram_timeout: Duration,
    /// Deadline for one media download / Reddit video acquisition operation.
    pub media_download_timeout: Duration,
    /// Maximum time spent reading one Reddit gallery metadata page.
    pub gallery_lookup_timeout: Duration,
    pub post_limit: usize,
    pub request_gap: Duration,
    pub send_gap: Duration,
    pub flush_min: Duration,
    pub reddit_base: String,
    pub telegram_token: String,
    pub telegram_base: String,
    pub state_path: PathBuf,
    pub port: u16,
    pub self_ping: Option<(String, Duration)>,
    pub browser: BrowserSettings,
    pub ffmpeg_bin: String,
    pub ffprobe_bin: String,
    pub photo_max_dim: u32,
    pub max_photo_bytes: u64,
    pub max_upload_bytes: u64,
    pub send_backlog: bool,
    pub spoiler_mode: SpoilerMode,
    pub include_nsfw: bool,
    pub dedupe_media: bool,
    pub caption: CaptionOpts,
    pub recycle_cycles: u64,
    pub seen_cap: usize,
    /// Total listing pages fetched per subreddit per cycle while catching up (1 = no paging).
    pub catchup_max_pages: usize,
    /// Posts older than this are never sent while catching up (ZERO = no limit).
    pub catchup_max_age: Duration,
    /// Due retries attempted per cycle.
    pub retry_batch: usize,
    /// A failing post is given up on (marked seen) after this many attempts.
    pub max_delivery_attempts: u8,
    /// Total time allowed to look up one gallery's media list.
    pub gallery_resolve_timeout: Duration,
    /// Stop the browser between polls to free memory on small hosts.
    pub browser_idle_shutdown: bool,
    /// Send silent videos (Reddit "gifs") as Telegram animations.
    pub silent_as_animation: bool,
    pub health_enabled: bool,
    pub temp_dir: Option<PathBuf>,
}

type Get<'a> = &'a dyn Fn(&str) -> Option<String>;

fn num<T>(get: Get, key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match get(key) {
        None => Ok(default),
        Some(v) => v.trim().parse::<T>().map_err(|e| anyhow!("{key}: {e}")),
    }
}

fn truthy(v: &str) -> bool {
    matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

fn flag(get: Get, key: &str, default: bool) -> bool {
    get(key).map(|v| truthy(&v)).unwrap_or(default)
}

/// First of several env var names that is set (lets the old Python bot's names keep working).
fn first(get: Get, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|k| get(k))
}

fn check_limit(v: usize) -> Result<usize> {
    if v == 0 || v > 50 {
        bail!("post limit must be 1..=50 (got {v})");
    }
    Ok(v)
}

fn duration_secs(get: Get, key: &str, default: u64, min: u64, max: u64) -> Result<Duration> {
    let value = num(get, key, default)?;
    if !(min..=max).contains(&value) {
        bail!("{key} must be between {min} and {max} seconds (got {value})");
    }
    Ok(Duration::from_secs(value))
}

fn s(get: Get, key: &str, default: &str) -> String {
    get(key).unwrap_or_else(|| default.to_string())
}

static DOTENV: OnceLock<HashMap<String, String>> = OnceLock::new();

/// Real environment variables win; values from a `.env` file (see `load_dotenv`) are the fallback.
pub fn env_get(k: &str) -> Option<String> {
    std::env::var(k)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| {
            DOTENV
                .get()
                .and_then(|m| m.get(k))
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
}

/// Parses `KEY=VALUE` lines (comments, `export `, and surrounding quotes allowed).
pub fn parse_dotenv(text: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for line in text.lines() {
        let line = line.trim().trim_start_matches('\u{feff}');
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, value)) = line.split_once('=') else { continue };
        let key = key.trim();
        if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            continue;
        }
        let mut value = value.trim();
        for quote in ['"', '\''] {
            if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
                value = &value[1..value.len() - 1];
                break;
            }
        }
        out.insert(key.to_string(), value.to_string());
    }
    out
}

/// Loads `.env` (or the file named by ENV_FILE) so hosts without an environment-variable panel
/// work too. Returns the file used and how many keys it defined; values are never logged.
pub fn load_dotenv() -> Option<(PathBuf, usize)> {
    let candidates: Vec<PathBuf> = match std::env::var("ENV_FILE") {
        Ok(f) if !f.trim().is_empty() => vec![PathBuf::from(f.trim())],
        _ => vec![PathBuf::from(".env"), PathBuf::from("/home/container/.env")],
    };
    for path in candidates {
        if let Ok(text) = std::fs::read_to_string(&path) {
            let map = parse_dotenv(&text);
            let n = map.len();
            let _ = DOTENV.set(map);
            return Some((path, n));
        }
    }
    None
}

/// First existing file among the usual panel locations, else the bare name (PATH lookup).
pub fn discover_binary(name: &str) -> String {
    for dir in [".", "/home/container", "/home/container/bin", "/usr/local/bin", "/usr/bin"] {
        let candidate = std::path::Path::new(dir).join(name);
        if candidate.is_file() {
            return candidate.to_string_lossy().into_owned();
        }
    }
    name.to_string()
}

fn norm_name(n: &str) -> Result<String> {
    let n = n.trim().trim_start_matches('/').trim_start_matches("r/").trim_matches('/').to_string();
    if n.is_empty() || !n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        bail!("invalid subreddit name {n:?} (letters, digits, underscore only)");
    }
    Ok(n)
}

fn add_sub(
    subs: &mut Vec<SubConfig>,
    name: &str,
    chat: Option<&str>,
    default_chat: Option<&str>,
    sort: Sort,
    media_only: bool,
    limit: usize,
) -> Result<()> {
    let name = norm_name(name)?;
    let chat = chat
        .or(default_chat)
        .ok_or_else(|| anyhow!("subreddit {name}: no chat id given and DEFAULT_CHAT_ID is not set"))?
        .to_string();
    // Keep distinct sort/filter streams, but collapse duplicate entries for the
    // same stream and retain the largest requested limit.
    if let Some(existing) = subs.iter_mut().find(|x| {
        x.name.eq_ignore_ascii_case(&name)
            && x.chat_id == chat
            && x.sort == sort
            && x.media_only == media_only
    }) {
        existing.limit = existing.limit.max(limit);
        return Ok(());
    }
    subs.push(SubConfig { name, chat_id: chat, sort, media_only, limit });
    Ok(())
}

impl BrowserSettings {
    pub fn from_lookup(get: Get) -> Result<Self> {
        Ok(Self {
            bin: s(get, "LIGHTPANDA_BIN", &discover_binary("lightpanda")),
            port: num(get, "LIGHTPANDA_PORT", 9222u16)?,
            extra_args: get("LIGHTPANDA_EXTRA_ARGS")
                .map(|v| v.split_whitespace().map(String::from).collect())
                .unwrap_or_default(),
            page_timeout: Duration::from_secs(num(get, "PAGE_TIMEOUT_SECS", 90u64)?),
            reddit_session_cookie: get("REDDIT_SESSION_COOKIE")
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty()),
        })
    }
}

impl Config {
    pub fn from_env(require_subs: bool) -> Result<Self> {
        Self::from_lookup(&env_get, require_subs)
    }

    pub fn from_lookup(get: Get, require_subs: bool) -> Result<Self> {
        let default_sort = match first(get, &["SORT", "SUBREDDIT_DEFAULT_SORT"]) {
            Some(v) => Sort::parse(&v)?,
            None => Sort::New,
        };
        // Same default as the Python bot: SKIP_TEXT_ONLY_POSTS=true  ==  MEDIA_ONLY=true.
        let default_media_only = first(get, &["MEDIA_ONLY", "SKIP_TEXT_ONLY_POSTS"]).map(|v| truthy(&v)).unwrap_or(true);
        let default_limit = check_limit(match first(get, &["POST_LIMIT", "SUBREDDIT_DEFAULT_LIMIT"]) {
            Some(v) => v.trim().parse::<usize>().map_err(|e| anyhow!("POST_LIMIT: {e}"))?,
            None => 12,
        })?;
        let default_chat = get("DEFAULT_CHAT_ID");
        let max_subs: usize = num(get, "MAX_SUBREDDITS", 100usize)?.clamp(1, 500);

        let mut subs = Vec::new();
        // 1) SUBREDDIT_n_NAME / _CHAT_ID / _SORT / _MEDIA_ONLY (chat id optional if DEFAULT_CHAT_ID set)
        for i in 1..=max_subs {
            let name = get(&format!("SUBREDDIT_{i}_NAME"));
            let chat = get(&format!("SUBREDDIT_{i}_CHAT_ID"));
            match name {
                Some(n) => {
                    let sort = match get(&format!("SUBREDDIT_{i}_SORT")) {
                        Some(v) => Sort::parse(&v)?,
                        None => default_sort,
                    };
                    let mo = flag(get, &format!("SUBREDDIT_{i}_MEDIA_ONLY"), default_media_only);
                    let limit = match get(&format!("SUBREDDIT_{i}_LIMIT")) {
                        Some(v) => check_limit(v.trim().parse::<usize>().map_err(|e| anyhow!("SUBREDDIT_{i}_LIMIT: {e}"))?)?,
                        None => default_limit,
                    };
                    add_sub(&mut subs, &n, chat.as_deref(), default_chat.as_deref(), sort, mo, limit)?;
                }
                None if chat.is_some() => bail!("SUBREDDIT_{i}_CHAT_ID is set without SUBREDDIT_{i}_NAME"),
                None => {}
            }
        }
        // 2) SUBREDDITS="a,b=@otherchat,c"
        if let Some(list) = get("SUBREDDITS") {
            for item in list.split(',').map(str::trim).filter(|x| !x.is_empty()) {
                let (n, c) = match item.split_once('=') {
                    Some((n, c)) => (n, Some(c.trim()).filter(|c| !c.is_empty())),
                    None => (item, None),
                };
                add_sub(&mut subs, n, c, default_chat.as_deref(), default_sort, default_media_only, default_limit)?;
            }
        }
        if subs.len() > max_subs {
            bail!("{} subreddits configured but MAX_SUBREDDITS={max_subs}; raise it if you really want more", subs.len());
        }
        if require_subs && subs.is_empty() {
            bail!("no subreddits configured (set SUBREDDITS=a,b,c with DEFAULT_CHAT_ID, or SUBREDDIT_1_NAME/_CHAT_ID ...)");
        }

        let telegram_token = get("TELEGRAM_BOT_TOKEN").ok_or_else(|| anyhow!("TELEGRAM_BOT_TOKEN is required"))?;

        let interval_secs = num(get, "POLL_INTERVAL_SECS", 900u64)?.max(60);
        let post_limit = default_limit;

        let self_ping = if flag(get, "SELF_PING_ENABLED", true) {
            get("RENDER_EXTERNAL_URL").map(|u| {
                (
                    u.trim_end_matches('/').to_string(),
                    Duration::from_secs(num(get, "SELF_PING_INTERVAL_SECS", 600u64).unwrap_or(600).max(5)),
                )
            })
        } else {
            None
        };

        let group_max: usize = num(get, "GROUP_MAX_SUBS", 8usize)?.clamp(1, 15);
        let groups = build_groups(&subs, flag(get, "GROUP_REQUESTS", true), group_max);

        Ok(Config {
            subs,
            groups,
            interval: Duration::from_secs(interval_secs),
            // Stop starting new page loads when this much of the interval is used; the
            // remaining groups continue next cycle (round-robin), so any sub count degrades gracefully.
            cycle_budget: Duration::from_secs(num(get, "CYCLE_BUDGET_SECS", interval_secs * 4 / 5)?.max(30)),
            post_delivery_timeout: duration_secs(get, "POST_DELIVERY_TIMEOUT_SECS", 180, 15, 600)?,
            telegram_timeout: duration_secs(get, "TELEGRAM_TIMEOUT_SECS", 120, 10, 600)?,
            media_download_timeout: duration_secs(get, "MEDIA_DOWNLOAD_TIMEOUT_SECS", 120, 10, 600)?,
            gallery_lookup_timeout: duration_secs(get, "GALLERY_LOOKUP_TIMEOUT_SECS", 30, 5, 90)?,
            post_limit,
            request_gap: Duration::from_secs(num(get, "REQUEST_GAP_SECS", 5u64)?),
            send_gap: Duration::from_millis(num(get, "SEND_GAP_MS", 3000u64)?),
            flush_min: Duration::from_secs(match get("STATE_FLUSH_MIN_SECS") {
                Some(v) => v.trim().parse::<u64>().map_err(|e| anyhow!("STATE_FLUSH_MIN_SECS: {e}"))?,
                None => 60,
            }),
            reddit_base: s(get, "REDDIT_BASE_URL", "https://old.reddit.com").trim_end_matches('/').to_string(),
            telegram_token,
            telegram_base: s(get, "TELEGRAM_API_BASE", "https://api.telegram.org").trim_end_matches('/').to_string(),
            state_path: PathBuf::from(s(get, "STATE_PATH", "/home/container/data/seen_posts.json")),
            // Render sets PORT; Pterodactyl-style panels (Altare) set SERVER_PORT.
            port: match first(get, &["PORT", "SERVER_PORT"]) {
                Some(v) => v.trim().parse::<u16>().map_err(|e| anyhow!("PORT: {e}"))?,
                None => 10000,
            },
            self_ping,
            browser: BrowserSettings::from_lookup(get)?,
            ffmpeg_bin: s(get, "FFMPEG_BIN", &discover_binary("ffmpeg")),
            ffprobe_bin: s(get, "FFPROBE_BIN", &discover_binary("ffprobe")),
            // Telegram needs width+height <= 10000; it displays photos at <= 2560 px anyway.
            photo_max_dim: num(get, "PHOTO_MAX_DIM", 2560u32)?.clamp(320, 4999),
            max_photo_bytes: 10 * 1024 * 1024,
            max_upload_bytes: match first(get, &["MAX_UPLOAD_BYTES", "MAX_MEDIA_DOWNLOAD_BYTES"]) {
                Some(v) => v.trim().parse::<u64>().map_err(|e| anyhow!("MAX_UPLOAD_BYTES: {e}"))?,
                None => 50 * 1024 * 1024,
            }
            .min(50 * 1024 * 1024),
            send_backlog: flag(get, "SEND_BACKLOG_ON_FIRST_RUN", false),
            spoiler_mode: match get("SPOILER_MODE") {
                Some(v) => SpoilerMode::parse(&v)?,
                None if flag(get, "BLUR_SPOILERS", true) => SpoilerMode::Mark,
                None => SpoilerMode::Ignore,
            },
            include_nsfw: flag(get, "INCLUDE_NSFW", true),
            dedupe_media: flag(get, "DEDUPE_MEDIA", true),
            caption: CaptionOpts {
                title: flag(get, "INCLUDE_TITLE", true),
                footer: flag(get, "INCLUDE_FOOTER", true),
                link: flag(get, "INCLUDE_REDDIT_LINK", true),
            },
            recycle_cycles: num(get, "BROWSER_RECYCLE_CYCLES", 12u64)?.max(1),
            seen_cap: num(get, "SEEN_CAP", 20000usize)?.clamp(100, 30000),
            catchup_max_pages: if flag(get, "CATCHUP_ENABLED", true) {
                num(get, "CATCHUP_MAX_PAGES", 6usize)?.clamp(1, 30)
            } else {
                1
            },
            catchup_max_age: Duration::from_secs(num(get, "CATCHUP_MAX_AGE_HOURS", 72u64)?.min(24 * 365) * 3600),
            retry_batch: num(get, "RETRY_BATCH", 3usize)?.clamp(1, 20),
            max_delivery_attempts: num(get, "MAX_DELIVERY_ATTEMPTS", 8u8)?.max(1),
            gallery_resolve_timeout: duration_secs(get, "GALLERY_RESOLVE_TIMEOUT_SECS", 120, 10, 600)?,
            browser_idle_shutdown: flag(get, "BROWSER_IDLE_SHUTDOWN", true),
            silent_as_animation: flag(get, "SILENT_VIDEO_AS_ANIMATION", true),
            health_enabled: flag(get, "HEALTH_ENABLED", true),
            temp_dir: get("TEMP_DIR").map(PathBuf::from).or_else(|| {
                // Panel hosts: /tmp may be a tiny tmpfs; the server's own data disk is safer.
                let home = PathBuf::from("/home/container");
                home.is_dir().then(|| home.join("tmp"))
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)], req: bool) -> Result<Config> {
        let m: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Config::from_lookup(&|k| m.get(k).cloned(), req)
    }

    #[test]
    fn numbered_subs_and_defaults() {
        let mut m: Vec<(String, String)> = vec![("TELEGRAM_BOT_TOKEN".into(), "x".into())];
        for (i, n) in ["a", "r/b", "c", "d", "e"].iter().enumerate() {
            m.push((format!("SUBREDDIT_{}_NAME", i + 1), n.to_string()));
            m.push((format!("SUBREDDIT_{}_CHAT_ID", i + 1), format!("@{}", i + 1)));
        }
        let p: Vec<(&str, &str)> = m.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
        let c = cfg(&p, true).unwrap();
        assert_eq!(c.subs.len(), 5);
        assert_eq!(c.groups.len(), 5); // five different chats -> five groups
        assert_eq!(c.subs[1].name, "b");
        assert_eq!(c.interval.as_secs(), 900);
        assert_eq!(c.cycle_budget.as_secs(), 720);
        assert_eq!(c.spoiler_mode, SpoilerMode::Mark);
        assert!(c.self_ping.is_none());
    }

    #[test]
    fn many_subs_one_channel_are_grouped() {
        let c = cfg(
            &[("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@anime"), ("SUBREDDITS", "anime, r/manga ,animememes,AnimeART,x1,x2,x3,x4,x5,x6")],
            true,
        )
        .unwrap();
        assert_eq!(c.subs.len(), 10);
        assert_eq!(c.groups.len(), 2); // 8 + 2
        assert_eq!(c.groups[0].names.len(), 8);
        assert_eq!(c.groups[0].names[1], "manga");
        let c = cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@a"), ("SUBREDDITS", "a,b"), ("GROUP_REQUESTS", "false")], true).unwrap();
        assert_eq!(c.groups.len(), 2);
    }

    #[test]
    fn list_entry_can_override_chat_and_dedupes() {
        let c = cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@a"), ("SUBREDDITS", "one,two=@b,one")], true).unwrap();
        assert_eq!(c.subs.len(), 2);
        assert_eq!(c.subs[1].chat_id, "@b");
    }

    #[test]
    fn same_subreddit_can_be_configured_for_new_and_hot() {
        let c = cfg(
            &[
                ("TELEGRAM_BOT_TOKEN", "x"),
                ("DEFAULT_CHAT_ID", "@a"),
                ("SUBREDDIT_1_NAME", "anime"),
                ("SUBREDDIT_1_SORT", "new"),
                ("SUBREDDIT_2_NAME", "ANIME"),
                ("SUBREDDIT_2_SORT", "hot"),
            ],
            true,
        )
        .unwrap();
        assert_eq!(c.subs.len(), 2);
        assert_eq!(c.groups.len(), 2);
        assert_eq!(c.groups[0].sort, Sort::New);
        assert_eq!(c.groups[1].sort, Sort::Hot);
    }

    #[test]
    fn only_exact_duplicate_subreddit_entries_are_ignored() {
        let c = cfg(
            &[
                ("TELEGRAM_BOT_TOKEN", "x"),
                ("DEFAULT_CHAT_ID", "@a"),
                ("SUBREDDIT_1_NAME", "anime"),
                ("SUBREDDIT_1_SORT", "new"),
                ("SUBREDDIT_2_NAME", "anime"),
                ("SUBREDDIT_2_SORT", "new"),
                ("SUBREDDIT_2_LIMIT", "20"),
            ],
            true,
        )
        .unwrap();
        assert_eq!(c.subs.len(), 1);
        assert_eq!(c.subs[0].limit, 20);
    }

    #[test]
    fn nsfw_is_included_by_default_and_can_be_disabled() {
        let default = cfg(&[("TELEGRAM_BOT_TOKEN", "x")], false).unwrap();
        assert!(default.include_nsfw);
        let disabled = cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("INCLUDE_NSFW", "false")], false).unwrap();
        assert!(!disabled.include_nsfw);
    }

    #[test]
    fn media_only_per_sub_override_splits_groups() {
        let c = cfg(
            &[("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@a"), ("MEDIA_ONLY", "true"),
              ("SUBREDDIT_1_NAME", "a"), ("SUBREDDIT_2_NAME", "b"), ("SUBREDDIT_2_MEDIA_ONLY", "false")],
            true,
        )
        .unwrap();
        assert!(c.subs[0].media_only && !c.subs[1].media_only);
        assert_eq!(c.groups.len(), 2);
    }

    #[test]
    fn python_bot_env_names_still_work() {
        let c = cfg(
            &[("TELEGRAM_BOT_TOKEN", "x"), ("SUBREDDIT_1_NAME", "aww"), ("SUBREDDIT_1_CHAT_ID", "@a"), ("SUBREDDIT_1_LIMIT", "20"),
              ("SUBREDDIT_2_NAME", "pics"), ("SUBREDDIT_2_CHAT_ID", "@a"), ("SUBREDDIT_DEFAULT_SORT", "hot"),
              ("SUBREDDIT_DEFAULT_LIMIT", "7"), ("SKIP_TEXT_ONLY_POSTS", "false"), ("STATE_FLUSH_MIN_SECS", "300"), ("BLUR_SPOILERS", "false")],
            true,
        )
        .unwrap();
        assert_eq!(c.groups.len(), 1);
        assert_eq!(c.groups[0].limits, vec![20, 7]);
        assert_eq!(c.subs[1].sort, Sort::Hot);
        assert!(!c.subs[0].media_only);
        assert_eq!(c.flush_min.as_secs(), 300);
        assert_eq!(c.spoiler_mode, SpoilerMode::Ignore);
    }

    #[test]
    fn defaults_match_python_bot() {
        let c = cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("SUBREDDIT_1_NAME", "a"), ("SUBREDDIT_1_CHAT_ID", "@a")], true).unwrap();
        assert!(c.subs[0].media_only); // SKIP_TEXT_ONLY_POSTS defaults to true there
        assert_eq!(c.subs[0].limit, 12);
        assert_eq!(c.flush_min.as_secs(), 60);
        assert!(c.caption.title && c.caption.footer && c.caption.link);
    }

    #[test]
    fn rejects_bad_configs() {
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("SUBREDDIT_1_NAME", "a")], true).is_err()); // no chat, no default
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("SUBREDDIT_1_CHAT_ID", "@a")], true).is_err());
        assert!(cfg(&[("SUBREDDIT_1_NAME", "a"), ("SUBREDDIT_1_CHAT_ID", "@a")], true).is_err()); // no token
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x")], true).is_err());
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x")], false).is_ok());
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@a"), ("SUBREDDITS", "bad/name")], true).is_err());
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@a"), ("SUBREDDITS", "a,b,c"), ("MAX_SUBREDDITS", "2")], true).is_err());
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("SPOILER_MODE", "nope")], false).is_err());
    }

    #[test]
    fn operation_timeouts_have_bounded_defaults_and_overrides() {
        let defaults = cfg(&[("TELEGRAM_BOT_TOKEN", "x")], false).unwrap();
        assert_eq!(defaults.post_delivery_timeout, Duration::from_secs(180));
        assert_eq!(defaults.telegram_timeout, Duration::from_secs(120));
        assert_eq!(defaults.media_download_timeout, Duration::from_secs(120));
        assert_eq!(defaults.gallery_lookup_timeout, Duration::from_secs(30));

        let custom = cfg(&[
            ("TELEGRAM_BOT_TOKEN", "x"),
            ("POST_DELIVERY_TIMEOUT_SECS", "240"),
            ("TELEGRAM_TIMEOUT_SECS", "90"),
            ("MEDIA_DOWNLOAD_TIMEOUT_SECS", "75"),
            ("GALLERY_LOOKUP_TIMEOUT_SECS", "20"),
        ], false).unwrap();
        assert_eq!(custom.post_delivery_timeout, Duration::from_secs(240));
        assert_eq!(custom.telegram_timeout, Duration::from_secs(90));
        assert_eq!(custom.media_download_timeout, Duration::from_secs(75));
        assert_eq!(custom.gallery_lookup_timeout, Duration::from_secs(20));
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("TELEGRAM_TIMEOUT_SECS", "2")], false).is_err());
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("GALLERY_LOOKUP_TIMEOUT_SECS", "999")], false).is_err());
    }

    #[test]
    fn self_ping_needs_render_url_and_flag() {
        let base = [("TELEGRAM_BOT_TOKEN", "x"), ("RENDER_EXTERNAL_URL", "https://a.onrender.com/")];
        assert_eq!(cfg(&base, false).unwrap().self_ping.unwrap().0, "https://a.onrender.com");
        let off = [("TELEGRAM_BOT_TOKEN", "x"), ("RENDER_EXTERNAL_URL", "https://a"), ("SELF_PING_ENABLED", "false")];
        assert!(cfg(&off, false).unwrap().self_ping.is_none());
    }

    #[test]
    fn dotenv_parsing_handles_comments_quotes_and_export() {
        let m = parse_dotenv("# c\nTELEGRAM_BOT_TOKEN=\"abc:def\"\nexport SORT='new'\n\nBAD LINE\nSUBREDDITS = a,b \nX-Y=1\n");
        assert_eq!(m.get("TELEGRAM_BOT_TOKEN").map(String::as_str), Some("abc:def"));
        assert_eq!(m.get("SORT").map(String::as_str), Some("new"));
        assert_eq!(m.get("SUBREDDITS").map(String::as_str), Some("a,b"));
        assert_eq!(m.len(), 3);
    }

    #[test]
    fn catchup_defaults_and_overrides() {
        let d = cfg(&[("TELEGRAM_BOT_TOKEN", "x")], false).unwrap();
        assert_eq!(d.catchup_max_pages, 6);
        assert_eq!(d.catchup_max_age, Duration::from_secs(72 * 3600));
        assert_eq!((d.retry_batch, d.max_delivery_attempts), (3, 8));
        assert!(d.browser_idle_shutdown && d.silent_as_animation && d.health_enabled);
        let c = cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("CATCHUP_MAX_PAGES", "50"), ("CATCHUP_MAX_AGE_HOURS", "0"), ("SERVER_PORT", "25565")], false).unwrap();
        assert_eq!(c.catchup_max_pages, 30);
        assert!(c.catchup_max_age.is_zero());
        assert_eq!(c.port, 25565);
        let off = cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("CATCHUP_ENABLED", "false")], false).unwrap();
        assert_eq!(off.catchup_max_pages, 1);
    }
}
