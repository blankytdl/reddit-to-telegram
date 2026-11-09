use anyhow::{anyhow, bail, Result};
use std::{path::PathBuf, time::Duration};

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

/// Subreddits sharing chat/sort/media-only settings; fetched with ONE page load
/// via Reddit's `r/a+b+c` multi-subreddit listing.
#[derive(Clone, Debug)]
pub struct Group {
    pub names: Vec<String>,
    pub limits: Vec<usize>,
    pub chat_id: String,
    pub sort: Sort,
    pub media_only: bool,
}

impl Group {
    /// Posts to look at in the combined listing (Reddit allows at most 100 per page).
    pub fn total_limit(&self) -> usize {
        self.limits.iter().sum::<usize>().clamp(1, 100)
    }
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

#[derive(Clone)]
pub struct GithubSettings {
    pub token: String,
    pub repo: String,
    pub branch: String,
    pub api_base: String,
    /// Folder of the old Python bot's per-subreddit files (GITHUB_SEEN_PATH), imported once.
    pub seen_dir: String,
}

/// Deliberately no `Debug` derive: holds secrets.
#[derive(Clone)]
pub struct Config {
    pub subs: Vec<SubConfig>,
    pub groups: Vec<Group>,
    pub interval: Duration,
    pub cycle_budget: Duration,
    pub post_limit: usize,
    pub request_gap: Duration,
    pub send_gap: Duration,
    pub flush_min: Duration,
    pub reddit_base: String,
    pub telegram_token: String,
    pub telegram_base: String,
    pub github: Option<GithubSettings>,
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
    pub dedupe_media: bool,
    pub caption: CaptionOpts,
    pub recycle_cycles: u64,
    pub seen_cap: usize,
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

fn s(get: Get, key: &str, default: &str) -> String {
    get(key).unwrap_or_else(|| default.to_string())
}

pub fn env_get(k: &str) -> Option<String> {
    std::env::var(k).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
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
    if subs.iter().any(|x| x.name.eq_ignore_ascii_case(&name) && x.chat_id == chat) {
        return Ok(()); // exact duplicate
    }
    subs.push(SubConfig { name, chat_id: chat, sort, media_only, limit });
    Ok(())
}

impl BrowserSettings {
    pub fn from_lookup(get: Get) -> Result<Self> {
        Ok(Self {
            bin: s(get, "LIGHTPANDA_BIN", "/usr/local/bin/lightpanda"),
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

        let github = match (get("GITHUB_TOKEN"), get("GITHUB_REPO")) {
            (Some(token), Some(repo)) => Some(GithubSettings {
                token,
                repo,
                branch: s(get, "GITHUB_STATE_BRANCH", "bot-state"),
                api_base: s(get, "GITHUB_API_BASE", "https://api.github.com"),
                seen_dir: s(get, "GITHUB_SEEN_PATH", "seen").trim_matches('/').to_string(),
            }),
            (None, None) => None,
            _ => bail!("GITHUB_TOKEN and GITHUB_REPO must be set together"),
        };

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
            post_limit,
            request_gap: Duration::from_secs(num(get, "REQUEST_GAP_SECS", 5u64)?),
            send_gap: Duration::from_millis(num(get, "SEND_GAP_MS", 3000u64)?),
            flush_min: Duration::from_secs(match first(get, &["STATE_FLUSH_MIN_SECS", "GITHUB_SYNC_INTERVAL_SECONDS"]) {
                Some(v) => v.trim().parse::<u64>().map_err(|e| anyhow!("STATE_FLUSH_MIN_SECS: {e}"))?,
                None => 60,
            }),
            reddit_base: s(get, "REDDIT_BASE_URL", "https://old.reddit.com").trim_end_matches('/').to_string(),
            telegram_token,
            telegram_base: s(get, "TELEGRAM_API_BASE", "https://api.telegram.org").trim_end_matches('/').to_string(),
            github,
            state_path: PathBuf::from(s(get, "STATE_PATH", "/tmp/rtb-state/seen.json")),
            port: num(get, "PORT", 10000u16)?,
            self_ping,
            browser: BrowserSettings::from_lookup(get)?,
            ffmpeg_bin: s(get, "FFMPEG_BIN", "ffmpeg"),
            ffprobe_bin: s(get, "FFPROBE_BIN", "ffprobe"),
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
            dedupe_media: flag(get, "DEDUPE_MEDIA", true),
            caption: CaptionOpts {
                title: flag(get, "INCLUDE_TITLE", true),
                footer: flag(get, "INCLUDE_FOOTER", true),
                link: flag(get, "INCLUDE_REDDIT_LINK", true),
            },
            recycle_cycles: num(get, "BROWSER_RECYCLE_CYCLES", 12u64)?.max(1),
            seen_cap: num(get, "SEEN_CAP", 20000usize)?.clamp(100, 30000),
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
              ("SUBREDDIT_DEFAULT_LIMIT", "7"), ("SKIP_TEXT_ONLY_POSTS", "false"), ("GITHUB_SYNC_INTERVAL_SECONDS", "300"), ("BLUR_SPOILERS", "false")],
            true,
        )
        .unwrap();
        assert_eq!(c.groups.len(), 1);
        assert_eq!(c.groups[0].limits, vec![20, 7]);
        assert_eq!(c.groups[0].total_limit(), 27);
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
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("GITHUB_TOKEN", "t")], false).is_err());
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@a"), ("SUBREDDITS", "bad/name")], true).is_err());
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@a"), ("SUBREDDITS", "a,b,c"), ("MAX_SUBREDDITS", "2")], true).is_err());
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("SPOILER_MODE", "nope")], false).is_err());
    }

    #[test]
    fn self_ping_needs_render_url_and_flag() {
        let base = [("TELEGRAM_BOT_TOKEN", "x"), ("RENDER_EXTERNAL_URL", "https://a.onrender.com/")];
        assert_eq!(cfg(&base, false).unwrap().self_ping.unwrap().0, "https://a.onrender.com");
        let off = [("TELEGRAM_BOT_TOKEN", "x"), ("RENDER_EXTERNAL_URL", "https://a"), ("SELF_PING_ENABLED", "false")];
        assert!(cfg(&off, false).unwrap().self_ping.is_none());
    }
}
