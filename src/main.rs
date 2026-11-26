mod catchup;
mod config;
mod health;
mod media_downloader;
mod reddit_browser;
mod reddit_parser;
mod scheduler;
mod seen_store;
mod self_ping;
mod telegram;

use anyhow::{anyhow, Result};
use config::{env_get, BrowserSettings, Config, Sort};
use reqwest::Client;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{timeout, Instant};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{fmt::MakeWriter, prelude::*, EnvFilter};

const LOG_ROTATE_BYTES: u64 = 5 * 1024 * 1024;

struct RotatingLog {
    path: PathBuf,
    file: File,
    written: u64,
}

#[derive(Clone)]
struct LogMakeWriter(Arc<Mutex<RotatingLog>>);

struct LogWriter(Arc<Mutex<RotatingLog>>);

impl<'a> MakeWriter<'a> for LogMakeWriter {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(self.0.clone())
    }
}

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut log = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if log.written.saturating_add(buf.len() as u64) > LOG_ROTATE_BYTES {
            log.file.flush()?;
            let previous = log.path.with_extension("log.1");
            match fs::remove_file(&previous) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            fs::rename(&log.path, &previous)?;
            log.file = OpenOptions::new().create(true).append(true).open(&log.path)?;
            log.written = 0;
        }
        let n = log.file.write(buf)?;
        log.written = log.written.saturating_add(n as u64);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).file.flush()
    }
}

fn init_tracing() {
    let filter = match env_get("RUST_LOG") {
        Some(v) => EnvFilter::try_new(v).unwrap_or_else(|_| EnvFilter::new("info")),
        None => EnvFilter::new("info"),
    };
    let path = PathBuf::from(env_get("LOG_PATH").unwrap_or_else(|| "/home/container/logs/bot.log".into()));
    let file_writer = (|| -> io::Result<LogMakeWriter> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        if fs::metadata(&path).map(|m| m.len() >= LOG_ROTATE_BYTES).unwrap_or(false) {
            let previous = path.with_extension("log.1");
            match fs::remove_file(&previous) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            fs::rename(&path, &previous)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let written = file.metadata()?.len();
        Ok(LogMakeWriter(Arc::new(Mutex::new(RotatingLog { path: path.clone(), file, written }))))
    })();

    let console_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    match file_writer {
        Ok(writer) => {
            tracing_subscriber::registry()
                .with(filter)
                .with(console_layer)
                .with(tracing_subscriber::fmt::layer().with_ansi(false).with_writer(writer))
                .init();
            tracing::info!(log_path = %path.display(), "file logging enabled; console logging remains enabled");
        }
        Err(e) => {
            eprintln!("Cannot open log file {}: {e}; continuing with console-only logging", path.display());
            tracing_subscriber::registry().with(filter).with(console_layer).init();
        }
    }
}

fn http_client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent(concat!("reddit-telegram-bot/", env!("CARGO_PKG_VERSION")))
        .pool_max_idle_per_host(1)
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(120))
        .build()?)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let dotenv = config::load_dotenv();
    init_tracing();
    if let Some((path, keys)) = dotenv {
        info!(file = %path.display(), keys, "loaded settings from env file (real environment variables take precedence)");
    }
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("fetch-test") => fetch_test(&args).await,
        Some("media-test") => media_test(&args).await,
        _ => run().await,
    }
}

async fn wait_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = term.recv() => {},
                _ = tokio::signal::ctrl_c() => {},
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn run() -> Result<()> {
    let cfg = Arc::new(Config::from_env(true)?);
    let configured_subreddits = cfg.subs.iter().map(|s| s.name.as_str()).collect::<Vec<_>>().join(",");
    let configured_groups = cfg.groups.iter().map(|g| g.names.join("+")).collect::<Vec<_>>().join(" | ");
    info!(
        subs = cfg.subs.len(),
        subreddits = %configured_subreddits,
        groups = %configured_groups,
        page_loads_per_cycle = cfg.groups.len(),
        media_only = cfg.subs.iter().all(|s| s.media_only),
        interval_secs = cfg.interval.as_secs(),
        limit = cfg.post_limit,
        "starting"
    );
    let http = http_client()?;
    let token = CancellationToken::new();
    let health = Arc::new(health::Health::new());

    if let Some(dir) = &cfg.temp_dir {
        match fs::create_dir_all(dir) {
            Ok(()) => md_set_temp_dir(dir.clone()),
            Err(e) => tracing::warn!(dir = %dir.display(), "cannot create temp dir ({e}); using the system temp dir"),
        }
    }
    let stale = media_downloader::cleanup_temp(Duration::ZERO);
    if stale > 0 {
        info!(stale, "removed temporary media files left over from a previous run");
    }
    preflight(&cfg).await?;

    tokio::spawn({
        let t = token.clone();
        async move {
            wait_signal().await;
            info!("shutdown signal received");
            t.cancel();
        }
    });

    // Bind the port first so Render's port scan succeeds while state is restored.
    let hs = if cfg.health_enabled {
        tokio::spawn(health::serve(cfg.port, health.clone(), token.clone()))
    } else {
        info!("health server disabled (HEALTH_ENABLED=false)");
        tokio::spawn(async { Ok::<(), anyhow::Error>(()) })
    };

    info!(state_path = %cfg.state_path.display(), "using host-local seen-state storage");
    let store = seen_store::SeenStore::open(cfg.state_path.clone(), cfg.seen_cap).await?;

    if let Some((url, every)) = cfg.self_ping.clone() {
        tokio::spawn(self_ping::run(http.clone(), url, every, token.clone()));
    } else {
        info!("self-ping disabled (not on Render or SELF_PING_ENABLED=false)");
    }

    let tg = telegram::Telegram::new(http.clone(), cfg.telegram_base.clone(), cfg.telegram_token.clone(), cfg.telegram_timeout);
    let sched = scheduler::Scheduler::new(
        cfg.clone(),
        reddit_browser::Browser::new(cfg.browser.clone()),
        store,
        scheduler::Deliverer::new(cfg.clone(), http.clone(), tg),
        health,
        token.clone(),
    );
    sched.run().await;
    token.cancel();
    let _ = timeout(Duration::from_secs(5), hs).await;
    info!("bye");
    Ok(())
}

fn md_set_temp_dir(dir: PathBuf) {
    media_downloader::set_temp_dir(dir);
}

/// True if `bin` is an existing file (when it contains a path separator) or is found on PATH.
fn binary_exists(bin: &str) -> bool {
    if bin.contains('/') {
        return std::path::Path::new(bin).is_file();
    }
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(bin).is_file()))
        .unwrap_or(false)
}

/// Runs a binary with `-version` and returns its first output line.
async fn binary_version(bin: &str, arg: &str) -> Option<String> {
    let out = timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(bin)
            .arg(arg)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    let text = String::from_utf8_lossy(if out.stdout.is_empty() { &out.stderr } else { &out.stdout }).to_string();
    text.lines().next().map(|l| l.trim().to_string())
}

/// Startup diagnostics: tells the operator exactly which helper binaries were found and what
/// degrades without them, instead of failing mysteriously later.
async fn preflight(cfg: &Config) -> Result<()> {
    let browser = &cfg.browser.bin;
    if !binary_exists(browser) {
        return Err(anyhow!(
            "Lightpanda browser not found at {browser:?}. Upload the lightpanda binary to the server (for example /home/container/lightpanda), \
             make it executable, or set LIGHTPANDA_BIN to its path."
        ));
    }
    info!(path = %browser, "Lightpanda browser found");
    match binary_version(&cfg.ffmpeg_bin, "-version").await {
        Some(v) => info!(path = %cfg.ffmpeg_bin, version = %v, "ffmpeg found"),
        None => tracing::warn!(
            path = %cfg.ffmpeg_bin,
            "ffmpeg not found: Reddit videos will be sent WITHOUT audio, oversized images cannot be resized and gallery videos are skipped. Upload an ffmpeg binary or set FFMPEG_BIN."
        ),
    }
    match binary_version(&cfg.ffprobe_bin, "-version").await {
        Some(v) => info!(path = %cfg.ffprobe_bin, version = %v, "ffprobe found"),
        None => tracing::warn!(
            path = %cfg.ffprobe_bin,
            "ffprobe not found: videos are sent without width/height/duration/thumbnail and image resizing is unavailable. Set FFPROBE_BIN."
        ),
    }
    Ok(())
}

/// `reddit-telegram-bot fetch-test <subreddit> [new|hot]` : browser + parser only.
async fn fetch_test(args: &[String]) -> Result<()> {
    let sub = args.get(2).ok_or_else(|| anyhow!("usage: fetch-test <subreddit> [new|hot]"))?;
    let sort = args.get(3).map(|s| Sort::parse(s)).transpose()?.unwrap_or(Sort::New);
    let bs = BrowserSettings::from_lookup(&env_get)?;
    let base = env_get("REDDIT_BASE_URL").unwrap_or_else(|| "https://old.reddit.com".into());
    let url = format!("{}/r/{}/{}/?limit=10", base.trim_end_matches('/'), sub, sort.path());
    let mut b = reddit_browser::Browser::new(bs);
    let t0 = Instant::now();
    let res = b.fetch(&url).await;
    b.shutdown().await;
    let json = res?;
    println!("fetched in {:.1}s", t0.elapsed().as_secs_f32());
    match reddit_parser::parse_page(&json, 10)? {
        reddit_parser::PageStatus::Ok(posts) => {
            for p in &posts {
                println!("{} | u/{} | {:?} | {}", p.id, p.author, p.media, p.title);
            }
            println!("{} posts", posts.len());
            if posts.is_empty() {
                std::process::exit(2);
            }
        }
        reddit_parser::PageStatus::RateLimited => {
            println!("RESULT: rate limited");
            std::process::exit(3);
        }
        reddit_parser::PageStatus::Blocked(r) => {
            println!("RESULT: blocked ({r})");
            std::process::exit(4);
        }
    }
    Ok(())
}

/// `reddit-telegram-bot media-test <chat_id> <media_url>` : real download + upload path.
async fn media_test(args: &[String]) -> Result<()> {
    let chat = args.get(2).ok_or_else(|| anyhow!("usage: media-test <chat_id> <media_url> [spoiler]"))?;
    let url = args.get(3).ok_or_else(|| anyhow!("usage: media-test <chat_id> <media_url> [spoiler]"))?;
    let cfg = Arc::new(Config::from_env(false)?);
    let http = http_client()?;
    let tg = telegram::Telegram::new(http.clone(), cfg.telegram_base.clone(), cfg.telegram_token.clone(), cfg.telegram_timeout);
    let d = scheduler::Deliverer::new(cfg.clone(), http, tg);
    let post = reddit_parser::Post {
        id: "t3_mediatest".into(),
        title: "media-test".into(),
        author: "bot".into(),
        permalink: "https://www.reddit.com/".into(),
        source_url: String::new(),
        media: reddit_parser::classify_media(url),
        subreddit: "test".into(),
        spoiler: args.get(4).map(|a| a == "spoiler").unwrap_or(false),
        nsfw: false,
        created_ms: 0,
        crosspost: false,
    };
    println!("classified as {:?}", post.media);
    d.deliver(chat, &post, false).await?;
    println!("delivered");
    Ok(())
}
