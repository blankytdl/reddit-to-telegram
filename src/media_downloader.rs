//! Bounded, streaming downloads to temp files + FFmpeg stream-copy muxing for
//! Reddit DASH video/audio. Temp files are removed on Drop (success or failure).
use anyhow::{anyhow, bail, Result};
use futures_util::StreamExt;
use reqwest::Client;
use std::{
    fmt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};

static COUNTER: AtomicU64 = AtomicU64::new(0);
static TEMP_DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// Chooses where temporary media files live (call once at startup).
pub fn set_temp_dir(dir: PathBuf) {
    let _ = TEMP_DIR.set(dir);
}

fn temp_root() -> PathBuf {
    TEMP_DIR.get().cloned().unwrap_or_else(std::env::temp_dir)
}

/// Removes leftover `rtb-*` files (crash/kill leftovers) older than `min_age`.
/// Panel hosts keep temp files on the same quota as the server disk, so stale files must not pile up.
pub fn cleanup_temp(min_age: Duration) -> usize {
    let Ok(entries) = std::fs::read_dir(temp_root()) else { return 0 };
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("rtb-") {
            continue;
        }
        let old_enough = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .map_or(true, |age| age >= min_age);
        if old_enough && entry.path().is_file() && std::fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    removed
}

pub struct TempFile {
    path: PathBuf,
}

impl TempFile {
    pub fn new(ext: &str) -> Self {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = temp_root().join(format!("rtb-{}-{}.{}", std::process::id(), n, ext));
        Self { path }
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub async fn size(&self) -> u64 {
        tokio::fs::metadata(&self.path).await.map(|m| m.len()).unwrap_or(0)
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug)]
pub struct TooLarge(pub u64);
impl fmt::Display for TooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exceeds size limit (>= {} bytes)", self.0)
    }
}
impl std::error::Error for TooLarge {}

pub fn is_too_large(e: &anyhow::Error) -> bool {
    e.downcast_ref::<TooLarge>().is_some()
}

pub fn ext_of(url: &str) -> String {
    let p = url.split(['?', '#']).next().unwrap_or("");
    match p.rsplit_once('.') {
        Some((_, e)) if e.len() <= 5 && e.chars().all(|c| c.is_ascii_alphanumeric()) => e.to_ascii_lowercase(),
        _ => "bin".into(),
    }
}

pub fn ext_from_content_type(ct: &str) -> Option<&'static str> {
    match ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase().as_str() {
        "image/jpeg" | "image/jpg" | "image/pjpeg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/webp" => Some("webp"),
        "image/gif" => Some("gif"),
        "video/mp4" => Some("mp4"),
        _ => None,
    }
}

/// Streams `url` into a temp file; aborts as soon as `max` bytes are exceeded.
pub async fn download(client: &Client, url: &str, max: u64, ext: &str, request_timeout: Duration) -> Result<TempFile> {
    let resp = client
        .get(url)
        .timeout(request_timeout)
        .send()
        .await
        .map_err(|e| anyhow!(e.without_url()))?;
    if !resp.status().is_success() {
        bail!("download failed: HTTP {}", resp.status().as_u16());
    }
    if let Some(len) = resp.content_length() {
        if len > max {
            return Err(TooLarge(len).into());
        }
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    // An HTML/JSON error page served with HTTP 200 must never be uploaded as media.
    if content_type.starts_with("text/") || content_type.starts_with("application/json") {
        bail!("download returned {} instead of media", content_type.split(';').next().unwrap_or(""));
    }
    // Trust the real content type over the URL: preview URLs ending in .gif can be MP4s,
    // `.jpg` can be WebP, etc. A wrong extension makes Telegram reject or mis-handle the file.
    let tf = TempFile::new(ext_from_content_type(&content_type).unwrap_or(ext));
    let mut f = tokio::fs::File::create(tf.path()).await?;
    let mut total: u64 = 0;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| anyhow!(e.without_url()))?;
        total += chunk.len() as u64;
        if total > max {
            return Err(TooLarge(total).into());
        }
        f.write_all(&chunk).await?;
    }
    f.flush().await?;
    Ok(tf)
}

/// Returns Some(content_length) if the URL answers a HEAD with success.
async fn probe(client: &Client, url: &str, request_timeout: Duration) -> Option<Option<u64>> {
    let r = client.head(url).timeout(request_timeout.min(Duration::from_secs(10))).send().await.ok()?;
    if r.status().is_success() {
        Some(r.content_length())
    } else {
        None
    }
}

const AUDIO_RESERVE: u64 = 6 * 1024 * 1024;

/// Used only when the DASH manifest cannot be read. Reddit's newer transcodes use CMAF_* names;
/// older uploads use DASH_*. Not every resolution exists for every post.
fn legacy_video_names() -> Vec<String> {
    ["1080", "720", "480", "360", "270", "240", "220", "144", "96"]
        .iter()
        .flat_map(|q| [format!("CMAF_{q}.mp4"), format!("DASH_{q}.mp4")])
        .collect()
}

fn legacy_audio_names() -> Vec<String> {
    ["CMAF_AUDIO_128", "DASH_AUDIO_128", "CMAF_AUDIO_64", "DASH_AUDIO_64", "DASH_audio", "audio"]
        .iter()
        .map(|n| format!("{n}.mp4"))
        .collect()
}

/// Reads the `<BaseURL>` entries of a v.redd.it DASH manifest and returns
/// (video file names best-first, audio file names best-first).
pub fn parse_mpd_renditions(xml: &str) -> (Vec<String>, Vec<String>) {
    let mut video: Vec<(u32, String)> = Vec::new();
    let mut audio: Vec<(u32, String)> = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find("<BaseURL>") {
        let after = &rest[i + "<BaseURL>".len()..];
        let Some(j) = after.find("</BaseURL>") else { break };
        let raw = after[..j].trim();
        rest = &after[j + "</BaseURL>".len()..];
        let name = raw.split('?').next().unwrap_or(raw);
        let lower = name.to_ascii_lowercase();
        if name.is_empty() || name.contains('/') || !lower.ends_with(".mp4") {
            continue;
        }
        let quality = lower
            .trim_end_matches(".mp4")
            .rsplit('_')
            .next()
            .and_then(|n| n.parse::<u32>().ok())
            .unwrap_or(0);
        if lower.contains("audio") {
            audio.push((quality, name.to_string()));
        } else if lower.starts_with("dash_") || lower.starts_with("cmaf_") {
            video.push((quality, name.to_string()));
        }
    }
    video.sort_by(|a, b| b.0.cmp(&a.0));
    audio.sort_by(|a, b| b.0.cmp(&a.0));
    (
        video.into_iter().map(|v| v.1).collect(),
        audio.into_iter().map(|a| a.1).collect(),
    )
}

async fn manifest_renditions(client: &Client, base: &str, request_timeout: Duration) -> Option<(Vec<String>, Vec<String>)> {
    let url = format!("{base}/DASHPlaylist.mpd");
    let resp = client
        .get(&url)
        .timeout(request_timeout.min(Duration::from_secs(15)))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() || resp.content_length().is_some_and(|l| l > 1_000_000) {
        return None;
    }
    let text = resp.text().await.ok()?;
    let (video, audio) = parse_mpd_renditions(&text);
    if video.is_empty() {
        None
    } else {
        Some((video, audio))
    }
}

/// Reads the real rendition list from the DASH manifest, then downloads the best video that fits
/// (falling back to lower renditions when one is too large or fails) plus the best audio, and
/// remuxes with `ffmpeg -c copy` (no re-encoding). Returns the final mp4.
pub async fn fetch_reddit_video(client: &Client, ffmpeg: &str, id: &str, max_total: u64, request_timeout: Duration) -> Result<TempFile> {
    let base = format!("https://v.redd.it/{id}");
    let (video_names, audio_names, trusted) = match manifest_renditions(client, &base, request_timeout).await {
        Some((v, a)) => (v, a, true),
        None => (legacy_video_names(), legacy_audio_names(), false),
    };

    let mut video: Option<TempFile> = None;
    let mut last_err: Option<anyhow::Error> = None;
    for name in &video_names {
        let url = format!("{base}/{name}");
        match probe(client, &url, request_timeout).await {
            Some(Some(len)) if len.saturating_add(AUDIO_RESERVE) > max_total => {
                last_err = Some(TooLarge(len).into());
                continue;
            }
            Some(_) => {}
            None if trusted => {} // listed in the manifest: try it even if HEAD is refused
            None => continue,
        }
        match download(client, &url, max_total, "mp4", request_timeout).await {
            Ok(t) => {
                video = Some(t);
                break;
            }
            Err(e) => {
                tracing::warn!("video rendition {name} failed ({e}); trying a lower one");
                last_err = Some(e);
            }
        }
    }
    let Some(video) = video else {
        return Err(last_err.unwrap_or_else(|| anyhow!("no suitable v.redd.it rendition found")));
    };

    let mut audio: Option<TempFile> = None;
    for name in audio_names.iter().take(3) {
        let url = format!("{base}/{name}");
        if !trusted && probe(client, &url, request_timeout).await.is_none() {
            continue;
        }
        match download(client, &url, 20 * 1024 * 1024, "mp4", request_timeout).await {
            Ok(a) => {
                audio = Some(a);
                break;
            }
            Err(e) => tracing::warn!("audio rendition {name} failed ({e})"),
        }
    }
    let Some(audio) = audio else { return Ok(video) }; // silent clip

    let out = TempFile::new("mp4");
    match mux(ffmpeg, video.path(), audio.path(), out.path()).await {
        Ok(()) => Ok(out), // video & audio temp files dropped here
        Err(e) => {
            tracing::warn!("ffmpeg mux failed ({e:#}); sending video without audio");
            Ok(video)
        }
    }
}

/// Downloads/remuxes a Reddit gallery DASH or HLS manifest into one MP4 file.
/// The output is size-capped and retains both video and audio streams when present.
pub async fn fetch_video_manifest(ffmpeg: &str, url: &str, max_total: u64) -> Result<TempFile> {
    let out = TempFile::new("mp4");
    let size_cap = max_total.saturating_add(1);
    let mut cmd = Command::new(ffmpeg);
    cmd.args([
        "-nostdin", "-loglevel", "error", "-y", "-threads", "1",
        "-rw_timeout", "30000000", "-i", url,
        "-map", "0:v:0", "-map", "0:a?",
        "-c", "copy", "-t", "240", "-fs", &size_cap.to_string(),
        "-movflags", "+faststart",
    ])
    .arg(out.path())
    .stdin(Stdio::null())
    .kill_on_drop(true);
    let status = timeout(Duration::from_secs(300), cmd.status())
        .await
        .map_err(|_| anyhow!("gallery video remux timed out"))??;
    if !status.success() {
        bail!("gallery video remux exited with {status}");
    }
    let size = out.size().await;
    if size == 0 {
        bail!("gallery video remux produced an empty file");
    }
    if size > max_total {
        return Err(TooLarge(size).into());
    }
    Ok(out)
}

async fn mux(ffmpeg: &str, video: &Path, audio: &Path, out: &Path) -> Result<()> {
    let mut cmd = Command::new(ffmpeg);
    cmd.args(["-nostdin", "-loglevel", "error", "-y", "-threads", "1", "-i"])
        .arg(video)
        .arg("-i")
        .arg(audio)
        .args(["-c", "copy", "-movflags", "+faststart"])
        .arg(out)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let status = timeout(Duration::from_secs(180), cmd.status())
        .await
        .map_err(|_| anyhow!("ffmpeg timed out"))??;
    if !status.success() {
        bail!("ffmpeg exited with {status}");
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VideoInfo {
    pub width: u64,
    pub height: u64,
    pub duration: u64,
    pub has_video: bool,
    pub has_audio: bool,
}

pub fn parse_video_info(json: &str) -> Result<VideoInfo> {
    let v: serde_json::Value = serde_json::from_str(json)?;
    let streams = v["streams"].as_array().ok_or_else(|| anyhow!("ffprobe output had no streams"))?;
    let mut info = VideoInfo::default();
    let mut stream_duration = 0f64;
    for s in streams {
        match s["codec_type"].as_str() {
            Some("video") if !info.has_video => {
                info.has_video = true;
                info.width = s["width"].as_u64().unwrap_or(0);
                info.height = s["height"].as_u64().unwrap_or(0);
                stream_duration = s["duration"].as_str().and_then(|d| d.parse::<f64>().ok()).unwrap_or(0.0);
            }
            Some("audio") => info.has_audio = true,
            _ => {}
        }
    }
    let format_duration = v["format"]["duration"].as_str().and_then(|d| d.parse::<f64>().ok()).unwrap_or(0.0);
    let d = if format_duration > 0.0 { format_duration } else { stream_duration };
    info.duration = d.round() as u64;
    Ok(info)
}

/// Width/height/duration/audio presence, so Telegram shows the right aspect ratio and length.
pub async fn probe_video(ffprobe: &str, path: &Path) -> Result<VideoInfo> {
    let out = timeout(
        Duration::from_secs(20),
        Command::new(ffprobe)
            .args(["-v", "error", "-show_entries", "stream=codec_type,width,height,duration:format=duration", "-of", "json"])
            .arg(path)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| anyhow!("ffprobe timed out"))??;
    parse_video_info(&String::from_utf8_lossy(&out.stdout))
}

/// Small JPEG preview (<= 320 px, well under Telegram's 200 KB cap) for sendVideo/sendAnimation.
pub async fn make_thumbnail(ffmpeg: &str, input: &Path, duration: u64) -> Result<TempFile> {
    // Seeking past the end exits 0 but writes nothing, so fall back to the first frame.
    let first_try = if duration >= 2 { "1.0" } else { "0" };
    for at in [first_try, "0"] {
        let out = TempFile::new("jpg");
        let mut cmd = Command::new(ffmpeg);
        cmd.args(["-nostdin", "-loglevel", "error", "-y", "-threads", "1", "-ss", at, "-i"])
            .arg(input)
            .args([
                "-frames:v", "1", "-vf",
                "scale=320:320:force_original_aspect_ratio=decrease:force_divisible_by=2",
                "-q:v", "5",
            ])
            .arg(out.path())
            .stdin(Stdio::null())
            .kill_on_drop(true);
        let status = timeout(Duration::from_secs(30), cmd.status())
            .await
            .map_err(|_| anyhow!("thumbnail extraction timed out"))??;
        let size = out.size().await;
        if status.success() && size > 0 && size <= 200_000 {
            return Ok(out);
        }
    }
    bail!("could not create a thumbnail")
}

/// Stream-copies into an mp4 with the index (moov atom) at the front so Telegram can stream and
/// preview it. Downloaded clips (imgur, v.redd.it video-only) often have it at the end.
pub async fn faststart(ffmpeg: &str, input: &Path) -> Result<TempFile> {
    let out = TempFile::new("mp4");
    let mut cmd = Command::new(ffmpeg);
    cmd.args(["-nostdin", "-loglevel", "error", "-y", "-threads", "1", "-i"])
        .arg(input)
        .args(["-map", "0:v:0", "-map", "0:a:0?", "-c", "copy", "-movflags", "+faststart"])
        .arg(out.path())
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let status = timeout(Duration::from_secs(120), cmd.status())
        .await
        .map_err(|_| anyhow!("faststart remux timed out"))??;
    if !status.success() || out.size().await == 0 {
        bail!("faststart remux failed ({status})");
    }
    Ok(out)
}

/// Reads width/height with ffprobe (header only, cheap).
async fn probe_dims(ffprobe: &str, path: &Path) -> Result<(u64, u64)> {
    let out = timeout(
        Duration::from_secs(20),
        Command::new(ffprobe)
            .args(["-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height", "-of", "csv=p=0"])
            .arg(path)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| anyhow!("ffprobe timed out"))??;
    parse_dims(&String::from_utf8_lossy(&out.stdout))
}

pub fn parse_dims(s: &str) -> Result<(u64, u64)> {
    let line = s.lines().next().unwrap_or("").trim();
    let (w, h) = line.split_once(',').ok_or_else(|| anyhow!("unexpected ffprobe output {line:?}"))?;
    Ok((w.trim().parse()?, h.trim().parse()?))
}

/// Downscales an image to a JPEG whose longest side is <= `max_dim` so it can be sent as a
/// native Telegram photo. Refuses absurdly large pixel counts (decoder RAM ~4 bytes/pixel).
pub async fn shrink_image(ffmpeg: &str, ffprobe: &str, input: &Path, max_dim: u32, quality: u8) -> Result<TempFile> {
    let (w, h) = probe_dims(ffprobe, input).await?;
    if w * h > 50_000_000 {
        bail!("image is {w}x{h}; too many pixels to resize within the memory budget");
    }
    let out = TempFile::new("jpg");
    // Scale down, then pad tall/wide strips with white so the ratio stays under Telegram's 20:1
    // limit (same idea as the Python bot's Pillow padding) -> still a native photo.
    let vf = format!(
        "scale='min({max_dim},iw)':'min({max_dim},ih)':force_original_aspect_ratio=decrease,\
         pad='max(iw,ceil(ih/19))':'max(ih,ceil(iw/19))':'(ow-iw)/2':'(oh-ih)/2':white"
    );
    let mut cmd = Command::new(ffmpeg);
    cmd.args(["-nostdin", "-loglevel", "error", "-y", "-threads", "1", "-i"])
        .arg(input)
        .args(["-frames:v", "1", "-vf", &vf, "-pix_fmt", "yuvj420p", "-q:v", &quality.to_string()])
        .arg(out.path())
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let status = timeout(Duration::from_secs(90), cmd.status())
        .await
        .map_err(|_| anyhow!("ffmpeg resize timed out"))??;
    if !status.success() {
        bail!("ffmpeg resize exited with {status}");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #[test]
    fn parses_ffprobe_dims() {
        assert_eq!(super::parse_dims("6000,4000\n").unwrap(), (6000, 4000));
        assert!(super::parse_dims("").is_err());
    }

    use super::*;

    #[test]
    fn content_type_decides_extension() {
        assert_eq!(ext_from_content_type("image/webp"), Some("webp"));
        assert_eq!(ext_from_content_type("Video/MP4; charset=x"), Some("mp4"));
        assert_eq!(ext_from_content_type("image/jpeg"), Some("jpg"));
        assert_eq!(ext_from_content_type("application/octet-stream"), None);
    }

    #[test]
    fn parses_dash_manifest_renditions() {
        let xml = "<MPD><BaseURL>DASH_480.mp4</BaseURL><BaseURL>DASH_1080.mp4?x=1</BaseURL>\
                   <BaseURL>CMAF_720.mp4</BaseURL><BaseURL>DASH_AUDIO_64.mp4</BaseURL>\
                   <BaseURL>CMAF_AUDIO_128.mp4</BaseURL><BaseURL>https://evil/x.mp4</BaseURL>\
                   <BaseURL>sub/dir.mp4</BaseURL><BaseURL>DASH_audio.mp4</BaseURL></MPD>";
        let (video, audio) = parse_mpd_renditions(xml);
        assert_eq!(video, vec!["DASH_1080.mp4", "CMAF_720.mp4", "DASH_480.mp4"]);
        assert_eq!(audio, vec!["CMAF_AUDIO_128.mp4", "DASH_AUDIO_64.mp4", "DASH_audio.mp4"]);
        assert!(parse_mpd_renditions("<MPD></MPD>").0.is_empty());
    }

    #[test]
    fn parses_ffprobe_video_info() {
        let with_audio = r#"{"streams":[{"codec_type":"video","width":360,"height":640,"duration":"3.0"},{"codec_type":"audio"}],"format":{"duration":"3.2"}}"#;
        let i = parse_video_info(with_audio).unwrap();
        assert_eq!((i.width, i.height, i.duration, i.has_video, i.has_audio), (360, 640, 3, true, true));
        let silent = r#"{"streams":[{"codec_type":"video","width":640,"height":360,"duration":"7.6"}],"format":{}}"#;
        let i = parse_video_info(silent).unwrap();
        assert_eq!((i.duration, i.has_audio), (8, false));
        assert!(parse_video_info("{}").is_err());
    }

    #[test]
    fn ext_detection() {
        assert_eq!(ext_of("https://i.redd.it/a.JPG?x=1"), "jpg");
        assert_eq!(ext_of("https://v.redd.it/abc"), "bin");
    }

    #[test]
    fn tempfile_removed_on_drop() {
        let p;
        {
            let t = TempFile::new("txt");
            std::fs::write(t.path(), b"x").unwrap();
            p = t.path().to_path_buf();
            assert!(p.exists());
        }
        assert!(!p.exists());
    }
}
