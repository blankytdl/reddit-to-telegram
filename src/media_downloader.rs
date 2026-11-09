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

pub struct TempFile {
    path: PathBuf,
}

impl TempFile {
    pub fn new(ext: &str) -> Self {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rtb-{}-{}.{}", std::process::id(), n, ext));
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

/// Streams `url` into a temp file; aborts as soon as `max` bytes are exceeded.
pub async fn download(client: &Client, url: &str, max: u64, ext: &str) -> Result<TempFile> {
    let resp = client
        .get(url)
        .timeout(Duration::from_secs(300))
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
    let tf = TempFile::new(ext);
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
async fn probe(client: &Client, url: &str) -> Option<Option<u64>> {
    let r = client.head(url).timeout(Duration::from_secs(20)).send().await.ok()?;
    if r.status().is_success() {
        Some(r.content_length())
    } else {
        None
    }
}

/// Picks the best DASH rendition that fits, downloads video (+audio) and
/// remuxes with `ffmpeg -c copy` (no re-encoding). Returns the final mp4.
pub async fn fetch_reddit_video(client: &Client, ffmpeg: &str, id: &str, max_total: u64) -> Result<TempFile> {
    let base = format!("https://v.redd.it/{id}");
    let audio_reserve: u64 = 6 * 1024 * 1024;
    let mut chosen: Option<String> = None;
    for q in ["DASH_1080", "DASH_720", "DASH_480", "DASH_360", "DASH_240"] {
        let u = format!("{base}/{q}.mp4");
        if let Some(len) = probe(client, &u).await {
            if len.map_or(true, |l| l + audio_reserve <= max_total) {
                chosen = Some(u);
                break;
            }
        }
    }
    let video_url = chosen.ok_or_else(|| anyhow!("no suitable v.redd.it rendition found or all too large"))?;

    let mut audio_url = None;
    for a in ["DASH_AUDIO_128", "DASH_AUDIO_64", "DASH_audio", "audio"] {
        let u = format!("{base}/{a}.mp4");
        if probe(client, &u).await.is_some() {
            audio_url = Some(u);
            break;
        }
    }

    let video = download(client, &video_url, max_total, "mp4").await?;
    let Some(audio_url) = audio_url else { return Ok(video) }; // silent clip
    let audio = match download(client, &audio_url, 20 * 1024 * 1024, "mp4").await {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!("audio download failed ({e}); sending video without audio");
            return Ok(video);
        }
    };
    let out = TempFile::new("mp4");
    match mux(ffmpeg, video.path(), audio.path(), out.path()).await {
        Ok(()) => Ok(out), // video & audio temp files dropped here
        Err(e) => {
            tracing::warn!("ffmpeg mux failed ({e:#}); sending video without audio");
            Ok(video)
        }
    }
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
