//! Direct Telegram Bot API integration. Files are uploaded as streamed
//! multipart bodies (never fully loaded in RAM). Errors never contain the token.
use reqwest::{
    multipart::{Form, Part},
    Body, Client,
};
use serde_json::{json, Value};
use std::{fmt, future::Future, path::Path, time::Duration};
use tokio::time::sleep;
use tokio_util::io::ReaderStream;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Photo,
    Animation,
    Video,
    Document,
}

impl FileKind {
    fn method(self) -> &'static str {
        match self {
            FileKind::Photo => "sendPhoto",
            FileKind::Animation => "sendAnimation",
            FileKind::Video => "sendVideo",
            FileKind::Document => "sendDocument",
        }
    }
    fn field(self) -> &'static str {
        match self {
            FileKind::Photo => "photo",
            FileKind::Animation => "animation",
            FileKind::Video => "video",
            FileKind::Document => "document",
        }
    }
}

/// code 0 = network error (retryable), 1 = local error, otherwise Telegram/HTTP code.
#[derive(Debug)]
pub struct TgError {
    pub code: u16,
    pub desc: String,
    pub retry_after: Option<u64>,
}

impl fmt::Display for TgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "telegram error {}: {}", self.code, self.desc)
    }
}
impl std::error::Error for TgError {}

pub struct Telegram {
    client: Client,
    base: String,
    token: String,
}

fn mime_for(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).map(|e| e.to_ascii_lowercase()).as_deref() {
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("webp") => "image/webp",
        Some("gif") => "image/gif",
        Some("mp4") => "video/mp4",
        _ => "application/octet-stream",
    }
}

async fn file_part(path: &Path) -> Result<Part, TgError> {
    let local = |e: std::io::Error| TgError { code: 1, desc: e.to_string(), retry_after: None };
    let file = tokio::fs::File::open(path).await.map_err(local)?;
    let len = file.metadata().await.map_err(local)?.len();
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file").to_string();
    Part::stream_with_length(Body::wrap_stream(ReaderStream::with_capacity(file, 64 * 1024)), len)
        .file_name(name)
        .mime_str(mime_for(path))
        .map_err(|e| TgError { code: 1, desc: e.to_string(), retry_after: None })
}

impl Telegram {
    pub fn new(client: Client, base: String, token: String) -> Self {
        Self { client, base, token }
    }

    fn url(&self, method: &str) -> String {
        format!("{}/bot{}/{}", self.base, self.token, method)
    }

    async fn parse(resp: Result<reqwest::Response, reqwest::Error>) -> Result<(), TgError> {
        let resp = resp.map_err(|e| TgError { code: 0, desc: e.without_url().to_string(), retry_after: None })?;
        let status = resp.status().as_u16();
        let v: Value = resp.json().await.unwrap_or(Value::Null);
        if v["ok"].as_bool() == Some(true) {
            return Ok(());
        }
        Err(TgError {
            code: v["error_code"].as_u64().map(|c| c as u16).unwrap_or(status),
            desc: v["description"].as_str().unwrap_or("unknown error").to_string(),
            retry_after: v["parameters"]["retry_after"].as_u64(),
        })
    }

    async fn retry<F, Fut>(&self, mut f: F) -> Result<(), TgError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), TgError>>,
    {
        let mut attempt: u64 = 0;
        loop {
            attempt += 1;
            match f().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let wait = if e.code == 429 {
                        Some(e.retry_after.unwrap_or(5).min(120) + 1)
                    } else if e.code == 0 || e.code >= 500 {
                        Some(5 * attempt)
                    } else {
                        None
                    };
                    match wait {
                        Some(w) if attempt < 3 => {
                            warn!(code = e.code, wait_secs = w, "telegram call failed; retrying");
                            sleep(Duration::from_secs(w)).await;
                        }
                        _ => return Err(e),
                    }
                }
            }
        }
    }

    pub async fn send_message(&self, chat: &str, html: &str) -> Result<(), TgError> {
        let url = self.url("sendMessage");
        let body = json!({
            "chat_id": chat, "text": html, "parse_mode": "HTML", "disable_web_page_preview": false
        });
        let (url, body) = (&url, &body);
        self.retry(move || async move {
            Self::parse(self.client.post(url).json(body).timeout(Duration::from_secs(30)).send().await).await
        })
        .await
    }

    async fn upload_once(&self, url: &str, chat: &str, kind: FileKind, path: &Path, caption: &str, spoiler: bool) -> Result<(), TgError> {
        let part = file_part(path).await?;
        let mut form = Form::new()
            .text("chat_id", chat.to_string())
            .text("caption", caption.to_string())
            .text("parse_mode", "HTML");
        if kind == FileKind::Video {
            form = form.text("supports_streaming", "true");
        }
        if spoiler && kind != FileKind::Document {
            form = form.text("has_spoiler", "true");
        }
        form = form.part(kind.field(), part);
        Self::parse(self.client.post(url).multipart(form).timeout(Duration::from_secs(600)).send().await).await
    }

    pub async fn send_file(&self, chat: &str, kind: FileKind, path: &Path, caption: &str, spoiler: bool) -> Result<(), TgError> {
        let url = self.url(kind.method());
        let url = &url;
        self.retry(move || self.upload_once(url, chat, kind, path, caption, spoiler)).await
    }

    async fn group_once(&self, url: &str, chat: &str, files: &[&Path], caption: &str, spoiler: bool) -> Result<(), TgError> {
        let mut form = Form::new().text("chat_id", chat.to_string());
        let mut media = Vec::with_capacity(files.len());
        for (i, path) in files.iter().enumerate() {
            let name = format!("file{i}");
            form = form.part(name.clone(), file_part(path).await?);
            let mut m = json!({ "type": "photo", "media": format!("attach://{name}") });
            if i == 0 && !caption.is_empty() {
                m["caption"] = json!(caption);
                m["parse_mode"] = json!("HTML");
            }
            if spoiler {
                m["has_spoiler"] = json!(true);
            }
            media.push(m);
        }
        form = form.text("media", Value::Array(media).to_string());
        Self::parse(self.client.post(url).multipart(form).timeout(Duration::from_secs(900)).send().await).await
    }

    /// Photo album (2..=10 photos). The caption is shown on the album via its first item.
    pub async fn send_media_group(&self, chat: &str, files: &[&Path], caption: &str, spoiler: bool) -> Result<(), TgError> {
        let url = self.url("sendMediaGroup");
        let url = &url;
        self.retry(move || self.group_once(url, chat, files, caption, spoiler)).await
    }
}
