"""
Reddit → Telegram bot using only public RSS feeds
(No Reddit API / no OAuth needed)

Polls several subreddits, each posting to its own Telegram channel, through
a single bot. Subreddits are always checked in the same fixed order, with a
delay between each, and each individual subreddit's feed is never re-fetched
faster than Reddit's observed RSS rate limit (~1 request/minute/feed) allows.
"""

import base64
import gc
import os
import re
import signal
import sys
import time
import json
import html
import tempfile
import threading
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
import feedparser
import imageio_ffmpeg

from config import (
    SUBREDDITS,
    INCLUDE_TITLE, INCLUDE_FOOTER, INCLUDE_REDDIT_LINK,
    BLUR_SPOILERS,
    SKIP_TEXT_ONLY_POSTS,
    MIN_SECONDS_BETWEEN_FETCHES_PER_SUB,
    DELAY_BETWEEN_SUBREDDITS,
    SCHEDULER_TICK_SECONDS,
    DEFAULT_429_BACKOFF_SECONDS,
    DELAY_BETWEEN_POSTS,
    MAX_MEDIA_DOWNLOAD_BYTES,
    GITHUB_TOKEN,
    GITHUB_REPO,
    GITHUB_STATE_BRANCH,
    GITHUB_SEEN_PATH,
    GITHUB_SYNC_INTERVAL_SECONDS,
)
from telegram_sender import send_message, send_photo, send_video, send_animation

# ───────────────────────── settings ─────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept": "application/atom+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp")
GIF_EXTS   = (".gif",)
VIDEO_EXTS = (".mp4", ".webm")

# ───────────────────────── fake health server (Render free tier) ─────────────────────────
# Render's free plan only offers the "Web Service" type, which requires the
# app to bind to $PORT and answer HTTP requests, or Render's own health
# check will consider the deploy dead. The bot itself has no reason to serve
# HTTP — this endpoint exists purely so Render's health check (and our own
# self-pinger below) have something to hit.
# Health is based on actual bot-loop progress, not HTTP requests. Keep the
# threshold above the maximum bounded media operation and normal subreddit cycle.
_last_tick = time.monotonic()

def _mark_progress():
    global _last_tick
    _last_tick = time.monotonic()

HEALTH_STALE_SECONDS = int(os.environ.get("HEALTH_STALE_SECONDS", "900"))

# Shared Reddit cooldown: when one feed receives a 429, pause all feeds.
_reddit_cooldown_until = 0.0
_reddit_cooldown_lock = threading.Lock()
_last_scheduler_report = 0.0

def _set_reddit_cooldown(seconds: float):
    global _reddit_cooldown_until
    seconds = max(30.0, min(float(seconds), 900.0))
    with _reddit_cooldown_lock:
        _reddit_cooldown_until = max(_reddit_cooldown_until, time.monotonic() + seconds)

def _reddit_cooldown_remaining() -> float:
    with _reddit_cooldown_lock:
        return max(0.0, _reddit_cooldown_until - time.monotonic())

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        idle = time.monotonic() - _last_tick
        healthy = idle <= HEALTH_STALE_SECONDS
        status = 200 if healthy else 503
        state = "ok" if healthy else "stalled"
        body = f"{state} - bot loop last made progress {idle:.0f}s ago (limit {HEALTH_STALE_SECONDS}s)\n".encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # keep Render's logs from filling up with health-check noise

def start_health_server():
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"Health check server listening on 0.0.0.0:{port}")

# ───────────────────────── self-pinger (no external service needed) ─────────────────────────
# Render sets RENDER_EXTERNAL_URL automatically for every Web Service, to
# that service's own public URL. Rather than relying on an external cron/
# uptime service to hit it, the bot pings itself on an interval well under
# Render's ~15-minute idle-spindown window. This only helps while the
# instance is awake and running this loop — it can't wake itself up once
# Render has actually spun it down (nothing is running at that point to do
# the pinging), so keep the interval comfortably below 15 minutes so the
# instance never gets the chance to go idle in the first place.
SELF_PING_INTERVAL_SECONDS = 600  # 10 minutes

def start_self_pinger():
    url = os.environ.get("RENDER_EXTERNAL_URL")
    if not url:
        print("RENDER_EXTERNAL_URL not set (not running on Render, or an older "
              "Render env) — skipping self-pinger. Set up an external pinger "
              "(cron-job.org / UptimeRobot) instead if this instance needs to "
              "stay awake.")
        return

    def loop():
        while True:
            time.sleep(SELF_PING_INTERVAL_SECONDS)
            try:
                requests.get(url, timeout=15)
                print(f"Self-ping OK → {url}")
            except Exception as e:
                print(f"Self-ping failed → {url}: {e}")

    threading.Thread(target=loop, daemon=True).start()
    print(f"Self-pinger started, hitting {url} every {SELF_PING_INTERVAL_SECONDS}s")


# Where "seen" post IDs are persisted. Defaults to a local ./seen folder for
# running elsewhere, but on Render this should point at a mounted persistent
# disk (e.g. set DATA_DIR=/var/data in the service's Environment tab, with a
# disk mounted at /var/data) — otherwise these files vanish on every deploy
# or restart, and the bot will re-post everything it already sent.
DATA_DIR = os.environ.get("DATA_DIR", ".")
SEEN_DIR = Path(DATA_DIR) / "seen"
FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()

# ───────────────────────── GitHub-backed seen backup ─────────────────────────
# Optional. See config.py for setup notes and the "use a separate branch"
# warning. Disabled entirely (no network calls) unless both GITHUB_TOKEN
# and GITHUB_REPO are set.
_GITHUB_ENABLED = bool(GITHUB_TOKEN and GITHUB_REPO)
_github_shas: dict[str, str] = {}          # sub_name -> last known blob sha
_github_dirty: set[str] = set()            # sub_names changed since last push
_github_lock = threading.Lock()


def _github_headers() -> dict:
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }


def _github_contents_url(sub_name: str) -> str:
    return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_SEEN_PATH}/{sub_name}.json"


def github_fetch_seen(sub_name: str) -> Optional[set]:
    """
    Try to restore one subreddit's seen-IDs file from GITHUB_STATE_BRANCH.
    Returns None (and does nothing) if disabled, the file doesn't exist yet
    (first run), or anything goes wrong — callers should fall back to an
    empty set in that case, same as if this feature didn't exist.
    """
    if not _GITHUB_ENABLED:
        return None
    try:
        r = requests.get(
            _github_contents_url(sub_name),
            headers=_github_headers(),
            params={"ref": GITHUB_STATE_BRANCH},
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            _github_shas[sub_name] = data["sha"]
            content = base64.b64decode(data["content"]).decode("utf-8")
            return set(json.loads(content))
        if r.status_code == 404:
            return None  # branch/file doesn't exist yet — normal on first run
        print(f"[github] fetch {sub_name} → HTTP {r.status_code}: {r.text[:200]}")
        return None
    except Exception as e:
        print(f"[github] fetch {sub_name} errored: {e}")
        return None


def github_push_seen(sub_name: str, seen: set) -> bool:
    """
    Push one subreddit's current seen-IDs set to GITHUB_STATE_BRANCH as a
    commit. Creates the branch (from the repo's default branch) and the
    file on first use. Best-effort: logs and gives up on failure rather
    than raising, since losing a backup write should never crash the bot.
    """
    if not _GITHUB_ENABLED:
        return False

    body = json.dumps(sorted(seen)[-3000:]).encode("utf-8")
    payload = {
        "message": f"seen: update {sub_name} [skip render deploy]",
        "content": base64.b64encode(body).decode("ascii"),
        "branch": GITHUB_STATE_BRANCH,
    }
    sha = _github_shas.get(sub_name)
    if sha:
        payload["sha"] = sha

    try:
        r = requests.put(
            _github_contents_url(sub_name),
            headers=_github_headers(),
            json=payload,
            timeout=20,
        )
        if r.status_code in (200, 201):
            _github_shas[sub_name] = r.json()["content"]["sha"]
            print(f"[github] pushed {sub_name}: {len(seen)} seen IDs")
            return True

        if r.status_code == 404:
            # Almost always means GITHUB_STATE_BRANCH doesn't exist yet.
            # Create it off the repo's default branch, then retry once.
            if _github_create_branch_if_missing():
                _github_shas.pop(sub_name, None)
                payload.pop("sha", None)
                r2 = requests.put(
                    _github_contents_url(sub_name),
                    headers=_github_headers(),
                    json=payload,
                    timeout=20,
                )
                if r2.status_code in (200, 201):
                    _github_shas[sub_name] = r2.json()["content"]["sha"]
                    print(f"[github] pushed {sub_name}: {len(seen)} seen IDs (retry succeeded)")
                    return True
                print(f"[github] retry push {sub_name} → HTTP {r2.status_code}: {r2.text[:200]}")
            return False

        if r.status_code == 409:
            # sha we had is stale (e.g. file edited outside the bot) —
            # refetch the current sha and retry once.
            fresh = github_fetch_seen(sub_name)
            if fresh is not None:
                payload["sha"] = _github_shas.get(sub_name)
                r2 = requests.put(
                    _github_contents_url(sub_name),
                    headers=_github_headers(),
                    json=payload,
                    timeout=20,
                )
                if r2.status_code in (200, 201):
                    _github_shas[sub_name] = r2.json()["content"]["sha"]
                    print(f"[github] pushed {sub_name}: {len(seen)} seen IDs (retry succeeded)")
                    return True
            print(f"[github] push {sub_name} conflict, will retry next sync")
            return False

        print(f"[github] push {sub_name} → HTTP {r.status_code}: {r.text[:200]} (will retry next sync)")
        return False
    except Exception as e:
        print(f"[github] push {sub_name} errored: {e} (will retry next sync)")
        return False


def _github_create_branch_if_missing() -> bool:
    """Create GITHUB_STATE_BRANCH from the repo's default branch, if needed."""
    try:
        repo_r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}",
            headers=_github_headers(), timeout=15,
        )
        repo_r.raise_for_status()
        default_branch = repo_r.json()["default_branch"]

        ref_r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/git/ref/heads/{default_branch}",
            headers=_github_headers(), timeout=15,
        )
        ref_r.raise_for_status()
        base_sha = ref_r.json()["object"]["sha"]

        create_r = requests.post(
            f"https://api.github.com/repos/{GITHUB_REPO}/git/refs",
            headers=_github_headers(), timeout=15,
            json={"ref": f"refs/heads/{GITHUB_STATE_BRANCH}", "sha": base_sha},
        )
        if create_r.status_code in (200, 201) or "already exists" in create_r.text:
            print(f"[github] state branch '{GITHUB_STATE_BRANCH}' ready")
            return True
        print(f"[github] couldn't create branch '{GITHUB_STATE_BRANCH}': {create_r.text[:200]}")
        return False
    except Exception as e:
        print(f"[github] couldn't create branch '{GITHUB_STATE_BRANCH}': {e}")
        return False

def start_github_syncer():
    """
    Background thread: every GITHUB_SYNC_INTERVAL_SECONDS, push every
    subreddit whose seen set changed since the last round, as a backup
    commit to GITHUB_STATE_BRANCH. No-op if the feature is disabled.
    """
    if not _GITHUB_ENABLED:
        print("[github] GITHUB_TOKEN/GITHUB_REPO not set — seen-state backup disabled "
              "(local-file-only; state resets on every Render deploy/restart).")
        return

    def loop():
        while True:
            time.sleep(GITHUB_SYNC_INTERVAL_SECONDS)
            flush_dirty_seen_to_github()

    threading.Thread(target=loop, daemon=True).start()
    print(f"[github] seen-state backup enabled → {GITHUB_REPO}@{GITHUB_STATE_BRANCH}, "
          f"syncing every {GITHUB_SYNC_INTERVAL_SECONDS}s")


def flush_dirty_seen_to_github():
    """
    Push changed subreddit state. Keep failed writes dirty for the next round,
    and only clear a subreddit after success if its state hasn't changed
    while the network request was in flight.
    """
    if not _GITHUB_ENABLED:
        return
    with _github_lock:
        pending = {
            name: set(_current_seen[name])
            for name in _github_dirty
            if name in _current_seen
        }
    for sub_name, snapshot in pending.items():
        if github_push_seen(sub_name, snapshot):
            with _github_lock:
                # If a post was added during the upload, leave it dirty so
                # the newer state is pushed on the next sync.
                if _current_seen.get(sub_name) == snapshot:
                    _github_dirty.discard(sub_name)
        # On failure, leave the name in _github_dirty; next periodic round
        # will retry automatically.


# ───────────────────────── per-subreddit seen tracking ─────────────────────────
_current_seen: dict[str, set] = {}   # sub_name -> live seen set, for the syncer to read

def seen_path(sub_name: str) -> Path:
    SEEN_DIR.mkdir(exist_ok=True)
    return SEEN_DIR / f"{sub_name}.json"

def load_seen(sub_name: str) -> set:
    p = seen_path(sub_name)
    if p.exists():
        try:
            seen = set(json.loads(p.read_text(encoding="utf-8")))
            _current_seen[sub_name] = seen
            return seen
        except Exception:
            pass

    # No local copy (fresh container, or Render just wiped it on redeploy) —
    # try restoring from the GitHub state branch before giving up and
    # starting blank, which would otherwise re-post everything.
    restored = github_fetch_seen(sub_name)
    if restored is not None:
        seen_path(sub_name).write_text(json.dumps(list(restored)), encoding="utf-8")
        print(f"[{sub_name}] Restored {len(restored)} seen IDs from GitHub state branch")
        _current_seen[sub_name] = restored
        return restored

    _current_seen[sub_name] = set()
    return set()

def save_seen(sub_name: str, seen: set):
    seen_path(sub_name).write_text(json.dumps(list(seen)[-3000:]), encoding="utf-8")
    _current_seen[sub_name] = seen
    if _GITHUB_ENABLED:
        with _github_lock:
            _github_dirty.add(sub_name)

# ───────────────────────── spoiler detection ─────────────────────────
def is_spoiler_entry(entry, subreddit: str) -> bool:
    """
    Best-effort spoiler detection from public RSS. Reddit's RSS does not
    expose a true `spoiler: true` boolean (that only exists on the
    deprecated JSON API), so this checks flair/category tags and the title
    for spoiler markers instead. Good enough to catch the common cases
    (flaired "Spoiler" posts, "[SPOILER]" titles) but not guaranteed 100%.
    """
    for t in entry.get("tags", []) or []:
        term = (t.get("term") or "").lower()
        label = (t.get("label") or "").lower() if t.get("label") else ""
        if "spoiler" in term or "spoiler" in label:
            return True

    title = (entry.get("title") or "").lower()
    if re.search(r'\[\s*spoiler', title) or "(spoiler)" in title:
        return True

    summary = entry.get("summary", "") or entry.get("description", "") or ""
    if "spoiler" in summary.lower():
        return True

    return False

# ───────────────────────── helpers ─────────────────────────
def build_caption(title: str, subreddit: str, author: str, permalink: str) -> str:
    parts = []
    if INCLUDE_TITLE and title:
        parts.append(html.escape(title.strip()))
    if INCLUDE_FOOTER:
        parts.append(f"r/{subreddit} • u/{author}")
    if INCLUDE_REDDIT_LINK and permalink:
        parts.append(f'<a href="https://reddit.com{permalink}">Open on Reddit</a>')
    return "\n\n".join(parts)

def download_bytes(url: str, timeout: int = 90, max_bytes: int = MAX_MEDIA_DOWNLOAD_BYTES) -> bytes | None:
    """
    Stream the download and enforce a hard size cap, instead of pulling an
    arbitrarily large response fully into memory via r.content. A single
    unexpectedly huge file (or someone's 80MB "meme") is exactly the kind of
    thing that can OOM a small container even before Pillow gets involved.
    """
    deadline = time.monotonic() + min(timeout, 45)
    try:
        with requests.get(url, headers=HEADERS, timeout=(10, 15), stream=True) as r:
            if r.status_code != 200:
                print(f"  Media download HTTP {r.status_code}: {url}")
                return None

            content_length = r.headers.get("Content-Length")
            if content_length and content_length.isdigit() and int(content_length) > max_bytes:
                print(f"  Skipping media (reports {int(content_length)/1e6:.1f}MB, cap is {max_bytes/1e6:.0f}MB): {url}")
                return None

            chunks = []
            total = 0
            for chunk in r.iter_content(256 * 1024):
                if time.monotonic() > deadline:
                    print(f"  Aborting media download after time limit: {url}")
                    return None
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_bytes:
                    print(f"  Aborting download, exceeded {max_bytes/1e6:.0f}MB cap: {url}")
                    return None
                chunks.append(chunk)
            return b"".join(chunks)
    except Exception as e:
        print(f"  Media download failed ({type(e).__name__}): {url}: {e}")
        return None

def download_to_file(
    url: str,
    path: str,
    max_bytes: int = MAX_MEDIA_DOWNLOAD_BYTES,
    deadline_seconds: int = 45,
) -> bool:
    """Download media with a size cap, elapsed-time limit, and reliable cleanup."""
    deadline = time.monotonic() + deadline_seconds
    total = 0
    try:
        with requests.get(url, headers=HEADERS, timeout=(10, 15), stream=True) as r:
            if r.status_code != 200:
                print(f"  Media download HTTP {r.status_code}: {url}")
                return False
            length = r.headers.get("Content-Length")
            if length and length.isdigit() and int(length) > max_bytes:
                print(f"  Media exceeds {max_bytes / 1e6:.0f}MB cap: {url}")
                return False
            with open(path, "wb") as f:
                for chunk in r.iter_content(256 * 1024):
                    if time.monotonic() > deadline:
                        raise TimeoutError("media download exceeded total deadline")
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > max_bytes:
                        raise ValueError(f"media exceeded {max_bytes / 1e6:.0f}MB cap")
                    f.write(chunk)
        return total > 0
    except Exception as e:
        print(f"  Media download failed ({type(e).__name__}): {url}: {e}")
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass
        return False

# ───────────────────────── media handlers ─────────────────────────
def handle_image(chat_id: str, url: str, caption: str, spoiler: bool) -> bool:
    data = download_bytes(url)
    return bool(data) and send_photo(chat_id, data, caption=caption, spoiler=spoiler)

def handle_gif(chat_id: str, url: str, caption: str, spoiler: bool) -> bool:
    data = download_bytes(url)
    return bool(data) and send_animation(chat_id, data, caption=caption, spoiler=spoiler)

def handle_direct_video(chat_id: str, url: str, caption: str, spoiler: bool) -> bool:
    data = download_bytes(url)
    return bool(data) and send_video(chat_id, data, caption=caption, spoiler=spoiler)

def handle_reddit_video(chat_id: str, video_url: str, caption: str, spoiler: bool) -> bool:
    with tempfile.TemporaryDirectory() as tmp:
        video_path  = os.path.join(tmp, "video.mp4")
        audio_path  = os.path.join(tmp, "audio.mp4")
        merged_path = os.path.join(tmp, "merged.mp4")

        print(f"  Video download started: {video_url}")
        if not download_to_file(video_url, video_path):
            return False

        base = video_url.rsplit("/", 1)[0]
        audio_candidates = [
            f"{base}/DASH_audio.mp4",
            f"{base}/DASH_AUDIO_128.mp4",
            f"{base}/DASH_AUDIO_64.mp4",
            f"{base}/audio",
        ]

        audio_ok = False
        audio_deadline = time.monotonic() + 120
        for aurl in audio_candidates:
            if time.monotonic() >= audio_deadline:
                print("  Audio track attempts stopped: 120s total budget reached")
                break
            try:
                if os.path.exists(audio_path):
                    os.remove(audio_path)
            except OSError:
                pass
            if download_to_file(aurl, audio_path, deadline_seconds=35) and os.path.getsize(audio_path) > 2000:
                audio_ok = True
                break

        final = video_path
        if audio_ok:
            try:
                subprocess.run(
                    [
                        FFMPEG, "-y",
                        "-i", video_path,
                        "-i", audio_path,
                        "-c:v", "copy",
                        "-c:a", "aac",
                        "-map", "0:v:0",
                        "-map", "1:a:0",
                        "-shortest",
                        merged_path,
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=180,
                )
                final = merged_path
            except Exception:
                pass

        # Stream from disk into the multipart upload instead of duplicating
        # the entire video in RAM. Telegram's sender accepts file-like objects.
        with open(final, "rb") as f:
            return send_video(chat_id, f, caption=caption, spoiler=spoiler)

# ───────────────────────── media extraction ─────────────────────────
def extract_media_url(entry) -> str | None:
    candidates = []

    for m in entry.get("media_content", []) or []:
        url = m.get("url")
        if url:
            candidates.append(url)

    for e in entry.get("enclosures", []) or []:
        url = e.get("href") or e.get("url")
        if url:
            candidates.append(url)

    summary = entry.get("summary", "") or entry.get("description", "") or ""
    if entry.get("content"):
        summary += " " + entry.content[0].get("value", "")
    summary = html.unescape(summary)

    patterns = [
        r'https://(?:i\.redd\.it|preview\.redd\.it|external-preview\.redd\.it)/[^\s"\'<>]+?\.(?:jpg|jpeg|png|webp|gif)',
        r'https://v\.redd\.it/[^\s"\'<>]+',
        r'https://[^\s"\'<>]+\.(?:jpg|jpeg|png|webp|gif)(?:\?[^\s"\'<>]*)?',
        r'href="(https://[^"]+)"[^>]*>\s*\[link\]',
        r'<img[^>]+src="(https://[^"]+)"',
    ]

    for pat in patterns:
        matches = re.findall(pat, summary, re.IGNORECASE)        for m in matches:
            if isinstance(m, tuple):
                m = m[0]
            candidates.append(m)

    clean = []
    for url in candidates:
        url = url.replace("&amp;", "&")
        if any(x in url.lower() for x in ["i.redd.it", "preview.redd.it", "v.redd.it", ".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"]):
            clean.append(url)

    for url in clean:
        if "i.redd.it" in url:
            return url
    for url in clean:
        if "v.redd.it" in url:
            return url
    for url in clean:
        if any(url.lower().endswith(ext) for ext in IMAGE_EXTS + GIF_EXTS + VIDEO_EXTS):
            return url

    return clean[0] if clean else None

def process_entry(entry: dict, seen: set, channel_seen: set, sub: dict):
    _mark_progress()
    subreddit = sub["name"]
    chat_id = sub["chat_id"]

    link = entry.get("link", "")
    title = entry.get("title", "")
    author = entry.get("author", "unknown").replace("/u/", "").replace("u/", "")
    # Reddit RSS links end with a title slug, not the post ID. Extract the
    # stable ID from /comments/<id>/ so title/URL variations cannot bypass dedupe.
    match = re.search(r"/comments/([a-z0-9]+)(?:/|$)", urlparse(link).path, re.IGNORECASE)
    post_id = match.group(1).lower() if match else (
        urlparse(link).path.rstrip("/").split("/")[-1].lower() if link else None
    )

    if not post_id or post_id in seen:
        return

    # Multiple configured subreddit feeds may contain the same Reddit post.
    # Deduplicate per Telegram destination, not just per subreddit.
    if post_id in channel_seen:
        seen.add(post_id)
        save_seen(subreddit, seen)
        print(f"↪ [{subreddit}] Suppressed duplicate for destination {chat_id}: {post_id}")
        return

    permalink = urlparse(link).path
    caption = build_caption(title, subreddit, author, permalink)
    spoiler = BLUR_SPOILERS and is_spoiler_entry(entry, subreddit)

    media_url = extract_media_url(entry)
    lower = (media_url or "").lower()

    print(f"  [{subreddit}] Media found: {media_url or 'None'}{' (spoiler)' if spoiler else ''}")

    try:
        if media_url and any(lower.endswith(ext) for ext in IMAGE_EXTS):
            delivered = handle_image(chat_id, media_url, caption, spoiler)
        elif media_url and any(lower.endswith(ext) for ext in GIF_EXTS):
            delivered = handle_gif(chat_id, media_url, caption, spoiler)
        elif media_url and "v.redd.it" in lower:
            if not media_url.endswith(".mp4"):
                media_url = media_url.rstrip("/") + "/DASH_720.mp4"
            delivered = handle_reddit_video(chat_id, media_url, caption, spoiler)
        elif media_url and any(lower.endswith(ext) for ext in VIDEO_EXTS):
            delivered = handle_direct_video(chat_id, media_url, caption, spoiler)
        elif SKIP_TEXT_ONLY_POSTS:
            seen.add(post_id)
            channel_seen.add(post_id)
            save_seen(subreddit, seen)
            print(f"– [{subreddit}] Skipped (no media): {title[:70]}")
            return
        else:
            text = caption if caption else html.escape(title)
            if link:
                text += f"\n\n{link}"
            delivered = send_message(chat_id, text)

        _mark_progress()
        # If media hosting rejects a download, deliver the Reddit permalink
        # instead. This avoids repeatedly retrying the same inaccessible media.
        if not delivered and media_url:
            fallback = caption or html.escape(title)
            if link:
                fallback += f"\\n\\n{link}"
            print(f"  [{subreddit}] Media failed; trying Reddit-link fallback")
            delivered = send_message(chat_id, fallback)

        if delivered:
            seen.add(post_id)
            channel_seen.add(post_id)
            save_seen(subreddit, seen)
            print(f"✓ [{subreddit}] Posted: {title[:70]}")
        else:
            print(f"✗ [{subreddit}] Delivery failed for {post_id}; will retry on a later fetch")
    except Exception as e:
        print(f"✗ [{subreddit}] Failed {post_id}: {e}")

# ───────────────────────── fetch (returns backoff seconds, or 0) ─────────────────────────
def fetch_and_process(sub: dict, seen: set, channel_seen: set) -> float:
    """
    Fetch one subreddit's RSS feed and process new entries.
    Returns 0 on a normal fetch, or a number of seconds to additionally
    back off before this subreddit is tried again (e.g. on a 429).
    """
    subreddit = sub["name"]
    sort = sub["sort"].lower()
    limit = sub["limit"]

    if sort == "new":
        url = f"https://www.reddit.com/r/{subreddit}/new.rss"
    else:
        url = f"https://www.reddit.com/r/{subreddit}/.rss"

    print(f"\nFetching → {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=25)

        if resp.status_code == 429:
            backoff = DEFAULT_429_BACKOFF_SECONDS
            retry_after = resp.headers.get("Retry-After")
            reset = resp.headers.get("x-ratelimit-reset")
            if retry_after and retry_after.isdigit():
                backoff = max(backoff, int(retry_after))
            elif reset and reset.replace(".", "", 1).isdigit():
                backoff = max(backoff, int(float(reset)))
            _set_reddit_cooldown(backoff)
            print(f"RSS 429 for r/{subreddit}, backing off {backoff}s (shared cooldown)")
            return backoff

        if resp.status_code != 200:
            print(f"RSS status {resp.status_code} for r/{subreddit}")
            if resp.status_code in (403, 429, 500, 502, 503, 504):
                _set_reddit_cooldown(DEFAULT_429_BACKOFF_SECONDS)
            return DEFAULT_429_BACKOFF_SECONDS

        feed = feedparser.parse(resp.content)
        entries = feed.entries[:limit]
        _mark_progress()
        print(f"RSS fetched for r/{subreddit}: {len(entries)} entries")

        for entry in reversed(entries):
            process_entry(entry, seen, channel_seen, sub)
            # Give the OS a chance to reclaim memory between posts instead
            # of decoding/uploading media back-to-back with no gap — this
            # is what turns transient per-post memory use into a spike big
            # enough to trip the container's OOM killer.
            gc.collect()
            time.sleep(DELAY_BETWEEN_POSTS)

        return 0

    except Exception as e:
        print(f"Fetch error for r/{subreddit}: {e}")
        return DEFAULT_429_BACKOFF_SECONDS

# ───────────────────────── main loop ─────────────────────────
def _install_shutdown_flush():
    """
    Best-effort: when Render stops the container (SIGTERM on a redeploy,
    scaling event, etc.), push whatever seen-state changes happened since
    the last periodic sync before the process actually exits, so the last
    few posts aren't lost from the backup. Falls through to default
    behavior if anything about this goes wrong.
    """
    def handler(signum, frame):
        try:
            flush_dirty_seen_to_github()
        finally:
            sys.exit(0)

    try:
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)
    except Exception:
        pass  # e.g. not the main thread, or platform doesn't support it


def main():
    start_health_server()
    start_self_pinger()
    start_github_syncer()
    _install_shutdown_flush()

    names = ", ".join(f"r/{s['name']}" for s in SUBREDDITS)
    print(f"Bot started → {names}")
    print("Using public RSS only (no Reddit API)")
    print(
        f"Per-feed cooldown: {MIN_SECONDS_BETWEEN_FETCHES_PER_SUB}s, "
        f"delay between subreddits: {DELAY_BETWEEN_SUBREDDITS}s"
    )

    seen = {s["name"]: load_seen(s["name"]) for s in SUBREDDITS}
    # Union the persisted per-subreddit IDs for each Telegram destination. This
    # also deduplicates posts shared by overlapping subreddit feeds after restart.
    channel_seen: dict[str, set] = {}
    for sub in SUBREDDITS:
        channel_seen.setdefault(str(sub["chat_id"]), set()).update(seen[sub["name"]])

    # due_at: subreddit is eligible to be fetched once time.time() >= due_at.
    due_at = {s["name"]: 0.0 for s in SUBREDDITS}

    while True:
        global _last_tick, _last_scheduler_report
        _mark_progress()
        cooldown = _reddit_cooldown_remaining()
        if cooldown > 0:
            if time.monotonic() - _last_scheduler_report >= 30:
                print(f"[scheduler] Reddit-wide cooldown; next check in about {cooldown:.0f}s")
                _last_scheduler_report = time.monotonic()
            time.sleep(min(SCHEDULER_TICK_SECONDS, max(1.0, cooldown)))
            continue

        fetched_any = False
        for sub in SUBREDDITS:
            name = sub["name"]
            now = time.time()
            if now < due_at[name]:
                continue

            fetched_any = True
            _mark_progress()
            print(f"[scheduler] Checking r/{name}")
            backoff = fetch_and_process(sub, seen[name], channel_seen.setdefault(str(sub["chat_id"]), set()))
            due_at[name] = time.time() + MIN_SECONDS_BETWEEN_FETCHES_PER_SUB + backoff
            if _reddit_cooldown_remaining() > 0:
                break
            time.sleep(DELAY_BETWEEN_SUBREDDITS)

        if not fetched_any and time.monotonic() - _last_scheduler_report >= 60:
            wait_for = min((max(0.0, due - time.time()) for due in due_at.values()), default=0.0)
            print(f"[scheduler] Idle normally; next feed due in about {wait_for:.0f}s")
            _last_scheduler_report = time.monotonic()

        _mark_progress()
        time.sleep(SCHEDULER_TICK_SECONDS)

if __name__ == "__main__":
    main()