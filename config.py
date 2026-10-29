# ───────────────────────── Telegram ─────────────────────────
import os
import re

# One bot, used for all channels. The token now comes from an environment
# variable instead of being hardcoded here, so it never has to live in a
# plain-text file again. Set it in Render's dashboard under the service's
# "Environment" tab as TELEGRAM_BOT_TOKEN.
#
# IMPORTANT: the token that used to be hardcoded here was shared in a plain
# text file. Revoke/regenerate it with @BotFather (/revoke) before deploying
# and use the new one as the env var value — treat the old one as burned.
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError(
        "TELEGRAM_BOT_TOKEN environment variable is not set. "
        "Set it in Render's dashboard (Environment tab) before starting the service."
    )

# ───────────────────────── "Seen" state backup (GitHub) ─────────────────────────
# Render's free plan has no persistent disk — the local `seen/*.json` files
# (which stop the bot re-posting things it already sent) are wiped on every
# deploy and every restart. This optional feature backs them up into the
# bot's own GitHub repo, on a dedicated branch, so they survive redeploys —
# literally saved as part of the code, not on the container's disk.
#
# All of this is OPTIONAL: leave GITHUB_TOKEN / GITHUB_REPO unset and the
# bot behaves exactly as before (local-file-only, reset on every deploy).
#
# Setup:
#   1. Create a GitHub fine-grained personal access token scoped to ONLY
#      this repository, with "Contents" permission set to Read and write.
#      (Settings → Developer settings → Fine-grained tokens on GitHub.)
#   2. Set GITHUB_TOKEN to that token's value and GITHUB_REPO to
#      "your-username/your-repo-name" in Render's Environment tab.
#   3. Leave GITHUB_STATE_BRANCH on its default ("bot-state") — the bot
#      creates that branch itself on first run if it doesn't exist yet.
#
# IMPORTANT — use a branch Render does NOT deploy from. The bot commits to
# GITHUB_STATE_BRANCH periodically. If that were the same branch Render
# watches for auto-deploy, every state commit would trigger a redeploy,
# which restarts the bot in a loop. Keep it a separate branch (the default)
# and leave Render's service pointed at your normal branch (e.g. "main") —
# then these commits never reach Render at all, they just sit in git as a
# backup the bot reads back from on startup.
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")          # "owner/repo"
GITHUB_STATE_BRANCH = os.environ.get("GITHUB_STATE_BRANCH", "bot-state")
GITHUB_SEEN_PATH = os.environ.get("GITHUB_SEEN_PATH", "seen").strip("/")

# How often to push changed "seen" files to GitHub. The 60-second default
# narrows the restart-loss window while batching posts into one commit per
# changed subreddit instead of committing after every single post. If
# GITHUB_SYNC_INTERVAL_SECONDS is set in the host environment, that value
# overrides this default.
GITHUB_SYNC_INTERVAL_SECONDS = int(os.environ.get("GITHUB_SYNC_INTERVAL_SECONDS", "60"))

if bool(GITHUB_TOKEN) != bool(GITHUB_REPO):
    raise RuntimeError(
        "Set BOTH GITHUB_TOKEN and GITHUB_REPO to enable GitHub-backed seen "
        "state, or neither to disable it — one without the other is almost "
        "certainly a misconfiguration."
    )

# ───────────────────────── Subreddits → Channels ─────────────────────────
# One entry per subreddit. They are polled IN THIS ORDER, one at a time.
# "chat_id" can be a channel @username (bot must be admin there) or a numeric id.
#
# Fully configurable via environment variables — no code edits needed.
# For each subreddit N (starting at 1), set:
#   SUBREDDIT_1_NAME     - subreddit name, without "r/"           (required)
#   SUBREDDIT_1_CHAT_ID  - Telegram channel @username or numeric id (required)
#   SUBREDDIT_1_SORT     - "new" or "hot"                          (optional, default "new")
#   SUBREDDIT_1_LIMIT    - how many latest posts to look at        (optional, default 12)
# ...then SUBREDDIT_2_NAME / SUBREDDIT_2_CHAT_ID / etc for the next one, and so
# on. The bot keeps reading SUBREDDIT_<N>_NAME for increasing N until it hits
# a number with no NAME set, so you can add or remove subreddits just by
# adding/removing env vars — no fixed limit on how many.
#
# Multiple subreddits → one channel is supported out of the box: just reuse
# the same CHAT_ID value on more than one SUBREDDIT_N entry. Each subreddit
# still gets its own "seen" tracking and its own fetch schedule; they just
# happen to both post into the same Telegram channel.
#
# Example (Render dashboard → Environment tab):
#   SUBREDDIT_1_NAME=aww
#   SUBREDDIT_1_CHAT_ID=@my_animals_channel
#   SUBREDDIT_2_NAME=EarthPorn
#   SUBREDDIT_2_CHAT_ID=-1001234567890
#   SUBREDDIT_2_SORT=hot
#   SUBREDDIT_2_LIMIT=20
#   SUBREDDIT_3_NAME=aww2                 # a second subreddit ...
#   SUBREDDIT_3_CHAT_ID=@my_animals_channel   # ... sharing the SAME channel as #1
DEFAULT_SORT = os.environ.get("SUBREDDIT_DEFAULT_SORT", "new")
DEFAULT_LIMIT = int(os.environ.get("SUBREDDIT_DEFAULT_LIMIT", "12"))

SUBREDDITS = []
_i = 1
while True:
    _name = os.environ.get(f"SUBREDDIT_{_i}_NAME")
    if not _name:
        break
    _chat_id = os.environ.get(f"SUBREDDIT_{_i}_CHAT_ID")
    if not _chat_id:
        raise RuntimeError(
            f"SUBREDDIT_{_i}_NAME is set to '{_name}' but SUBREDDIT_{_i}_CHAT_ID "
            "is missing. Every subreddit needs a matching chat id env var."
        )
    _sort = os.environ.get(f"SUBREDDIT_{_i}_SORT", DEFAULT_SORT)
    _limit = int(os.environ.get(f"SUBREDDIT_{_i}_LIMIT", str(DEFAULT_LIMIT)))
    SUBREDDITS.append({
        "name": re.sub(r"^/?(?:r/)?", "", _name.strip(), flags=re.I),
        "chat_id": _chat_id.strip(),
        "sort": _sort,
        "limit": _limit,
    })
    _i += 1

if not SUBREDDITS:
    raise RuntimeError(
        "No subreddits configured. Set SUBREDDIT_1_NAME and SUBREDDIT_1_CHAT_ID "
        "(and optionally SUBREDDIT_1_SORT / SUBREDDIT_1_LIMIT) in Render's "
        "Environment tab, adding SUBREDDIT_2_*, SUBREDDIT_3_*, etc. for more."
    )

# ───────────────────────── Timing / rate limits ─────────────────────────
# Reddit throttles its public RSS feeds to roughly 1 request per MINUTE per
# feed (confirmed as of 2025-2026; unauthenticated requests that go faster
# than this reliably get HTTP 429). This is PER FEED (per subreddit), not
# shared across your whole bot, but we still keep a comfortable safety
# margin above the observed limit.

# Minimum seconds between two fetches of the SAME subreddit's RSS feed.
# Keep this >= 65s. 75s gives headroom if Reddit's window drifts.
MIN_SECONDS_BETWEEN_FETCHES_PER_SUB = 75

# Seconds to wait between finishing one subreddit and starting the next,
# so requests to different feeds are spaced out rather than fired back to back.
DELAY_BETWEEN_SUBREDDITS = 10

# How often the main loop "checks the clock" to see if any subreddit is due.
SCHEDULER_TICK_SECONDS = 5

# If Reddit returns a 429 for a subreddit and gives no usable Retry-After/
# x-ratelimit-reset header, back off by this many extra seconds before
# trying that subreddit again.
DEFAULT_429_BACKOFF_SECONDS = 90

# Seconds to wait between posting one entry and starting the next, within
# the same subreddit fetch. This isn't about Reddit's rate limit — it's to
# avoid processing a burst of media (download + Pillow decode + upload)
# back-to-back with no gap, which is what tips a small/low-memory host over
# into an OOM kill even though no single post is that heavy on its own.
DELAY_BETWEEN_POSTS = 3

# Hard cap (bytes) on any single media download. Protects against a single
# oversized file (an uncompressed screenshot, a long GIF, etc.) spiking
# memory past what a small container has available. 25MB comfortably covers
# normal Reddit images/short clips.
MAX_MEDIA_DOWNLOAD_BYTES = 25 * 1024 * 1024

# ───────────────────────── Caption options ─────────────────────────
INCLUDE_TITLE       = True
INCLUDE_FOOTER      = True
INCLUDE_REDDIT_LINK = True

# If True, posts with no image/gif/video attached (link posts, self-text
# posts) are skipped entirely instead of being sent to Telegram as a plain
# text message. They're still marked "seen" so they aren't re-checked on
# every future fetch.
#
# Configurable via the SKIP_TEXT_ONLY_POSTS env var ("true"/"false", case
# insensitive). Defaults to true (skip them) if left unset.
SKIP_TEXT_ONLY_POSTS = os.environ.get("SKIP_TEXT_ONLY_POSTS", "true").strip().lower() in (
    "1", "true", "yes", "on",
)

# ───────────────────────── Spoiler handling ─────────────────────────
# Reddit's public RSS does not expose a real "spoiler": true field (that only
# exists on the now-deprecated JSON API), so this is a best-effort heuristic
# based on the post's flair/category tags and title. When True, media that
# looks spoiler-flagged is sent to Telegram blurred using Telegram's native
# spoiler feature, rather than being skipped.
BLUR_SPOILERS = True
