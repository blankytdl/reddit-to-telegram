# Reddit → Telegram bot (Rust, Lightpanda)

Replacement for the RSS-based bot. It drives a real headless browser (Lightpanda, one persistent
process, one page at a time) to read `old.reddit.com` listings, then forwards posts and media to
Telegram via the Bot API.

> Build and test the Rust application with `cargo test` and `cargo build --release` before deployment.

Your existing Python implementation is untouched: this lives in its own directory.

## How it works
- `reddit_browser.rs` – spawns `lightpanda serve`, talks CDP over WebSocket, opens one target, navigates,
  runs an extraction script in the page, closes the target. Browser is recycled every `BROWSER_RECYCLE_CYCLES` cycles
  and restarted on any error/timeout.
- `reddit_parser.rs` – JSON → posts; detects "Too Many Requests"/block pages (heuristic); classifies media.
- `media_downloader.rs` – streaming, size-capped downloads to temp files (deleted on drop); Reddit DASH
  video+audio are remuxed with `ffmpeg -c copy` (no re-encode).
- `telegram.rs` – direct Bot API, streamed multipart uploads, retry on 429 (`retry_after`) / 5xx.
- `seen_store.rs` – bounded seen IDs and initialized-subreddit markers saved to one local JSON file on the host.
- `main.rs` – logs to both the Altare console and a separate rotating log file; log files never share the seen-state directory.
- `scheduler.rs` – sequential cycle every 15 min, exponential backoff on rate limit, per-post failure cap.
- `health.rs`, `self_ping.rs` – `/health` (200/503) and optional best-effort self-ping.

SQLite and remote state services are deliberately not used. The bot stores its bounded state in a local JSON file; it needs no GitHub token, database, or external state service.

## New options (anime-channel setup)
```
DEFAULT_CHAT_ID=@animechannel
SUBREDDITS=anime,manga,animememes,AnimeART     # many subs -> one channel
MEDIA_ONLY=true                                 # only images / GIFs / videos, except flagged NSFW posts when INCLUDE_NSFW=true
INCLUDE_NSFW=true                               # send Reddit-marked 18+ posts
SPOILER_MODE=mark                               # mark | skip | ignore
```
- **Many subs, one channel:** `SUBREDDITS` (comma list; `name=@chat` overrides the channel) and/or numbered
  `SUBREDDIT_n_*` vars. Subs with the same channel/sort/media-only settings are fetched in **one page load** via
  `r/a+b+c` (`GROUP_MAX_SUBS`, default 8), so 30 subs ≈ 4 page loads instead of 30. If a combined page comes back empty
  the bot retries each sub separately. The same image/video posted in several subs is sent to a channel once (`DEDUPE_MEDIA`).
- **Media only:** `MEDIA_ONLY=true` (global) or `SUBREDDIT_n_MEDIA_ONLY` (per sub) drops ordinary text/link posts. Reddit-marked NSFW posts are still forwarded when `INCLUDE_NSFW=true`; unsupported external media is sent as the Reddit post link.
- **NSFW:** `INCLUDE_NSFW=true` (default) keeps posts Reddit marks with its `over18`/NSFW listing marker. The browser still needs a valid `REDDIT_SESSION_COOKIE` from an account authorized to view mature content; enable the mature-content preference on Reddit and Old Reddit before copying the session cookie. This does not bypass Reddit age checks or restrictions.
- **Separate files:** seen IDs are stored at `/home/container/data/seen_posts.json`; console logs are also written to `/home/container/logs/bot.log` (with one `.log.1` backup at 5 MiB). Set `STATE_PATH` and `LOG_PATH` independently to change these locations.
- **Spoilers:** posts flagged as spoilers are sent with Telegram's spoiler overlay (`has_spoiler`) and the title in a
  spoiler tag; `skip` drops them, `ignore` treats them normally. Detection = old.reddit's spoiler stamp/flair or a
  `[spoiler]` tag in the title — selectors are **unverified** against live Reddit. Documents (oversized images) cannot be overlaid.
- **Subreddit limit:** default cap 100 (`MAX_SUBREDDITS`, max 500). Memory does not grow with the sub count (sequential,
  streamed); *time* does. No safe maximum has been measured. Instead the scheduler enforces a per-cycle time budget
  (`CYCLE_BUDGET_SECS`, default 80% of the interval): when it runs out it stops and continues round-robin next cycle, and logs a warning
  ("remaining groups run next cycle"). If you see that warning, raise `POLL_INTERVAL_SECS` or trim subs.
- Telegram sends to one channel are spaced by `SEND_GAP_MS` (3 s) to stay under ~20 messages/minute; a busy 30-sub channel
  may therefore lag. Dirty seen-state data is written locally no more often than `STATE_FLUSH_MIN_SECS`; it is also saved on shutdown.
- **Host-local persistence:** the default Altare path is `/home/container/data/seen_posts.json`. The directory is created automatically. Keep this path on the server's persistent filesystem; do not put it under a temporary/cache directory. On a host that replaces its entire filesystem on redeploy, mount or select a persistent data directory and set `STATE_PATH` to that location.

## Migrating from the Python bot (reference repo)
- **Env vars carry over unchanged:** `TELEGRAM_BOT_TOKEN`, `SUBREDDIT_<N>_NAME/_CHAT_ID/_SORT/_LIMIT`, `SUBREDDIT_DEFAULT_SORT/_LIMIT`
  (default limit 12), `SKIP_TEXT_ONLY_POSTS` (default `true`, = `MEDIA_ONLY`), `BLUR_SPOILERS`, `MAX_MEDIA_DOWNLOAD_BYTES`, plus `INCLUDE_TITLE/_FOOTER/_REDDIT_LINK`
  (these three were hardcoded in `config.py`; now env vars). Per-sub `_LIMIT` is honoured; grouped subs load `sum(limits)` posts (max 100).
- **Local state only:** existing host state is loaded from `STATE_PATH`. There is no GitHub API integration or remote backup. Back up the JSON file yourself before deleting or recreating the server. If the state file is missing, the first run seeds existing Reddit posts instead of sending the whole backlog (`SEND_BACKLOG_ON_FIRST_RUN=true` changes this).
- **Same caption layout:** title / `r/sub • u/author` / "Open on Reddit" link. Spoiler detection keeps the Python heuristics
  (title `[spoiler`/`(spoiler)`, flair) plus old.reddit's spoiler stamp. Seen-tracking is per destination channel, like `channel_seen`.
- **Tall/wide images** are padded with white to stay under Telegram's 20:1 limit (as the Pillow code did), via `ffmpeg`.
- **Intentional differences:** it reads old.reddit through a real browser instead of RSS; polls every 15 min (your spec) instead of each sub every ≥75 s;
  Lightpanda's own User-Agent is used (the Python bot sent a Chrome UA); audio is stream-copied instead of re-encoded to AAC.
- **Two bugs in the reference `main.py` as uploaded:** `extract_media_url` has `matches = re.findall(...)        for m in matches:` on one line
  (a SyntaxError, so that file won't start), and the Reddit-link fallback appends a literal `\n\n` (escaped backslashes) instead of newlines. Not carried over.

## Images as native Telegram photos
Images are sent with `sendPhoto`. If an image is over 10 MB, or Telegram rejects it (dimensions), it is downscaled once with
`ffmpeg` (longest side `PHOTO_MAX_DIM`, default 2560 px, JPEG, one image at a time) and still sent as a photo. Only if that is
impossible — undecodable file or >50 megapixels (refused to protect the 512 MB limit) —
is the original sent as a document. Very tall or wide strips are padded with white (ratio ≤19:1) instead of being rejected. Notes: a resized PNG loses transparency (flattened JPEG); resizing is the only image
processing in the bot and costs a few CPU-seconds at 0.1 CPU, only for oversized images. Untested.

## Reddit galleries
Gallery posts (`reddit.com/gallery/<id>`) are sent as photo **albums**, in gallery order, 10 per album (galleries of 11–20 images
become two albums); the title/caption sits on the first album. To learn the image list the bot opens the post's
`/comments/<id>.json` document **in the same browser** (one extra page load per gallery post — still browser-based, but it is
Reddit's unofficial JSON view, not an API client; it is a separate step you may want to know about). Image URLs are rebuilt as
`i.redd.it/<media_id>.<ext>`. Oversized images are resized like normal photos; animated items are sent as separate animations
after the album. If the configured Reddit host does not return gallery JSON, the bot retries the request against the other canonical Reddit host. In media-only mode, unresolved galleries never fall back to a plain link: the post stays unseen and is retried with per-post exponential backoff. Outside media-only mode, the original-post link remains the fallback. If an album fails after
an earlier one was sent, the post is still marked done to avoid re-sending. Untested: whether Lightpanda renders that JSON view as
readable text, and whether Reddit rate-limits these extra loads.

## Behaviour you should know
- **First run seeds, doesn't flood.** For each subreddit+channel with no history, existing posts are recorded as seen and
  *not* sent (`SEND_BACKLOG_ON_FIRST_RUN=true` to change).
- If the local state file is corrupt or cannot be read, the bot **exits** rather than silently resetting deduplication history.
  If the state file is missing, the first run seeds existing Reddit posts instead of sending the whole backlog.
- Posts are marked seen only after successful delivery or an intentional filter/first-run seed. Failed deliveries stay unseen and are retried with per-post exponential backoff (based on the poll interval, capped at 6 hours); restarting the process clears the in-memory retry delay but does not lose the unseen post.
- Media: photo → `sendPhoto` (oversized/rejected → resized, still a photo; document only as last resort); GIF → `sendAnimation`;
  MP4 / v.redd.it → `sendVideo`. Over 50 MB or undownloadable → a link message. Galleries, text posts and
  other hosts are sent as links. Downloads only from `i.redd.it`, `preview.redd.it`, `v.redd.it`, `i.imgur.com` over https.
- Reddit video: tries `DASH_1080…240` and `DASH_AUDIO_*` by HEAD probing (not parsing the MPD). Untested against live Reddit.
- Reddit blocks/CAPTCHAs/login walls are reported and skipped with backoff. Old Reddit now requires a logged-in session.
  For the Lightpanda browser flow, set `REDDIT_SESSION_COOKIE` in the host's secret environment variables to the value
  of the `reddit_session` cookie from an account that can access Old Reddit. Never put the cookie in this file, a commit,
  or a chat message. The cookie is a login credential and may expire; replace it in the host settings if access stops.
  The bot does not bypass CAPTCHA or other access controls. Lightpanda sends its own User-Agent; it is not spoofed.

## Deploy on Altare
1. Build with `cargo build --release` and start `./target/release/reddit-telegram-bot`.
2. Set the required Telegram and subreddit environment variables from `.env.example`.
3. Set `STATE_PATH=/home/container/data/seen_posts.json` and `LOG_PATH=/home/container/logs/bot.log`; these directories are created automatically and are separate.
4. Set `INCLUDE_NSFW=true` (default). On the Reddit account used for `REDDIT_SESSION_COOKIE`, enable mature content in Reddit settings and in [Old Reddit preferences](https://old.reddit.com/prefs/).
5. Install Lightpanda at the configured `LIGHTPANDA_BIN` path and make it executable.
6. Add your bot as an administrator of each target Telegram channel.
7. Check startup logs for `file logging enabled`, `seen state restored from host storage`, or `no local seen-state file yet`.

There is no GitHub token, GitHub state branch, or GitHub API dependency. The local JSON file is the only seen-state store. A host-side backup/export is recommended before server reinstall or deletion.

Never commit `.env`. Tokens are never logged (Telegram URLs are stripped from errors).

## Testing
```
cargo test                      # unit tests: config, parser, store, health, caption, backoff
scripts/verify.sh               # real browser/Reddit/Telegram/restart/self-ping + RSS/CPU measurements
docker run --cpus=0.1 --memory=512m ...   # approximate Render Free limits locally
```
Subcommands: `reddit-telegram-bot fetch-test <sub> [new|hot]` (browser + parser only), `media-test <chat_id> <url>`.

## Honest limitations
- Render Free spins down after ~15 min without inbound traffic. Self-pinging the public URL only helps while the instance is
  running; it cannot wake a stopped one. If it spins down, polling stops until something external hits the URL
  (e.g. an uptime monitor) — and 750 free hours/month is ~31 days × 24 h = 744 h for ONE always-on service.
- Local state is only as durable as the host's data directory. Keep `STATE_PATH` on persistent server storage and export the JSON file before reinstalling or deleting the server. A crash between sending and flushing can re-send some recent posts.
- On platforms with ephemeral container filesystems, including an unmounted Render container, state is lost when the instance is replaced. Use a persistent disk or a host with persistent storage.
- Reddit may block Render's datacenter IPs or Lightpanda's client. The compatibility of Lightpanda with `old.reddit.com` is unknown until you run `fetch-test`.
- `old.reddit.com` markup selectors (`#siteTable > div.thing`, `data-*`) are assumed from memory of the site.
- Lightpanda is AGPL-3.0; running it as an unmodified separate process for a private bot is generally fine, but check against your own use.
- Obscura and Moli were **not evaluated** (could not verify they exist/are maintained from this environment).
- At 0.1 CPU, page loads will be slow; `PAGE_TIMEOUT_SECS` defaults to 90 s. Measure it.
