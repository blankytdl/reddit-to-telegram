# Reddit → Telegram bot (Rust, Lightpanda, Render Free)

Replacement for the RSS-based bot. It drives a real headless browser (Lightpanda, one persistent
process, one page at a time) to read `old.reddit.com` listings, then forwards posts and media to
Telegram via the Bot API.

> **STATUS: written but NOT compiled or tested.** It was authored in a sandbox with no Rust toolchain
> and no network access. Nothing below has been run against Reddit, Telegram, GitHub or Render. Run
> `scripts/verify.sh` and fill in `docs/COMPATIBILITY_AND_RESOURCES.md` before trusting it.
> Expect to fix a few compile errors on first `cargo build`.

Your existing Python implementation is untouched: this lives in its own directory.

## How it works
- `reddit_browser.rs` – spawns `lightpanda serve`, talks CDP over WebSocket, opens one target, navigates,
  runs an extraction script in the page, closes the target. Browser is recycled every `BROWSER_RECYCLE_CYCLES` cycles
  and restarted on any error/timeout.
- `reddit_parser.rs` – JSON → posts; detects "Too Many Requests"/block pages (heuristic); classifies media.
- `media_downloader.rs` – streaming, size-capped downloads to temp files (deleted on drop); Reddit DASH
  video+audio are remuxed with `ffmpeg -c copy` (no re-encode).
- `telegram.rs` – direct Bot API, streamed multipart uploads, retry on 429 (`retry_after`) / 5xx.
- `seen_store.rs` – seen IDs in memory, local JSON file, GitHub Contents API backup on branch `bot-state`.
- `scheduler.rs` – sequential cycle every 15 min, exponential backoff on rate limit, per-post failure cap.
- `health.rs`, `self_ping.rs` – `/health` (200/503) and optional best-effort self-ping.

SQLite was deliberately not used: a small JSON file plus the GitHub copy is simpler and avoids a C build dependency.

## New options (anime-channel setup)
```
DEFAULT_CHAT_ID=@animechannel
SUBREDDITS=anime,manga,animememes,AnimeART     # many subs -> one channel
MEDIA_ONLY=true                                 # only images / GIFs / videos
SPOILER_MODE=mark                               # mark | skip | ignore
```
- **Many subs, one channel:** `SUBREDDITS` (comma list; `name=@chat` overrides the channel) and/or numbered
  `SUBREDDIT_n_*` vars. Subs with the same channel/sort/media-only settings are fetched in **one page load** via
  `r/a+b+c` (`GROUP_MAX_SUBS`, default 8), so 30 subs ≈ 4 page loads instead of 30. If a combined page comes back empty
  the bot retries each sub separately. The same image/video posted in several subs is sent to a channel once (`DEDUPE_MEDIA`).
- **Media only:** `MEDIA_ONLY=true` (global) or `SUBREDDIT_n_MEDIA_ONLY` (per sub) drops text/link/gallery posts.
- **Spoilers:** posts flagged as spoilers are sent with Telegram's spoiler overlay (`has_spoiler`) and the title in a
  spoiler tag; `skip` drops them, `ignore` treats them normally. Detection = old.reddit's spoiler stamp/flair or a
  `[spoiler]` tag in the title — selectors are **unverified** against live Reddit. Documents (oversized images) cannot be overlaid.
- **Subreddit limit:** default cap 100 (`MAX_SUBREDDITS`, max 500). Memory does not grow with the sub count (sequential,
  streamed); *time* does. No safe maximum has been measured. Instead the scheduler enforces a per-cycle time budget
  (`CYCLE_BUDGET_SECS`, default 80% of the interval): when it runs out it stops and continues round-robin next cycle, and logs a warning
  ("remaining groups run next cycle"). If you see that warning, raise `POLL_INTERVAL_SECS` or trim subs.
- Telegram sends to one channel are spaced by `SEND_GAP_MS` (3 s) to stay under ~20 messages/minute; a busy 30-sub channel
  may therefore lag. State is pushed to GitHub at most every `STATE_FLUSH_MIN_SECS` plus at the end of each cycle and on shutdown.

## Migrating from the Python bot (reference repo)
- **Env vars carry over unchanged:** `TELEGRAM_BOT_TOKEN`, `SUBREDDIT_<N>_NAME/_CHAT_ID/_SORT/_LIMIT`, `SUBREDDIT_DEFAULT_SORT/_LIMIT`
  (default limit 12), `SKIP_TEXT_ONLY_POSTS` (default `true`, = `MEDIA_ONLY`), `BLUR_SPOILERS`, `GITHUB_TOKEN/_REPO/_STATE_BRANCH`,
  `GITHUB_SEEN_PATH`, `GITHUB_SYNC_INTERVAL_SECONDS` (default 60), `MAX_MEDIA_DOWNLOAD_BYTES`, plus `INCLUDE_TITLE/_FOOTER/_REDDIT_LINK`
  (these three were hardcoded in `config.py`; now env vars). Per-sub `_LIMIT` is honoured; grouped subs load `sum(limits)` posts (max 100).
- **Old state is imported.** On the first start with no `seen_posts.json` on the state branch, the bot reads the old
  `seen/<sub>.json` files (`GITHUB_SEEN_PATH`) from the same branch, so nothing already posted is sent again; those subs also skip first-run seeding.
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
after the album. If the gallery can't be resolved the bot sends a link (or retries, in media-only mode). If an album fails after
an earlier one was sent, the post is still marked done to avoid re-sending. Untested: whether Lightpanda renders that JSON view as
readable text, and whether Reddit rate-limits these extra loads.

## Behaviour you should know
- **First run seeds, doesn't flood.** For each subreddit+channel with no history, existing posts are recorded as seen and
  *not* sent (`SEND_BACKLOG_ON_FIRST_RUN=true` to change).
- If GitHub is configured and the restore fails after 5 tries, the bot **exits** instead of starting with empty
  history (prevents duplicate floods). Render will restart it.
- Posts are marked seen only after successful delivery; a post failing 3 cycles in a row is skipped.
- Media: photo → `sendPhoto` (oversized/rejected → resized, still a photo; document only as last resort); GIF → `sendAnimation`;
  MP4 / v.redd.it → `sendVideo`. Over 50 MB or undownloadable → a link message. Galleries, text posts and
  other hosts are sent as links. Downloads only from `i.redd.it`, `preview.redd.it`, `v.redd.it`, `i.imgur.com` over https.
- Reddit video: tries `DASH_1080…240` and `DASH_AUDIO_*` by HEAD probing (not parsing the MPD). Untested against live Reddit.
- Reddit blocks/CAPTCHAs/logins are reported and skipped. **There is no bypass logic.** Lightpanda sends its own User-Agent;
  it is not spoofed.

## Deploy on Render
1. Pick a Lightpanda release from https://github.com/lightpanda-io/browser/releases. Download the Linux x86_64 asset,
   run `sha256sum` on it, note the tag. (The asset name in the Dockerfile, `lightpanda-x86_64-linux`, is from memory — check it,
   override with the `LIGHTPANDA_ASSET` build arg if different.)
2. In the repo: `cargo generate-lockfile` and commit `Cargo.lock` (pins dependencies). Then remove nothing else; `Cargo.toml` uses semver ranges.
3. Create a GitHub fine-grained token with **Contents: read/write** on one repo (can be private). No need to create the branch; the bot does.
4. Render → New → Blueprint (uses `render.yaml`), or New Web Service → Docker. Set the env vars from `.env.example`,
   including `LIGHTPANDA_VERSION` and `LIGHTPANDA_SHA256` (used as Docker build args — the build fails with a clear message if missing).
5. Add your bot as admin of each target channel.
6. Check logs for `first run: existing posts marked seen`, then new posts on the next cycle.

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
- Render deploys can briefly run two instances; with a shared GitHub state file that can cause duplicates or a last-write-wins overwrite.
- Filesystem is ephemeral; only the GitHub branch survives restarts. Crash between sending and flushing can re-send ≤1 subreddit's posts.
- Reddit may block Render's datacenter IPs or Lightpanda's client. The compatibility of Lightpanda with `old.reddit.com` is unknown until you run `fetch-test`.
- `old.reddit.com` markup selectors (`#siteTable > div.thing`, `data-*`) are assumed from memory of the site.
- Lightpanda is AGPL-3.0; running it as an unmodified separate process for a private bot is generally fine, but check against your own use.
- Obscura and Moli were **not evaluated** (could not verify they exist/are maintained from this environment).
- At 0.1 CPU, page loads will be slow; `PAGE_TIMEOUT_SECS` defaults to 90 s. Measure it.
