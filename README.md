# Reddit → Telegram Bot

Polls one or more subreddits via their public RSS feeds (no Reddit API / OAuth
needed) and posts new entries — images, GIFs, and videos — into Telegram
channels. One bot can feed any number of subreddits into any number of
channels, including several subreddits into the same channel.

Built to run as a free Render Web Service, with an optional GitHub-backed
backup of "seen post" state so a redeploy or restart doesn't cause reposts.

## How it works

- Each subreddit is polled in a fixed round-robin order, one at a time, with
  a delay between subreddits and a minimum interval between two fetches of
  the *same* subreddit (Reddit's public RSS throttles to roughly one request
  per minute per feed).
- New posts are sent to their configured Telegram channel as a photo, GIF
  (animation), or video, with an optional caption (title / footer / link
  back to Reddit).
- Text-only posts (link posts, self-text posts with no media) can be skipped
  entirely or sent as plain text messages — your choice.
- A lightweight heuristic blurs posts that look spoiler-tagged, using
  Telegram's native spoiler feature.
- Already-sent posts are tracked per subreddit in local `seen/*.json` files
  so they aren't resent. On Render's free tier there's no persistent disk,
  so this state is wiped on every deploy/restart — see the optional GitHub
  backup below if you want it to survive that.
- A small HTTP server plus a self-ping loop keep the free Render instance
  from spinning down from inactivity.

## Requirements

- Python 3 with the packages in `requirements.txt` (`requests`, `feedparser`,
  `imageio-ffmpeg`, `Pillow`).
- A Telegram bot token from [@BotFather](https://t.me/BotFather).
- The bot added as an **admin** to every Telegram channel it will post to.

## Environment variables

All configuration is done through environment variables — no code edits
needed. Set these in Render's dashboard under the service's **Environment**
tab (or in a local `.env` if you run it yourself).

### Required

| Variable | Description |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Your bot's token from @BotFather. |
| `SUBREDDIT_1_NAME` | First subreddit to poll, without the `r/` prefix (e.g. `aww`). |
| `SUBREDDIT_1_CHAT_ID` | Telegram channel to post it to — `@channel_username` or a numeric chat id. |

### Adding more subreddits

Add another numbered pair for each additional subreddit — `SUBREDDIT_2_*`,
`SUBREDDIT_3_*`, and so on. The bot keeps reading increasing numbers until it
hits one with no `_NAME` set, so there's no fixed limit — just add or remove
env vars to change the list.

| Variable | Description | Default |
|---|---|---|
| `SUBREDDIT_<N>_NAME` | Subreddit name for entry N. | — (required per entry) |
| `SUBREDDIT_<N>_CHAT_ID` | Telegram channel for entry N. | — (required per entry) |
| `SUBREDDIT_<N>_SORT` | `new` or `hot`. | `SUBREDDIT_DEFAULT_SORT`, or `new` |
| `SUBREDDIT_<N>_LIMIT` | How many latest posts to look at per fetch. | `SUBREDDIT_DEFAULT_LIMIT`, or `12` |
| `SUBREDDIT_DEFAULT_SORT` | Fallback sort for any entry that doesn't set its own. | `new` |
| `SUBREDDIT_DEFAULT_LIMIT` | Fallback limit for any entry that doesn't set its own. | `12` |

**Multiple subreddits into one channel:** just give two `SUBREDDIT_<N>_CHAT_ID`
entries the same value, e.g.:

```
SUBREDDIT_1_NAME=aww
SUBREDDIT_1_CHAT_ID=@my_animals_channel
SUBREDDIT_2_NAME=AnimalsBeingDerps
SUBREDDIT_2_CHAT_ID=@my_animals_channel
```

Each subreddit still gets its own independent "seen" tracking and fetch
schedule — they just happen to both post into the same channel.

### Post filtering & captions

| Variable | Description | Default |
|---|---|---|
| `SKIP_TEXT_ONLY_POSTS` | `true`/`false` — skip link/self-text posts with no media entirely, instead of sending them as a plain text message. | `true` |

`INCLUDE_TITLE`, `INCLUDE_FOOTER`, `INCLUDE_REDDIT_LINK`, and `BLUR_SPOILERS`
are set directly in `config.py` (not yet exposed as env vars) if you want to
change caption content or spoiler blurring.

### Optional: GitHub-backed "seen" state backup

Render's free plan has no persistent disk, so local `seen/*.json` files are
wiped on every deploy/restart. This optional feature backs them up into your
bot's own GitHub repo on a dedicated branch, so state survives redeploys.
Leave both unset to disable it (the bot behaves exactly as before).

| Variable | Description | Default |
|---|---|---|
| `GITHUB_TOKEN` | Fine-grained GitHub PAT, scoped to this repo only, with Contents: Read and write. | — |
| `GITHUB_REPO` | `"your-username/your-repo-name"`. | — |
| `GITHUB_STATE_BRANCH` | Branch the bot commits "seen" state to. **Must not** be the branch Render deploys from — otherwise every state commit triggers a redeploy loop. | `bot-state` |
| `GITHUB_SEEN_PATH` | Path within that branch where seen files are stored. | `seen` |
| `GITHUB_SYNC_INTERVAL_SECONDS` | How often to push changed seen files to GitHub. | `300` |

Setup:
1. Create a GitHub fine-grained personal access token scoped to only this
   repository, with "Contents" permission set to Read and write (GitHub →
   Settings → Developer settings → Fine-grained tokens).
2. Set `GITHUB_TOKEN` to that token's value and `GITHUB_REPO` to
   `"your-username/your-repo-name"`.
3. Leave `GITHUB_STATE_BRANCH` on its default (`bot-state`) — the bot creates
   that branch itself on first run if it doesn't exist.
4. Keep Render's service pointed at your normal branch (e.g. `main`) so the
   state commits never trigger a redeploy.

You must set both `GITHUB_TOKEN` and `GITHUB_REPO` together, or leave both
unset — setting only one raises a startup error.

## Deploying to Render

`render.yaml` is a [Render Blueprint](https://render.com/docs/blueprint-spec)
that defines the service as a free-tier Python web service. To deploy:

1. Push this project to a GitHub repo.
2. In Render, create a new **Blueprint** and point it at that repo.
3. Fill in the required environment variables in the Render dashboard
   (`TELEGRAM_BOT_TOKEN`, `SUBREDDIT_1_NAME`, `SUBREDDIT_1_CHAT_ID`, etc.) —
   `render.yaml` scaffolds the keys but leaves secret/site-specific values
   blank for you to fill in.
4. Deploy. Render sets `PORT` and `RENDER_EXTERNAL_URL` automatically;
   `main.py` uses both for its health-check server and self-ping loop.

### Staying awake on the free tier

`main.py` runs a background thread that pings its own
`RENDER_EXTERNAL_URL` every 10 minutes, which counts as inbound traffic and
resets Render's ~15-minute idle-spindown timer — no external cron/uptime
service needed. Caveat: this only works once the instance is already awake
and that loop is running. If Render does spin it down (e.g. a slow cold
start after a deploy pushes past 15 minutes before the loop starts pinging),
nothing wakes it back up until an actual visitor (or you) hits the URL. For a
true always-on guarantee, use an external pinger or Render's paid Background
Worker plan instead.

## Files

| File | Purpose |
|---|---|
| `main.py` | Polling loop, RSS parsing, media handling, health server, self-pinger. |
| `config.py` | All configuration, read from environment variables. |
| `telegram_sender.py` | Telegram Bot API calls (send photo/video/animation/message). |
| `requirements.txt` | Python dependencies. |
| `render.yaml` | Render Blueprint — service definition and env var scaffolding. |
