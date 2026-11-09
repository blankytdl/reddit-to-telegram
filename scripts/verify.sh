#!/usr/bin/env bash
# Runs the real checks and records REAL measurements in verify-report.txt.
# Needs: cargo, lightpanda, ffmpeg, GNU time (/usr/bin/time), network access.
# Env: TELEGRAM_BOT_TOKEN, SUBREDDIT_1..5_NAME/CHAT_ID, LIGHTPANDA_BIN,
#      TEST_SUB (default: first sub), TEST_CHAT_ID, TEST_IMAGE_URL, TEST_GIF_URL, TEST_VIDEO_URL
set -u
OUT=verify-report.txt; : > "$OUT"
BIN=./target/release/reddit-telegram-bot
log(){ echo "$*" | tee -a "$OUT"; }
step(){ log; log "=== $1"; }

# Peak combined RSS (MB) of bot + browser + ffmpeg while PID $1 is alive.
watch_rss(){ local pid=$1 peak=0 cur
  while kill -0 "$pid" 2>/dev/null; do
    cur=$(ps -eo rss,comm | awk '/reddit-telegram|lightpanda|ffmpeg/ {s+=$1} END{print s+0}')
    [ "$cur" -gt "$peak" ] && peak=$cur; sleep 0.5; done
  echo $((peak/1024)); }

measure(){ # measure <label> <cmd...>   (limit CPU yourself with docker --cpus=0.1 for a Render-like run)
  local label=$1; shift
  /usr/bin/time -f "cpu_user=%Us cpu_sys=%Ss wall=%es" -o /tmp/time.out "$@" > /tmp/measure.out 2>&1 &
  local pid=$!; local peak; peak=$(watch_rss $pid); wait $pid; local rc=$?
  cat /tmp/measure.out >> "$OUT"
  log "[$label] exit=$rc peak_total_rss_mb=$peak $(cat /tmp/time.out)"; return $rc; }

step "1. compile";   cargo build --release 2>&1 | tail -3 | tee -a "$OUT"
step "2. unit tests"; cargo test --release 2>&1 | tail -15 | tee -a "$OUT"

SUB=${TEST_SUB:-${SUBREDDIT_1_NAME:-}}
step "3-5. browser startup, real Reddit navigation, post extraction (r/$SUB new + hot)"
measure fetch-new $BIN fetch-test "$SUB" new
measure fetch-hot $BIN fetch-test "$SUB" hot

step "6-8. media forwarding via the real download/upload path"
for k in IMAGE GIF VIDEO; do v=TEST_${k}_URL
  if [ -n "${!v:-}" ] && [ -n "${TEST_CHAT_ID:-}" ]; then measure "media-$k" $BIN media-test "$TEST_CHAT_ID" "${!v}"
  else log "[media-$k] SKIPPED (set $v and TEST_CHAT_ID)"; fi; done

step "9-10. duplicate prevention + restart/state restoration (5 subs, local state file)"
export STATE_PATH=/tmp/rtb-verify/seen.json SEND_BACKLOG_ON_FIRST_RUN=true POST_LIMIT=${POST_LIMIT:-3} PORT=${PORT:-18080}
rm -rf /tmp/rtb-verify
measure run1 timeout -s TERM 420 $BIN; D1=$(grep -c "delivered post" /tmp/measure.out)
measure run2 timeout -s TERM 420 $BIN; D2=$(grep -c "delivered post" /tmp/measure.out)
log "run1 delivered=$D1  run2 delivered=$D2  (run2 must be 0 unless new posts appeared)"

step "11. self-ping"
export RENDER_EXTERNAL_URL=http://127.0.0.1:$PORT SELF_PING_INTERVAL_SECS=5
timeout -s TERM 30 $BIN > /tmp/ping.out 2>&1; grep -c "self-ping ok" /tmp/ping.out | sed 's/^/self-ping ok count: /' | tee -a "$OUT"
step "done - report in $OUT"
