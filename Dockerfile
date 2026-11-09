# ---- build stage ----
FROM rust:1.88-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

# ---- runtime stage (glibc, needed by the Lightpanda binary) ----
FROM debian:bookworm-slim
# Lightpanda is pinned: you MUST supply a release tag and its SHA-256 (see README).
# Pinned to the official Lightpanda 1.0.3 Linux x86_64 release.
ARG LIGHTPANDA_VERSION=1.0.3
ARG LIGHTPANDA_SHA256=53eb34da09159dcf8f04865dbfe3e63e9a652062684495aa7ced5ddd7c82e8b0
ARG LIGHTPANDA_ASSET=lightpanda-x86_64-linux

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl ffmpeg \
 && rm -rf /var/lib/apt/lists/*

RUN test -n "$LIGHTPANDA_VERSION" && test -n "$LIGHTPANDA_SHA256" \
 || { echo "ERROR: set LIGHTPANDA_VERSION and LIGHTPANDA_SHA256 build args (see README)"; exit 1; }
RUN curl -fsSL -o /usr/local/bin/lightpanda \
      "https://github.com/lightpanda-io/browser/releases/download/${LIGHTPANDA_VERSION}/${LIGHTPANDA_ASSET}" \
 && echo "${LIGHTPANDA_SHA256}  /usr/local/bin/lightpanda" | sha256sum -c - \
 && chmod +x /usr/local/bin/lightpanda

COPY --from=build /src/target/release/reddit-telegram-bot /usr/local/bin/reddit-telegram-bot
RUN useradd -r -m bot
USER bot
ENV LIGHTPANDA_BIN=/usr/local/bin/lightpanda \
    LIGHTPANDA_DISABLE_TELEMETRY=true \
    STATE_PATH=/tmp/rtb-state/seen.json \
    RUST_LOG=info
CMD ["reddit-telegram-bot"]
