//! Host-local seen-post store.
//!
//! The state file lives on the host's persistent server filesystem. No remote
//! database, GitHub token, API calls, or background sync process is required.
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashSet, VecDeque},
    io,
    path::{Path, PathBuf},
    time::Duration,
};
use tracing::{info, warn};

#[derive(Serialize)]
struct StateRef<'a> {
    ids: &'a VecDeque<String>,
    subs: Vec<&'a str>,
}

#[derive(Deserialize, Default)]
struct State {
    #[serde(default)]
    ids: Vec<String>,
    #[serde(default)]
    subs: Vec<String>,
}

pub struct SeenStore {
    order: VecDeque<String>,
    set: HashSet<String>,
    subs: HashSet<String>,
    dirty: bool,
    path: PathBuf,
    cap: usize,
    last_flush: Option<tokio::time::Instant>,
}

impl SeenStore {
    pub fn new(path: PathBuf, cap: usize) -> Self {
        Self {
            order: VecDeque::new(),
            set: HashSet::new(),
            subs: HashSet::new(),
            dirty: false,
            path,
            cap,
            last_flush: None,
        }
    }

    /// Load seen IDs and initialized-sub markers from the host's local state file.
    /// A corrupt file is fatal rather than silently resetting deduplication state.
    pub async fn open(path: PathBuf, cap: usize) -> Result<Self> {
        let mut st = Self::new(path, cap);
        let legacy_path = PathBuf::from("/tmp/rtb-state/seen.json");
        let bytes = match tokio::fs::read(&st.path).await {
            Ok(bytes) => Some(bytes),
            Err(e) if e.kind() == io::ErrorKind::NotFound && st.path != legacy_path => {
                // Migration from the previous build's temporary default, if that file
                // still exists in this container. No network lookup is performed.
                match tokio::fs::read(&legacy_path).await {
                    Ok(bytes) => {
                        info!(from = %legacy_path.display(), to = %st.path.display(), "migrating previous local seen-state file");
                        Some(bytes)
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => None,
                    Err(e) => return Err(e).with_context(|| format!("cannot read legacy local state {}", legacy_path.display())),
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e).with_context(|| format!("cannot read local seen-state file: {}", st.path.display())),
        };

        if let Some(bytes) = bytes {
            let state: State = serde_json::from_slice(&bytes)
                .with_context(|| format!("local seen-state file is corrupt: {}", st.path.display()))?;
            st.apply(state);
            // If loaded from the former /tmp location, immediately persist a copy
            // in the new host-persistent location.
            if !tokio::fs::try_exists(&st.path).await.unwrap_or(false) {
                write_file(&st.path, &bytes).await?;
            }
            info!(ids = st.order.len(), path = %st.path.display(), "seen state restored from host storage");
        } else {
            info!(path = %st.path.display(), "no local seen-state file yet; a new one will be created");
        }
        Ok(st)
    }

    fn apply(&mut self, state: State) {
        for id in state.ids {
            self.insert_inner(id);
        }
        self.subs.extend(state.subs.into_iter().map(|s| s.to_ascii_lowercase()));
    }

    fn insert_inner(&mut self, id: String) -> bool {
        if self.set.contains(&id) {
            return false;
        }
        self.set.insert(id.clone());
        self.order.push_back(id);
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }

    pub fn contains(&self, id: &str) -> bool {
        self.set.contains(id)
    }

    pub fn insert(&mut self, id: &str) {
        if self.insert_inner(id.to_string()) {
            self.dirty = true;
        }
    }

    #[allow(dead_code)]
    pub fn sub_initialized(&self, sub: &str) -> bool {
        self.subs.contains(&sub.to_ascii_lowercase())
    }

    /// True if this subreddit was already running for this destination channel.
    pub fn sub_known(&self, chat: &str, sub: &str) -> bool {
        let sub = sub.to_ascii_lowercase();
        self.subs.contains(&format!("{chat}|{sub}").to_ascii_lowercase())
            || self.subs.contains(&format!("legacy|{sub}"))
    }

    pub fn mark_sub(&mut self, sub: &str) {
        if self.subs.insert(sub.to_ascii_lowercase()) {
            self.dirty = true;
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        let mut subs: Vec<&str> = self.subs.iter().map(String::as_str).collect();
        subs.sort_unstable();
        Ok(serde_json::to_vec(&StateRef { ids: &self.order, subs })?)
    }

    /// Persist changes locally. The temp-file + rename pattern avoids leaving a
    /// half-written JSON state file if the process is interrupted.
    pub async fn flush(&mut self) {
        if !self.dirty {
            return;
        }
        let bytes = match self.snapshot() {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!("cannot serialize local seen state: {e:#}");
                return;
            }
        };
        if let Err(e) = write_file(&self.path, &bytes).await {
            warn!(path = %self.path.display(), "local seen-state save failed (will retry): {e:#}");
            return;
        }
        self.last_flush = Some(tokio::time::Instant::now());
        self.dirty = false;
        info!(ids = self.order.len(), path = %self.path.display(), "seen state saved to host storage");
    }

    /// Save only when dirty and the configured local save interval has elapsed.
    pub async fn flush_if_due(&mut self, min: Duration) {
        if !self.dirty {
            return;
        }
        if let Some(last) = self.last_flush {
            if last.elapsed() < min {
                return;
            }
        }
        self.flush().await;
    }
}

async fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            tokio::fs::create_dir_all(dir)
                .await
                .with_context(|| format!("cannot create state directory {}", dir.display()))?;
        }
    }
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, bytes)
        .await
        .with_context(|| format!("cannot write temporary state file {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("cannot replace state file {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rtb-test-{}-{name}.json", std::process::id()))
    }

    #[test]
    fn sub_known_is_case_insensitive_for_channel_and_sub() {
        let mut s = SeenStore::new(tmp("case"), 10);
        assert!(!s.sub_known("@AnimeChannel", "Anime"));
        s.mark_sub(&crate::scheduler::sub_key("@AnimeChannel", "Anime"));
        assert!(s.sub_known("@AnimeChannel", "Anime"));
        assert!(s.sub_known("@animechannel", "ANIME"));
        assert!(!s.sub_known("@AnimeChannel", "manga"));
    }

    #[tokio::test]
    async fn dedup_and_cap() {
        let mut s = SeenStore::new(tmp("cap"), 3);
        for id in ["a", "b", "c", "a", "d"] {
            s.insert(id);
        }
        assert!(!s.contains("a"));
        assert!(s.contains("b") && s.contains("c") && s.contains("d"));
    }

    #[tokio::test]
    async fn local_roundtrip_simulates_restart() {
        let p = tmp("roundtrip");
        let _ = std::fs::remove_file(&p);
        let mut s = SeenStore::new(p.clone(), 100);
        s.insert("t3_x");
        s.mark_sub("Foo");
        s.flush().await;

        let mut s2 = SeenStore::open(p.clone(), 100).await.unwrap();
        assert!(s2.contains("t3_x"));
        assert!(s2.sub_initialized("foo"));
        assert!(!s2.contains("t3_y"));
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn corrupt_state_is_not_silently_reset() {
        let p = tmp("corrupt");
        tokio::fs::write(&p, b"{not-json").await.unwrap();
        assert!(SeenStore::open(p.clone(), 100).await.is_err());
        let _ = std::fs::remove_file(&p);
    }
}
