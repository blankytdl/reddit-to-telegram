//! Pure helpers for catching up on posts that were missed (bot offline, rate-limited, a busy
//! subreddit pushing posts off the first page, ...). Kept free of I/O so they are easy to test.

/// How many consecutive already-known posts at the old end of a page mean "we have reached
/// territory processed before" and paging can stop.
pub const ANCHOR_STREAK: usize = 3;

/// Posts older than the last checkpoint by up to this much are still examined: Reddit's spam
/// filter and moderators can approve a post hours after it was created, and it then appears
/// *inside* already-processed territory of /new.
pub const SINCE_SLACK_MS: u64 = 3 * 60 * 60 * 1000;

/// `known` is a newest-first page. True when its last `streak` entries are all known.
pub fn reached_anchor(known: &[bool], streak: usize) -> bool {
    if known.is_empty() || streak == 0 {
        return false;
    }
    let n = streak.min(known.len());
    known[known.len() - n..].iter().all(|k| *k)
}

/// Newest timestamp such that it and every older post (that has a timestamp) is known.
/// Rows are `(created_ms, known)`. Unknown posts stop the checkpoint from advancing past
/// them, so a post skipped by the cycle budget is still inside the next catch-up window.
pub fn contiguous_known_ms(mut rows: Vec<(u64, bool)>) -> u64 {
    rows.retain(|r| r.0 > 0);
    rows.sort_by_key(|r| r.0);
    let mut newest = 0;
    for (ms, known) in rows {
        if known {
            newest = ms;
        } else {
            break;
        }
    }
    newest
}

/// Oldest post time (ms) worth fetching: the later of "checkpoint minus slack" and
/// "now minus the maximum catch-up age". 0 means no limit.
pub fn catchup_floor_ms(checkpoint_newest_ms: u64, now_ms: u64, max_age_ms: u64) -> u64 {
    let since = if checkpoint_newest_ms > 0 {
        checkpoint_newest_ms.saturating_sub(SINCE_SLACK_MS)
    } else {
        0
    };
    let age = if max_age_ms > 0 { now_ms.saturating_sub(max_age_ms) } else { 0 };
    since.max(age)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_needs_known_tail() {
        assert!(!reached_anchor(&[], 3));
        assert!(!reached_anchor(&[false, false, false, false], 3));
        assert!(reached_anchor(&[false, false, true, true, true], 3));
        assert!(!reached_anchor(&[true, true, false, true, true], 3));
        // a short page that is entirely known is an anchor too
        assert!(reached_anchor(&[true, true], 3));
        assert!(!reached_anchor(&[true], 0));
    }

    #[test]
    fn checkpoint_stops_at_first_unknown_post() {
        let rows = vec![(300, false), (100, true), (200, true), (400, true), (0, false)];
        assert_eq!(contiguous_known_ms(rows), 200);
        assert_eq!(contiguous_known_ms(vec![(10, true), (20, true)]), 20);
        assert_eq!(contiguous_known_ms(vec![(10, false)]), 0);
        assert_eq!(contiguous_known_ms(vec![]), 0);
    }

    #[test]
    fn floor_uses_the_later_of_checkpoint_and_max_age() {
        let hour = 3_600_000u64;
        let now = 100 * hour;
        // checkpoint 2h ago, slack 3h -> 5h ago; max age 72h -> 28h ago; later = 5h ago
        assert_eq!(catchup_floor_ms(now - 2 * hour, now, 72 * hour), now - 5 * hour);
        // checkpoint older than max age: max age wins
        assert_eq!(catchup_floor_ms(now - 90 * hour, now, 72 * hour), now - 72 * hour);
        // no checkpoint, no max age -> unlimited
        assert_eq!(catchup_floor_ms(0, now, 0), 0);
        assert_eq!(catchup_floor_ms(0, now, 72 * hour), now - 72 * hour);
    }
}
