//! Turns the JSON produced by the in-page extraction script into typed posts,
//! detects rate-limit / block pages, and classifies media URLs.
use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RawPost {
    #[serde(default)]
    fullname: String,
    #[serde(default)]
    author: String,
    #[serde(default)]
    permalink: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    stickied: bool,
    #[serde(default)]
    promoted: bool,
    #[serde(default)]
    subreddit: String,
    #[serde(default)]
    spoiler: bool,
}

#[derive(Debug, Deserialize)]
struct RawPage {
    #[serde(default)]
    title: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    posts: Vec<RawPost>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Media {
    Image(String),
    Gif(String),
    Mp4(String),
    /// v.redd.it video id
    RedditVideo(String),
    /// reddit.com/gallery/<post id>
    Gallery(String),
    None,
}

#[derive(Debug, Clone)]
pub struct Post {
    pub id: String,
    pub title: String,
    pub author: String,
    pub permalink: String,
    pub source_url: String,
    pub media: Media,
    pub subreddit: String,
    pub spoiler: bool,
}

impl Media {
    /// Stable identity of the media itself (for cross-post de-duplication).
    pub fn key(&self) -> Option<&str> {
        match self {
            Media::Image(u) | Media::Gif(u) | Media::Mp4(u) | Media::RedditVideo(u) | Media::Gallery(u) => Some(u),
            Media::None => None,
        }
    }
}

/// Heuristic for posts whose spoiler status only shows up in the title.
pub fn title_has_spoiler_tag(title: &str) -> bool {
    let t = title.to_ascii_lowercase();
    if t.starts_with("spoiler:") || t.contains("(spoiler)") {
        return true;
    }
    t.match_indices('[').any(|(i, _)| t[i + 1..].trim_start().starts_with("spoiler"))
}

#[derive(Debug)]
pub enum PageStatus {
    Ok(Vec<Post>),
    RateLimited,
    Blocked(String),
}

/// Hosts we are willing to download from (SSRF / abuse guard). https only.
const MEDIA_HOSTS: &[&str] = &["i.redd.it", "preview.redd.it", "v.redd.it", "i.imgur.com"];

pub fn classify_media(url: &str) -> Media {
    let u = url.trim();
    let Some(rest) = u.strip_prefix("https://") else { return Media::None };
    let (host, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let host = host.to_ascii_lowercase();
    if matches!(host.as_str(), "www.reddit.com" | "old.reddit.com" | "reddit.com") {
        let path = path.split(['?', '#']).next().unwrap_or("");
        if let Some(id) = path.strip_prefix("/gallery/") {
            let id = id.trim_matches('/');
            if !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric()) {
                return Media::Gallery(id.to_string());
            }
        }
        return Media::None;
    }
    if !MEDIA_HOSTS.contains(&host.as_str()) {
        return Media::None;
    }
    let path = path.split(['?', '#']).next().unwrap_or("");
    if host == "v.redd.it" {
        let id = path.trim_start_matches('/').split('/').next().unwrap_or("");
        return if id.is_empty() { Media::None } else { Media::RedditVideo(id.to_string()) };
    }
    let lower = path.to_ascii_lowercase();
    let ext = match lower.rsplit_once('.') {
        Some((_, e)) => e,
        None => return Media::None,
    };
    let clean = format!("https://{host}{path}");
    match ext {
        "jpg" | "jpeg" | "png" | "webp" => Media::Image(clean),
        "gif" => Media::Gif(clean),
        "mp4" => Media::Mp4(clean),
        "gifv" => Media::Mp4(clean[..clean.len() - 4].to_string() + "mp4"),
        _ => Media::None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GalleryItem {
    pub url: String,
    pub animated: bool,
}

/// Parses the post JSON (`/comments/<id>.json`) of a gallery post into ordered image URLs.
/// URLs are rebuilt as https://i.redd.it/<media_id>.<ext> (no signed preview URLs needed).
/// Accept only Reddit image hosts from gallery metadata. Reddit's JSON can HTML-escape
/// ampersands in signed preview URLs; decode those before passing the URL to reqwest.
fn gallery_source_url(raw: &str) -> Option<String> {
    let url = raw.replace("&amp;", "&");
    [
        "https://i.redd.it/",
        "https://preview.redd.it/",
        "https://external-preview.redd.it/",
    ]
    .iter()
    .any(|prefix| url.starts_with(prefix))
    .then_some(url)
}

pub fn parse_gallery(json: &str) -> Result<Vec<GalleryItem>> {
    let v: serde_json::Value = serde_json::from_str(json.trim()).context("gallery data was not valid JSON")?;
    let d = &v[0]["data"]["children"][0]["data"];
    let items = d["gallery_data"]["items"].as_array().ok_or_else(|| anyhow!("post has no gallery data"))?;
    let meta = &d["media_metadata"];
    let mut out = Vec::new();
    for it in items {
        let Some(id) = it["media_id"].as_str() else { continue };
        if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
            continue;
        }
        let m = &meta[id];
        if m["status"].as_str() != Some("valid") {
            continue;
        }
        let ext = match m["m"].as_str().unwrap_or("") {
            "image/jpg" | "image/jpeg" => "jpg",
            "image/png" => "png",
            "image/gif" => "gif",
            "image/webp" => "webp",
            _ => continue,
        };
        let animated = ext == "gif" || m["e"].as_str() == Some("AnimatedImage");
        // Prefer Reddit's actual source URL (which may use .jpeg rather than .jpg, or
        // include a required signed query string). Keep the ID-based URL as a fallback
        // for older metadata that does not provide a usable source URL.
        let fallback = format!("https://i.redd.it/{id}.{ext}");
        let url = if animated {
            fallback
        } else {
            m["s"]["u"]
                .as_str()
                .and_then(gallery_source_url)
                .unwrap_or(fallback)
        };
        out.push(GalleryItem { url, animated });
    }
    if out.is_empty() {
        bail!("gallery contained no usable images");
    }
    Ok(out)
}

pub fn parse_page(json: &str, limit: usize) -> Result<PageStatus> {
    let page: RawPage = serde_json::from_str(json).context("extraction JSON was not valid")?;
    let haystack = format!("{} {}", page.title, page.text).to_ascii_lowercase();
    let page_url = page.url.to_ascii_lowercase();

    if page.posts.is_empty() {
        if page_url.contains("/login/")
            || haystack.contains("log in to use old reddit")
            || haystack.contains("accounts are required to access old reddit")
        {
            return Ok(PageStatus::Blocked("old Reddit login wall (reason=lor2)".into()));
        }
        if haystack.contains("too many requests") {
            return Ok(PageStatus::RateLimited);
        }
        for needle in ["whoa there, pardner", "blocked", "captcha", "verify you are", "access denied", "log in to continue"] {
            if haystack.contains(needle) {
                return Ok(PageStatus::Blocked(needle.to_string()));
            }
        }
    }

    let posts = page
        .posts
        .into_iter()
        .filter(|p| p.fullname.starts_with("t3_") && !p.title.is_empty() && !p.stickied && !p.promoted)
        .take(limit)
        .map(|p| {
            let permalink = if p.permalink.starts_with('/') {
                format!("https://www.reddit.com{}", p.permalink)
            } else {
                p.permalink.clone()
            };
            let external = p.url.starts_with("http") && !p.url.contains("reddit.com/r/");
            Post {
                media: classify_media(&p.url),
                source_url: if external { p.url.clone() } else { String::new() },
                spoiler: p.spoiler || title_has_spoiler_tag(&p.title),
                subreddit: p.subreddit,
                // Same id format as the Python bot (bare lowercase base-36), so old state matches.
                id: p.fullname.trim_start_matches("t3_").to_ascii_lowercase(),
                title: p.title,
                author: if p.author.is_empty() { "[deleted]".into() } else { p.author },
                permalink,
            }
        })
        .collect();
    Ok(PageStatus::Ok(posts))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_media() {
        assert_eq!(classify_media("https://i.redd.it/a.JPG?x=1"), Media::Image("https://i.redd.it/a.JPG".into()));
        assert_eq!(classify_media("https://i.redd.it/a.webp"), Media::Image("https://i.redd.it/a.webp".into()));
        assert_eq!(classify_media("https://i.redd.it/a.gif"), Media::Gif("https://i.redd.it/a.gif".into()));
        assert_eq!(classify_media("https://v.redd.it/abc123"), Media::RedditVideo("abc123".into()));
        assert_eq!(classify_media("https://i.imgur.com/x.gifv"), Media::Mp4("https://i.imgur.com/x.mp4".into()));
        assert_eq!(classify_media("https://i.imgur.com/x.mp4"), Media::Mp4("https://i.imgur.com/x.mp4".into()));
        assert_eq!(classify_media("http://i.redd.it/a.png"), Media::None);
        assert_eq!(classify_media("https://evil.example/a.png"), Media::None);
        assert_eq!(classify_media("https://www.reddit.com/gallery/abc"), Media::Gallery("abc".into()));
    }

    const GOOD: &str = r#"{"title":"new posts : x","text":"","posts":[
      {"fullname":"t3_aaa","author":"bob","permalink":"/r/x/comments/aaa/t/","url":"https://i.redd.it/a.png","title":"One","stickied":false,"promoted":false},
      {"fullname":"t3_bbb","author":"","permalink":"/r/x/comments/bbb/t/","url":"/r/x/comments/bbb/t/","title":"Self post","stickied":false,"promoted":false},
      {"fullname":"t3_ccc","author":"z","permalink":"/r/x/comments/ccc/t/","url":"https://i.redd.it/c.gif","title":"Pinned","stickied":true,"promoted":false},
      {"fullname":"t3_ddd","author":"z","permalink":"/r/x/comments/ddd/t/","url":"https://v.redd.it/vid","title":"Vid","stickied":false,"promoted":false},
      {"fullname":"t1_zzz","author":"z","permalink":"","url":"","title":"comment","stickied":false,"promoted":false}]}"#;

    #[test]
    fn extracts_posts_and_skips_stickied() {
        let PageStatus::Ok(p) = parse_page(GOOD, 10).unwrap() else { panic!() };
        assert_eq!(p.len(), 3);
        assert_eq!(p[0].id, "aaa");
        assert_eq!(p[0].permalink, "https://www.reddit.com/r/x/comments/aaa/t/");
        assert_eq!(p[1].author, "[deleted]");
        assert_eq!(p[1].media, Media::None);
        assert_eq!(p[2].media, Media::RedditVideo("vid".into()));
    }

    #[test]
    fn spoiler_and_subreddit_fields() {
        let j = r#"{"title":"x","text":"","posts":[
          {"fullname":"t3_a","author":"b","permalink":"/r/x/comments/a/","url":"https://i.redd.it/a.png","title":"Ep 5 [Spoiler] twist","subreddit":"anime"},
          {"fullname":"t3_b","author":"b","permalink":"/r/x/comments/b/","url":"https://i.redd.it/b.png","title":"plain","spoiler":true,"subreddit":"manga"},
          {"fullname":"t3_c","author":"b","permalink":"/r/x/comments/c/","url":"https://i.redd.it/c.png","title":"clean"}]}"#;
        let PageStatus::Ok(p) = parse_page(j, 10).unwrap() else { panic!() };
        assert!(p[0].spoiler && p[1].spoiler && !p[2].spoiler);
        assert_eq!(p[0].subreddit, "anime");
        assert!(title_has_spoiler_tag("x [ SPOILER ]") && title_has_spoiler_tag("y (spoiler)"));
        assert!(!title_has_spoiler_tag("no spoilers here"));
        assert_eq!(p[0].media.key(), Some("https://i.redd.it/a.png"));
        assert_eq!(Media::None.key(), None);
    }

    #[test]
    fn gallery_links_and_json() {
        assert_eq!(classify_media("https://www.reddit.com/gallery/abc123"), Media::Gallery("abc123".into()));
        assert_eq!(classify_media("https://www.reddit.com/r/x/comments/abc/t/"), Media::None);
        let j = r#"[{"data":{"children":[{"data":{"gallery_data":{"items":[{"media_id":"aaa"},{"media_id":"bbb"},{"media_id":"ccc"},{"media_id":"ddd"},{"media_id":"../x"}]},
          "media_metadata":{"aaa":{"status":"valid","e":"Image","m":"image/jpg"},"bbb":{"status":"valid","e":"AnimatedImage","m":"image/gif"},
          "ccc":{"status":"failed","m":"image/png"},"ddd":{"status":"valid","e":"Image","m":"image/png"}}}}]}}]"#;
        let g = parse_gallery(j).unwrap();
        assert_eq!(g.len(), 3);
        assert_eq!(g[0], GalleryItem { url: "https://i.redd.it/aaa.jpg".into(), animated: false });
        assert!(g[1].animated);
        let with_source = r#" [{"data":{"children":[{"data":{"gallery_data":{"items":[{"media_id":"source1"}]},
          "media_metadata":{"source1":{"status":"valid","e":"Image","m":"image/jpeg",
          "s":{"u":"https://preview.redd.it/source1.jpeg?width=1200&amp;format=pjpg&amp;auto=webp&amp;s=signature"}}}}}]}}]"#;
        let source_gallery = parse_gallery(with_source).unwrap();
        assert_eq!(source_gallery[0].url, "https://preview.redd.it/source1.jpeg?width=1200&format=pjpg&auto=webp&s=signature");
        let untrusted_source = r#" [{"data":{"children":[{"data":{"gallery_data":{"items":[{"media_id":"source2"}]},
          "media_metadata":{"source2":{"status":"valid","e":"Image","m":"image/jpeg",
          "s":{"u":"https://evil.example/image.jpg"}}}}}]}}]"#;
        assert_eq!(parse_gallery(untrusted_source).unwrap()[0].url, "https://i.redd.it/source2.jpg");
        assert_eq!(g[2].url, "https://i.redd.it/ddd.png");
        assert!(parse_gallery("[]").is_err());
        assert!(parse_gallery("<html>").is_err());
    }

    #[test]
    fn post_titles_cannot_fake_a_rate_limit_or_block() {
        let j = r#"{"title":"new posts","text":"too many requests blocked","posts":[
          {"fullname":"t3_a","author":"b","permalink":"/r/x/comments/a/","url":"","title":"Too Many Requests lol"}]}"#;
        assert!(matches!(parse_page(j, 5).unwrap(), PageStatus::Ok(v) if v.len() == 1));
    }

    #[test]
    fn respects_limit() {
        let PageStatus::Ok(p) = parse_page(GOOD, 1).unwrap() else { panic!() };
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn detects_rate_limit_and_block() {
        let rl = r#"{"title":"Too Many Requests","text":"","posts":[]}"#;
        assert!(matches!(parse_page(rl, 5).unwrap(), PageStatus::RateLimited));
        let bl = r#"{"title":"blocked","text":"whoa there, pardner!","posts":[]}"#;
        assert!(matches!(parse_page(bl, 5).unwrap(), PageStatus::Blocked(_)));
        let old_reddit_login = r#"{"title":"Log in to use Old Reddit","text":"Accounts are required to access old Reddit.","url":"https://old.reddit.com/login/?reason=lor2&dest=%2Fr%2Fanime%2Fnew%2F","posts":[]}"#;
        assert!(matches!(parse_page(old_reddit_login, 5).unwrap(), PageStatus::Blocked(reason) if reason.contains("login wall")));
        let empty = r#"{"title":"x","text":"","url":"https://old.reddit.com/r/anime/new/","posts":[]}"#;
        assert!(matches!(parse_page(empty, 5).unwrap(), PageStatus::Ok(v) if v.is_empty()));
        assert!(parse_page("not json", 5).is_err());
    }
}
