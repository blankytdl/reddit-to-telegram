"""
Telegram sending helpers.

- Every function takes an explicit chat_id, so one bot can post to many channels.
- Photo/video/animation sends accept `spoiler=True` to use Telegram's native
  blurred-media ("media spoiler") feature.
- Photos are pre-processed with Pillow to fix dimensions/aspect ratios that
  Telegram's photo endpoint rejects (see fix_image_bytes below), instead of
  failing send_photo outright.
- Handles Telegram's own 429 (Too Many Requests) by honoring `retry_after`.
"""

import gc
import io
import time

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
from PIL import Image

from config import TELEGRAM_BOT_TOKEN

API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Telegram photo constraints (see Bot API docs for sendPhoto):
# - width + height must not exceed 10000
# - the ratio of width to height must not exceed 20
MAX_DIMENSION_SUM = 10000
MAX_ASPECT_RATIO = 19.0  # stay a bit under Telegram's hard cap of 20

# Hard cap on the longest side regardless of ratio. This isn't a Telegram
# requirement — it's a memory-safety cap. A single ~6000x4000 photo decodes
# to well over 100MB of raw pixel data once Pillow opens it, which is enough
# on its own to OOM a small container (Pterodactyl/VPS instances are often
# capped at a few hundred MB). Capping here keeps peak memory bounded no
# matter how large the source image is.
MAX_LONG_SIDE = 4096


# ───────────────────────── image fixing ─────────────────────────
def fix_image_bytes(data: bytes) -> bytes:
    """
    Inspect an image and correct resolutions/aspect ratios that Telegram's
    sendPhoto would reject (extreme panoramas/strips, oversized dimensions,
    unsupported color modes), while keeping memory use bounded. Returns the
    original bytes unchanged if nothing needed fixing, or if the data isn't
    something Pillow can open (it will be sent as-is and Telegram can still
    decide what to do with it).
    """
    try:
        img = Image.open(io.BytesIO(data))
        # img.size reads only the header at this point — no decode yet.
        if img.format == "JPEG":
            # Tells the decoder to produce a smaller image directly during
            # decompression instead of decoding at full resolution and only
            # downscaling afterwards. For a huge photo this is the
            # difference between a ~20MB and a ~300MB allocation.
            img.draft("RGB", (MAX_LONG_SIDE, MAX_LONG_SIDE))
        elif img.size[0] * img.size[1] > 40_000_000:
            # Non-JPEG formats (PNG/WEBP) can't be partially decoded like
            # above. If it's pathologically large (>40MP), don't risk
            # decoding it fully — send it through untouched instead.
            img.close()
            return data
        img.load()
    except Exception:
        return data

    w, h = img.size
    if w <= 0 or h <= 0:
        return data

    changed = False

    # Fix extreme aspect ratios by padding the shorter side.
    long_side, short_side = max(w, h), min(w, h)
    if short_side > 0 and (long_side / short_side) > MAX_ASPECT_RATIO:
        work_mode = "RGBA" if img.mode == "RGBA" else "RGB"
        img = img.convert(work_mode)
        if w >= h:
            new_h = int(w / MAX_ASPECT_RATIO) + 1
            pad = new_h - h
            canvas = Image.new(work_mode, (w, new_h), "white")
            canvas.paste(img, (0, pad // 2))
        else:
            new_w = int(h / MAX_ASPECT_RATIO) + 1
            pad = new_w - w
            canvas = Image.new(work_mode, (new_w, h), "white")
            canvas.paste(img, (pad // 2, 0))
        img = canvas
        w, h = img.size
        changed = True

    # Fix oversized total dimensions by scaling down proportionally.
    if (w + h) > MAX_DIMENSION_SUM:
        scale = (MAX_DIMENSION_SUM - 200) / (w + h)  # extra safety margin
        new_w = max(1, int(w * scale))
        new_h = max(1, int(h * scale))
        img = img.resize((new_w, new_h), Image.LANCZOS)
        changed = True

    # Normalize weird color modes (P, CMYK, 1-bit, etc.) that can also
    # cause send failures.
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGB")
        changed = True

    # Hard memory-safety cap: shrink anything still huge on its long side,
    # independent of the ratio/sum checks above (a perfectly normal-ratio
    # 8000x6000 photo would pass both of those but still be huge in memory).
    w, h = img.size
    if max(w, h) > MAX_LONG_SIDE:
        scale = MAX_LONG_SIDE / max(w, h)
        img = img.resize((max(1, int(w * scale)), max(1, int(h * scale))), Image.LANCZOS)
        changed = True

    if not changed:
        img.close()
        return data

    buf = io.BytesIO()
    if img.mode == "RGBA":
        img.save(buf, format="PNG")
    else:
        img.save(buf, format="JPEG", quality=92)
    result = buf.getvalue()

    img.close()
    del img, buf
    gc.collect()
    return result


# ───────────────────────── low-level request helper ─────────────────────────
def _post(method: str, data: dict, files: dict | None = None, timeout: int = 120):
    url = f"{API_BASE}/{method}"
    for attempt in range(3):
        try:
            if files and any(hasattr(item[1], "read") for item in files.values()):
                fields = dict(data or {})
                fields.update(files)
                encoder = MultipartEncoder(fields=fields)
                response = requests.post(url, data=encoder, headers={"Content-Type": encoder.content_type}, timeout=timeout)
            else:
                response = requests.post(url, data=data, files=files, timeout=timeout)
        except Exception as e:
            print(f"  Telegram request error ({method}): {e}")
            return None

        with response as resp:
            if resp.status_code == 200:
                try:
                    result = resp.json()
                except ValueError:
                    result = None
                return result if isinstance(result, dict) and result.get("ok") is True else None

            if resp.status_code == 429:
                retry_after = 5
                try:
                    retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
                except Exception:
                    pass
                print(f"  Telegram 429 on {method}, waiting {retry_after}s...")
                time.sleep(retry_after + 1)
                continue

            print(f"  Telegram error {resp.status_code} on {method}: {resp.text[:200]}")
            return None

    return None


# ───────────────────────── public API ─────────────────────────
def send_message(chat_id: str, text: str) -> bool:
    return _post(
        "sendMessage",
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        },
    )


def send_photo(chat_id: str, data: bytes, caption: str = "", spoiler: bool = False) -> bool:
    fixed = fix_image_bytes(data)
    payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
    if spoiler:
        payload["has_spoiler"] = True
    return _post("sendPhoto", data=payload, files={"photo": ("photo.jpg", fixed)}) is not None


def send_video(chat_id: str, data, caption: str = "", spoiler: bool = False) -> bool:
    payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
    if spoiler:
        payload["has_spoiler"] = True
    return _post("sendVideo", data=payload, files={"video": ("video.mp4", data, "video/mp4")}) is not None


def send_animation(chat_id: str, data: bytes, caption: str = "", spoiler: bool = False) -> bool:
    payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
    if spoiler:
        payload["has_spoiler"] = True
    return _post("sendAnimation", data=payload, files={"animation": ("animation.gif", data)}) is not None
