import re
import hashlib


def normalize_channel_name(name: str) -> str:
    if not name:
        return ""
    name = name.lower()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'[^a-z0-9 ]', '', name)
    return name.strip()


def is_valid_url(url: str) -> bool:
    if not url:
        return False
    return url.startswith("http://") or url.startswith("https://")


def clean_url(url: str) -> str:
    if not url:
        return ""
    return url.strip()


def get_url_hash(url: str) -> str:
    if not url:
        return ""
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def extract_resolution(text: str) -> int:
    if not text:
        return 0

    text = text.lower()

    if "4k" in text:
        return 2160
    elif "1080" in text or "fullhd" in text:
        return 1080
    elif "720" in text:
        return 720
    elif "480" in text:
        return 480

    return 0


def is_stream_url(url: str) -> bool:
    if not url:
        return False

    url = url.lower()

    stream_extensions = [
        ".m3u8",
        ".ts",
        ".mp4",
        ".mpd",
        ".flv",
        ".avi"
    ]

    return any(ext in url for ext in stream_extensions)


def safe_int(value, default=0):
    try:
        return int(value)
    except:
        return default
