import hashlib
from urllib.parse import urlparse
import re

def normalize_channel_name(name: str) -> str:
    """Clean and normalize channel names for comparison."""
    if not name:
        return ""
    # Remove special characters and extra spaces
    name = re.sub(r'[^\w\s-]', '', name.lower())
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def extract_resolution(text: str) -> str:
    """Extract resolution hint from text."""
    text = text.lower()
    if '4k' in text or '2160' in text or 'uhd' in text:
        return '4k'
    elif '1080' in text or 'fhd' in text or 'full hd' in text:
        return 'fhd'
    elif '720' in text or 'hd' in text:
        return 'hd'
    elif '480' in text or 'sd' in text:
        return 'sd'
    return 'unknown'

def get_url_hash(url: str) -> str:
    """Generate hash for URL deduplication."""
    return hashlib.md5(url.encode()).hexdigest()

def is_valid_url(url: str) -> bool:
    """Basic URL validation."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False
