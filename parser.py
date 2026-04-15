import re
import asyncio
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
from utils.logger import logger
from utils.helpers import is_valid_url, normalize_channel_name

class M3UParser:
    MAX_LINES = 500_000          # Safety limit
    MAX_STREAM_NAME_LENGTH = 200
    ALLOWED_SCHEMES = {'http', 'https', 'rtmp', 'rtsp', 'mms', 'udp'}

    def __init__(self, source_url: str = ""):
        self.source_url = source_url

    def parse(self, content: str, source_url: str = "") -> List[Dict[str, Any]]:
        """
        Parse M3U content with safeguards against malformed or oversized input.
        Returns list of stream dicts.
        """
        if not content:
            logger.warning(f"Empty content from {source_url}")
            return []

        lines = content.splitlines()
        if len(lines) > self.MAX_LINES:
            logger.error(f"Source {source_url} exceeds max lines ({len(lines)}), truncating")
            lines = lines[:self.MAX_LINES]

        streams = []
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith('#EXTINF:'):
                metadata = self._parse_extinf(line)
                i += 1
                # Skip any comment lines before URL
                while i < len(lines) and lines[i].strip().startswith('#'):
                    i += 1
                if i < len(lines):
                    url = lines[i].strip()
                    # Handle relative URLs
                    if url and not url.startswith(('http://', 'https://')):
                        if source_url:
                            url = urljoin(source_url, url)
                        else:
                            url = ""
                    if url and self._is_valid_stream_url(url):
                        stream = self._create_stream_dict(metadata, url, source_url, line)
                        streams.append(stream)
                    else:
                        logger.debug(f"Skipping invalid URL: {url[:100]}")
            i += 1

        logger.info(f"Parsed {len(streams)} valid streams from {source_url}")
        return streams

    def _parse_extinf(self, line: str) -> Dict[str, str]:
        """Extract attributes with proper unescaping."""
        attrs = {}
        # Match key="value" pairs (value may contain escaped quotes)
        pattern = r'([a-zA-Z-]+)="((?:[^"\\]|\\.)*)"'
        for key, value in re.findall(pattern, line):
            # Unescape any escaped quotes
            value = value.replace('\\"', '"')
            attrs[key] = value

        # Extract channel name (everything after the first unquoted comma)
        # Handle case where name contains commas by only splitting on first comma not inside quotes
        parts = self._split_on_first_comma(line)
        if len(parts) > 1:
            attrs['name'] = parts[1].strip()
        else:
            attrs['name'] = 'Unknown'
        return attrs

    def _split_on_first_comma(self, line: str) -> List[str]:
        """Split EXTINF line on first comma not inside quotes."""
        in_quotes = False
        for i, ch in enumerate(line):
            if ch == '"':
                in_quotes = not in_quotes
            elif ch == ',' and not in_quotes:
                return [line[:i], line[i+1:]]
        return [line, '']

    def _is_valid_stream_url(self, url: str) -> bool:
        """Validate URL scheme and basic format."""
        from urllib.parse import urlparse
        try:
            parsed = urlparse(url)
            return parsed.scheme in self.ALLOWED_SCHEMES and bool(parsed.netloc)
        except:
            return False

    def _create_stream_dict(self, metadata: Dict[str, str], url: str, source_url: str, raw_extinf: str) -> Dict[str, Any]:
        """Build standardized stream dictionary."""
        name = metadata.get('name', 'Unknown')
        if len(name) > self.MAX_STREAM_NAME_LENGTH:
            name = name[:self.MAX_STREAM_NAME_LENGTH]
        return {
            'name': name,
            'url': url,
            'group': metadata.get('group-title', ''),
            'tvg_id': metadata.get('tvg-id', ''),
            'tvg_logo': metadata.get('tvg-logo', ''),
            'tvg_chno': metadata.get('tvg-chno', ''),
            'source': source_url,
            'raw_metadata': raw_extinf,
            'normalized_name': normalize_channel_name(name)
        }
