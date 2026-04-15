import re
from typing import List, Dict, Any
from utils.helpers import normalize_channel_name, is_valid_url
from utils.logger import logger

class M3UParser:
    def parse(self, content: str, source_url: str) -> List[Dict[str, Any]]:
        """Parse M3U content into stream entries."""
        streams = []
        lines = content.splitlines()
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith('#EXTINF:'):
                # Parse EXTINF line
                metadata = self._parse_extinf(line)
                # Next line should be URL
                i += 1
                if i < len(lines):
                    url = lines[i].strip()
                    if url and not url.startswith('#') and is_valid_url(url):
                        stream = {
                            'name': metadata.get('name', 'Unknown'),
                            'url': url,
                            'group': metadata.get('group-title', ''),
                            'tvg_id': metadata.get('tvg-id', ''),
                            'tvg_logo': metadata.get('tvg-logo', ''),
                            'source': source_url,
                            'raw_metadata': line
                        }
                        streams.append(stream)
            i += 1
        logger.info(f"Parsed {len(streams)} streams from {source_url}")
        return streams
    
    def _parse_extinf(self, line: str) -> Dict[str, str]:
        """Extract attributes from #EXTINF line."""
        # Format: #EXTINF:-1 tvg-id="xxx" group-title="xxx",Channel Name
        attrs = {}
        # Extract key="value" pairs
        pattern = r'([a-zA-Z-]+)="([^"]*)"'
        matches = re.findall(pattern, line)
        for key, value in matches:
            attrs[key] = value
        
        # Extract channel name (after last comma)
        parts = line.split(',', 1)
        if len(parts) > 1:
            attrs['name'] = parts[1].strip()
        else:
            attrs['name'] = 'Unknown'
        
        return attrs
