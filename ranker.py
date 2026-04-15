from typing import List, Dict, Any
from collections import defaultdict
from utils.helpers import normalize_channel_name

class Ranker:
    def __init__(self, config: dict):
        self.top_per_channel = config['ranking']['top_per_channel']
        self.min_score = config['ranking']['min_score']
        # Optional: alias mapping for channel name normalization (e.g., "BBC One HD" -> "BBC One")
        self.name_aliases = config.get('aliases', {})

    def rank(self, streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Group by channel (using normalized name with aliases) and keep top N by score."""
        # Filter by min_score
        qualified = [s for s in streams if s.get('score', 0) >= self.min_score]

        # Group by normalized name, applying aliases
        channels = defaultdict(list)
        for s in qualified:
            norm = s.get('normalized_name') or normalize_channel_name(s['name'])
            # Apply alias mapping if exists
            canonical = self.name_aliases.get(norm, norm)
            channels[canonical].append(s)

        ranked = []
        for canonical_name, channel_streams in channels.items():
            # Sort descending by score
            channel_streams.sort(key=lambda x: x.get('score', 0), reverse=True)
            # Keep top N
            ranked.extend(channel_streams[:self.top_per_channel])

        # Final sort by score for 'all' playlist
        ranked.sort(key=lambda x: x.get('score', 0), reverse=True)
        return ranked
