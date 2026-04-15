from typing import List, Dict, Any
from collections import defaultdict
from utils.helpers import normalize_channel_name

class Ranker:
    def __init__(self, config: dict):
        self.top_per_channel = config['ranking']['top_per_channel']
        self.min_score = config['ranking']['min_score']
    
    def rank(self, streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sort streams by score within each channel and keep top N."""
        # Group by normalized channel name
        channels = defaultdict(list)
        for s in streams:
            if s.get('score', 0) >= self.min_score:
                norm_name = normalize_channel_name(s['name'])
                channels[norm_name].append(s)
        
        ranked = []
        for norm_name, channel_streams in channels.items():
            # Sort by score descending
            channel_streams.sort(key=lambda x: x.get('score', 0), reverse=True)
            # Keep top N
            ranked.extend(channel_streams[:self.top_per_channel])
        
        # Also sort overall by score for the 'all' playlist
        ranked.sort(key=lambda x: x.get('score', 0), reverse=True)
        return ranked
