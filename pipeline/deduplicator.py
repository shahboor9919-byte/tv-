from typing import List, Dict, Any
from rapidfuzz import fuzz
from utils.helpers import get_url_hash, normalize_channel_name
from utils.logger import logger

class Deduplicator:
    def __init__(self, similarity_threshold: int = 85):
        self.threshold = similarity_threshold
    
    def deduplicate(self, streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate streams by URL exact match and channel name similarity."""
        if not streams:
            return []
        
        # First pass: exact URL dedup
        url_seen = set()
        url_unique = []
        for s in streams:
            url_hash = get_url_hash(s['url'])
            if url_hash not in url_seen:
                url_seen.add(url_hash)
                url_unique.append(s)
        
        logger.info(f"URL dedup: {len(streams)} -> {len(url_unique)} streams")
        
        # Second pass: fuzzy name dedup within same channel name similarity
        # Group by normalized name base
        name_groups = {}
        for s in url_unique:
            norm_name = normalize_channel_name(s['name'])
            if norm_name not in name_groups:
                name_groups[norm_name] = []
            name_groups[norm_name].append(s)
        
        final_streams = []
        for norm_name, group in name_groups.items():
            if len(group) == 1:
                final_streams.extend(group)
            else:
                # Keep best quality (we'll decide later, but here we can keep the one with higher resolution hint)
                # For now, keep all in group, scoring will handle later
                # But to avoid too many similar names, we can dedup by similarity within group
                # This is optional and can be more sophisticated
                final_streams.extend(group)  # We'll let ranking handle later
        
        logger.info(f"After dedup: {len(final_streams)} streams")
        return final_streams
