from typing import List, Dict, Any, Tuple, Optional
from rapidfuzz import fuzz
from utils.helpers import get_url_hash, normalize_channel_name, extract_resolution
from utils.logger import logger
from collections import defaultdict

class Deduplicator:
    def __init__(self, similarity_threshold: int = 88):
        self.threshold = similarity_threshold

    def deduplicate(self, streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicates: first exact URL, then fuzzy name within groups."""
        if not streams:
            return []

        # Pass 1: Exact URL dedup (keep first occurrence)
        url_seen = set()
        url_unique = []
        for s in streams:
            url_hash = get_url_hash(s['url'])
            if url_hash not in url_seen:
                url_seen.add(url_hash)
                url_unique.append(s)

        logger.info(f"URL dedup: {len(streams)} -> {len(url_unique)} streams")

        # Pass 2: Fuzzy name dedup by grouping on normalized name base
        # First, group by a rough key to avoid O(N^2) global comparisons
        rough_groups = defaultdict(list)
        for s in url_unique:
            norm = s.get('normalized_name') or normalize_channel_name(s['name'])
            # Use first few chars as rough bucket
            key = norm[:3] if len(norm) >= 3 else norm
            rough_groups[key].append(s)

        final_streams = []
        for key, group in rough_groups.items():
            if len(group) <= 1:
                final_streams.extend(group)
                continue

            # Within each rough group, find clusters of similar names
            clustered = self._cluster_similar_names(group)
            for cluster in clustered:
                # Keep the best stream from each cluster based on resolution hint and URL format
                best = self._select_best_stream(cluster)
                final_streams.append(best)

        logger.info(f"Fuzzy dedup: {len(url_unique)} -> {len(final_streams)} streams")
        return final_streams

    def _cluster_similar_names(self, streams: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Cluster streams by name similarity using a greedy approach."""
        clusters = []
        used = set()

        for i, s1 in enumerate(streams):
            if i in used:
                continue
            cluster = [s1]
            used.add(i)
            name1 = s1.get('normalized_name') or normalize_channel_name(s1['name'])
            for j, s2 in enumerate(streams[i+1:], start=i+1):
                if j in used:
                    continue
                name2 = s2.get('normalized_name') or normalize_channel_name(s2['name'])
                similarity = fuzz.ratio(name1, name2)
                if similarity >= self.threshold:
                    cluster.append(s2)
                    used.add(j)
            clusters.append(cluster)
        return clusters

    def _select_best_stream(self, cluster: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Select the best stream from a cluster of duplicates based on heuristics."""
        if len(cluster) == 1:
            return cluster[0]

        # Score each stream based on resolution hint and format preference
        def quality_score(stream: Dict[str, Any]) -> int:
            score = 0
            text = f"{stream.get('name', '')} {stream.get('group', '')}".lower()
            res = extract_resolution(text)
            res_map = {'4k': 40, 'fhd': 30, 'hd': 20, 'sd': 10, 'unknown': 0}
            score += res_map.get(res, 0)

            url = stream['url'].lower()
            if '.m3u8' in url:
                score += 20
            elif '.ts' in url:
                score += 10
            return score

        # Sort descending by quality score
        cluster.sort(key=quality_score, reverse=True)
        best = cluster[0]
        logger.debug(f"Selected best from cluster: '{best['name']}' (score: {quality_score(best)})")
        return best
