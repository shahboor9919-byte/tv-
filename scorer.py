from typing import Dict, Any, List
from utils.helpers import extract_resolution
import math

class Scorer:
    def __init__(self, config: dict):
        self.config = config['scoring']
        self.speed_weight = self.config['speed_weight']
        self.resolution_weight = self.config['resolution_weight']
        self.stability_weight = self.config['stability_weight']
        self.format_weight = self.config['format_weight']
        self.res_boost = self.config['resolution_boost']
        self.format_boost = self.config['format_boost']
        self.penalty_slow = self.config['penalty_slow']
        self.penalty_unstable = self.config['penalty_unstable']
        self.max_latency = config['validation'].get('max_latency', 3.0)
        self.penalty_redirect = 5

    def score_stream(self, stream: Dict[str, Any]) -> float:
        """Compute quality score (0-100)."""
        if not stream.get('valid', False):
            # For fast mode or invalid, return 0
            return 0.0

        # Speed score
        latency = stream.get('latency', self.max_latency)
        if latency <= 0:
            speed_score = 100
        else:
            speed_score = max(0, 100 * (1 - (latency / self.max_latency)))

        # Resolution score
        text = f"{stream.get('name', '')} {stream.get('group', '')} {stream.get('tvg_id', '')}"
        res = extract_resolution(text)
        res_score = self.res_boost.get(res, 0)

        # Stability score based on status code
        status = stream.get('status')
        if status == 200:
            stability_score = 100
        elif isinstance(status, int) and 200 <= status < 300:
            stability_score = 80  # Redirects etc
        else:
            stability_score = 0

        # Format score
        url = stream['url'].lower()
        if '.m3u8' in url:
            format_score = self.format_boost['m3u8']
        elif '.ts' in url:
            format_score = self.format_boost['ts']
        else:
            format_score = self.format_boost['other']

        # Combine weighted
        score = (speed_score * self.speed_weight +
                 res_score * self.resolution_weight +
                 stability_score * self.stability_weight +
                 format_score * self.format_weight)

        # Penalties
        if latency > self.max_latency:
            score -= self.penalty_slow
        if status and isinstance(status, int) and status >= 300:
            score -= self.penalty_redirect
        if not stream.get('valid', False):
            score -= self.penalty_unstable

        return max(0, min(100, score))

    def score_all(self, streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for s in streams:
            s['score'] = self.score_stream(s)
        return streams
