import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

class StreamCache:
    def __init__(self, cache_file: str = "stream_cache.json", ttl_hours: int = 24):
        self.cache_file = cache_file
        self.ttl = timedelta(hours=ttl_hours)
        self.cache = self.load()
    
    def load(self) -> Dict[str, Any]:
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save(self):
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f, indent=2)
    
    def get(self, url: str) -> Optional[Dict]:
        if url in self.cache:
            entry = self.cache[url]
            cached_time = datetime.fromisoformat(entry['timestamp'])
            if datetime.now() - cached_time < self.ttl:
                return entry['data']
        return None
    
    def set(self, url: str, data: Dict):
        self.cache[url] = {
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
