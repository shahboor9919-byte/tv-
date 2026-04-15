from typing import List, Dict, Any
import re

class Classifier:
    def __init__(self, config: dict):
        self.categories = config['categories']
        # Compile regex patterns for efficiency if needed (keywords are simple strings)
        self.compiled = {}
        for cat, rules in self.categories.items():
            patterns = []
            for kw in rules['keywords']:
                # Escape for regex, allow word boundaries?
                patterns.append(re.escape(kw))
            if patterns:
                self.compiled[cat] = re.compile('|'.join(patterns), re.IGNORECASE)

    def classify(self, streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for stream in streams:
            text = f"{stream.get('name', '')} {stream.get('group', '')} {stream.get('tvg_id', '')}"
            assigned = []
            for cat, pattern in self.compiled.items():
                if pattern.search(text):
                    assigned.append(cat)
            if not assigned:
                assigned.append('international')
            stream['categories'] = assigned
        return streams
