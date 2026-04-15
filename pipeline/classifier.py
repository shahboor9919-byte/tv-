from typing import List, Dict, Any

class Classifier:
    def __init__(self, config: dict):
        self.categories = config['categories']
    
    def classify(self, streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Assign categories to streams based on keyword matching."""
        for stream in streams:
            # Combine searchable text
            text = f"{stream.get('name', '')} {stream.get('group', '')} {stream.get('tvg_id', '')}".lower()
            assigned = []
            for cat, rules in self.categories.items():
                if any(kw.lower() in text for kw in rules['keywords']):
                    assigned.append(cat)
            if not assigned:
                assigned.append('international')  # fallback
            stream['categories'] = assigned
        
        return streams
