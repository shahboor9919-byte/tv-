import os
from typing import List, Dict, Any
from utils.logger import logger

class M3UWriter:
    def __init__(self, config: dict):
        self.config = config['output']
        self.base_dir = self.config['base_dir']
        os.makedirs(self.base_dir, exist_ok=True)
    
    def write_playlist(self, filename: str, streams: List[Dict[str, Any]], group_by_category: bool = True):
        """Write streams to an M3U file."""
        path = os.path.join(self.base_dir, filename)
        with open(path, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            
            if group_by_category:
                # Group by category
                by_cat = {}
                for s in streams:
                    for cat in s.get('categories', ['international']):
                        if cat not in by_cat:
                            by_cat[cat] = []
                        by_cat[cat].append(s)
                
                for cat, cat_streams in by_cat.items():
                    f.write(f"#EXTGRP:{cat}\n")
                    for s in cat_streams:
                        self._write_entry(f, s, cat)
            else:
                for s in streams:
                    self._write_entry(f, s, s.get('group', ''))
        
        logger.info(f"Written {len(streams)} streams to {path}")
    
    def _write_entry(self, f, stream: Dict[str, Any], group: str):
        """Write a single M3U entry."""
        # Build EXTINF line
        attrs = []
        if stream.get('tvg_id'):
            attrs.append(f'tvg-id="{stream["tvg_id"]}"')
        if stream.get('tvg_logo'):
            attrs.append(f'tvg-logo="{stream["tvg_logo"]}"')
        attrs.append(f'group-title="{group}"')
        attr_str = ' '.join(attrs)
        
        name = stream.get('name', 'Unknown')
        f.write(f'#EXTINF:-1 {attr_str},{name}\n')
        f.write(f'{stream["url"]}\n')
    
    def generate_all(self, streams: List[Dict[str, Any]]):
        """Generate all configured playlist files."""
        files_config = self.config['files']
        
        # All streams (already ranked)
        self.write_playlist(files_config['all'], streams)
        
        # Category-specific playlists
        categories = {
            'arabic': lambda s: 'arabic' in s.get('categories', []),
            'sports': lambda s: 'sports' in s.get('categories', []),
        }
        
        for cat, filter_func in categories.items():
            if cat in files_config:
                filtered = [s for s in streams if filter_func(s)]
                self.write_playlist(files_config[cat], filtered)
        
        # Premium-like: top scored streams across all categories
        premium = streams[:500]  # Take top 500 by score
        self.write_playlist(files_config['premium'], premium)
