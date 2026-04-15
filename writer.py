import os
import aiofiles
import asyncio
from typing import List, Dict, Any
from utils.logger import logger

class M3UWriter:
    def __init__(self, config: dict):
        self.config = config['output']
        self.base_dir = self.config['base_dir']
        os.makedirs(self.base_dir, exist_ok=True)

    async def write_playlist(self, filename: str, streams: List[Dict[str, Any]], group_by_category: bool = True):
        """Async write streams to M3U file."""
        path = os.path.join(self.base_dir, filename)
        async with aiofiles.open(path, 'w', encoding='utf-8') as f:
            await f.write("#EXTM3U\n")
            if group_by_category:
                by_cat = {}
                for s in streams:
                    for cat in s.get('categories', ['international']):
                        if cat not in by_cat:
                            by_cat[cat] = []
                        by_cat[cat].append(s)
                for cat, cat_streams in by_cat.items():
                    await f.write(f"#EXTGRP:{cat}\n")
                    for s in cat_streams:
                        await self._write_entry(f, s, cat)
            else:
                for s in streams:
                    await self._write_entry(f, s, s.get('group', ''))
        logger.info(f"Written {len(streams)} streams to {path}")

    async def _write_entry(self, f, stream: Dict[str, Any], group: str):
        """Write a single M3U entry."""
        attrs = []
        if stream.get('tvg_id'):
            attrs.append(f'tvg-id="{stream["tvg_id"]}"')
        if stream.get('tvg_logo'):
            attrs.append(f'tvg-logo="{stream["tvg_logo"]}"')
        if stream.get('tvg_chno'):
            attrs.append(f'tvg-chno="{stream["tvg_chno"]}"')
        attrs.append(f'group-title="{group}"')
        # Add Kodi property for logo if needed
        if stream.get('tvg_logo'):
            attrs.append(f'logo="{stream["tvg_logo"]}"')
        attr_str = ' '.join(attrs)
        name = stream.get('name', 'Unknown')
        await f.write(f'#EXTINF:-1 {attr_str},{name}\n')
        await f.write(f'{stream["url"]}\n')

    async def generate_all(self, streams: List[Dict[str, Any]]):
        """Generate all configured playlists concurrently."""
        tasks = []
        files_config = self.config['files']
        # All streams
        tasks.append(self.write_playlist(files_config['all'], streams))
        # Category playlists
        for cat in ['arabic', 'sports']:
            if cat in files_config:
                filtered = [s for s in streams if cat in s.get('categories', [])]
                tasks.append(self.write_playlist(files_config[cat], filtered))
        # Premium
        premium = streams[:500]
        tasks.append(self.write_playlist(files_config['premium'], premium))
        await asyncio.gather(*tasks)
