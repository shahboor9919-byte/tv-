import aiosqlite
import json
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import asyncio

class StreamCache:
    def __init__(self, db_path: str = "stream_cache.db", ttl_hours: int = 24):
        self.db_path = db_path
        self.ttl_seconds = ttl_hours * 3600
        self._init_lock = asyncio.Lock()
        self._initialized = False

    async def _ensure_table(self):
        async with self._init_lock:
            if self._initialized:
                return
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS stream_cache (
                        url TEXT PRIMARY KEY,
                        data TEXT,
                        timestamp REAL
                    )
                ''')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON stream_cache(timestamp)')
                await db.commit()
            self._initialized = True

    async def get(self, url: str) -> Optional[Dict[str, Any]]:
        await self._ensure_table()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT data, timestamp FROM stream_cache WHERE url = ?', (url,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    data_str, ts = row
                    if time.time() - ts < self.ttl_seconds:
                        return json.loads(data_str)
                    else:
                        # Expired, delete it asynchronously
                        asyncio.create_task(self._delete_expired(url))
        return None

    async def set(self, url: str, data: Dict[str, Any]):
        await self._ensure_table()
        async with aiosqlite.connect(self.db_path) as db:
            data_str = json.dumps(data)
            await db.execute(
                'INSERT OR REPLACE INTO stream_cache (url, data, timestamp) VALUES (?, ?, ?)',
                (url, data_str, time.time())
            )
            await db.commit()

    async def _delete_expired(self, url: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('DELETE FROM stream_cache WHERE url = ?', (url,))
            await db.commit()

    async def cleanup_expired(self):
        """Remove all expired entries."""
        await self._ensure_table()
        cutoff = time.time() - self.ttl_seconds
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('DELETE FROM stream_cache WHERE timestamp < ?', (cutoff,))
            await db.commit()
