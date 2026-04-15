import asyncio
import aiohttp
from typing import List, Tuple
from utils.logger import logger

class Fetcher:
    def __init__(self, config: dict):
        self.config = config
        self.sources = [s for s in config['sources'] if s.get('enabled', True)]
        self.timeout = config['fetch']['timeout']
        self.retries = config['fetch']['retries']
        self.retry_delay = config['fetch']['retry_delay']
        self.parallel = config['fetch']['parallel_requests']
    
    async def fetch_source(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, str]:
        """Fetch a single source with retries."""
        for attempt in range(self.retries):
            try:
                async with session.get(url, timeout=self.timeout) as response:
                    if response.status == 200:
                        content = await response.text()
                        logger.info(f"Fetched {url} ({len(content)} bytes)")
                        return url, content
                    else:
                        logger.warning(f"Attempt {attempt+1}/{self.retries} for {url} returned {response.status}")
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching {url} (attempt {attempt+1})")
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
            
            if attempt < self.retries - 1:
                await asyncio.sleep(self.retry_delay * (attempt + 1))
        
        logger.error(f"Failed to fetch {url} after {self.retries} attempts")
        return url, ""
    
    async def fetch_all(self) -> List[Tuple[str, str]]:
        """Fetch all sources in parallel with concurrency limit."""
        semaphore = asyncio.Semaphore(self.parallel)
        
        async def fetch_with_limit(url):
            async with semaphore:
                async with aiohttp.ClientSession() as session:
                    return await self.fetch_source(session, url)
        
        tasks = [fetch_with_limit(s['url']) for s in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        valid_results = []
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Fetch task failed: {res}")
            else:
                valid_results.append(res)
        return valid_results
