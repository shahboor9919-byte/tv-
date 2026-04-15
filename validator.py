import asyncio
import aiohttp
import time
import hashlib
from typing import List, Dict, Any, Optional, Set
from utils.logger import logger
from cache.stream_cache import StreamCache

class Validator:
    def __init__(self, config: dict, cache: Optional[StreamCache] = None):
        self.config = config
        self.timeout = config['validation']['timeout']
        self.parallel = config['validation']['parallel_checks']
        self.check_method = config['validation'].get('check_method', 'head')
        self.fast_mode = config.get('fast_mode', False)
        self.max_latency = config['validation'].get('max_latency', 3.0)
        self.cache = cache or StreamCache(config.get('cache_file', 'stream_cache.json'))
        self.blacklisted_domains: Set[str] = set(config.get('blacklist', {}).get('domains', []))
        # Exponential backoff settings
        self.max_retries = 2
        self.base_backoff = 0.5

    async def validate_stream(self, session: aiohttp.ClientSession, stream: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a single stream with caching and retries."""
        if self.fast_mode:
            stream['status'] = 'fast_mode'
            stream['latency'] = -1
            stream['valid'] = True
            stream['content_type'] = ''
            return stream

        url = stream['url']
        # Check blacklist
        if any(domain in url for domain in self.blacklisted_domains):
            stream['status'] = 'blacklisted'
            stream['latency'] = self.timeout
            stream['valid'] = False
            stream['content_type'] = ''
            return stream

        # Check cache
        cached = await self.cache.get(url)
        if cached:
            stream.update(cached)
            logger.debug(f"Cache hit for {url[:60]}")
            return stream

        # Perform validation with retries
        for attempt in range(self.max_retries + 1):
            try:
                start = time.time()
                if self.check_method == 'head':
                    async with session.head(url, timeout=self.timeout, allow_redirects=True) as response:
                        latency = time.time() - start
                        stream['status'] = response.status
                        stream['latency'] = latency
                        stream['content_type'] = response.headers.get('Content-Type', '')
                        stream['valid'] = 200 <= response.status < 400
                        # Additional check: if it's a playlist, ensure it's not empty?
                        break
                else:
                    # GET with range to avoid downloading entire stream
                    headers = {'Range': 'bytes=0-1024'}
                    async with session.get(url, timeout=self.timeout, headers=headers) as response:
                        chunk = await response.content.read(1024)
                        latency = time.time() - start
                        stream['status'] = response.status
                        stream['latency'] = latency
                        stream['content_type'] = response.headers.get('Content-Type', '')
                        stream['valid'] = 200 <= response.status < 400 and len(chunk) > 0
                        break
            except asyncio.TimeoutError:
                stream['status'] = 'timeout'
                stream['latency'] = self.timeout
                stream['valid'] = False
                stream['content_type'] = ''
            except (aiohttp.ClientError, ConnectionRefusedError, OSError) as e:
                stream['status'] = f'error: {type(e).__name__}'
                stream['latency'] = self.timeout
                stream['valid'] = False
                stream['content_type'] = ''
            except Exception as e:
                logger.error(f"Unexpected error validating {url}: {e}")
                stream['status'] = 'unknown_error'
                stream['latency'] = self.timeout
                stream['valid'] = False
                stream['content_type'] = ''
                break

            if attempt < self.max_retries:
                backoff = self.base_backoff * (2 ** attempt)
                await asyncio.sleep(backoff)

        # Cache result (even failures for TTL to avoid rechecking dead links)
        cache_data = {
            'status': stream['status'],
            'latency': stream['latency'],
            'valid': stream['valid'],
            'content_type': stream['content_type']
        }
        await self.cache.set(url, cache_data)
        return stream

    async def validate_all(self, streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate all streams with early filtering of known dead."""
        if self.fast_mode:
            logger.info("Fast mode: skipping actual validation")
            return [await self.validate_stream(None, s) for s in streams]

        # Pre-filter using cache to identify already known dead (but still validate to update TTL)
        # We'll still validate all, but cache will short-circuit actual requests.
        semaphore = asyncio.Semaphore(self.parallel)
        async def validate_with_limit(session, stream):
            async with semaphore:
                return await self.validate_stream(session, stream)

        connector = aiohttp.TCPConnector(limit=self.parallel, force_close=True, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [validate_with_limit(session, s) for s in streams]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle exceptions from gather
        final_results = []
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Validation task failed: {res}")
                # Create a dummy failed stream
                final_results.append({'valid': False, 'status': 'task_failure'})
            else:
                final_results.append(res)

        valid_count = sum(1 for r in final_results if r.get('valid', False))
        logger.info(f"Validation complete: {valid_count}/{len(final_results)} streams alive")
        return final_results
