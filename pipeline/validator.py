import asyncio
import aiohttp
import time
from typing import List, Dict, Any, Tuple
from utils.logger import logger

class Validator:
    def __init__(self, config: dict):
        self.config = config
        self.timeout = config['validation']['timeout']
        self.parallel = config['validation']['parallel_checks']
        self.check_method = config['validation'].get('check_method', 'head')
        self.fast_mode = config.get('fast_mode', False)
    
    async def validate_stream(self, session: aiohttp.ClientSession, stream: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a single stream and add validation metrics."""
        if self.fast_mode:
            # In fast mode, assume all streams are valid
            stream['status'] = 'unknown'
            stream['latency'] = 0.0
            stream['valid'] = True
            stream['content_type'] = ''
            return stream
        
        url = stream['url']
        try:
            start = time.time()
            if self.check_method == 'head':
                async with session.head(url, timeout=self.timeout, allow_redirects=True) as response:
                    latency = time.time() - start
                    stream['status'] = response.status
                    stream['latency'] = latency
                    stream['content_type'] = response.headers.get('Content-Type', '')
                    stream['valid'] = response.status < 400
            else:
                # GET first few bytes
                async with session.get(url, timeout=self.timeout) as response:
                    await response.content.read(1024)  # Read a small chunk
                    latency = time.time() - start
                    stream['status'] = response.status
                    stream['latency'] = latency
                    stream['content_type'] = response.headers.get('Content-Type', '')
                    stream['valid'] = response.status < 400
        except asyncio.TimeoutError:
            stream['status'] = 'timeout'
            stream['latency'] = self.timeout
            stream['valid'] = False
            stream['content_type'] = ''
        except Exception as e:
            stream['status'] = f'error: {str(e)}'
            stream['latency'] = self.timeout
            stream['valid'] = False
            stream['content_type'] = ''
        
        return stream
    
    async def validate_all(self, streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate all streams with concurrency control."""
        if self.fast_mode:
            logger.info("Fast mode: skipping validation")
            return [await self.validate_stream(None, s) for s in streams]
        
        semaphore = asyncio.Semaphore(self.parallel)
        
        async def validate_with_limit(session, stream):
            async with semaphore:
                return await self.validate_stream(session, stream)
        
        connector = aiohttp.TCPConnector(limit=self.parallel, force_close=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [validate_with_limit(session, s) for s in streams]
            results = await asyncio.gather(*tasks)
        
        valid_count = sum(1 for r in results if r.get('valid', False))
        logger.info(f"Validation complete: {valid_count}/{len(results)} streams alive")
        return results
