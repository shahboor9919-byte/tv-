#!/usr/bin/env python3
import asyncio
import yaml
import argparse
import time
from pathlib import Path

from utils.logger import setup_logger  # ✅ استيراد الدالة بدلاً من المتغير

# ✅ إنشاء كائن logger
logger = setup_logger()

from pipeline.fetcher import Fetcher
from pipeline.parser import M3UParser
from pipeline.deduplicator import Deduplicator
from pipeline.validator import Validator
from pipeline.scorer import Scorer
from pipeline.classifier import Classifier
from pipeline.ranker import Ranker
from pipeline.writer import M3UWriter
from cache.stream_cache import StreamCache

async def run_pipeline(config_path: str):
    start_time = time.time()
    logger.info("Starting IPTV aggregation pipeline")

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Initialize cache
    cache = StreamCache(config.get('cache_file', 'stream_cache.db'), ttl_hours=24)

    # Stage 1: Fetch
    logger.info("Stage 1: Fetching sources")
    fetcher = Fetcher(config)
    sources_content = await fetcher.fetch_all()

    # Stage 2: Parse
    logger.info("Stage 2: Parsing M3U")
    parser = M3UParser()
    all_streams = []
    for source_url, content in sources_content:
        if content:
            streams = parser.parse(content, source_url)
            all_streams.extend(streams)
    logger.info(f"Total parsed streams: {len(all_streams)}")
    if not all_streams:
        logger.error("No streams parsed, exiting")
        return

    # Stage 3: Deduplicate
    logger.info("Stage 3: Deduplication")
    dedup = Deduplicator()
    streams = dedup.deduplicate(all_streams)

    # Stage 4: Validate (with caching)
    logger.info("Stage 4: Validation")
    validator = Validator(config, cache=cache)
    streams = await validator.validate_all(streams)

    # Stage 5: Score
    logger.info("Stage 5: Scoring")
    scorer = Scorer(config)
    streams = scorer.score_all(streams)

    # Stage 6: Classify
    logger.info("Stage 6: Classification")
    classifier = Classifier(config)
    streams = classifier.classify(streams)

    # Stage 7: Rank
    logger.info("Stage 7: Ranking")
    ranker = Ranker(config)
    streams = ranker.rank(streams)

    # Stage 8: Output (async write)
    logger.info("Stage 8: Generating playlists")
    writer = M3UWriter(config)
    await writer.generate_all(streams)

    # Cleanup cache expired entries (background)
    asyncio.create_task(cache.cleanup_expired())

    elapsed = time.time() - start_time
    logger.info(f"Pipeline completed in {elapsed:.2f}s. Final streams: {len(streams)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", default="config.yaml")
    parser.add_argument("--fast", action="store_true")
    args = parser.parse_args()
    if args.fast:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
        config['fast_mode'] = True
        with open(args.config, 'w') as f:
            yaml.dump(config, f)
    asyncio.run(run_pipeline(args.config))
