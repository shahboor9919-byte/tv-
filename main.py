#!/usr/bin/env python3
import asyncio
import yaml
import argparse
import time
from pathlib import Path

from utils.logger import logger
from pipeline.fetcher import Fetcher
from pipeline.parser import M3UParser
from pipeline.deduplicator import Deduplicator
from pipeline.validator import Validator
from pipeline.scorer import Scorer
from pipeline.classifier import Classifier
from pipeline.ranker import Ranker
from pipeline.writer import M3UWriter

async def run_pipeline(config_path: str):
    """Execute the full IPTV aggregation pipeline."""
    start_time = time.time()
    logger.info("Starting IPTV aggregation pipeline")
    
    # Load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
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
    
    # Stage 4: Validate
    logger.info("Stage 4: Validation")
    validator = Validator(config)
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
    
    # Stage 8: Output
    logger.info("Stage 8: Generating playlists")
    writer = M3UWriter(config)
    writer.generate_all(streams)
    
    elapsed = time.time() - start_time
    logger.info(f"Pipeline completed in {elapsed:.2f} seconds. Final streams: {len(streams)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PRO IPTV Aggregation Engine")
    parser.add_argument("--config", "-c", default="config.yaml", help="Path to config file")
    parser.add_argument("--fast", action="store_true", help="Enable fast mode (skip validation)")
    args = parser.parse_args()
    
    # Override config fast_mode if flag provided
    if args.fast:
        import yaml
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
        config['fast_mode'] = True
        with open(args.config, 'w') as f:
            yaml.dump(config, f)
    
    asyncio.run(run_pipeline(args.config))
