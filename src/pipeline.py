#!/usr/bin/env python3
"""
CETESB CADRI Scraping Pipeline

Main orchestrator for data collection:
1. Search companies by seed (razao social)
2. Scrape detail pages for CADRI documents and extract URLs

Note: PDF download and parsing are handled by separate modules
"""

import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime
import signal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from config import CSV_DIR, RESUME_ENABLED, CHECKPOINT_INTERVAL
from store_csv import CSVSchemas, CSVStore
from seeds import SeedManager, AdaptiveSearchStrategy
from scrape_list import ListScraper
from scrape_detail import DetailScraper
from logging_conf import logger, metrics, setup_logging


class Pipeline:
    """Main pipeline orchestrator"""

    def __init__(self, resume: bool = True):
        self.resume = resume
        self.seed_manager = SeedManager()
        self.adaptive_strategy = AdaptiveSearchStrategy(self.seed_manager)
        self.checkpoint_counter = 0
        self.running = True

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal, stopping gracefully...")
        self.running = False

    def checkpoint(self):
        """Save checkpoint for resume capability"""
        self.checkpoint_counter += 1

        if self.checkpoint_counter % CHECKPOINT_INTERVAL == 0:
            self.seed_manager.save_state()
            logger.info(f"Checkpoint saved at {self.checkpoint_counter} operations")

    async def stage_list(self, seeds: list = None, max_seeds: int = 10):
        """Stage 1: Search for companies and get detail page URLs"""
        logger.info("=== Starting Stage 1: List Scraping ===")

        if seeds:
            # Use provided seeds
            seed_list = seeds
        else:
            # Get seeds from manager
            seed_list = self.adaptive_strategy.get_adaptive_seeds(max_seeds)

        if not seed_list:
            logger.warning("No seeds available for search")
            return []

        scraper = ListScraper(self.seed_manager)
        all_urls = []

        for seed in seed_list:
            if not self.running:
                break

            results = await scraper.search_by_razao_social(seed)
            urls = [r['url'] for r in results]
            all_urls.extend(urls)

            # Log performance for adaptive strategy
            self.adaptive_strategy.log_performance(
                seed, len(results), 10.0  # placeholder time
            )

            self.checkpoint()

        # Save URLs for next stage with optimization info
        if all_urls:
            urls_file = CSV_DIR / "pending_urls.csv"
            import pandas as pd

            # Convert to DataFrame with additional fields for optimization
            urls_data = []
            for url_info in all_urls:
                if isinstance(url_info, dict):
                    urls_data.append(url_info)
                else:
                    # Convert string URL to dict
                    urls_data.append({'url': url_info})

            df_urls = pd.DataFrame(urls_data)

            # Ensure required columns exist
            if 'url' not in df_urls.columns:
                df_urls['url'] = df_urls.get('url', '')

            df_urls.to_csv(urls_file, index=False)

            enhanced_count = len([u for u in urls_data if u.get('skip_detail_stage')])
            logger.info(f"Saved {len(all_urls)} URLs for processing")
            if enhanced_count > 0:
                logger.info(f"  - {enhanced_count} URLs have complete data (will skip detail stage)")
                logger.info(f"  - {len(all_urls) - enhanced_count} URLs need detail scraping")

        return all_urls

    def stage_detail(self, urls: list = None):
        """Stage 2: Scrape detail pages for CADRI documents (optimized)"""
        logger.info("=== Starting Stage 2: Detail Scraping (Optimized) ===")

        if urls is None:
            # Load from saved file
            urls_file = CSV_DIR / "pending_urls.csv"
            if urls_file.exists():
                import pandas as pd
                df = pd.read_csv(urls_file)

                # Check if we have enhanced extraction results
                if 'skip_detail_stage' in df.columns:
                    # Filter URLs that need detail scraping
                    urls_to_process = df[df.get('skip_detail_stage', False) != True]['url'].tolist()
                    skipped_count = len(df) - len(urls_to_process)

                    if skipped_count > 0:
                        logger.info(f"Skipping {skipped_count} URLs that already have complete data")

                    urls = urls_to_process
                else:
                    urls = df['url'].tolist()
            else:
                logger.warning("No URLs to process")
                return 0

        if not urls:
            logger.info("All URLs already processed with enhanced extraction - skipping detail stage")
            return 0

        logger.info(f"Processing {len(urls)} URLs that need detail scraping")

        with DetailScraper() as scraper:
            total_docs = scraper.process_url_list(urls)

        self.checkpoint()
        return total_docs


    async def run_all(self, max_iterations: int = 5):
        """Run data collection pipeline (list + detail only)"""
        logger.info("=== Starting Data Collection Pipeline ===")
        logger.info("Note: PDF download and parsing are handled separately")
        CSVSchemas.init_all()

        for iteration in range(1, max_iterations + 1):
            if not self.running:
                break

            logger.info(f"\n--- Iteration {iteration}/{max_iterations} ---")

            # Stage 1: List scraping
            urls = await self.stage_list()

            if not urls:
                logger.info("No more URLs found, stopping")
                break

            # Stage 2: Detail scraping
            docs = self.stage_detail(urls)

            # Show metrics
            logger.info(f"\nIteration {iteration} complete:")
            logger.info(f"  URLs found: {len(urls)}")
            logger.info(f"  Documents found: {docs}")
            logger.info(f"  {metrics.get_summary()}")

            # Check if we should continue
            if docs == 0:
                logger.info("No new documents found, stopping")
                break

        logger.info("\n=== Data Collection Complete ===")
        logger.info(metrics.get_summary())
        logger.info("\n📊 Data collected successfully!")
        logger.info("📁 Results saved in:")
        logger.info("   - data/csv/empresas.csv")
        logger.info("   - data/csv/cadri_documentos.csv")
        logger.info("\n⚠️  Next steps:")
        logger.info("   1. Use interactive_pdf_downloader.py to download PDFs")
        logger.info("   2. Use pdf_parser_standalone.py to parse downloaded PDFs")

    async def run_stage(self, stage: str, **kwargs):
        """Run a specific stage"""
        CSVSchemas.init_all()

        if stage == 'list':
            seeds = kwargs.get('seeds', [])
            if isinstance(seeds, str):
                seeds = [s.strip() for s in seeds.split(',')]
            await self.stage_list(seeds)

        elif stage == 'detail':
            self.stage_detail()

        elif stage == 'all':
            max_iter = kwargs.get('max_iterations', 5)
            await self.run_all(max_iter)

        else:
            logger.error(f"Unknown stage: {stage}")
            sys.exit(1)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='CETESB CADRI Data Collection Pipeline')

    parser.add_argument(
        '--stage',
        choices=['list', 'detail', 'all'],
        default='all',
        help='Pipeline stage to run (list, detail, or all)'
    )

    parser.add_argument(
        '--seeds',
        type=str,
        help='Comma-separated seeds for list stage (e.g., CEM,ACE,AGR)'
    )

    parser.add_argument(
        '--max-iterations',
        type=int,
        default=5,
        help='Maximum iterations for complete pipeline'
    )

    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh, ignore saved state'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level'
    )

    parser.add_argument(
        '--reset-seeds',
        action='store_true',
        help='Reset seed manager state'
    )

    args = parser.parse_args()

    # Setup logging
    logger_instance, _ = setup_logging(level=args.log_level)

    # Print header
    print("\n" + "="*60)
    print("CETESB CADRI Data Collection Pipeline")
    print(f"Stage: {args.stage}")
    print(f"Resume: {not args.no_resume}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")

    # Create pipeline
    pipeline = Pipeline(resume=not args.no_resume)

    # Reset seeds if requested
    if args.reset_seeds:
        pipeline.seed_manager.reset_queue()
        logger.info("Seed state reset")

    # Run pipeline
    try:
        asyncio.run(
            pipeline.run_stage(
                args.stage,
                seeds=args.seeds,
                max_iterations=args.max_iterations
            )
        )
    except KeyboardInterrupt:
        logger.info("\nPipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        sys.exit(1)

    print("\n" + "="*60)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()