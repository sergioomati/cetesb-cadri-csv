#!/usr/bin/env python3
"""
CETESB CADRI Scraping Pipeline

Main orchestrator for complete data collection:
1. Search companies by seed (razao social)
2. Scrape detail pages for CADRI documents and extract URLs
3. Download PDFs using direct method and interactive fallback
4. Parse PDFs to extract waste data
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

    async def stage_list(self, seeds: list = None, cnpjs: list = None, max_seeds: int = 10):
        """Stage 1: Search for companies and get detail page URLs"""
        logger.info("=== Starting Stage 1: List Scraping ===")

        scraper = ListScraper(self.seed_manager)
        all_urls = []

        if cnpjs:
            # Use CNPJ list (priority over seeds)
            logger.info(f"Searching by {len(cnpjs)} CNPJs")
            search_list = cnpjs
            search_method = 'cnpj'
        elif seeds:
            # Use provided seeds
            logger.info(f"Searching by {len(seeds)} seeds")
            search_list = seeds
            search_method = 'seed'
        else:
            # Get seeds from manager
            search_list = self.adaptive_strategy.get_adaptive_seeds(max_seeds)
            search_method = 'seed'

        if not search_list:
            logger.warning("No search terms available")
            return []

        for search_term in search_list:
            if not self.running:
                break

            if search_method == 'cnpj':
                results = await scraper.search_by_cnpj(search_term)
            else:
                results = await scraper.search_by_razao_social(search_term)

            # Store original results for CSV (with metadata)
            # But extract URLs for detail scraping pipeline
            for r in results:
                if isinstance(r, dict):
                    all_urls.append(r)  # Keep full dict for CSV
                else:
                    all_urls.append({'url': r})  # Convert string to dict for CSV

            # Log performance for adaptive strategy (only for seeds)
            if search_method == 'seed':
                self.adaptive_strategy.log_performance(
                    search_term, len(results), 10.0  # placeholder time
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

        # Extract URL strings from dicts if needed (for CNPJ search results)
        url_strings = []
        for url_item in urls:
            if isinstance(url_item, dict):
                url_strings.append(url_item.get('url', url_item))
            else:
                url_strings.append(url_item)

        with DetailScraper() as scraper:
            total_docs = scraper.process_url_list(url_strings)

        self.checkpoint()
        return total_docs

    async def stage_pdf(self, doc_type: str = None):
        """Stage 3: Download PDFs using direct method with interactive fallback"""
        logger.info("=== Starting Stage 3: PDF Download ===")

        # Import PDF downloaders
        from pathlib import Path
        sys.path.append(str(Path(__file__).parent.parent))

        try:
            from cert_mov_direct_downloader import CertMovDirectDownloader
            from interactive_pdf_downloader import InteractivePDFDownloader
        except ImportError as e:
            logger.error(f"Could not import PDF downloaders: {e}")
            return {"success": 0, "failed": 0}

        total_stats = {"success": 0, "failed": 0, "skipped": 0, "already_exists": 0}

        # First: Try direct downloads for CERT MOV RESIDUOS INT AMB
        if not doc_type or doc_type == "CERT MOV RESIDUOS INT AMB":
            logger.info("Attempting direct downloads for CERT MOV RESIDUOS INT AMB...")
            async with CertMovDirectDownloader() as direct_downloader:
                direct_stats = await direct_downloader.download_cert_mov_documents()

                # Merge stats
                for key in total_stats:
                    total_stats[key] += direct_stats.get(key, 0)

                logger.info(f"Direct download stats: {direct_stats}")

        # Second: Use interactive downloader for failures
        logger.info("Using interactive downloader for failed documents...")
        async with InteractivePDFDownloader() as interactive_downloader:
            interactive_stats = await interactive_downloader.download_failed_documents(doc_type)

            # Merge stats
            for key in total_stats:
                if key in interactive_stats:
                    total_stats[key] += interactive_stats[key]

            logger.info(f"Interactive download stats: {interactive_stats}")

        logger.info(f"Total PDF download stats: {total_stats}")
        self.checkpoint()
        return total_stats

    def stage_parse(self, force_reparse: bool = False):
        """Stage 4: Parse downloaded PDFs to extract waste data"""
        logger.info("=== Starting Stage 4: PDF Parsing ===")

        # Import PDF parser
        from pathlib import Path
        sys.path.append(str(Path(__file__).parent.parent))

        try:
            from pdf_parser_standalone import PDFParser
        except ImportError as e:
            logger.error(f"Could not import PDF parser: {e}")
            return {"parsed": 0, "failed": 0}

        with PDFParser() as parser:
            stats = parser.parse_all_pdfs(force_reparse=force_reparse)

        logger.info(f"PDF parsing stats: {stats}")
        self.checkpoint()
        return stats


    async def run_all(self, max_iterations: int = 5, cnpjs: list = None):
        """Run complete pipeline (all stages)"""
        logger.info("=== Starting Complete CADRI Pipeline ===")
        CSVSchemas.init_all()

        total_docs = 0

        # For CNPJ mode, we typically only need one iteration since we have specific targets
        if cnpjs:
            logger.info(f"Running in CNPJ mode with {len(cnpjs)} CNPJs")
            max_iterations = 1

        for iteration in range(1, max_iterations + 1):
            if not self.running:
                break

            logger.info(f"\n--- Iteration {iteration}/{max_iterations} ---")

            # Stage 1: List scraping
            if cnpjs and iteration == 1:
                # Use CNPJs on first iteration
                urls = await self.stage_list(cnpjs=cnpjs)
            else:
                # Use regular seed-based approach
                urls = await self.stage_list()

            if not urls:
                logger.info("No more URLs found, stopping")
                break

            # Stage 2: Detail scraping
            docs = self.stage_detail(urls)
            total_docs += docs

            # Show metrics
            logger.info(f"\nIteration {iteration} complete:")
            logger.info(f"  URLs found: {len(urls)}")
            logger.info(f"  Documents found: {docs}")
            logger.info(f"  {metrics.get_summary()}")

            # Check if we should continue
            if docs == 0:
                logger.info("No new documents found, stopping")
                break

        # Stage 3: PDF Download (after all data collection)
        if total_docs > 0:
            logger.info("\n=== Starting PDF Download Stage ===")
            pdf_stats = await self.stage_pdf()

            # Stage 4: PDF Parsing (after downloads)
            logger.info("\n=== Starting PDF Parsing Stage ===")
            parse_stats = self.stage_parse()

            logger.info("\n=== Complete Pipeline Summary ===")
            logger.info(f"Data collection: {metrics.get_summary()}")
            logger.info(f"PDF downloads: {pdf_stats}")
            logger.info(f"PDF parsing: {parse_stats}")

        logger.info("\n📊 Pipeline completed successfully!")
        logger.info("📁 Results saved in:")
        logger.info("   - data/csv/empresas.csv (company data)")
        logger.info("   - data/csv/cadri_documentos.csv (document metadata)")
        logger.info("   - data/csv/cadri_itens.csv (waste item details)")
        logger.info("   - data/pdf/ (downloaded PDFs)")

    async def run_stage(self, stage: str, **kwargs):
        """Run a specific stage"""
        CSVSchemas.init_all()

        if stage == 'list':
            seeds = kwargs.get('seeds', [])
            cnpjs = kwargs.get('cnpjs', [])
            if isinstance(seeds, str):
                seeds = [s.strip() for s in seeds.split(',')]
            await self.stage_list(seeds=seeds, cnpjs=cnpjs)

        elif stage == 'detail':
            self.stage_detail()

        elif stage == 'pdf':
            await self.stage_pdf()

        elif stage == 'parse':
            self.stage_parse()

        elif stage == 'all':
            max_iter = kwargs.get('max_iterations', 5)
            cnpjs = kwargs.get('cnpjs', [])
            await self.run_all(max_iter, cnpjs=cnpjs)

        else:
            logger.error(f"Unknown stage: {stage}")
            sys.exit(1)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='CETESB CADRI Data Collection Pipeline')

    parser.add_argument(
        '--stage',
        choices=['list', 'detail', 'pdf', 'parse', 'all'],
        default='all',
        help='Pipeline stage to run (list, detail, pdf, parse, or all)'
    )

    parser.add_argument(
        '--seeds',
        type=str,
        help='Comma-separated seeds for list stage (e.g., CEM,ACE,AGR)'
    )

    parser.add_argument(
        '--cnpj-file',
        type=str,
        help='XLSX file with CNPJ list (column "cnpj" expected)'
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

    # Load CNPJs from file if provided
    cnpjs = []
    cnpj_file = getattr(args, 'cnpj_file', None)
    if cnpj_file:
        try:
            from cnpj_loader import load_cnpjs, validate_cnpj_file
            from pathlib import Path

            cnpj_file_path = Path(cnpj_file)

            # Validate file first
            if validate_cnpj_file(str(cnpj_file_path)):
                cnpjs = load_cnpjs(str(cnpj_file_path))
                logger.info(f"Loaded {len(cnpjs)} CNPJs from {cnpj_file_path}")
            else:
                logger.error(f"Invalid CNPJ file: {cnpj_file_path}")
                sys.exit(1)

        except Exception as e:
            logger.error(f"Error loading CNPJ file: {e}")
            sys.exit(1)

    # Run pipeline
    try:
        asyncio.run(
            pipeline.run_stage(
                args.stage,
                seeds=args.seeds,
                cnpjs=cnpjs,
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