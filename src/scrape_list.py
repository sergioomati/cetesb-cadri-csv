import asyncio
from typing import List, Dict
from urllib.parse import urljoin
import time

from browser import BrowserManager, FormHelper
from config import BASE_URL_LICENCIAMENTO, MAX_PAGES
from seeds import SeedManager
from logging_conf import logger, metrics


class ListScraper:
    """Scrape process list from CETESB search"""

    SEARCH_URL = f"{BASE_URL_LICENCIAMENTO}/processo_consulta.asp"

    # Form selectors
    FORM_SELECTORS = {
        'razao_social': 'input[name="razao"]',
        'municipio': 'input[name="municipio"]',
        'submit': 'input[type="submit"]'
    }

    # Pagination selectors (various formats found)
    NEXT_BUTTON_SELECTORS = [
        'a:has-text("Próxima")',
        'a:has-text("Proxima")',
        'a:has-text(">")',
        'input[value="Próxima"]',
        'input[value="Proxima"]',
        'a[href*="pagina="]:has-text(">")'
    ]

    def __init__(self, seed_manager: SeedManager):
        self.seed_manager = seed_manager
        self.browser_manager = BrowserManager()

    async def search_by_razao_social(self, seed: str) -> List[Dict[str, str]]:
        """
        Search by company name (razao social)

        Args:
            seed: Search string (min 3 chars)

        Returns:
            List of process links
        """
        if len(seed) < 3:
            logger.warning(f"Seed too short: '{seed}' (min 3 chars)")
            return []

        results = []
        start_time = time.time()

        try:
            async with self.browser_manager as browser:
                page = await browser.new_page()

                # Navigate to search page
                await page.goto(self.SEARCH_URL, wait_until='networkidle')
                logger.debug(f"Loaded search page for seed: {seed}")

                # Fill and submit form
                form_data = {
                    self.FORM_SELECTORS['razao_social']: seed
                }

                success = await FormHelper.submit_form(
                    page,
                    form_data,
                    self.FORM_SELECTORS['submit'],
                    wait_for='table, .erro, .error'
                )

                if not success:
                    logger.error(f"Failed to submit form for seed: {seed}")
                    return []

                # Check for no results
                error_element = await page.query_selector('.erro, .error, :has-text("nenhum resultado")')
                if error_element:
                    logger.info(f"No results for seed: {seed}")
                    return []

                # Extract links from all pages
                results = await FormHelper.handle_pagination(
                    page,
                    self.NEXT_BUTTON_SELECTORS,
                    MAX_PAGES,
                    self._extract_process_links
                )

                # Add discovered company names to seed manager
                if results:
                    company_names = list(set(r.get('razao_social', '') for r in results if r.get('razao_social')))
                    self.seed_manager.add_discovered_text(company_names[:50])  # Limit to avoid too many seeds

        except Exception as e:
            logger.error(f"Error searching with seed '{seed}': {e}")

        # Log performance
        elapsed = time.time() - start_time
        metrics.increment('searches')

        if self.seed_manager.should_refine(seed, len(results)):
            logger.info(f"Seed '{seed}' returned {len(results)} results, will refine")
            # Queue refined seeds
            refined = self.seed_manager.refine_seed(seed)
            for r in refined:
                self.seed_manager.seed_queue.append(r)

        logger.info(f"Seed '{seed}': {len(results)} links in {elapsed:.1f}s")
        return results

    async def _extract_process_links(self, page) -> List[Dict[str, str]]:
        """Extract process links and comprehensive company data from current page"""
        results = []

        try:
            # Get page HTML for enhanced extraction
            html_content = await page.content()
            page_url = page.url

            # Try enhanced extraction first
            enhanced_results = await self._extract_enhanced_data(html_content, page_url)
            if enhanced_results:
                results.extend(enhanced_results)
                logger.info(f"Enhanced extraction found {len(enhanced_results)} companies with full data")
                return results

            # Fallback to original link extraction
            logger.debug("Enhanced extraction failed, falling back to link extraction")

            # Find all links to processo_resultado2.asp
            link_elements = await page.query_selector_all('a[href*="processo_resultado2.asp"]')

            for elem in link_elements:
                try:
                    href = await elem.get_attribute('href')
                    if not href:
                        continue

                    # Make absolute URL
                    full_url = urljoin(self.SEARCH_URL, href)

                    # Try to get company name from row
                    razao_social = ""
                    row = await elem.evaluate_handle('el => el.closest("tr")')
                    if row:
                        row_text = await row.inner_text()
                        # Usually company name is in first or second column
                        parts = row_text.split('\t')
                        if len(parts) > 1:
                            razao_social = parts[1].strip()

                    results.append({
                        'url': full_url,
                        'razao_social': razao_social,
                        'extraction_method': 'basic_links'
                    })

                except Exception as e:
                    logger.debug(f"Error extracting link: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error extracting process data: {e}")

        return results

    async def _extract_enhanced_data(self, html_content: str, page_url: str) -> List[Dict]:
        """Extract comprehensive data using enhanced extractor"""
        from results_extractor import ResultsPageExtractor, extract_company_and_documents
        from store_csv import CSVStore, CSVSchemas
        import pandas as pd
        from config import CSV_EMPRESAS, CSV_CADRI_DOCS

        results = []

        try:
            # Use the enhanced extractor
            extractor = ResultsPageExtractor(html_content, page_url)

            # Detect if this is a company details page or search results page
            page_type = extractor._detect_page_type()

            if page_type == "company_details":
                # Single company with full details
                company, documents = extract_company_and_documents(html_content, page_url)

                if company and company.get('cnpj'):
                    logger.info(f"Found detailed company data: {company.get('razao_social', 'Unknown')}")

                    # Save company data immediately
                    df_company = pd.DataFrame([company])
                    CSVStore.upsert(df_company, CSV_EMPRESAS, keys=['cnpj'])

                    # Save documents if found
                    if documents:
                        df_docs = pd.DataFrame(documents)
                        CSVStore.upsert(df_docs, CSV_CADRI_DOCS, keys=['numero_documento'])
                        logger.info(f"Found {len(documents)} documents for {company.get('razao_social')}")

                    # Return company info for further processing
                    results.append({
                        'url': company.get('url_detalhe', page_url),
                        'razao_social': company.get('razao_social', ''),
                        'cnpj': company.get('cnpj', ''),
                        'extraction_method': 'enhanced_details',
                        'has_documents': len(documents) > 0,
                        'skip_detail_stage': True  # Flag to skip detail scraping
                    })

            elif page_type == "search_results":
                # Multiple companies in search results
                # For now, fall back to link extraction
                # TODO: Implement extraction of multiple companies from search results
                logger.debug("Search results page detected, using link extraction")
                return []

        except Exception as e:
            logger.error(f"Enhanced extraction failed: {e}")
            return []

        return results

    async def run_batch(self, seeds: List[str]) -> List[Dict[str, str]]:
        """Run searches for multiple seeds"""
        all_results = []

        for seed in seeds:
            results = await self.search_by_razao_social(seed)
            all_results.extend(results)

            # Add delay between searches
            if seeds.index(seed) < len(seeds) - 1:
                await asyncio.sleep(2)

        # Remove duplicates by URL
        unique_results = []
        seen_urls = set()

        for result in all_results:
            url = result['url']
            if url not in seen_urls:
                seen_urls.add(url)
                unique_results.append(result)

        logger.info(f"Batch search complete: {len(unique_results)} unique links from {len(seeds)} seeds")
        return unique_results


async def main():
    """Test list scraping"""
    seed_manager = SeedManager()
    scraper = ListScraper(seed_manager)

    # Test with a single seed
    test_seed = "CEM"
    results = await scraper.search_by_razao_social(test_seed)

    print(f"\nResults for '{test_seed}':")
    for i, result in enumerate(results[:5], 1):
        print(f"{i}. {result['razao_social']}")
        print(f"   {result['url']}")

    # Test batch
    batch_seeds = seed_manager.get_batch_seeds(3)
    if batch_seeds:
        print(f"\nTesting batch with seeds: {batch_seeds}")
        batch_results = await scraper.run_batch(batch_seeds)
        print(f"Total batch results: {len(batch_results)}")


if __name__ == "__main__":
    asyncio.run(main())