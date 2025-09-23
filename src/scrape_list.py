import asyncio
import re
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
        'cnpj': 'input[name="cgc"]',  # CETESB usa campo 'cgc' (Cadastro Geral de Contribuintes) para CNPJ
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

    async def search_by_cnpj(self, cnpj: str) -> List[Dict[str, str]]:
        """
        Search by CNPJ

        Args:
            cnpj: CNPJ string (should be 14 digits)

        Returns:
            List of process links
        """
        # Validar CNPJ (deve ter 14 dígitos)
        clean_cnpj = re.sub(r'[^\d]', '', cnpj.strip())
        if len(clean_cnpj) != 14:
            logger.warning(f"CNPJ inválido: '{cnpj}' (deve ter 14 dígitos)")
            return []

        # DEBUG_CNPJ: Log detalhado do início da busca
        logger.info(f"[DEBUG_CNPJ] Iniciando busca por CNPJ: original='{cnpj}', limpo='{clean_cnpj}'")

        results = []
        start_time = time.time()

        try:
            async with self.browser_manager as browser:
                page = await browser.new_page()

                # Navigate to search page
                await page.goto(self.SEARCH_URL, wait_until='networkidle')
                logger.debug(f"Loaded search page for CNPJ: {clean_cnpj}")

                # DEBUG_CNPJ: Screenshot da página inicial
                from pathlib import Path
                debug_dir = Path("data/debug")
                debug_dir.mkdir(parents=True, exist_ok=True)
                await page.screenshot(path=str(debug_dir / f"DEBUG_CNPJ_search_{clean_cnpj}_01_loaded.png"))

                # DEBUG_CNPJ: Testar diferentes seletores para campo CNPJ
                cnpj_selectors = [
                    'input[name="cgc"]',   # Campo correto CETESB (Cadastro Geral de Contribuintes)
                    'input[name="cnpj"]',  # Seletor padrão
                    'input[id="cgc"]',
                    'input[id="cnpj"]',
                    '#cnpj',
                    '#cgc',
                    'input[placeholder*="cnpj" i]',
                    'input[placeholder*="CNPJ"]',
                    'input[name*="cnpj" i]',
                    'input[id*="cnpj" i]'
                ]

                cnpj_field = None
                working_selector = None
                original_selector = self.FORM_SELECTORS['cnpj']  # Salvar seletor original

                logger.info(f"[DEBUG_CNPJ] Testando {len(cnpj_selectors)} seletores para campo CNPJ...")
                for idx, selector in enumerate(cnpj_selectors):
                    try:
                        test_field = await page.query_selector(selector)
                        if test_field:
                            is_visible = await test_field.is_visible()
                            is_enabled = await test_field.is_enabled()
                            field_attrs = await test_field.evaluate('el => ({name: el.name, id: el.id, type: el.type, placeholder: el.placeholder})')

                            logger.info(f"[DEBUG_CNPJ] Seletor {idx+1} '{selector}': ✅ Encontrado - Visível: {is_visible}, Habilitado: {is_enabled}")
                            logger.info(f"[DEBUG_CNPJ] Atributos do campo: {field_attrs}")

                            if is_visible and is_enabled:
                                cnpj_field = test_field
                                working_selector = selector
                                logger.info(f"[DEBUG_CNPJ] ✅ Usando seletor: '{selector}'")
                                break
                        else:
                            logger.info(f"[DEBUG_CNPJ] Seletor {idx+1} '{selector}': ❌ Não encontrado")
                    except Exception as e:
                        logger.info(f"[DEBUG_CNPJ] Seletor {idx+1} '{selector}': ❌ Erro: {e}")

                if not cnpj_field:
                    logger.error(f"[DEBUG_CNPJ] Campo CNPJ não encontrado com nenhum seletor testado")

                    # DEBUG_CNPJ: Listar todos os inputs disponíveis
                    all_inputs = await page.evaluate("""
                        () => {
                            const inputs = Array.from(document.querySelectorAll('input'));
                            return inputs.map(input => ({
                                tagName: input.tagName,
                                type: input.type,
                                name: input.name,
                                id: input.id,
                                placeholder: input.placeholder,
                                visible: !input.hidden && input.offsetParent !== null
                            }));
                        }
                    """)
                    logger.error(f"[DEBUG_CNPJ] Inputs disponíveis na página: {all_inputs}")
                    return []

                # DEBUG_CNPJ: Atualizar seletor se encontrou um melhor
                if working_selector and working_selector != original_selector:
                    logger.info(f"[DEBUG_CNPJ] ⚠️ Seletor original '{original_selector}' não funcionou")
                    logger.info(f"[DEBUG_CNPJ] ✅ Usando seletor alternativo: '{working_selector}'")
                    # Temporariamente atualizar o seletor para esta execução
                    self.FORM_SELECTORS['cnpj'] = working_selector

                # DEBUG_CNPJ: Testar diferentes formatos de CNPJ
                cnpj_formats = [
                    clean_cnpj,  # Sem formatação
                    f"{clean_cnpj[:2]}.{clean_cnpj[2:5]}.{clean_cnpj[5:8]}/{clean_cnpj[8:12]}-{clean_cnpj[12:]}",  # Com formatação
                ]

                successful_format = None
                for format_idx, cnpj_format in enumerate(cnpj_formats):
                    logger.info(f"[DEBUG_CNPJ] Testando formato {format_idx + 1}: '{cnpj_format}'")

                    # Limpar campo
                    await cnpj_field.click()
                    await cnpj_field.fill("")
                    await asyncio.sleep(0.2)

                    # Preencher com formato atual
                    await cnpj_field.type(cnpj_format)
                    await asyncio.sleep(0.3)

                    # Verificar valor preenchido
                    filled_value = await cnpj_field.input_value()
                    logger.info(f"[DEBUG_CNPJ] Valor preenchido no campo (formato {format_idx + 1}): '{filled_value}'")

                    # Disparar eventos JavaScript para este formato
                    await page.evaluate(f"""
                        () => {{
                            const cnpjField = document.querySelector('{self.FORM_SELECTORS['cnpj']}');
                            if (cnpjField) {{
                                ['input', 'change', 'blur'].forEach(eventType => {{
                                    const event = new Event(eventType, {{ bubbles: true }});
                                    cnpjField.dispatchEvent(event);
                                }});
                            }}
                        }}
                    """)

                    # Verificar se o campo aceita o formato (não foi limpo automaticamente)
                    final_value = await cnpj_field.input_value()
                    if final_value == cnpj_format or (final_value and len(final_value) >= 11):
                        logger.info(f"[DEBUG_CNPJ] ✅ Formato {format_idx + 1} aceito pelo campo: '{final_value}'")
                        successful_format = cnpj_format
                        break
                    else:
                        logger.info(f"[DEBUG_CNPJ] ❌ Formato {format_idx + 1} rejeitado pelo campo")

                if not successful_format:
                    logger.error(f"[DEBUG_CNPJ] Nenhum formato de CNPJ foi aceito pelo campo")
                    successful_format = clean_cnpj  # Usar formato limpo como fallback

                # DEBUG_CNPJ: Screenshot após preenchimento
                await page.screenshot(path=str(debug_dir / f"DEBUG_CNPJ_search_{clean_cnpj}_02_filled.png"))

                # DEBUG_CNPJ: Disparar eventos JavaScript
                await page.evaluate(f"""
                    () => {{
                        const cnpjField = document.querySelector('{self.FORM_SELECTORS['cnpj']}');
                        if (cnpjField) {{
                            ['input', 'change', 'blur'].forEach(eventType => {{
                                const event = new Event(eventType, {{ bubbles: true }});
                                cnpjField.dispatchEvent(event);
                            }});
                        }}
                    }}
                """)

                # DEBUG_CNPJ: Verificar botão submit
                submit_btn = await page.query_selector(self.FORM_SELECTORS['submit'])
                if not submit_btn:
                    logger.error(f"[DEBUG_CNPJ] Botão submit não encontrado com seletor: {self.FORM_SELECTORS['submit']}")
                    return []

                submit_visible = await submit_btn.is_visible()
                submit_enabled = await submit_btn.is_enabled()
                logger.info(f"[DEBUG_CNPJ] Botão submit - Visível: {submit_visible}, Habilitado: {submit_enabled}")

                # Fill and submit form with CNPJ (usando formato aceito)
                form_data = {
                    self.FORM_SELECTORS['cnpj']: successful_format
                }

                # DEBUG_CNPJ: Submeter formulário
                logger.info(f"[DEBUG_CNPJ] Submetendo formulário...")
                success = await FormHelper.submit_form(
                    page,
                    form_data,
                    self.FORM_SELECTORS['submit'],
                    wait_for='table, .erro, .error'
                )

                # DEBUG_CNPJ: Screenshot após submissão
                await page.screenshot(path=str(debug_dir / f"DEBUG_CNPJ_search_{clean_cnpj}_03_submitted.png"))

                if not success:
                    logger.error(f"[DEBUG_CNPJ] Failed to submit form for CNPJ: {clean_cnpj}")
                    # DEBUG_CNPJ: Capturar HTML da página em caso de erro
                    page_content = await page.content()
                    debug_html_file = debug_dir / f"DEBUG_CNPJ_error_{clean_cnpj}.html"
                    with open(debug_html_file, 'w', encoding='utf-8') as f:
                        f.write(page_content)
                    logger.info(f"[DEBUG_CNPJ] HTML da página salvo em: {debug_html_file}")
                    return []

                # Check for no results
                error_element = await page.query_selector('.erro, .error, :has-text("nenhum resultado")')
                if error_element:
                    logger.info(f"[DEBUG_CNPJ] No results for CNPJ: {clean_cnpj}")
                    error_text = await error_element.inner_text()
                    logger.info(f"[DEBUG_CNPJ] Texto do erro: '{error_text}'")
                    return []

                # DEBUG_CNPJ: Log sobre resultados encontrados
                logger.info(f"[DEBUG_CNPJ] Formulário submetido com sucesso, extraindo links...")

                # Extract links from all pages
                results = await FormHelper.handle_pagination(
                    page,
                    self.NEXT_BUTTON_SELECTORS,
                    MAX_PAGES,
                    self._extract_process_links
                )

                # DEBUG_CNPJ: Screenshot final com resultados
                await page.screenshot(path=str(debug_dir / f"DEBUG_CNPJ_search_{clean_cnpj}_04_results.png"))

                # For CNPJ searches, we don't need to add discovered company names to seed manager
                # since we're searching for specific companies

        except Exception as e:
            logger.error(f"[DEBUG_CNPJ] Error searching with CNPJ '{clean_cnpj}': {e}")
            # DEBUG_CNPJ: Log stack trace completo
            import traceback
            logger.error(f"[DEBUG_CNPJ] Stack trace: {traceback.format_exc()}")

        finally:
            # DEBUG_CNPJ: Restaurar seletor original se foi alterado
            if 'original_selector' in locals():
                self.FORM_SELECTORS['cnpj'] = original_selector
                logger.debug(f"[DEBUG_CNPJ] Seletor original restaurado: '{original_selector}'")

        # Log performance
        elapsed = time.time() - start_time
        metrics.increment('searches')

        # DEBUG_CNPJ: Log final detalhado
        logger.info(f"[DEBUG_CNPJ] CNPJ '{clean_cnpj}': {len(results)} links in {elapsed:.1f}s")
        logger.info(f"[DEBUG_CNPJ] Screenshots salvos em: data/debug/DEBUG_CNPJ_search_{clean_cnpj}_*.png")

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