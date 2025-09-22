#!/usr/bin/env python3
"""
Teste de Extração Aprimorada de Dados de Resultados

Script para testar a nova funcionalidade de extração completa de dados
das páginas de resultado da CETESB, incluindo dados de cadastramento
completos e tabela de documentos.

Baseado na análise da estrutura mostrada na imagem.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from results_extractor import ResultsPageExtractor, extract_company_and_documents
from scrape_list import ListScraper
from seeds import SeedManager
from config import DATA_DIR
from logging_conf import setup_logging, logger


class ResultsExtractionTester:
    """Test enhanced results extraction"""

    def __init__(self):
        self.test_dir = DATA_DIR / "test_results"
        self.test_dir.mkdir(exist_ok=True)
        self.session_dir = self.test_dir / f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.session_dir.mkdir(exist_ok=True)

    def save_test_data(self, data: dict, filename: str, description: str = ""):
        """Save test data for analysis"""
        json_file = self.session_dir / f"{filename}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Saved test data: {json_file}")
        if description:
            logger.info(f"  Description: {description}")

        return json_file

    def save_html(self, content: str, filename: str, description: str = ""):
        """Save HTML content for analysis"""
        html_file = self.session_dir / f"{filename}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(f"<!-- Saved: {datetime.now().isoformat()} -->\n")
            f.write(f"<!-- Description: {description} -->\n")
            f.write(content)

        logger.info(f"Saved HTML: {html_file}")
        return html_file

    async def test_search_and_extract(self, search_term: str):
        """Test complete search and extraction flow"""
        logger.info(f"=== TESTING SEARCH AND EXTRACTION: '{search_term}' ===")

        seed_manager = SeedManager()
        scraper = ListScraper(seed_manager)

        try:
            # Perform search
            results = await scraper.search_by_razao_social(search_term)

            logger.info(f"Search found {len(results)} results")

            # Save search results
            self.save_test_data(results, f"search_results_{search_term}", f"Search results for '{search_term}'")

            # Test extraction on first few results
            extraction_results = []

            for i, result in enumerate(results[:3]):  # Test first 3 results
                logger.info(f"\n--- Testing result {i+1}: {result.get('razao_social', 'Unknown')} ---")

                extraction_result = await self.test_single_url(result['url'], f"result_{i+1}")
                extraction_result['search_term'] = search_term
                extraction_result['search_index'] = i
                extraction_result['original_result'] = result

                extraction_results.append(extraction_result)

                # Log summary
                company = extraction_result.get('company', {})
                documents = extraction_result.get('documents', [])

                logger.info(f"Result {i+1} summary:")
                logger.info(f"  Company: {company.get('razao_social', 'Not extracted')}")
                logger.info(f"  CNPJ: {company.get('cnpj', 'Not extracted')}")
                logger.info(f"  Address: {company.get('logradouro', 'Not extracted')}")
                logger.info(f"  Documents: {len(documents)}")

            # Save extraction results
            self.save_test_data(extraction_results, f"extraction_results_{search_term}",
                              f"Extraction results for '{search_term}'")

            return extraction_results

        except Exception as e:
            logger.error(f"Error in search and extraction test: {e}")
            return []

    async def test_single_url(self, url: str, test_name: str = "single_test"):
        """Test extraction from a single URL"""
        logger.info(f"Testing single URL: {url}")

        try:
            # Use ListScraper to get the page content
            seed_manager = SeedManager()
            scraper = ListScraper(seed_manager)

            async with scraper.browser_manager as browser:
                page = await browser.new_page()
                await page.goto(url, wait_until='networkidle')

                # Get HTML content
                html_content = await page.content()

                # Save HTML for analysis
                self.save_html(html_content, f"{test_name}_page", f"Page content for {url}")

                # Test extraction
                company, documents = extract_company_and_documents(html_content, url)

                # Test with direct extractor for more details
                extractor = ResultsPageExtractor(html_content, url)
                page_type = extractor._detect_page_type()

                # Create comprehensive test result
                test_result = {
                    'url': url,
                    'test_name': test_name,
                    'timestamp': datetime.now().isoformat(),
                    'page_type': page_type,
                    'company': company,
                    'documents': documents,
                    'extraction_stats': {
                        'company_fields_extracted': sum(1 for v in company.values() if v),
                        'documents_found': len(documents),
                        'has_cnpj': bool(company.get('cnpj')),
                        'has_razao_social': bool(company.get('razao_social')),
                        'has_address': bool(company.get('logradouro')),
                        'has_documents': len(documents) > 0
                    }
                }

                # Save test result
                self.save_test_data(test_result, f"{test_name}_result", f"Extraction result for {url}")

                return test_result

        except Exception as e:
            logger.error(f"Error testing single URL {url}: {e}")
            return {
                'url': url,
                'test_name': test_name,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def test_html_samples(self):
        """Test extraction with known HTML samples"""
        logger.info("=== TESTING WITH HTML SAMPLES ===")

        # Sample based on the image structure
        sample_html = """
        <!DOCTYPE html>
        <html>
        <head><title>Resultado da Consulta</title></head>
        <body>
            <table bgcolor="#6699CC" width="100%">
                <tr><td align="center"><font color="white"><b>Resultado da Consulta</b></font></td></tr>
            </table>

            <table bgcolor="#CCCCCC" width="100%">
                <tr><td align="center"><b>Dados do Cadastramento</b></td></tr>
            </table>

            <table>
                <tr>
                    <td><b>Razão Social</b> - SUZANO PAPEL E CELULOSE</td>
                    <td><b>Nº S/Nº</b></td>
                </tr>
                <tr>
                    <td><b>Logradouro</b> - ESTR. MUN. SANTO AGOSTINHO</td>
                    <td><b>CEP</b> - 01221-750</td>
                </tr>
                <tr>
                    <td><b>Complemento</b> - </td>
                    <td><b>CNPJ</b> - 16.404.287/0100-37</td>
                </tr>
                <tr>
                    <td><b>Bairro</b> - SANTO AGOSTINHO</td>
                    <td><b>Nº do Cadastro na CETESB</b> - 545-00163636</td>
                </tr>
                <tr>
                    <td><b>Município</b> - SAO JOSE DOS CAMPOS</td>
                    <td><b>Descrição da Atividade</b> - FLORESTAMENTO E REFLORESTAMENTO</td>
                </tr>
            </table>

            <table border="1">
                <tr bgcolor="#CCCCCC">
                    <th>SD Nº</th>
                    <th>Data da SD</th>
                    <th>Nº Processo</th>
                    <th>Objeto da Solicitação</th>
                    <th>Nº Documento</th>
                    <th>Situação</th>
                    <th>Desde</th>
                </tr>
                <tr>
                    <td>03004636</td>
                    <td>16/08/2004</td>
                    <td>03/00564/04</td>
                    <td>CERT MOV RESIDUOS INT AMB</td>
                    <td><a href="#">3000839</a></td>
                    <td>Emitida</td>
                    <td>01/10/2004</td>
                </tr>
                <tr>
                    <td>57001212</td>
                    <td>05/12/2008</td>
                    <td>57/00377/08</td>
                    <td>CERT MOV RESIDUOS INT AMB</td>
                    <td><a href="#">57000887</a></td>
                    <td>Emitida</td>
                    <td>16/03/2009</td>
                </tr>
                <tr>
                    <td>57000613</td>
                    <td>23/04/2009</td>
                    <td>57/00229/09</td>
                    <td>CERT MOV RESIDUOS INT AMB</td>
                    <td><a href="#">57000122</a></td>
                    <td>Emitida</td>
                    <td>07/05/2009</td>
                </tr>
            </table>
        </body>
        </html>
        """

        logger.info("Testing with sample HTML based on CETESB structure")

        # Save sample HTML
        self.save_html(sample_html, "sample_cetesb", "Sample HTML based on CETESB structure")

        # Test extraction
        company, documents = extract_company_and_documents(sample_html, "http://test.sample")

        logger.info("Sample extraction results:")
        logger.info(f"Company data:")
        for key, value in company.items():
            if value:
                logger.info(f"  {key}: {value}")

        logger.info(f"Documents found: {len(documents)}")
        for i, doc in enumerate(documents, 1):
            logger.info(f"  Doc {i}: {doc.get('numero_documento')} - {doc.get('tipo_documento')} ({doc.get('situacao')})")

        # Save results
        sample_result = {
            'company': company,
            'documents': documents,
            'extraction_stats': {
                'company_fields_extracted': sum(1 for v in company.values() if v),
                'documents_found': len(documents)
            }
        }

        self.save_test_data(sample_result, "sample_extraction", "Sample HTML extraction result")

        return sample_result

    def generate_test_report(self):
        """Generate comprehensive test report"""
        logger.info("=== GENERATING TEST REPORT ===")

        # List all test files
        json_files = list(self.session_dir.glob("*.json"))
        html_files = list(self.session_dir.glob("*.html"))

        report = {
            'session_info': {
                'session_dir': str(self.session_dir),
                'timestamp': datetime.now().isoformat(),
                'files_generated': {
                    'json_files': [f.name for f in json_files],
                    'html_files': [f.name for f in html_files]
                }
            },
            'summary': {
                'total_tests': len([f for f in json_files if 'result' in f.name]),
                'json_files': len(json_files),
                'html_files': len(html_files)
            }
        }

        # Save report
        report_file = self.save_test_data(report, "test_report", "Comprehensive test session report")

        logger.info(f"Test session complete!")
        logger.info(f"Files saved in: {self.session_dir}")
        logger.info(f"Report: {report_file}")

        return report


async def main():
    """Main test function"""
    import argparse

    parser = argparse.ArgumentParser(description="Test enhanced results extraction")
    parser.add_argument('--search', help='Test with search term (e.g., PETROBRAS)')
    parser.add_argument('--url', help='Test with specific URL')
    parser.add_argument('--sample', action='store_true', help='Test with HTML sample')
    parser.add_argument('--all', action='store_true', help='Run all tests')
    parser.add_argument('--log-level', default='INFO', help='Log level')

    args = parser.parse_args()

    # Setup logging
    setup_logging(level=args.log_level)

    tester = ResultsExtractionTester()

    logger.info("=== STARTING RESULTS EXTRACTION TESTS ===")
    logger.info(f"Test session: {tester.session_dir}")

    try:
        if args.sample or args.all:
            tester.test_html_samples()

        if args.search or args.all:
            search_term = args.search if args.search else "PETROBRAS"
            await tester.test_search_and_extract(search_term)

        if args.url:
            await tester.test_single_url(args.url, "manual_url")

        if args.all:
            # Test multiple search terms
            test_terms = ["CEMIG", "BRASKEM", "VALE"]
            for term in test_terms:
                await tester.test_search_and_extract(term)

        # Generate final report
        tester.generate_test_report()

    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

    logger.info("=== TESTS COMPLETED ===")


if __name__ == "__main__":
    asyncio.run(main())