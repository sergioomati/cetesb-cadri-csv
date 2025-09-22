#!/usr/bin/env python3
"""
Debug script para testar o scraping de uma empresa específica

Permite testar todo o processo de busca → detalhes → documentos CADRI
para uma empresa específica, com logging detalhado e salvamento de HTML
para análise manual.

Uso:
    python debug_single_company.py --cnpj 12345678000100
    python debug_single_company.py --search "EMPRESA TESTE"
    python debug_single_company.py --url "https://licenciamento.cetesb.sp.gov.br/cetesb/processo_resultado2.asp?cgc=12345678000100"
"""

import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from config import CSV_DIR, DATA_DIR
from seeds import SeedManager
from scrape_list import ListScraper
from scrape_detail import DetailScraper
from logging_conf import logger, setup_logging
from utils_text import clean_cnpj, format_cnpj


class DebugSingleCompany:
    """Debug scraper for a single company"""

    def __init__(self):
        self.debug_dir = DATA_DIR / "debug"
        self.debug_dir.mkdir(exist_ok=True)
        self.session_dir = self.debug_dir / f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.session_dir.mkdir(exist_ok=True)

    def save_html(self, content: str, filename: str, description: str = ""):
        """Save HTML content for analysis"""
        html_file = self.session_dir / f"{filename}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(content)

        logger.info(f"Saved HTML: {html_file}")
        if description:
            logger.info(f"  Description: {description}")

        return html_file

    def save_json(self, data: dict, filename: str, description: str = ""):
        """Save JSON data for analysis"""
        json_file = self.session_dir / f"{filename}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Saved JSON: {json_file}")
        if description:
            logger.info(f"  Description: {description}")

        return json_file

    async def search_company(self, search_term: str) -> list:
        """Search for companies by name"""
        logger.info(f"=== SEARCHING FOR: '{search_term}' ===")

        seed_manager = SeedManager()
        scraper = ListScraper(seed_manager)

        try:
            results = await scraper.search_by_razao_social(search_term)

            logger.info(f"Found {len(results)} companies")

            # Save results
            self.save_json(results, "search_results", f"Search results for '{search_term}'")

            # Log first few results
            for i, result in enumerate(results[:5], 1):
                logger.info(f"  {i}. {result.get('razao_social', 'N/A')}")
                logger.info(f"     URL: {result.get('url', 'N/A')}")

            return results

        except Exception as e:
            logger.error(f"Error searching: {e}")
            return []

    def analyze_html_structure(self, html_content: str, description: str = ""):
        """Analyze HTML structure for debugging"""
        from bs4 import BeautifulSoup

        logger.info(f"=== ANALYZING HTML STRUCTURE: {description} ===")

        soup = BeautifulSoup(html_content, 'html.parser')

        # Basic structure
        logger.info(f"Title: {soup.title.string if soup.title else 'No title'}")
        logger.info(f"Total elements: {len(soup.find_all())}")

        # Tables
        tables = soup.find_all('table')
        logger.info(f"Tables found: {len(tables)}")
        for i, table in enumerate(tables):
            headers = table.find_all('th')
            rows = table.find_all('tr')
            logger.info(f"  Table {i+1}: {len(headers)} headers, {len(rows)} rows")
            if headers:
                header_texts = [h.get_text().strip() for h in headers[:5]]
                logger.info(f"    Headers: {header_texts}")

        # Links
        links = soup.find_all('a')
        pdf_links = [a for a in links if a.get('href') and ('pdf' in a.get('href').lower() or 'autenticidade' in a.get('href').lower())]
        logger.info(f"Total links: {len(links)}")
        logger.info(f"PDF/Auth links: {len(pdf_links)}")

        # Forms
        forms = soup.find_all('form')
        logger.info(f"Forms: {len(forms)}")

        # Text content analysis
        text_content = soup.get_text()
        cadri_mentions = text_content.lower().count('cadri')
        residuo_mentions = text_content.lower().count('resid')
        cert_mentions = text_content.lower().count('cert')

        logger.info(f"Text analysis:")
        logger.info(f"  'CADRI' mentions: {cadri_mentions}")
        logger.info(f"  'Resíduo' mentions: {residuo_mentions}")
        logger.info(f"  'Cert' mentions: {cert_mentions}")

        # Common classes and IDs
        elements_with_class = soup.find_all(class_=True)
        elements_with_id = soup.find_all(id=True)
        logger.info(f"Elements with class: {len(elements_with_class)}")
        logger.info(f"Elements with ID: {len(elements_with_id)}")

        return {
            'title': soup.title.string if soup.title else None,
            'total_elements': len(soup.find_all()),
            'tables': len(tables),
            'links': len(links),
            'pdf_links': len(pdf_links),
            'forms': len(forms),
            'cadri_mentions': cadri_mentions,
            'residuo_mentions': residuo_mentions,
            'cert_mentions': cert_mentions
        }

    def debug_detail_page(self, url: str) -> tuple:
        """Debug detail page scraping with enhanced logging"""
        logger.info(f"=== DEBUGGING DETAIL PAGE ===")
        logger.info(f"URL: {url}")

        # Create enhanced scraper with debug mode
        scraper = DetailScraper()

        try:
            # Monkey patch for debug mode
            original_scrape = scraper.scrape_detail_page

            def debug_scrape(url):
                import httpx
                from bs4 import BeautifulSoup
                import time
                import random
                from config import RATE_MIN, RATE_MAX, USER_AGENT
                from browser import RetryHelper

                company_info = {}
                documents = []

                try:
                    # Rate limiting
                    time.sleep(random.uniform(RATE_MIN, RATE_MAX))

                    # Fetch page
                    logger.info("Fetching page...")
                    response = RetryHelper.retry_sync(
                        lambda: scraper.client.get(url)
                    )
                    response.raise_for_status()
                    logger.info(f"Response status: {response.status_code}")
                    logger.info(f"Content length: {len(response.text)}")

                    # Save HTML
                    html_file = self.save_html(response.text, "detail_page", "Detail page HTML")

                    # Analyze structure
                    analysis = self.analyze_html_structure(response.text, "Detail Page")
                    self.save_json(analysis, "html_analysis", "HTML structure analysis")

                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Extract company info with debug
                    logger.info("=== EXTRACTING COMPANY INFO ===")
                    company_info = self.debug_extract_company_info(soup, url)
                    self.save_json(company_info, "company_info", "Extracted company information")

                    # Extract documents with debug
                    logger.info("=== EXTRACTING DOCUMENTS ===")
                    documents = self.debug_extract_cadri_documents(soup, company_info)
                    self.save_json(documents, "documents", "Extracted CADRI documents")

                except Exception as e:
                    logger.error(f"Error in debug scrape: {e}")
                    import traceback
                    logger.error(traceback.format_exc())

                return company_info, documents

            return debug_scrape(url)

        except Exception as e:
            logger.error(f"Error debugging detail page: {e}")
            return {}, []

    def debug_extract_company_info(self, soup, url):
        """Enhanced company info extraction with debug logging"""
        from utils_text import normalize_text

        info = {
            'cnpj': '',
            'razao_social': '',
            'municipio': '',
            'uf': ''
        }

        # Extract CNPJ from URL
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            cgc = params.get('cgc', [''])[0]
            info['cnpj'] = clean_cnpj(cgc)
            logger.info(f"CNPJ from URL: {info['cnpj']} (formatted: {format_cnpj(info['cnpj'])})")
        except Exception as e:
            logger.error(f"Error extracting CNPJ: {e}")

        # Try multiple patterns for company name
        logger.info("Searching for company name...")

        patterns_tried = []

        # Pattern 1: Look for CNPJ-related elements
        cnpj_elements = soup.find_all(string=lambda x: x and 'CNPJ' in x.upper())
        logger.info(f"Found {len(cnpj_elements)} elements mentioning CNPJ")

        for elem in cnpj_elements[:3]:
            try:
                parent = elem.parent
                siblings = parent.find_next_siblings()
                logger.info(f"CNPJ element context: {elem.strip()}")
                logger.info(f"Parent: {parent.name}, Siblings: {len(siblings)}")
                patterns_tried.append(f"CNPJ context: {elem.strip()[:50]}...")
            except:
                pass

        # Pattern 2: Look for table cells
        table_cells = soup.find_all(['td', 'th'])
        company_cells = [cell for cell in table_cells if cell.get_text() and len(cell.get_text().strip()) > 5]
        logger.info(f"Found {len(company_cells)} substantial table cells")

        for i, cell in enumerate(company_cells[:5]):
            text = cell.get_text().strip()
            logger.info(f"Cell {i+1}: {text[:100]}...")
            patterns_tried.append(f"Table cell: {text[:50]}...")

        # Pattern 3: Headers
        headers = soup.find_all(['h1', 'h2', 'h3', 'h4'])
        logger.info(f"Found {len(headers)} headers")

        for header in headers:
            text = header.get_text().strip()
            if len(text) > 3:
                logger.info(f"Header: {text}")
                patterns_tried.append(f"Header: {text}")
                if not info['razao_social'] and len(text) > 10:
                    info['razao_social'] = normalize_text(text)
                    logger.info(f"Set razao_social from header: {info['razao_social']}")

        # Pattern 4: Bold text
        bold_elements = soup.find_all(['b', 'strong'])
        logger.info(f"Found {len(bold_elements)} bold elements")

        for bold in bold_elements[:5]:
            text = bold.get_text().strip()
            if len(text) > 10:
                logger.info(f"Bold text: {text}")
                patterns_tried.append(f"Bold: {text}")

        # Save patterns for analysis
        self.save_json({
            'patterns_tried': patterns_tried,
            'cnpj_elements_found': len(cnpj_elements),
            'table_cells_found': len(company_cells),
            'headers_found': len(headers),
            'bold_elements_found': len(bold_elements)
        }, "extraction_patterns", "Patterns tried for company extraction")

        logger.info(f"Final company info: {info}")
        return info

    def debug_extract_cadri_documents(self, soup, company_info):
        """Enhanced document extraction with debug logging"""
        from config import TARGET_DOC_TYPE
        from utils_text import extract_document_number, parse_date_br

        documents = []
        patterns_tried = []

        logger.info(f"Looking for documents with type: '{TARGET_DOC_TYPE}'")

        # Search in page text
        page_text = soup.get_text()
        target_mentions = page_text.lower().count(TARGET_DOC_TYPE.lower())
        cadri_mentions = page_text.lower().count('cadri')
        residuo_mentions = page_text.lower().count('resid')

        logger.info(f"Text analysis:")
        logger.info(f"  Target type mentions: {target_mentions}")
        logger.info(f"  CADRI mentions: {cadri_mentions}")
        logger.info(f"  Resíduo mentions: {residuo_mentions}")

        # Pattern 1: Tables
        tables = soup.find_all('table')
        logger.info(f"Analyzing {len(tables)} tables...")

        for i, table in enumerate(tables):
            logger.info(f"\nTable {i+1}:")

            # Check headers
            headers = table.find_all('th')
            header_texts = [h.get_text().strip() for h in headers]
            logger.info(f"  Headers: {header_texts}")

            has_doc_header = any('documento' in h.lower() or 'tipo' in h.lower() for h in header_texts)
            logger.info(f"  Has document-related header: {has_doc_header}")

            # Check rows
            rows = table.find_all('tr')[1:]  # Skip header
            logger.info(f"  Data rows: {len(rows)}")

            patterns_tried.append({
                'type': 'table',
                'index': i,
                'headers': header_texts,
                'has_doc_header': has_doc_header,
                'rows': len(rows)
            })

            if has_doc_header:
                logger.info("  Analyzing rows for documents...")
                for j, row in enumerate(rows[:5]):  # Check first 5 rows
                    cells = row.find_all('td')
                    cell_texts = [cell.get_text().strip() for cell in cells]

                    row_text = ' '.join(cell_texts)
                    has_target = TARGET_DOC_TYPE.lower() in row_text.lower()
                    has_cadri = 'cadri' in row_text.lower()

                    logger.info(f"    Row {j+1}: {cell_texts}")
                    logger.info(f"    Has target type: {has_target}")
                    logger.info(f"    Has CADRI: {has_cadri}")

                    if has_target or has_cadri:
                        # Try to parse as document
                        doc = self.debug_parse_document_row(row, company_info, j+1)
                        if doc:
                            documents.append(doc)

        # Pattern 2: Direct text search
        logger.info(f"\nSearching for direct mentions of '{TARGET_DOC_TYPE}'...")
        doc_elements = soup.find_all(string=lambda x: x and TARGET_DOC_TYPE.lower() in x.lower())
        logger.info(f"Found {len(doc_elements)} elements with target type")

        for elem in doc_elements[:3]:
            try:
                parent = elem.parent
                context = parent.get_text()
                logger.info(f"Direct mention context: {context[:200]}...")

                patterns_tried.append({
                    'type': 'direct_mention',
                    'context': context[:100],
                    'element': str(parent)[:200]
                })
            except:
                pass

        # Pattern 3: Alternative document types
        alternative_types = [
            'CERTIFICADO',
            'CERT',
            'MOVIMENTACAO',
            'RESIDUO',
            'RESÍDUO'
        ]

        logger.info(f"\nSearching for alternative document types...")
        for alt_type in alternative_types:
            elements = soup.find_all(string=lambda x: x and alt_type.lower() in x.lower())
            if elements:
                logger.info(f"  '{alt_type}': {len(elements)} mentions")
                patterns_tried.append({
                    'type': 'alternative',
                    'keyword': alt_type,
                    'count': len(elements)
                })

        # Save analysis
        self.save_json({
            'target_type': TARGET_DOC_TYPE,
            'text_analysis': {
                'target_mentions': target_mentions,
                'cadri_mentions': cadri_mentions,
                'residuo_mentions': residuo_mentions
            },
            'patterns_tried': patterns_tried,
            'documents_found': len(documents)
        }, "document_extraction", "Document extraction analysis")

        logger.info(f"\nFinal result: {len(documents)} documents found")
        return documents

    def debug_parse_document_row(self, row, company_info, row_num):
        """Parse a table row with debug info"""
        from config import BASE_URL_AUTENTICIDADE, TARGET_DOC_TYPE
        from utils_text import extract_document_number, parse_date_br

        logger.info(f"  Parsing row {row_num}:")

        try:
            cells = row.find_all('td')
            cell_texts = [cell.get_text().strip() for cell in cells]

            logger.info(f"    Cells: {cell_texts}")

            doc_info = {
                'numero_documento': '',
                'tipo_documento': '',
                'data_emissao': '',
                'url_detalhe': '',
                'url_pdf': '',
                'status_pdf': 'pending',
                'cnpj': company_info.get('cnpj', ''),
                'razao_social': company_info.get('razao_social', ''),
                'debug_row': row_num,
                'debug_cells': cell_texts
            }

            # Check each cell
            for i, cell in enumerate(cells):
                text = cell.get_text().strip()

                # Document type
                if TARGET_DOC_TYPE in text:
                    doc_info['tipo_documento'] = TARGET_DOC_TYPE
                    logger.info(f"    Found document type in cell {i}: {text}")

                # Document number
                doc_num = extract_document_number(text)
                if doc_num and not doc_info['numero_documento']:
                    doc_info['numero_documento'] = doc_num
                    logger.info(f"    Found document number in cell {i}: {doc_num}")

                # Date
                date = parse_date_br(text)
                if date and not doc_info['data_emissao']:
                    doc_info['data_emissao'] = date
                    logger.info(f"    Found date in cell {i}: {date}")

                # Link
                link = cell.find('a')
                if link and link.get('href'):
                    doc_info['url_detalhe'] = link['href']
                    logger.info(f"    Found link in cell {i}: {link['href']}")

            # Generate PDF URL if we have document number
            if doc_info['numero_documento']:
                doc_info['url_pdf'] = f"{BASE_URL_AUTENTICIDADE}/autentica.php?idocmn=12&ndocmn={doc_info['numero_documento']}"
                logger.info(f"    Generated PDF URL: {doc_info['url_pdf']}")
                return doc_info
            else:
                logger.info(f"    No document number found, skipping row")

        except Exception as e:
            logger.error(f"    Error parsing row {row_num}: {e}")

        return None


async def main():
    """Main debug function"""
    parser = argparse.ArgumentParser(description="Debug single company scraping")
    parser.add_argument('--cnpj', help='CNPJ da empresa (apenas números)')
    parser.add_argument('--search', help='Buscar por razão social')
    parser.add_argument('--url', help='URL direta da página de detalhes')
    parser.add_argument('--log-level', default='DEBUG', help='Log level (DEBUG, INFO, WARNING, ERROR)')

    args = parser.parse_args()

    if not any([args.cnpj, args.search, args.url]):
        parser.error("Deve fornecer --cnpj, --search ou --url")

    # Setup logging
    setup_logging(level=args.log_level)

    debug = DebugSingleCompany()

    logger.info(f"=== DEBUG SESSION STARTED ===")
    logger.info(f"Session directory: {debug.session_dir}")

    # Step 1: Get URL
    target_url = None

    if args.url:
        target_url = args.url
        logger.info(f"Using provided URL: {target_url}")

    elif args.cnpj:
        cnpj_clean = clean_cnpj(args.cnpj)
        target_url = f"https://licenciamento.cetesb.sp.gov.br/cetesb/processo_resultado2.asp?cgc={cnpj_clean}"
        logger.info(f"Generated URL from CNPJ {format_cnpj(cnpj_clean)}: {target_url}")

    elif args.search:
        logger.info(f"Searching for companies with term: '{args.search}'")
        results = await debug.search_company(args.search)

        if not results:
            logger.error("No companies found")
            return

        # Use first result
        target_url = results[0]['url']
        logger.info(f"Using first result: {results[0].get('razao_social', 'N/A')}")
        logger.info(f"URL: {target_url}")

    # Step 2: Debug detail page
    if target_url:
        logger.info(f"\n{'='*60}")
        company_info, documents = debug.debug_detail_page(target_url)

        # Final summary
        logger.info(f"\n=== FINAL SUMMARY ===")
        logger.info(f"Company CNPJ: {company_info.get('cnpj', 'Not found')}")
        logger.info(f"Company Name: {company_info.get('razao_social', 'Not found')}")
        logger.info(f"Location: {company_info.get('municipio', 'N/A')}, {company_info.get('uf', 'N/A')}")
        logger.info(f"Documents found: {len(documents)}")

        for i, doc in enumerate(documents, 1):
            logger.info(f"  {i}. {doc.get('numero_documento', 'N/A')} - {doc.get('tipo_documento', 'N/A')}")
            if doc.get('url_pdf'):
                logger.info(f"     PDF: {doc['url_pdf']}")

        logger.info(f"\nDebug files saved in: {debug.session_dir}")
        logger.info("Analise os arquivos HTML e JSON para mais detalhes.")


if __name__ == "__main__":
    asyncio.run(main())