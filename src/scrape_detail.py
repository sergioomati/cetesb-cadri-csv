import httpx
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs
import time
import random
from pathlib import Path

from config import BASE_URL_AUTENTICIDADE, TARGET_DOC_TYPE, RATE_MIN, RATE_MAX, USER_AGENT
from utils_text import clean_cnpj, normalize_text, extract_document_number, parse_date_br
from store_csv import CSVStore, CSVSchemas
from logging_conf import logger, metrics
from browser import RetryHelper
from results_extractor import ResultsPageExtractor


class DetailScraper:
    """Scrape detail pages for CADRI documents"""

    def __init__(self, debug_mode: bool = False, debug_dir: str = None):
        self.client = httpx.Client(
            headers={'User-Agent': USER_AGENT},
            timeout=30.0,
            follow_redirects=True
        )
        self.debug_mode = debug_mode
        self.debug_dir = Path(debug_dir) if debug_dir else None

        if self.debug_mode and self.debug_dir:
            self.debug_dir.mkdir(parents=True, exist_ok=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def extract_cnpj_from_url(self, url: str) -> str:
        """Extract CNPJ from URL query parameter"""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        cgc = params.get('cgc', [''])[0]
        return clean_cnpj(cgc)

    def scrape_detail_page(self, url: str) -> Tuple[Dict, List[Dict]]:
        """
        Scrape detail page for company info and CADRI documents

        Returns:
            Tuple of (company_info, list_of_documents)
        """
        company_info = {}
        documents = []

        try:
            # Add rate limiting
            time.sleep(random.uniform(RATE_MIN, RATE_MAX))

            # Fetch page
            response = RetryHelper.retry_sync(
                lambda: self.client.get(url)
            )
            response.raise_for_status()

            # Save HTML in debug mode
            if self.debug_mode and self.debug_dir:
                self._save_debug_html(response.text, url, "detail_page")

            # Use the new enhanced extractor
            extractor = ResultsPageExtractor(response.text, url)
            company_data, documents_data = extractor.extract_all_data()

            # Convert to dictionary format for compatibility
            company_info = {
                'cnpj': company_data.cnpj,
                'razao_social': company_data.razao_social,
                'logradouro': company_data.logradouro,
                'complemento': company_data.complemento,
                'bairro': company_data.bairro,
                'municipio': company_data.municipio,
                'uf': company_data.uf,
                'cep': company_data.cep,
                'numero_cadastro_cetesb': company_data.numero_cadastro_cetesb,
                'descricao_atividade': company_data.descricao_atividade,
                'numero_s_numero': company_data.numero_s_numero,
                'url_detalhe': url,
                'data_source': 'results_page'
            }

            # Convert documents to dictionary format
            documents = []
            for doc in documents_data:
                documents.append({
                    'numero_documento': doc.numero_documento,
                    'tipo_documento': doc.tipo_documento,
                    'cnpj': company_data.cnpj,
                    'razao_social': company_data.razao_social,
                    'data_emissao': doc.data_emissao,
                    'url_detalhe': url,
                    'url_pdf': doc.url_pdf,
                    'status_pdf': 'pending',
                    'pdf_hash': '',
                    'sd_numero': doc.sd_numero,
                    'data_sd': doc.data_sd,
                    'numero_processo': doc.numero_processo,
                    'objeto_solicitacao': doc.objeto_solicitacao,
                    'situacao': doc.situacao,
                    'data_desde': doc.data_desde,
                    'data_source': 'results_page'
                })

            logger.info(f"Found {len(documents)} CADRI documents")

            metrics.increment('details_scraped')

        except Exception as e:
            logger.error(f"Error scraping detail page {url}: {e}")
            metrics.increment('errors')

        return company_info, documents

    def extract_company_info(self, soup: BeautifulSoup, url: str) -> Dict:
        """Extract company information from page"""
        info = {
            'cnpj': self.extract_cnpj_from_url(url),
            'razao_social': '',
            'municipio': '',
            'uf': ''
        }

        try:
            # Try to find company name in various places
            # Common patterns: bold text, headers, specific classes
            patterns = [
                soup.find('b', string=lambda x: x and 'CNPJ' in x),
                soup.find('strong', string=lambda x: x and 'Razão' in x),
                soup.find('td', string=lambda x: x and 'Razão Social' in x)
            ]

            for pattern in patterns:
                if pattern:
                    # Get next sibling or parent's next cell
                    next_elem = pattern.find_next_sibling() or pattern.parent.find_next_sibling('td')
                    if next_elem:
                        info['razao_social'] = normalize_text(next_elem.get_text())
                        break

            # Try to extract from title or header if not found
            if not info['razao_social']:
                h1 = soup.find('h1')
                if h1:
                    info['razao_social'] = normalize_text(h1.get_text())

            # Extract location (municipio/UF)
            location_patterns = [
                soup.find(string=lambda x: x and 'Município' in x),
                soup.find(string=lambda x: x and 'Cidade' in x)
            ]

            for pattern in location_patterns:
                if pattern:
                    parent = pattern.parent
                    text = parent.get_text()
                    # Extract city and state
                    parts = text.split('-')
                    if len(parts) >= 2:
                        info['municipio'] = parts[0].strip()
                        info['uf'] = parts[-1].strip()[:2]  # Get state abbreviation

        except Exception as e:
            logger.debug(f"Error extracting company info: {e}")

        return info

    def extract_cadri_documents(self, soup: BeautifulSoup, company_info: Dict) -> List[Dict]:
        """Extract CADRI documents from page"""
        documents = []

        try:
            # Find documents table
            # Common patterns: table with "Documento" header, or divs with document info
            tables = soup.find_all('table')

            for table in tables:
                # Check if this table contains documents
                headers = table.find_all('th')
                header_texts = [h.get_text().strip() for h in headers]

                if any('documento' in h.lower() or 'tipo' in h.lower() for h in header_texts):
                    # This is likely the documents table
                    rows = table.find_all('tr')[1:]  # Skip header row

                    for row in rows:
                        doc = self.parse_document_row(row, company_info)
                        if doc and doc['tipo_documento'] == TARGET_DOC_TYPE:
                            documents.append(doc)

            # If no table found, try other structures
            if not documents:
                # Look for divs or lists with document info
                doc_elements = soup.find_all(['div', 'li'], string=lambda x: x and TARGET_DOC_TYPE in x)
                for elem in doc_elements:
                    doc = self.parse_document_element(elem, company_info)
                    if doc:
                        documents.append(doc)

        except Exception as e:
            logger.debug(f"Error extracting documents: {e}")

        logger.info(f"Found {len(documents)} CADRI documents")
        return documents

    def parse_document_row(self, row, company_info: Dict) -> Optional[Dict]:
        """Parse a table row containing document info"""
        try:
            cells = row.find_all('td')
            if len(cells) < 2:
                return None

            doc_info = {
                'numero_documento': '',
                'tipo_documento': '',
                'data_emissao': '',
                'url_detalhe': '',
                'url_pdf': '',
                'status_pdf': 'pending',
                'cnpj': company_info.get('cnpj', ''),
                'razao_social': company_info.get('razao_social', '')
            }

            # Extract from cells (order may vary)
            for cell in cells:
                text = cell.get_text().strip()

                # Check for document type
                if TARGET_DOC_TYPE in text:
                    doc_info['tipo_documento'] = TARGET_DOC_TYPE

                # Check for document number
                doc_num = extract_document_number(text)
                if doc_num and not doc_info['numero_documento']:
                    doc_info['numero_documento'] = doc_num

                # Check for date
                date = parse_date_br(text)
                if date and not doc_info['data_emissao']:
                    doc_info['data_emissao'] = date

                # Check for link
                link = cell.find('a')
                if link and link.get('href'):
                    doc_info['url_detalhe'] = link['href']

            # Generate PDF URL if we have document number and date
            if doc_info['numero_documento'] and doc_info['data_emissao']:
                from pdf_url_builder import build_pdf_url, get_default_idocmn

                # Get idocmn based on document type
                idocmn = get_default_idocmn(doc_info.get('tipo_documento', 'DOCUMENTO'))

                # Build the direct PDF URL
                doc_info['url_pdf'] = build_pdf_url(
                    idocmn=idocmn,
                    ndocmn=doc_info['numero_documento'],
                    data_emissao=doc_info['data_emissao'],
                    versao="01"
                )

                # Also store the authentication URL for fallback
                doc_info['url_autenticidade'] = f"{BASE_URL_AUTENTICIDADE}/autentica.php?idocmn={idocmn}&ndocmn={doc_info['numero_documento']}"
                return doc_info
            elif doc_info['numero_documento']:
                # If we don't have date, at least store authentication URL
                idocmn = get_default_idocmn(doc_info.get('tipo_documento', 'DOCUMENTO'))
                doc_info['url_autenticidade'] = f"{BASE_URL_AUTENTICIDADE}/autentica.php?idocmn={idocmn}&ndocmn={doc_info['numero_documento']}"
                doc_info['url_pdf'] = ''  # Will be filled later
                return doc_info

        except Exception as e:
            logger.debug(f"Error parsing document row: {e}")

        return None

    def parse_document_element(self, elem, company_info: Dict) -> Optional[Dict]:
        """Parse a non-table element containing document info"""
        try:
            parent = elem.parent
            text = parent.get_text()

            if TARGET_DOC_TYPE not in text:
                return None

            doc_info = {
                'numero_documento': extract_document_number(text) or '',
                'tipo_documento': TARGET_DOC_TYPE,
                'data_emissao': parse_date_br(text) or '',
                'url_detalhe': '',
                'url_pdf': '',
                'status_pdf': 'pending',
                'cnpj': company_info.get('cnpj', ''),
                'razao_social': company_info.get('razao_social', '')
            }

            # Look for link
            link = parent.find('a')
            if link and link.get('href'):
                doc_info['url_detalhe'] = link['href']

            # Generate PDF URL if we have document number and date
            if doc_info['numero_documento'] and doc_info['data_emissao']:
                from pdf_url_builder import build_pdf_url, get_default_idocmn

                # Get idocmn based on document type
                idocmn = get_default_idocmn(doc_info.get('tipo_documento', 'DOCUMENTO'))

                # Build the direct PDF URL
                doc_info['url_pdf'] = build_pdf_url(
                    idocmn=idocmn,
                    ndocmn=doc_info['numero_documento'],
                    data_emissao=doc_info['data_emissao'],
                    versao="01"
                )

                # Also store the authentication URL for fallback
                doc_info['url_autenticidade'] = f"{BASE_URL_AUTENTICIDADE}/autentica.php?idocmn={idocmn}&ndocmn={doc_info['numero_documento']}"
                return doc_info
            elif doc_info['numero_documento']:
                # If we don't have date, at least store authentication URL
                idocmn = get_default_idocmn(doc_info.get('tipo_documento', 'DOCUMENTO'))
                doc_info['url_autenticidade'] = f"{BASE_URL_AUTENTICIDADE}/autentica.php?idocmn={idocmn}&ndocmn={doc_info['numero_documento']}"
                doc_info['url_pdf'] = ''  # Will be filled later
                return doc_info

        except Exception as e:
            logger.debug(f"Error parsing document element: {e}")

        return None

    def process_url_list(self, urls: List[str]) -> int:
        """Process a list of detail page URLs"""
        from config import CSV_EMPRESAS, CSV_CADRI_DOCS
        import pandas as pd

        total_docs = 0

        for i, url in enumerate(urls, 1):
            logger.info(f"Processing {i}/{len(urls)}: {url}")

            # Scrape page
            company_info, documents = self.scrape_detail_page(url)

            # Save company info
            if company_info and company_info.get('cnpj'):
                df_company = pd.DataFrame([company_info])
                CSVStore.upsert(df_company, CSV_EMPRESAS, keys=['cnpj'])

            # Save documents
            if documents:
                df_docs = pd.DataFrame(documents)
                CSVStore.upsert(df_docs, CSV_CADRI_DOCS, keys=['numero_documento'])
                total_docs += len(documents)

        logger.info(f"Processed {len(urls)} URLs, found {total_docs} CADRI documents")
        return total_docs

    def _save_debug_html(self, html_content: str, url: str, filename: str):
        """Save HTML content for debugging"""
        import hashlib
        from datetime import datetime

        if not self.debug_dir:
            return

        # Create safe filename
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_filename = f"{filename}_{url_hash}_{timestamp}.html"

        html_file = self.debug_dir / safe_filename
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(f"<!-- URL: {url} -->\n")
            f.write(f"<!-- Saved: {datetime.now().isoformat()} -->\n")
            f.write(html_content)

        logger.debug(f"Saved debug HTML: {html_file}")

    def _save_debug_json(self, data: dict, filename: str):
        """Save JSON data for debugging"""
        import json
        from datetime import datetime

        if not self.debug_dir:
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_file = self.debug_dir / f"{filename}_{timestamp}.json"

        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

        logger.debug(f"Saved debug JSON: {json_file}")

    def analyze_page_structure(self, soup) -> dict:
        """Analyze page structure for debugging"""
        analysis = {
            'title': soup.title.string if soup.title else None,
            'total_elements': len(soup.find_all()),
            'tables': len(soup.find_all('table')),
            'forms': len(soup.find_all('form')),
            'links': len(soup.find_all('a')),
            'inputs': len(soup.find_all('input')),
            'text_mentions': {}
        }

        # Analyze text content
        text_content = soup.get_text().lower()
        keywords = ['cadri', 'residuo', 'resíduo', 'certificado', 'cert', 'documento', 'movimentacao', 'movimentação']

        for keyword in keywords:
            analysis['text_mentions'][keyword] = text_content.count(keyword)

        # Table analysis
        tables = soup.find_all('table')
        table_info = []

        for i, table in enumerate(tables):
            headers = table.find_all('th')
            rows = table.find_all('tr')

            table_data = {
                'index': i,
                'headers': [h.get_text().strip() for h in headers],
                'rows': len(rows),
                'has_document_header': any('documento' in h.get_text().lower() for h in headers)
            }
            table_info.append(table_data)

        analysis['table_details'] = table_info

        if self.debug_mode and self.debug_dir:
            self._save_debug_json(analysis, "page_analysis")

        return analysis


def main():
    """Test detail scraping"""
    # Test URL (would come from list scraper)
    test_url = "https://licenciamento.cetesb.sp.gov.br/cetesb/processo_resultado2.asp?cgc=12345678000100"

    with DetailScraper() as scraper:
        company, docs = scraper.scrape_detail_page(test_url)

        print(f"Company: {company}")
        print(f"Documents found: {len(docs)}")
        for doc in docs:
            print(f"  - {doc['numero_documento']}: {doc['tipo_documento']}")


if __name__ == "__main__":
    main()