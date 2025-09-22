#!/usr/bin/env python3
"""
Improved CADRI Document Detection Patterns

Este módulo contém versões melhoradas dos patterns de detecção
de documentos CADRI, com base na análise de páginas reais.
"""

import re
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup, Tag
from dataclasses import dataclass

from utils_text import extract_document_number, parse_date_br, normalize_text
from config import TARGET_DOC_TYPE, BASE_URL_AUTENTICIDADE
from logging_conf import logger


@dataclass
class DocumentPattern:
    """Pattern for detecting documents"""
    name: str
    description: str
    selector: str
    confidence: float
    extractor_func: str


class ImprovedDocumentExtractor:
    """Enhanced document extraction with multiple fallback patterns"""

    # Variações do tipo de documento para busca mais flexível
    DOCUMENT_TYPE_VARIATIONS = [
        "CERT MOV RESIDUOS INT AMB",
        "CERTIFICADO MOVIMENTACAO RESIDUOS",
        "CERTIFICADO DE MOVIMENTACAO",
        "CADRI",
        "CERT MOV RES INT AMB",
        "CERT MOVIMENTACAO",
        "CERTIFICADO RESIDUOS"
    ]

    # Padrões de tabela comuns
    TABLE_PATTERNS = [
        DocumentPattern(
            name="standard_table",
            description="Table with Documento/Tipo headers",
            selector="table:has(th:contains('documento'), th:contains('tipo'))",
            confidence=0.9,
            extractor_func="extract_from_standard_table"
        ),
        DocumentPattern(
            name="headerless_table",
            description="Table without headers but with document content",
            selector="table:contains('CADRI')",
            confidence=0.7,
            extractor_func="extract_from_headerless_table"
        ),
        DocumentPattern(
            name="nested_table",
            description="Nested tables with document info",
            selector="table table",
            confidence=0.6,
            extractor_func="extract_from_nested_table"
        )
    ]

    # Padrões de não-tabela
    NON_TABLE_PATTERNS = [
        DocumentPattern(
            name="div_list",
            description="Divs containing document information",
            selector="div:contains('CADRI')",
            confidence=0.5,
            extractor_func="extract_from_div"
        ),
        DocumentPattern(
            name="paragraph_list",
            description="Paragraphs with document info",
            selector="p:contains('CERT')",
            confidence=0.4,
            extractor_func="extract_from_paragraph"
        ),
        DocumentPattern(
            name="list_items",
            description="List items with documents",
            selector="li:contains('CADRI')",
            confidence=0.6,
            extractor_func="extract_from_list_item"
        )
    ]

    def __init__(self, soup: BeautifulSoup, company_info: Dict):
        self.soup = soup
        self.company_info = company_info
        self.page_text = soup.get_text().lower()

    def extract_all_documents(self) -> List[Dict]:
        """Extract documents using all available patterns"""
        all_documents = []
        extraction_log = []

        # First, analyze page structure
        page_analysis = self._analyze_page_structure()
        logger.debug(f"Page analysis: {page_analysis}")

        # Try table patterns first (higher confidence)
        for pattern in self.TABLE_PATTERNS:
            try:
                docs = self._apply_pattern(pattern)
                if docs:
                    extraction_log.append(f"Pattern '{pattern.name}' found {len(docs)} documents")
                    all_documents.extend(docs)
                else:
                    extraction_log.append(f"Pattern '{pattern.name}' found no documents")
            except Exception as e:
                extraction_log.append(f"Pattern '{pattern.name}' failed: {e}")
                logger.debug(f"Pattern {pattern.name} failed: {e}")

        # If no documents found in tables, try non-table patterns
        if not all_documents:
            logger.info("No documents found in tables, trying non-table patterns")

            for pattern in self.NON_TABLE_PATTERNS:
                try:
                    docs = self._apply_pattern(pattern)
                    if docs:
                        extraction_log.append(f"Pattern '{pattern.name}' found {len(docs)} documents")
                        all_documents.extend(docs)
                    else:
                        extraction_log.append(f"Pattern '{pattern.name}' found no documents")
                except Exception as e:
                    extraction_log.append(f"Pattern '{pattern.name}' failed: {e}")
                    logger.debug(f"Pattern {pattern.name} failed: {e}")

        # Remove duplicates based on document number
        unique_documents = self._deduplicate_documents(all_documents)

        # Log extraction summary
        logger.info(f"Extraction summary: {len(unique_documents)} unique documents found")
        for log_entry in extraction_log:
            logger.debug(log_entry)

        return unique_documents

    def _analyze_page_structure(self) -> Dict:
        """Analyze page structure to guide extraction strategy"""
        analysis = {
            'has_tables': len(self.soup.find_all('table')) > 0,
            'table_count': len(self.soup.find_all('table')),
            'has_forms': len(self.soup.find_all('form')) > 0,
            'cadri_mentions': self.page_text.count('cadri'),
            'residuo_mentions': self.page_text.count('resid'),
            'cert_mentions': self.page_text.count('cert'),
            'target_type_exact': any(var.lower() in self.page_text for var in self.DOCUMENT_TYPE_VARIATIONS),
            'potential_doc_numbers': len(re.findall(r'\b\d{4,10}\b', self.page_text))
        }

        return analysis

    def _apply_pattern(self, pattern: DocumentPattern) -> List[Dict]:
        """Apply a specific extraction pattern"""
        extractor_method = getattr(self, pattern.extractor_func, None)
        if not extractor_method:
            logger.warning(f"Extractor method {pattern.extractor_func} not found")
            return []

        return extractor_method(pattern)

    def extract_from_standard_table(self, pattern: DocumentPattern) -> List[Dict]:
        """Extract from tables with standard headers"""
        documents = []
        tables = self.soup.find_all('table')

        for table in tables:
            headers = table.find_all('th')
            header_texts = [h.get_text().strip().lower() for h in headers]

            # Check if table has document-related headers
            has_doc_header = any('documento' in header or 'tipo' in header for header in header_texts)

            if has_doc_header:
                logger.debug(f"Found table with document headers: {header_texts}")

                # Determine column indices
                doc_number_col = self._find_column_index(header_texts, ['numero', 'número', 'n°', 'document'])
                type_col = self._find_column_index(header_texts, ['tipo', 'type', 'categoria'])
                date_col = self._find_column_index(header_texts, ['data', 'date', 'emissao', 'emissão'])

                rows = table.find_all('tr')[1:]  # Skip header

                for row in rows:
                    doc = self._extract_from_table_row(row, doc_number_col, type_col, date_col)
                    if doc:
                        documents.append(doc)

        return documents

    def extract_from_headerless_table(self, pattern: DocumentPattern) -> List[Dict]:
        """Extract from tables without clear headers"""
        documents = []
        tables = self.soup.find_all('table')

        for table in tables:
            table_text = table.get_text().lower()

            # Check if table contains relevant content
            if any(var.lower() in table_text for var in self.DOCUMENT_TYPE_VARIATIONS):
                logger.debug("Found headerless table with document content")

                rows = table.find_all('tr')

                for row in rows:
                    row_text = row.get_text()

                    # Check if row contains document type
                    if any(var.lower() in row_text.lower() for var in self.DOCUMENT_TYPE_VARIATIONS):
                        doc = self._extract_from_flexible_row(row)
                        if doc:
                            documents.append(doc)

        return documents

    def extract_from_nested_table(self, pattern: DocumentPattern) -> List[Dict]:
        """Extract from nested table structures"""
        documents = []

        # Find tables inside other tables
        outer_tables = self.soup.find_all('table')
        for outer_table in outer_tables:
            inner_tables = outer_table.find_all('table')

            for inner_table in inner_tables:
                table_text = inner_table.get_text().lower()

                if any(var.lower() in table_text for var in self.DOCUMENT_TYPE_VARIATIONS):
                    logger.debug("Found nested table with document content")

                    rows = inner_table.find_all('tr')
                    for row in rows:
                        doc = self._extract_from_flexible_row(row)
                        if doc:
                            documents.append(doc)

        return documents

    def extract_from_div(self, pattern: DocumentPattern) -> List[Dict]:
        """Extract from div elements"""
        documents = []

        # Find divs containing document information
        for var in self.DOCUMENT_TYPE_VARIATIONS:
            divs = self.soup.find_all('div', string=lambda x: x and var.lower() in x.lower())

            for div in divs:
                doc = self._extract_from_element(div, 'div')
                if doc:
                    documents.append(doc)

        return documents

    def extract_from_paragraph(self, pattern: DocumentPattern) -> List[Dict]:
        """Extract from paragraph elements"""
        documents = []

        paragraphs = self.soup.find_all('p')
        for p in paragraphs:
            p_text = p.get_text().lower()

            if any(var.lower() in p_text for var in self.DOCUMENT_TYPE_VARIATIONS):
                doc = self._extract_from_element(p, 'paragraph')
                if doc:
                    documents.append(doc)

        return documents

    def extract_from_list_item(self, pattern: DocumentPattern) -> List[Dict]:
        """Extract from list item elements"""
        documents = []

        list_items = self.soup.find_all('li')
        for li in list_items:
            li_text = li.get_text().lower()

            if any(var.lower() in li_text for var in self.DOCUMENT_TYPE_VARIATIONS):
                doc = self._extract_from_element(li, 'list_item')
                if doc:
                    documents.append(doc)

        return documents

    def _find_column_index(self, headers: List[str], keywords: List[str]) -> Optional[int]:
        """Find column index for specific keywords"""
        for i, header in enumerate(headers):
            if any(keyword in header for keyword in keywords):
                return i
        return None

    def _extract_from_table_row(self, row: Tag, doc_col: int = None, type_col: int = None, date_col: int = None) -> Optional[Dict]:
        """Extract document from table row with known column structure"""
        cells = row.find_all(['td', 'th'])

        if len(cells) < 2:
            return None

        doc_info = {
            'numero_documento': '',
            'tipo_documento': '',
            'data_emissao': '',
            'url_detalhe': '',
            'url_pdf': '',
            'status_pdf': 'pending',
            'cnpj': self.company_info.get('cnpj', ''),
            'razao_social': self.company_info.get('razao_social', ''),
            'extraction_method': 'structured_table'
        }

        # Extract based on column positions
        if doc_col is not None and doc_col < len(cells):
            doc_num = extract_document_number(cells[doc_col].get_text())
            if doc_num:
                doc_info['numero_documento'] = doc_num

        if type_col is not None and type_col < len(cells):
            type_text = cells[type_col].get_text().strip()
            if any(var.lower() in type_text.lower() for var in self.DOCUMENT_TYPE_VARIATIONS):
                doc_info['tipo_documento'] = self._normalize_document_type(type_text)

        if date_col is not None and date_col < len(cells):
            date = parse_date_br(cells[date_col].get_text())
            if date:
                doc_info['data_emissao'] = date

        # If structure-based extraction failed, try flexible extraction
        if not doc_info['numero_documento'] or not doc_info['tipo_documento']:
            flexible_doc = self._extract_from_flexible_row(row)
            if flexible_doc:
                # Merge results, preferring structured when available
                for key, value in flexible_doc.items():
                    if not doc_info.get(key) and value:
                        doc_info[key] = value

        # Only return if we have at least document number and type
        if doc_info['numero_documento'] and doc_info['tipo_documento']:
            # Generate PDF URL
            doc_info['url_pdf'] = f"{BASE_URL_AUTENTICIDADE}/autentica.php?idocmn=12&ndocmn={doc_info['numero_documento']}"
            return doc_info

        return None

    def _extract_from_flexible_row(self, row: Tag) -> Optional[Dict]:
        """Extract document from row without knowing structure"""
        row_text = row.get_text()
        cells = row.find_all(['td', 'th'])

        doc_info = {
            'numero_documento': '',
            'tipo_documento': '',
            'data_emissao': '',
            'url_detalhe': '',
            'url_pdf': '',
            'status_pdf': 'pending',
            'cnpj': self.company_info.get('cnpj', ''),
            'razao_social': self.company_info.get('razao_social', ''),
            'extraction_method': 'flexible_row'
        }

        # Check if row contains document type
        doc_type_found = False
        for var in self.DOCUMENT_TYPE_VARIATIONS:
            if var.lower() in row_text.lower():
                doc_info['tipo_documento'] = self._normalize_document_type(var)
                doc_type_found = True
                break

        if not doc_type_found:
            return None

        # Extract document number
        doc_num = extract_document_number(row_text)
        if doc_num:
            doc_info['numero_documento'] = doc_num
        else:
            return None

        # Extract date
        date = parse_date_br(row_text)
        if date:
            doc_info['data_emissao'] = date

        # Look for links
        link = row.find('a')
        if link and link.get('href'):
            doc_info['url_detalhe'] = link['href']

        # Generate PDF URL
        doc_info['url_pdf'] = f"{BASE_URL_AUTENTICIDADE}/autentica.php?idocmn=12&ndocmn={doc_info['numero_documento']}"

        return doc_info

    def _extract_from_element(self, element: Tag, element_type: str) -> Optional[Dict]:
        """Extract document from generic element"""
        element_text = element.get_text()

        doc_info = {
            'numero_documento': '',
            'tipo_documento': '',
            'data_emissao': '',
            'url_detalhe': '',
            'url_pdf': '',
            'status_pdf': 'pending',
            'cnpj': self.company_info.get('cnpj', ''),
            'razao_social': self.company_info.get('razao_social', ''),
            'extraction_method': f'element_{element_type}'
        }

        # Check for document type
        doc_type_found = False
        for var in self.DOCUMENT_TYPE_VARIATIONS:
            if var.lower() in element_text.lower():
                doc_info['tipo_documento'] = self._normalize_document_type(var)
                doc_type_found = True
                break

        if not doc_type_found:
            return None

        # Extract document number
        doc_num = extract_document_number(element_text)
        if doc_num:
            doc_info['numero_documento'] = doc_num
        else:
            return None

        # Extract date
        date = parse_date_br(element_text)
        if date:
            doc_info['data_emissao'] = date

        # Look for links in element or parent
        link = element.find('a') or (element.parent.find('a') if element.parent else None)
        if link and link.get('href'):
            doc_info['url_detalhe'] = link['href']

        # Generate PDF URL
        doc_info['url_pdf'] = f"{BASE_URL_AUTENTICIDADE}/autentica.php?idocmn=12&ndocmn={doc_info['numero_documento']}"

        return doc_info

    def _normalize_document_type(self, type_text: str) -> str:
        """Normalize document type to standard format"""
        type_text = type_text.strip().upper()

        # Map variations to standard type
        if any(var.upper() in type_text for var in self.DOCUMENT_TYPE_VARIATIONS):
            return TARGET_DOC_TYPE

        return type_text

    def _deduplicate_documents(self, documents: List[Dict]) -> List[Dict]:
        """Remove duplicate documents based on document number"""
        seen_numbers = set()
        unique_docs = []

        for doc in documents:
            doc_number = doc.get('numero_documento', '')
            if doc_number and doc_number not in seen_numbers:
                seen_numbers.add(doc_number)
                unique_docs.append(doc)

        return unique_docs


# Integration function for existing scraper
def extract_cadri_documents_improved(soup: BeautifulSoup, company_info: Dict) -> List[Dict]:
    """
    Improved version of extract_cadri_documents using enhanced patterns

    This function can replace the existing extract_cadri_documents method
    """
    try:
        extractor = ImprovedDocumentExtractor(soup, company_info)
        documents = extractor.extract_all_documents()

        logger.info(f"Enhanced extraction found {len(documents)} CADRI documents")

        # Log extraction methods used
        methods = [doc.get('extraction_method', 'unknown') for doc in documents]
        method_counts = {method: methods.count(method) for method in set(methods)}
        logger.debug(f"Extraction methods used: {method_counts}")

        return documents

    except Exception as e:
        logger.error(f"Error in enhanced document extraction: {e}")
        return []


def test_patterns():
    """Test the improved patterns with sample HTML"""
    sample_html = """
    <html>
    <body>
        <table>
            <tr>
                <th>Número do Documento</th>
                <th>Tipo</th>
                <th>Data de Emissão</th>
            </tr>
            <tr>
                <td>123456</td>
                <td>CERT MOV RESIDUOS INT AMB</td>
                <td>01/01/2024</td>
            </tr>
        </table>

        <div>
            Documento CADRI número 789012 emitido em 02/02/2024
        </div>
    </body>
    </html>
    """

    soup = BeautifulSoup(sample_html, 'html.parser')
    company_info = {'cnpj': '12345678000100', 'razao_social': 'Empresa Teste'}

    documents = extract_cadri_documents_improved(soup, company_info)

    print(f"Found {len(documents)} documents:")
    for doc in documents:
        print(f"  - {doc['numero_documento']}: {doc['tipo_documento']} ({doc['extraction_method']})")


if __name__ == "__main__":
    test_patterns()