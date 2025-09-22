#!/usr/bin/env python3
"""
Enhanced Results Page Extractor for CETESB

Extrai dados completos das páginas de resultado da CETESB, incluindo:
- Dados completos do cadastramento (endereço, CNPJ, atividade)
- Tabela de documentos quando disponível
- Informações estruturadas conforme mostrado na interface web

Baseado na análise da estrutura real das páginas de resultado.
"""

import re
from typing import Dict, List, Tuple, Optional
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin, urlparse, parse_qs
from dataclasses import dataclass

from utils_text import clean_cnpj, normalize_text, extract_document_number, parse_date_br
from config import BASE_URL_LICENCIAMENTO, BASE_URL_AUTENTICIDADE, TARGET_DOC_TYPE
from logging_conf import logger


@dataclass
class CompanyData:
    """Complete company data structure"""
    cnpj: str = ""
    razao_social: str = ""
    logradouro: str = ""
    complemento: str = ""
    bairro: str = ""
    municipio: str = ""
    uf: str = ""
    cep: str = ""
    numero_cadastro_cetesb: str = ""
    descricao_atividade: str = ""
    numero_s_numero: str = ""
    url_detalhe: str = ""
    data_source: str = "results_page"


@dataclass
class DocumentData:
    """Document data from results table"""
    numero_documento: str = ""
    tipo_documento: str = ""
    cnpj: str = ""
    razao_social: str = ""
    data_emissao: str = ""
    url_detalhe: str = ""
    url_pdf: str = ""
    status_pdf: str = "pending"
    pdf_hash: str = ""
    sd_numero: str = ""
    data_sd: str = ""
    numero_processo: str = ""
    objeto_solicitacao: str = ""
    situacao: str = ""
    data_desde: str = ""
    data_source: str = "results_page"


class ResultsPageExtractor:
    """Enhanced extractor for CETESB results pages"""

    # Padrões comuns de labels dos dados do cadastramento
    LABEL_PATTERNS = {
        'razao_social': [
            'razão social', 'razao social', 'empresa', 'nome'
        ],
        'logradouro': [
            'logradouro', 'endereço', 'endereco', 'rua', 'avenida'
        ],
        'complemento': [
            'complemento', 'compl'
        ],
        'bairro': [
            'bairro', 'distrito'
        ],
        'municipio': [
            'município', 'municipio', 'cidade'
        ],
        'uf': [
            'uf', 'estado'
        ],
        'cep': [
            'cep'
        ],
        'cnpj': [
            'cnpj', 'cgc'
        ],
        'numero_cadastro_cetesb': [
            'nº do cadastro na cetesb', 'numero do cadastro', 'cadastro cetesb', 'n° do cadastro'
        ],
        'descricao_atividade': [
            'descrição da atividade', 'descricao da atividade', 'atividade'
        ],
        'numero_s_numero': [
            'nº s/nº', 'n° s/n°', 'numero s/numero'
        ]
    }

    def __init__(self, html_content: str, source_url: str = ""):
        self.soup = BeautifulSoup(html_content, 'html.parser')
        self.source_url = source_url
        self.page_text = self.soup.get_text()

    def extract_all_data(self) -> Tuple[CompanyData, List[DocumentData]]:
        """Extract all available data from the page"""
        logger.debug(f"Extracting data from results page: {self.source_url}")

        # Detect page type
        page_type = self._detect_page_type()
        logger.debug(f"Detected page type: {page_type}")

        if page_type == "company_details":
            # Página de detalhes de uma empresa específica
            company = self._extract_company_details()
            documents = self._extract_documents_table()
        elif page_type == "search_results":
            # Página de lista de resultados de busca
            company = CompanyData()  # Empty for search results
            documents = []
            # For search results, we'll handle this differently in scrape_list.py
        else:
            # Unknown page type - try extraction anyway
            logger.warning(f"Unknown page type detected, attempting extraction anyway")

            # Try to extract company details even for unknown pages
            company = self._extract_company_details()

            # Try to extract documents
            documents = self._extract_documents_table()

            # Log what we found
            if company.razao_social or company.cnpj:
                logger.info(f"Successfully extracted company data from unknown page type")
            if documents:
                logger.info(f"Successfully extracted {len(documents)} documents from unknown page type")

        return company, documents

    def _detect_page_type(self) -> str:
        """Detect the type of results page"""
        # Normalize text for better detection (remove extra spaces/newlines)
        normalized_text = ' '.join(self.page_text.split()).lower()

        # Check for "Dados do Cadastramento" section with flexible matching
        company_indicators = [
            'dados do cadastramento',
            'dados cadastramento',  # Without "do"
            'resultado da consulta',
            'razão social',
            'logradouro',
            'nº do cadastro na cetesb',
            'descrição da atividade'
        ]

        # Count how many indicators are present
        indicators_found = sum(1 for indicator in company_indicators if indicator in normalized_text)

        # If we find multiple indicators, it's likely a company details page
        if indicators_found >= 2:
            logger.debug(f"Detected company details page with {indicators_found} indicators")
            return "company_details"

        # Also check HTML structure for company details
        # Look for specific table structures or elements
        company_table_found = False
        for table in self.soup.find_all('table'):
            table_text = table.get_text().lower()
            # Check if table contains company data
            if 'razão' in table_text or 'logradouro' in table_text or 'cadastramento' in table_text:
                company_table_found = True
                break

        if company_table_found:
            logger.debug("Detected company details page by table structure")
            return "company_details"

        # Check for search results table with multiple companies
        tables = self.soup.find_all('table')
        for table in tables:
            headers = table.find_all('th')
            if headers and any('processo' in h.get_text().lower() for h in headers):
                return "search_results"

        # Additional check: if page has SD Nº and document table structure
        if 'sd nº' in normalized_text or 'data da sd' in normalized_text:
            logger.debug("Detected company details page by document table")
            return "company_details"

        return "unknown"

    def _extract_company_details(self) -> CompanyData:
        """Extract complete company details from the registration section"""
        company = CompanyData()
        company.url_detalhe = self.source_url

        logger.debug("Extracting company details from registration section")

        # Try to find the registration data section
        registration_section = self._find_registration_section()

        if registration_section:
            company = self._extract_from_section(registration_section)
        else:
            # Fallback: extract from entire page
            logger.debug("Registration section not found, trying page-wide extraction")
            company = self._extract_from_page()

        # Extract CNPJ from URL if not found in content
        if not company.cnpj and self.source_url:
            company.cnpj = self._extract_cnpj_from_url()

        # Set data source
        company.data_source = "results_page"

        logger.debug(f"Extracted company: {company.razao_social} (CNPJ: {company.cnpj})")
        return company

    def _find_registration_section(self) -> Optional[Tag]:
        """Find the registration data section"""
        # Look for table containing company data specifically
        # The CETESB format has the data in nested tables

        # First, look for tables with specific company data patterns
        tables = self.soup.find_all('table')
        best_table = None
        best_score = 0

        for table in tables:
            table_text = ' '.join(table.get_text().split()).lower()
            score = 0

            # Score based on presence of key fields
            if 'razão social' in table_text:
                score += 2
            if 'logradouro' in table_text:
                score += 2
            if 'município' in table_text or 'municipio' in table_text:
                score += 1
            if 'cnpj' in table_text:
                score += 1
            if 'cep' in table_text:
                score += 1
            if 'bairro' in table_text:
                score += 1
            if 'cadastro na cetesb' in table_text:
                score += 2
            if 'dados do cadastramento' in table_text or 'dados cadastramento' in table_text:
                score += 3

            if score > best_score:
                best_score = score
                best_table = table

        if best_table and best_score >= 3:
            logger.debug(f"Found registration section with score {best_score}")
            return best_table

        # Fallback: look for "Dados do Cadastramento" text (handle split text)
        for table in tables:
            table_text = table.get_text()
            # Normalize to handle split text
            normalized = ' '.join(table_text.split()).lower()
            if 'dados' in normalized and 'cadastramento' in normalized:
                # Check proximity of words
                if normalized.index('cadastramento') - normalized.index('dados') < 20:
                    logger.debug("Found registration section by proximity match")
                    return table

        return None

    def _extract_from_section(self, section: Tag) -> CompanyData:
        """Extract company data from a specific section"""
        company = CompanyData()

        # First try CETESB-specific extraction
        company = self._extract_cetesb_format(section)

        # If that didn't work well, try generic extraction
        if not company.razao_social:
            # Extract using label-value pairs
            for field, patterns in self.LABEL_PATTERNS.items():
                value = self._find_value_for_patterns(section, patterns)
                if value:
                    setattr(company, field, value)

        return company

    def _extract_cetesb_format(self, section: Tag) -> CompanyData:
        """Extract data in CETESB-specific format"""
        company = CompanyData()

        # Get all text content and normalize
        text = ' '.join(section.get_text().split())

        # CETESB uses format: "Label - VALUE"
        # Extract each field with specific patterns

        # Razão Social (handle line breaks in label)
        match = re.search(r'Razão\s+Social\s*[-–]\s*([^•]+?)(?=Nº|Logradouro|Complemento|$)', text, re.IGNORECASE)
        if match:
            company.razao_social = match.group(1).strip()

        # Logradouro
        match = re.search(r'Logradouro\s*[-–]\s*([^•]+?)(?=Nº|Complemento|CEP|$)', text, re.IGNORECASE)
        if match:
            company.logradouro = match.group(1).strip()

        # Número (might be after Logradouro)
        match = re.search(r'(?<!S/)Nº\s+(\d+)', text, re.IGNORECASE)
        if match:
            company.numero_s_numero = match.group(1).strip()

        # Complemento
        match = re.search(r'Complemento\s*[-–]\s*([^•]+?)(?=Bairro|CEP|CNPJ|$)', text, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if value and value != '-':
                company.complemento = value

        # Bairro
        match = re.search(r'Bairro\s*[-–]\s*([^•]+?)(?=CEP|Município|$)', text, re.IGNORECASE)
        if match:
            company.bairro = match.group(1).strip()

        # CEP
        match = re.search(r'CEP\s*[-–]\s*(\d{5}[-]?\d{3})', text, re.IGNORECASE)
        if match:
            company.cep = match.group(1).strip()

        # Município
        match = re.search(r'Município\s*[-–]\s*([^•]+?)(?=CNPJ|Nº do Cadastro|$)', text, re.IGNORECASE)
        if match:
            company.municipio = match.group(1).strip()

        # CNPJ
        match = re.search(r'CNPJ\s*[-–]\s*(\d{2}[.\d/\-]*\d{2})', text, re.IGNORECASE)
        if match:
            company.cnpj = clean_cnpj(match.group(1).strip())

        # Número do Cadastro na CETESB
        match = re.search(r'Nº\s+do\s+Cadastro\s+na\s+CETESB\s*[-–]\s*([\d\-]+)', text, re.IGNORECASE)
        if match:
            company.numero_cadastro_cetesb = match.group(1).strip()

        # Descrição da Atividade
        match = re.search(r'Descrição\s+da\s+Atividade\s*[-–]\s*([^•]+?)(?=SD\s+Nº|$)', text, re.IGNORECASE)
        if match:
            company.descricao_atividade = match.group(1).strip()

        return company

    def _extract_from_page(self) -> CompanyData:
        """Extract company data from entire page as fallback"""
        company = CompanyData()

        # Use the whole page as source
        for field, patterns in self.LABEL_PATTERNS.items():
            value = self._find_value_for_patterns(self.soup, patterns)
            if value:
                setattr(company, field, value)

        return company

    def _find_value_for_patterns(self, container: Tag, patterns: List[str]) -> str:
        """Find value for field using multiple label patterns"""
        for pattern in patterns:
            value = self._find_value_for_pattern(container, pattern)
            if value:
                return value
        return ""

    def _find_value_for_pattern(self, container: Tag, pattern: str) -> str:
        """Find value for a specific label pattern"""
        # Strategy 1: Look for label followed by value in CETESB format
        # Format: "Label - Value" (most common in CETESB pages)
        text_content = container.get_text()

        # Clean up text content (normalize spaces)
        text_content = ' '.join(text_content.split())

        # Pattern for "Label - Value" format (CETESB specific)
        # The label might be in bold/colored text, followed by - and then the value
        regex_patterns = [
            rf'{re.escape(pattern)}\s*[-–]\s*([^A-Z\n\r\t]+?)(?=\s*(?:[A-Z][a-zÀ-ú]*\s*[-–]|$))',  # Standard pattern
            rf'{re.escape(pattern)}\s*[-–]\s*([^\n\r\t]+?)$',  # Value at end of text
            rf'{re.escape(pattern)}\s*[-–]\s*([^•]+?)(?=•|$)',  # Value until bullet point
        ]

        for regex_pattern in regex_patterns:
            match = re.search(regex_pattern, text_content, re.IGNORECASE | re.MULTILINE)
            if match:
                value = match.group(1).strip()
                # Clean up the value
                value = re.sub(r'\s+', ' ', value)  # Normalize spaces
                value = value.rstrip(' -')  # Remove trailing dash
                if value and value != '-' and not value.startswith('<'):  # Not empty or HTML tag
                    logger.debug(f"Found {pattern}: {value}")
                    return self._clean_extracted_value(value)

        # Strategy 2: Look for label element followed by value element
        label_elem = container.find(string=lambda x: x and pattern.lower() in x.lower())
        if label_elem:
            # Try different strategies to find the associated value
            value = self._find_associated_value(label_elem)
            if value:
                logger.debug(f"Found {pattern} via element: {value}")
                return self._clean_extracted_value(value)

        # Strategy 3: Look in table cells
        if container.name == 'table':
            value = self._find_in_table_cells(container, pattern)
            if value:
                logger.debug(f"Found {pattern} in table: {value}")
                return self._clean_extracted_value(value)

        return ""

    def _find_associated_value(self, label_elem) -> str:
        """Find value associated with a label element"""
        parent = label_elem.parent

        # Strategy 1: Next sibling
        if parent and parent.next_sibling:
            sibling = parent.next_sibling
            if hasattr(sibling, 'get_text'):
                value = sibling.get_text().strip()
                if value and value != '-':
                    return value

        # Strategy 2: Parent's next sibling
        if parent and parent.parent and parent.parent.next_sibling:
            sibling = parent.parent.next_sibling
            if hasattr(sibling, 'get_text'):
                value = sibling.get_text().strip()
                if value and value != '-':
                    return value

        # Strategy 3: Look in same table row
        row = label_elem.parent
        while row and row.name != 'tr':
            row = row.parent

        if row:
            cells = row.find_all(['td', 'th'])
            for i, cell in enumerate(cells):
                if label_elem in cell.descendants:
                    # Value might be in next cell
                    if i + 1 < len(cells):
                        value = cells[i + 1].get_text().strip()
                        if value and value != '-':
                            return value

        return ""

    def _find_in_table_cells(self, table: Tag, pattern: str) -> str:
        """Find value in table cells"""
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            for i, cell in enumerate(cells):
                cell_text = cell.get_text().strip()
                if pattern.lower() in cell_text.lower():
                    # Value might be in same cell (after colon) or next cell
                    if ':' in cell_text or '-' in cell_text:
                        # Same cell: "Label: Value"
                        parts = re.split(r'[-:]', cell_text, 1)
                        if len(parts) > 1:
                            value = parts[1].strip()
                            if value and value != '-':
                                return value
                    elif i + 1 < len(cells):
                        # Next cell
                        value = cells[i + 1].get_text().strip()
                        if value and value != '-':
                            return value
        return ""

    def _clean_extracted_value(self, value: str) -> str:
        """Clean and normalize extracted value"""
        # Remove extra whitespace
        value = ' '.join(value.split())

        # Remove common suffixes/prefixes that might be picked up
        value = re.sub(r'^[-:\s]+|[-:\s]+$', '', value)

        # Handle specific cases
        if value == '-' or value.lower() in ['n/a', 'não informado', '']:
            return ""

        return value.strip()

    def _extract_cnpj_from_url(self) -> str:
        """Extract CNPJ from URL parameters"""
        try:
            parsed = urlparse(self.source_url)
            params = parse_qs(parsed.query)
            cgc = params.get('cgc', [''])[0]
            return clean_cnpj(cgc)
        except:
            return ""

    def _extract_documents_table(self) -> List[DocumentData]:
        """Extract documents from the results table"""
        documents = []

        logger.debug("Extracting documents from table")

        # Find documents table
        doc_table = self._find_documents_table()
        if not doc_table:
            logger.debug("No documents table found")
            return documents

        # Extract table headers to determine column structure
        headers = doc_table.find_all('th')
        header_texts = [h.get_text().strip().lower() for h in headers]

        logger.debug(f"Found document table with headers: {header_texts}")

        # Map column indices
        col_map = self._map_document_columns(header_texts)

        # Extract data rows
        rows = doc_table.find_all('tr')[1:]  # Skip header row

        for row in rows:
            doc = self._extract_document_from_row(row, col_map)
            if doc and doc.numero_documento:
                documents.append(doc)

        logger.debug(f"Extracted {len(documents)} documents from table")
        return documents

    def _find_documents_table(self) -> Optional[Tag]:
        """Find the documents table in the page"""
        tables = self.soup.find_all('table')

        for table in tables:
            # Get normalized table text
            table_text = ' '.join(table.get_text().split()).lower()

            # Look for document table indicators in normalized text
            doc_indicators = [
                'sd nº', 'sd n°', 'data da sd',
                'nº processo', 'n° processo',
                'objeto da solicitação',
                'nº documento', 'n° documento',
                'situação', 'desde'
            ]

            # Count how many indicators are present
            indicators_found = sum(1 for indicator in doc_indicators if indicator in table_text)

            # If we find multiple indicators, it's likely the documents table
            if indicators_found >= 3:
                logger.debug(f"Found documents table with {indicators_found} indicators")
                return table

            # Also check if table has rows with dates and numbers (typical of document table)
            rows = table.find_all('tr')
            if len(rows) > 1:  # Has data rows
                # Check if first data row looks like document data
                first_data_row = rows[1] if len(rows) > 1 else None
                if first_data_row:
                    cells = first_data_row.find_all(['td', 'th'])
                    # Look for date patterns and number patterns
                    has_date = any(re.search(r'\d{2}/\d{2}/\d{4}', cell.get_text()) for cell in cells)
                    has_number = any(re.search(r'^\d{4,}$', cell.get_text().strip()) for cell in cells)

                    if has_date and has_number:
                        logger.debug("Found documents table by data pattern")
                        return table

        logger.debug("No documents table found")
        return None

    def _map_document_columns(self, headers: List[str]) -> Dict[str, int]:
        """Map document table columns to field names"""
        col_map = {}

        # Normalize headers for better matching
        normalized_headers = [' '.join(h.lower().split()) for h in headers]

        # Column mapping patterns (normalized)
        patterns = {
            'sd_numero': ['sd nº', 'sd n°', 'sd n', 'sd numero', 'sd no'],
            'data_sd': ['data da sd', 'data sd'],
            'numero_processo': ['nº processo', 'n° processo', 'processo', 'numero processo', 'no processo'],
            'objeto_solicitacao': ['objeto da solicitação', 'objeto da solicitacao', 'objeto', 'solicitação', 'solicitacao'],
            'numero_documento': ['nº documento', 'n° documento', 'documento', 'numero documento', 'no documento'],
            'situacao': ['situação', 'situacao', 'status'],
            'data_desde': ['desde', 'data desde']
        }

        for field, field_patterns in patterns.items():
            for i, header in enumerate(normalized_headers):
                # Check if any pattern matches
                for pattern in field_patterns:
                    if pattern in header:
                        col_map[field] = i
                        logger.debug(f"Mapped {field} to column {i} ('{headers[i]}')")
                        break
                if field in col_map:  # Break outer loop if found
                    break

        logger.debug(f"Column mapping: {col_map}")
        return col_map

    def _extract_document_from_row(self, row: Tag, col_map: Dict[str, int]) -> Optional[DocumentData]:
        """Extract document data from table row"""
        cells = row.find_all(['td', 'th'])

        if len(cells) < 3:  # Need at least a few cells
            return None

        doc = DocumentData()
        doc.data_source = "results_page"

        # Extract data based on column mapping
        for field, col_index in col_map.items():
            if col_index < len(cells):
                value = cells[col_index].get_text().strip()
                if value and value != '-':
                    setattr(doc, field, value)

        # If no column mapping worked, try flexible extraction
        if not doc.numero_documento:
            doc = self._extract_document_flexible(row)

        # Extract document number if not found
        if not doc.numero_documento:
            row_text = row.get_text()
            doc_num = extract_document_number(row_text)
            if doc_num:
                doc.numero_documento = doc_num

        # Identify document type - accept various types, not just CADRI
        row_text = row.get_text()
        row_text_lower = row_text.lower()

        # Check for different document types
        if TARGET_DOC_TYPE.lower() in row_text_lower:
            doc.tipo_documento = TARGET_DOC_TYPE
        elif 'cert' in row_text_lower and 'dispensa' in row_text_lower:
            doc.tipo_documento = "CERT DE DISPENSA DE LICENÇA"
        elif 'licença' in row_text_lower or 'licenca' in row_text_lower:
            doc.tipo_documento = self._extract_document_type_from_row(row)
        elif any(term in row_text_lower for term in ['cert', 'certificado', 'cadri']):
            doc.tipo_documento = self._extract_document_type_from_row(row)
        else:
            # Try to extract the full document type from the row
            doc.tipo_documento = self._extract_document_type_from_row(row)

        # Extract PDF URL from links in cells first
        cells = row.find_all(['td', 'th'])
        for cell in cells:
            pdf_url = self._extract_pdf_url_from_cell(cell)
            if pdf_url:
                doc.url_pdf = pdf_url
                logger.debug(f"Found PDF URL in cell: {pdf_url}")
                break

        # No fallback generation - only use real URLs from HTML
        if not doc.url_pdf:
            logger.debug(f"No PDF URL found in HTML for document {doc.numero_documento}")
            doc.url_pdf = ""  # Keep empty if no real link exists

        # Parse dates
        if doc.data_sd:
            parsed_date = parse_date_br(doc.data_sd)
            if parsed_date:
                doc.data_sd = parsed_date
                doc.data_emissao = parsed_date  # Use SD date as emission date

        if doc.data_desde:
            parsed_date = parse_date_br(doc.data_desde)
            if parsed_date:
                doc.data_desde = parsed_date

        return doc if doc.numero_documento else None

    def _extract_document_type_from_row(self, row: Tag) -> str:
        """Extract document type from table row"""
        cells = row.find_all(['td', 'th'])

        # Look for cell that contains document type
        for cell in cells:
            cell_text = cell.get_text().strip()

            # Skip cells with just numbers or dates
            if re.match(r'^[\d/\-]+$', cell_text):
                continue

            # Check if this cell contains document type keywords
            if any(keyword in cell_text.lower() for keyword in ['cert', 'licen', 'dispensa', 'parecer', 'auto']):
                # This is likely the document type
                return cell_text

        # Fallback to generic type
        return "DOCUMENTO"

    def _extract_pdf_url_from_cell(self, cell: Tag) -> Optional[str]:
        """Extrai APENAS URLs reais de PDF do HTML que apontam para autenticidade CETESB"""
        link = cell.find('a')
        if link and link.get('href'):
            url = link.get('href')

            # Validar se é realmente um link de autenticidade CETESB
            if 'autenticidade.cetesb' in url or 'autentica.php' in url:
                # Normalizar URL
                url = url.replace('&amp;', '&')
                if url.startswith('http://'):
                    url = url.replace('http://', 'https://')
                logger.debug(f"Found valid CETESB PDF URL: {url}")
                return url
            else:
                logger.debug(f"Ignoring non-CETESB URL: {url}")
        return None

    def _extract_document_flexible(self, row: Tag) -> DocumentData:
        """Flexible document extraction without column mapping"""
        doc = DocumentData()
        doc.data_source = "results_page"

        cells = row.find_all(['td', 'th'])

        # Extract numbers based on position in CETESB format:
        # Col 0: SD Nº, Col 4: Nº Documento
        numbers_found = []
        pdf_url_found = None

        # First pass: collect all numbers, their positions, and look for PDF links
        for i, cell in enumerate(cells):
            text = cell.get_text().strip()

            # Look for PDF URL in this cell first
            pdf_url = self._extract_pdf_url_from_cell(cell)
            if pdf_url:
                pdf_url_found = pdf_url
                logger.debug(f"Found PDF URL in cell {i}: {pdf_url}")

            # Look for document number (sequence of digits)
            if re.match(r'^\d{4,}$', text):
                numbers_found.append((i, text))

        # Assign PDF URL if found
        if pdf_url_found:
            doc.url_pdf = pdf_url_found

        # Assign based on CETESB table structure
        for pos, number in numbers_found:
            if pos == 0:  # First column = SD Nº
                doc.sd_numero = number
            elif pos == 4:  # Fifth column = Nº Documento
                doc.numero_documento = number
            elif not doc.sd_numero:  # Fallback: first number is SD
                doc.sd_numero = number
            elif not doc.numero_documento:  # Fallback: second number is Doc
                doc.numero_documento = number

        # Continue with other patterns
        for cell in cells:
            text = cell.get_text().strip()

            # Look for dates
            if re.match(r'\d{2}/\d{2}/\d{4}', text):
                if not doc.data_sd:
                    doc.data_sd = text
                elif not doc.data_desde:
                    doc.data_desde = text

            # Look for document type
            elif TARGET_DOC_TYPE.lower() in text.lower():
                doc.tipo_documento = TARGET_DOC_TYPE
                doc.objeto_solicitacao = text

            # Look for status indicators
            elif any(status in text.lower() for status in ['emitida', 'arquivada', 'pendente', 'cancelada']):
                doc.situacao = text

        # No fallback generation - only use real URLs from HTML
        if not doc.url_pdf:
            logger.debug(f"No PDF URL found in HTML for document {doc.numero_documento}")
            doc.url_pdf = ""  # Keep empty if no real link exists

        return doc


def extract_company_and_documents(html_content: str, source_url: str = "") -> Tuple[Dict, List[Dict]]:
    """
    Main function to extract company and documents data from results page

    Returns:
        Tuple of (company_dict, documents_list)
    """
    extractor = ResultsPageExtractor(html_content, source_url)
    company, documents = extractor.extract_all_data()

    # Convert to dictionaries for compatibility
    company_dict = {
        'cnpj': company.cnpj,
        'razao_social': company.razao_social,
        'logradouro': company.logradouro,
        'complemento': company.complemento,
        'bairro': company.bairro,
        'municipio': company.municipio,
        'uf': company.uf,
        'cep': company.cep,
        'numero_cadastro_cetesb': company.numero_cadastro_cetesb,
        'descricao_atividade': company.descricao_atividade,
        'numero_s_numero': company.numero_s_numero,
        'url_detalhe': company.url_detalhe,
        'data_source': company.data_source
    }

    documents_list = []
    for doc in documents:
        doc_dict = {
            'numero_documento': doc.numero_documento,
            'tipo_documento': doc.tipo_documento,
            'cnpj': doc.cnpj or company.cnpj,  # Use company CNPJ if not in document
            'razao_social': doc.razao_social or company.razao_social,
            'data_emissao': doc.data_emissao,
            'url_detalhe': doc.url_detalhe or company.url_detalhe,
            'url_pdf': doc.url_pdf,
            'status_pdf': doc.status_pdf,
            'pdf_hash': doc.pdf_hash,
            'sd_numero': doc.sd_numero,
            'data_sd': doc.data_sd,
            'numero_processo': doc.numero_processo,
            'objeto_solicitacao': doc.objeto_solicitacao,
            'situacao': doc.situacao,
            'data_desde': doc.data_desde,
            'data_source': doc.data_source
        }
        documents_list.append(doc_dict)

    return company_dict, documents_list


def test_extractor():
    """Test the extractor with sample HTML"""
    sample_html = """
    <html>
    <body>
        <h3>Resultado da Consulta</h3>
        <h4>Dados do Cadastramento</h4>
        <table>
            <tr><td>Razão Social:</td><td>EMPRESA TESTE LTDA</td></tr>
            <tr><td>CNPJ:</td><td>12.345.678/0001-90</td></tr>
            <tr><td>Logradouro:</td><td>RUA TESTE, 123</td></tr>
            <tr><td>Município:</td><td>SÃO PAULO</td></tr>
        </table>

        <table>
            <tr>
                <th>SD Nº</th><th>Data da SD</th><th>Nº Processo</th><th>Objeto da Solicitação</th><th>Nº Documento</th><th>Situação</th><th>Desde</th>
            </tr>
            <tr>
                <td>123456</td><td>01/01/2024</td><td>987654</td><td>CERT MOV RESIDUOS INT AMB</td><td>555666</td><td>Emitida</td><td>01/01/2024</td>
            </tr>
        </table>
    </body>
    </html>
    """

    company, documents = extract_company_and_documents(sample_html, "http://test.com")

    print("Company extracted:")
    for key, value in company.items():
        if value:
            print(f"  {key}: {value}")

    print(f"\nDocuments extracted: {len(documents)}")
    for doc in documents:
        print(f"  Doc {doc['numero_documento']}: {doc['tipo_documento']}")


if __name__ == "__main__":
    test_extractor()