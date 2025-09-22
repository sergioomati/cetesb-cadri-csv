import fitz  # PyMuPDF
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import pandas as pd

from config import PDF_DIR, CSV_CADRI_ITEMS
from utils_text import (
    normalize_text, extract_quantity_unit, is_valid_residue_name,
    normalize_classe, create_pdf_search_patterns
)
from store_csv import CSVStore, get_unparsed_pdfs
from logging_conf import logger, metrics


class PDFParser:
    """Extract waste information from CADRI PDFs"""

    def __init__(self):
        self.patterns = create_pdf_search_patterns()

    def parse_pdf(self, pdf_path: Path) -> List[Dict]:
        """
        Parse a single PDF and extract waste items

        Args:
            pdf_path: Path to PDF file

        Returns:
            List of waste items with metadata
        """
        items = []
        numero_documento = pdf_path.stem  # filename without extension

        try:
            doc = fitz.open(pdf_path)

            for page_num, page in enumerate(doc, 1):
                text = page.get_text()

                # Try different extraction methods
                # 1. Look for structured table
                table_items = self.extract_from_table(text, page_num)
                items.extend(table_items)

                # 2. Look for labeled fields
                if not table_items:
                    field_items = self.extract_from_fields(text, page_num)
                    items.extend(field_items)

            doc.close()

            # Add document number to all items
            for item in items:
                item['numero_documento'] = numero_documento

            # Remove duplicates
            items = self.deduplicate_items(items)

            logger.info(f"Extracted {len(items)} items from {pdf_path.name}")
            metrics.increment('pdfs_parsed')

        except Exception as e:
            logger.error(f"Error parsing PDF {pdf_path}: {e}")
            metrics.increment('errors')

        return items

    def extract_from_table(self, text: str, page_num: int) -> List[Dict]:
        """Extract items from table format"""
        items = []

        # Look for table headers
        header_pattern = re.compile(
            r'res[ií]duo.*classe.*estado.*quantidade',
            re.IGNORECASE
        )

        if not header_pattern.search(text):
            return items

        # Split text into lines
        lines = text.split('\n')

        # Find header line
        header_idx = -1
        for i, line in enumerate(lines):
            if header_pattern.search(line):
                header_idx = i
                break

        if header_idx == -1:
            return items

        # Process lines after header
        for i in range(header_idx + 1, min(header_idx + 50, len(lines))):
            line = lines[i].strip()

            if not line or len(line) < 10:
                continue

            # Try to parse as table row
            item = self.parse_table_row(line, page_num, i)
            if item:
                items.append(item)

        return items

    def parse_table_row(self, line: str, page_num: int, line_num: int) -> Optional[Dict]:
        """Parse a single table row"""
        # Try different delimiters
        delimiters = ['\t', '|', '  ']  # tab, pipe, double space

        parts = None
        for delim in delimiters:
            if delim in line:
                parts = [p.strip() for p in line.split(delim) if p.strip()]
                if len(parts) >= 3:  # Need at least residue, class, state
                    break

        if not parts or len(parts) < 3:
            # Try regex pattern
            match = self.patterns['table_row'].search(line)
            if match:
                parts = match.groups()
            else:
                return None

        try:
            item = {
                'residuo': parts[0],
                'classe': normalize_classe(parts[1]) if len(parts) > 1 else '',
                'estado_fisico': parts[2].lower() if len(parts) > 2 else '',
                'quantidade': '',
                'unidade': '',
                'pagina_origem': page_num,
                'raw_fragment': line[:200]  # Store fragment for debugging
            }

            # Extract quantity if available
            if len(parts) > 3:
                qty, unit = extract_quantity_unit(parts[3])
                item['quantidade'] = qty
                item['unidade'] = unit

            # Validate
            if is_valid_residue_name(item['residuo']):
                return item

        except Exception as e:
            logger.debug(f"Error parsing table row: {e}")

        return None

    def extract_from_fields(self, text: str, page_num: int) -> List[Dict]:
        """Extract items from labeled fields"""
        items = []

        # Look for each field pattern
        residuo = self.find_field_value('residuo', text)
        classe = self.find_field_value('classe', text)
        estado = self.find_field_value('estado_fisico', text)
        quantidade_raw = self.find_field_value('quantidade', text)

        if residuo and is_valid_residue_name(residuo):
            qty, unit = extract_quantity_unit(quantidade_raw) if quantidade_raw else ('', '')

            item = {
                'residuo': residuo,
                'classe': normalize_classe(classe) if classe else '',
                'estado_fisico': estado.lower() if estado else '',
                'quantidade': qty,
                'unidade': unit,
                'pagina_origem': page_num,
                'raw_fragment': text[:500]
            }

            items.append(item)

        return items

    def find_field_value(self, field_name: str, text: str) -> Optional[str]:
        """Find value for a specific field using regex"""
        pattern = self.patterns.get(field_name)
        if not pattern:
            return None

        match = pattern.search(text)
        if match:
            return match.group(1).strip()

        return None

    def deduplicate_items(self, items: List[Dict]) -> List[Dict]:
        """Remove duplicate items based on key fields"""
        seen = set()
        unique_items = []

        for item in items:
            # Create unique key
            key = (
                item['residuo'],
                item['classe'],
                item['estado_fisico'],
                item['quantidade']
            )

            if key not in seen:
                seen.add(key)
                unique_items.append(item)

        return unique_items

    def parse_all_pending(self) -> int:
        """Parse all unparsed PDFs"""
        unparsed = get_unparsed_pdfs()

        if not unparsed:
            logger.info("No unparsed PDFs found")
            return 0

        logger.info(f"Found {len(unparsed)} unparsed PDFs")
        total_items = 0

        for numero in unparsed:
            pdf_path = PDF_DIR / f"{numero}.pdf"

            if not pdf_path.exists():
                logger.warning(f"PDF file not found: {pdf_path}")
                continue

            items = self.parse_pdf(pdf_path)

            if items:
                # Save to CSV
                df_items = pd.DataFrame(items)
                CSVStore.upsert(
                    df_items,
                    CSV_CADRI_ITEMS,
                    keys=['numero_documento', 'residuo', 'classe', 'estado_fisico', 'quantidade']
                )
                total_items += len(items)

        logger.info(f"Parsed {len(unparsed)} PDFs, extracted {total_items} items")
        return total_items


class PDFTableExtractor:
    """Advanced table extraction using PyMuPDF's table detection"""

    @staticmethod
    def extract_tables(pdf_path: Path) -> List[List[List[str]]]:
        """
        Extract tables from PDF using PyMuPDF's table finder

        Returns:
            List of tables, where each table is a list of rows
        """
        all_tables = []

        try:
            doc = fitz.open(pdf_path)

            for page in doc:
                # Find tables on page
                tabs = page.find_tables()

                for tab in tabs:
                    # Extract table data
                    table_data = []
                    for row in tab.extract():
                        # Clean row data
                        clean_row = [cell.strip() if cell else '' for cell in row]
                        if any(clean_row):  # Skip empty rows
                            table_data.append(clean_row)

                    if table_data:
                        all_tables.append(table_data)

            doc.close()

        except Exception as e:
            logger.error(f"Error extracting tables from {pdf_path}: {e}")

        return all_tables


def main():
    """Test PDF parsing"""
    parser = PDFParser()

    # Test with a specific PDF
    test_pdfs = list(PDF_DIR.glob("*.pdf"))[:5]

    for pdf_path in test_pdfs:
        print(f"\nParsing: {pdf_path.name}")
        items = parser.parse_pdf(pdf_path)

        for item in items[:3]:  # Show first 3 items
            print(f"  Resíduo: {item['residuo']}")
            print(f"  Classe: {item['classe']}")
            print(f"  Estado: {item['estado_fisico']}")
            print(f"  Quantidade: {item['quantidade']} {item['unidade']}")
            print()

    # Parse all pending
    count = parser.parse_all_pending()
    print(f"\nParsed {count} items from pending PDFs")


if __name__ == "__main__":
    main()