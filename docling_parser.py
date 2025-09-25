#!/usr/bin/env python3
"""
Docling PDF Parser - Enhanced PDF parsing using IBM Docling for CETESB CADRI documents
"""

import sys
from pathlib import Path
import json
import re
from typing import List, Dict, Optional, Any
from datetime import datetime
import pandas as pd

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

try:
    from docling.document_converter import DocumentConverter
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    DOCLING_AVAILABLE = True
except ImportError:
    DOCLING_AVAILABLE = False
    print("Warning: Docling not available. Install with: pip install docling")

from store_csv import CSVStore
from logging_conf import logger
from config import PDF_DIR, CSV_CADRI_ITEMS, CSV_CADRI_DOCS


class DoclingPDFParser:
    """Enhanced PDF parser using IBM Docling for better structured data extraction"""

    def __init__(self):
        self.pdf_dir = PDF_DIR
        self.converter = None
        self.stats = {
            'processed': 0,
            'items_extracted': 0,
            'errors': 0,
            'no_items': 0,
            'skipped': 0,
            'docling_used': 0,
            'fallback_used': 0
        }

        # Initialize Docling converter if available
        if DOCLING_AVAILABLE:
            self._init_docling_converter()
        else:
            logger.warning("Docling not available - parser will use fallback mode only")

    def _init_docling_converter(self):
        """Initialize Docling document converter with optimized settings"""
        try:
            # Configure pipeline options for better PDF understanding
            pipeline_options = PdfPipelineOptions()
            pipeline_options.do_ocr = False  # Disable OCR for now (PDFs are text-based)
            pipeline_options.do_table_structure = True  # Enable table detection

            self.converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: pipeline_options
                }
            )
            logger.info("Docling converter initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize Docling converter: {e}")
            self.converter = None

    def parse_all_pdfs(self, force_reparse: bool = False, specific_document: str = None) -> Dict[str, int]:
        """Parse all PDFs in the directory using Docling enhanced parsing"""
        if not DOCLING_AVAILABLE or not self.converter:
            logger.error("Docling not available - cannot proceed with enhanced parsing")
            return self.stats

        pdf_files = list(self.pdf_dir.glob("*.pdf"))

        if specific_document:
            pdf_files = [f for f in pdf_files if f.stem == specific_document]

        if not pdf_files:
            logger.info("No PDF files found to process")
            return self.stats

        logger.info(f"📄 {len(pdf_files)} PDFs para processar")

        for pdf_file in pdf_files:
            try:
                # Check if already parsed (unless force reparse)
                if not force_reparse and self._is_already_parsed(pdf_file.stem):
                    logger.debug(f"Skipping {pdf_file.stem} - already parsed")
                    self.stats['skipped'] += 1
                    continue

                items = self.parse_pdf(pdf_file)

                if items:
                    # Save items to CSV
                    self._save_items_to_csv(items)
                    self.stats['items_extracted'] += len(items)
                    logger.info(f"📄 {pdf_file.stem}: {len(items)} itens extraídos")
                else:
                    self.stats['no_items'] += 1
                    logger.warning(f"📄 {pdf_file.stem}: Nenhum item extraído")

                self.stats['processed'] += 1

            except Exception as e:
                logger.error(f"Erro ao processar {pdf_file}: {e}")
                self.stats['errors'] += 1

        logger.info(f"PDF parsing stats: {self.stats}")
        return self.stats

    def parse_pdf(self, pdf_path: Path) -> List[Dict[str, Any]]:
        """Parse a single PDF using Docling enhanced extraction"""
        if not DOCLING_AVAILABLE or not self.converter:
            logger.error("Docling not available")
            return []

        items = []
        numero_documento = pdf_path.stem

        try:
            logger.debug(f"Processing PDF with Docling: {pdf_path}")

            # Convert PDF using Docling
            result = self.converter.convert(str(pdf_path))

            # Export to structured dictionary
            doc_dict = result.document.export_to_dict()
            self.stats['docling_used'] += 1

            logger.debug(f"Docling extraction complete for {numero_documento}")

            # Extract metadata and document info
            metadata = self._extract_document_metadata(doc_dict)

            # Extract individual residue items
            residue_items = self._extract_residue_items(doc_dict, numero_documento, metadata)

            items.extend(residue_items)

        except Exception as e:
            logger.error(f"Docling parsing failed for {pdf_path}: {e}")
            self.stats['fallback_used'] += 1
            # Could implement fallback to PyMuPDF here if needed

        return items

    def _extract_document_metadata(self, doc_dict: Dict[str, Any]) -> Dict[str, str]:
        """Extract document metadata from Docling parsed structure"""
        metadata = {}

        # Get main text content
        texts = doc_dict.get('texts', [])
        full_text = ' '.join([text.get('text', '') for text in texts])

        # Extract document information using regex patterns
        patterns = {
            'numero_documento': r'N°\s*(\d+)',
            'numero_processo': r'Processo\s*N°\s*([\d/]+)',
            'versao_documento': r'Versão:\s*(\d+)',
            'data_documento': r'Data:\s*(\d{2}/\d{2}/\d{4})',
            'data_validade': r'Validade\s*até:\s*(\d{2}/\d{2}/\d{4})',
            'tipo_documento': r'(CERTIFICADO DE MOVIMENTAÇÃO|CADRI)',
        }

        for field, pattern in patterns.items():
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                metadata[field] = match.group(1)

        # Extract entity information
        self._extract_entity_data(full_text, metadata)

        return metadata

    def _extract_entity_data(self, text: str, metadata: Dict[str, str]) -> None:
        """Extract geradora and destinacao entity data"""

        # Patterns for entity extraction
        geradora_patterns = {
            'geradora_nome': r'ENTIDADE GERADORA.*?Nome\s+([^\n]+)',
            'geradora_cadastro_cetesb': r'Cadastro na CETESB\s+([\d-]+)',
            'geradora_logradouro': r'Logradouro\s+([^\d\n]+?)(?=\d)',
            'geradora_numero': r'Número\s+(\d+)',
            'geradora_municipio': r'Município\s+([A-Z]+)',
            'geradora_uf': r'([A-Z]{2})\s*$',
            'geradora_cep': r'CEP\s+([\d-]+)',
            'geradora_atividade': r'Descrição da Atividade\s+([^\n]+)',
            'geradora_bacia_hidrografica': r'Bacia Hidrográfica\s+([^\n]+)',
        }

        destinacao_patterns = {
            'destino_entidade_nome': r'ENTIDADE DE DESTINAÇÃO.*?Nome\s+([^\n]+)',
            'destino_entidade_cadastro_cetesb': r'ENTIDADE DE DESTINAÇÃO.*?Cadastro na CETESB\s+([\d-]+)',
            'destino_entidade_logradouro': r'ENTIDADE DE DESTINAÇÃO.*?Logradouro\s+([^\d\n]+?)(?=\d)',
            'destino_entidade_numero': r'ENTIDADE DE DESTINAÇÃO.*?Número\s+(\d+)',
            'destino_entidade_municipio': r'ENTIDADE DE DESTINAÇÃO.*?Município\s+([A-Z]+)',
            'destino_entidade_uf': r'ENTIDADE DE DESTINAÇÃO.*?([A-Z]{2})\s*(?=Descrição|$)',
            'destino_entidade_cep': r'ENTIDADE DE DESTINAÇÃO.*?CEP\s+([\d-]+)',
            'destino_entidade_atividade': r'ENTIDADE DE DESTINAÇÃO.*?Descrição da Atividade\s+([^\n]+)',
            'destino_entidade_licenca': r'N°LIC./CERT.FUNCION.\s+(\d+)',
            'destino_entidade_data_licenca': r'Data LIC./CERTIFIC.\s+([\d/]+)',
        }

        # Extract geradora data
        for field, pattern in geradora_patterns.items():
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                metadata[field] = match.group(1).strip()

        # Extract destinacao data
        for field, pattern in destinacao_patterns.items():
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                metadata[field] = match.group(1).strip()

    def _extract_residue_items(self, doc_dict: Dict[str, Any], numero_documento: str, metadata: Dict[str, str]) -> List[Dict[str, Any]]:
        """Extract individual residue items from document structure"""
        items = []

        # Get main text content
        texts = doc_dict.get('texts', [])
        full_text = ' '.join([text.get('text', '') for text in texts])

        # Look for residue patterns: "01 Resíduo : A099 - ..."
        residue_pattern = r'(\d{2})\s+Resíduo\s*:\s*([A-Z]\d{3})\s*-\s*([^\n]+?)(?=\n|\s+Origem)'
        residue_matches = re.finditer(residue_pattern, full_text, re.IGNORECASE | re.DOTALL)

        for match in residue_matches:
            item_numero = match.group(1)
            numero_residuo = match.group(2)
            descricao_residuo = match.group(3).strip()

            # Find the complete block for this residue
            start_pos = match.start()

            # Look for next residue or end of relevant section
            next_pattern = r'\d{2}\s+Resíduo\s*:'
            next_match = re.search(next_pattern, full_text[start_pos + len(match.group(0)):])

            if next_match:
                end_pos = start_pos + len(match.group(0)) + next_match.start()
            else:
                # Take next 1500 characters as reasonable block size
                end_pos = min(start_pos + 1500, len(full_text))

            residue_block = full_text[start_pos:end_pos]

            # Extract structured fields from this block
            item_data = self._extract_residue_fields(residue_block)

            # Create complete item
            item = {
                'numero_documento': numero_documento,
                'item_numero': item_numero,
                'numero_residuo': numero_residuo,
                'descricao_residuo': descricao_residuo,
                'pagina_origem': 1,  # Default, could be enhanced
                'raw_fragment': self._clean_raw_fragment(residue_block),
                'updated_at': datetime.now().isoformat(),
                **item_data,
                **metadata
            }

            items.append(item)

        return items

    def _extract_residue_fields(self, block: str) -> Dict[str, str]:
        """Extract structured fields from a residue block"""
        fields = {}

        # Field extraction patterns
        patterns = {
            'classe_residuo': r'Classe\s*:\s*([IVX]+[AB]?)',
            'estado_fisico': r'Estado\s+Físico\s*:\s*(\w+)',
            'oii': r'O/I\s*:\s*([I/O]+)',
            'quantidade': r'Qtde\s*:\s*([0-9.,]+)',
            'unidade': r'Qtde\s*:\s*[0-9.,]+\s*([a-zA-Z/\s]+?)(?=\n|Composição)',
            'composicao_aproximada': r'Composição\s+Aproximada\s*:\s*([^\n]+)',
            'metodo_utilizado': r'Método\s+Utilizado\s*:\s*([^\n]+)',
            'cor_cheiro_aspecto': r'Cor[,\.]?\s*Cheiro[,\.]?\s*Aspecto\s*:\s*([^\n]+)',
            'destino_descricao': r'Destino\s*:\s*([^\n]+)',
        }

        for field, pattern in patterns.items():
            match = re.search(pattern, block, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                # Clean up the value
                if field == 'quantidade':
                    value = value.replace(',', '.')
                elif field == 'unidade':
                    value = value.split()[0] if value else ''
                fields[field] = value

        # Extract acondicionamento codes and descriptions
        self._extract_acondicionamento(block, fields)

        return fields

    def _extract_acondicionamento(self, block: str, fields: Dict[str, str]) -> None:
        """Extract acondicionamento codes and descriptions"""
        # Find all E## codes
        e_codes = re.findall(r'E\d{2}', block)
        if e_codes:
            # Remove duplicates while preserving order
            unique_codes = list(dict.fromkeys(e_codes))
            fields['acondicionamento_codigos'] = ','.join(unique_codes)

            # Extract descriptions
            descriptions = []
            for code in unique_codes:
                desc_pattern = rf'{code}\s*-\s*([^A-Z\n]+?)(?=\s*[A-Z]|Destino|$)'
                desc_match = re.search(desc_pattern, block, re.IGNORECASE)
                if desc_match:
                    desc = desc_match.group(1).strip()
                    descriptions.append(desc)

            if descriptions:
                fields['acondicionamento_descricoes'] = ' | '.join(descriptions)

    def _clean_raw_fragment(self, text: str) -> str:
        """Clean raw fragment text to prevent CSV formatting issues"""
        if not text:
            return ""

        # Remove excessive whitespace and newlines
        cleaned = re.sub(r'\s+', ' ', text)
        # Remove problematic characters that could break CSV
        cleaned = cleaned.replace('"', "'").replace('\n', ' ').replace('\r', ' ')
        # Limit length to prevent extremely long fields
        if len(cleaned) > 500:
            cleaned = cleaned[:500] + "..."

        return cleaned.strip()

    def _save_items_to_csv(self, items: List[Dict[str, Any]]) -> None:
        """Save extracted items to CSV with proper error handling"""
        if not items:
            return

        try:
            df = pd.DataFrame(items)

            # Ensure all columns from schema exist
            from store_csv import CSVSchemas
            expected_cols = CSVSchemas.CADRI_ITEMS_COLS

            # Add missing columns with empty values
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = ''

            # Reorder columns to match schema
            df = df[expected_cols]

            # Clean data to prevent CSV issues
            df = df.fillna('')  # Replace NaN with empty strings

            # Use the existing CSV store system
            CSVStore.upsert(
                df,
                CSV_CADRI_ITEMS,
                keys=['numero_documento', 'item_numero']
            )

        except Exception as e:
            logger.error(f"Erro ao salvar items: {e}")
            # Fallback save with basic structure
            try:
                df_basic = pd.DataFrame(items)
                df_basic.to_csv(CSV_CADRI_ITEMS, mode='a', header=not CSV_CADRI_ITEMS.exists(), index=False)
                logger.info(f"✅ Salvos {len(items)} itens em {CSV_CADRI_ITEMS} (fallback)")
            except Exception as e2:
                logger.error(f"Fallback save also failed: {e2}")

    def _is_already_parsed(self, numero_documento: str) -> bool:
        """Check if document is already parsed"""
        if not CSV_CADRI_ITEMS.exists():
            return False

        try:
            df = pd.read_csv(CSV_CADRI_ITEMS, dtype=str)
            return numero_documento in df['numero_documento'].values
        except:
            return False


def main():
    """Main function for standalone execution"""
    import argparse

    parser = argparse.ArgumentParser(description='Enhanced PDF Parser using Docling')
    parser.add_argument('--force-reparse', action='store_true', help='Force reparse all PDFs')
    parser.add_argument('--document', help='Parse specific document number')

    args = parser.parse_args()

    if not DOCLING_AVAILABLE:
        print("Error: Docling not available. Install with: pip install docling")
        sys.exit(1)

    parser_instance = DoclingPDFParser()
    stats = parser_instance.parse_all_pdfs(
        force_reparse=args.force_reparse,
        specific_document=args.document
    )

    print(f"\nParsing completed:")
    print(f"  Processed: {stats['processed']}")
    print(f"  Items extracted: {stats['items_extracted']}")
    print(f"  Errors: {stats['errors']}")
    print(f"  Docling used: {stats['docling_used']}")
    print(f"  Fallback used: {stats['fallback_used']}")


if __name__ == "__main__":
    main()