# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this CETESB CADRI data extraction project.

## Project Objective

Extract **two structured data tables**:
1. **Company data** (`empresas.csv`) - Cadastral information from CETESB site
2. **Technical data** (`cadri_itens.csv`) - Detailed waste information from CADRI documents (15 structured fields)

## Essential Commands

### Main Pipeline
```bash
# Complete data extraction pipeline
python -m src.pipeline --stage all

# Individual stages
python -m src.pipeline --stage list --seeds CEM,AGR,BIO
python -m src.pipeline --stage detail
```

### PDF Management
```bash
# Primary PDF download (direct method)
python cert_mov_direct_downloader.py

# Fallback PDF download (interactive Playwright)
python interactive_pdf_downloader.py

# PDF parsing with 15-field extraction
python pdf_parser_standalone.py
python pdf_parser_standalone.py --force-reparse
python pdf_parser_standalone.py --document 16000520
```

### Monitoring and Utilities
```bash
# Progress monitoring
python monitor_progress.py

# Data management utilities
python cadri_utils.py list-types
python cadri_utils.py count
python cadri_utils.py validate
```

### Development Tools
```bash
# Testing
pytest
pytest --cov=src --cov-report=html

# Code quality
black src/ tests/
flake8 src/ tests/
mypy src/
```

## Core Architecture

### 4-Stage Pipeline
1. **List** → Search companies via Playwright
2. **Detail** → Extract CADRI documents via httpx/BeautifulSoup
3. **Download** → Download PDFs using direct method + interactive fallback
4. **Parse** → Extract 15 structured fields from PDFs using PyMuPDF

### Key Components
- **src/pipeline.py**: Main orchestrator
- **cert_mov_direct_downloader.py**: Primary PDF downloader
- **interactive_pdf_downloader.py**: Fallback downloader
- **pdf_parser_standalone.py**: Enhanced PDF parser (15 fields)
- **src/store_csv.py**: CSV persistence with deduplication

## Data Output

### empresas.csv (Company Data)
- `cnpj`, `razao_social`, `municipio`, `uf`
- `numero_cadastro_cetesb`, `descricao_atividade`

### cadri_itens.csv (Technical Data - 15 Fields)
**Waste Identification:**
- `numero_residuo` (D099, F001, etc.)
- `descricao_residuo`, `classe_residuo` (I, IIA, IIB)
- `estado_fisico` (LIQUIDO, SOLIDO, GASOSO)

**Technical Characteristics:**
- `quantidade`, `unidade`, `oii`
- `composicao_aproximada`, `metodo_utilizado`
- `cor_cheiro_aspecto`

**Logistics:**
- `acondicionamento_codigos` (E01,E04,E05)
- `acondicionamento_descricoes`
- `destino_codigo`, `destino_descricao`

## Key Features

- **Idempotent operations**: Safe resume from interruption
- **15-field structured extraction**: Complete technical data from PDFs
- **Direct PDF download**: Optimized URL pattern discovery
- **Rate limiting**: Respects server limits
- **CSV persistence**: Ready-to-analyze structured data

## Configuration

Environment variables in `.env`:
- Rate limiting: `RATE_MIN`, `RATE_MAX`
- Browser: `HEADLESS`, `BROWSER_TIMEOUT`
- Limits: `MAX_PAGES`, `MAX_RETRIES`
- Paths: `DATA_DIR`, `CSV_DIR`, `PDF_DIR`

## Important Notes

- **No CNPJ substring search**: Requires exact CNPJ matches
- **Corporate stopwords blocked**: "LTDA", "ME", "EPP" don't return results
- **PDF structure dependency**: Parsing relies on consistent PDF layouts
- **Rate limiting required**: Must respect server limits to avoid blocks