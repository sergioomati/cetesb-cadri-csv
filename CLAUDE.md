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

# CNPJ-based extraction (NEW)
python -m src.pipeline --stage all --cnpj-file empresas.xlsx
python -m src.pipeline --stage list --cnpj-file lista_cnpjs.xlsx

# LLM-based parsing (NEW)
python -m src.pipeline --stage parse --parser-method llm
python -m src.pipeline --stage all --parser-method hybrid
```

### PDF Management
```bash
# Primary PDF download (direct method)
python cert_mov_direct_downloader.py

# Fallback PDF download (interactive Playwright)
python interactive_pdf_downloader.py

# PDF parsing with 47-field extraction
python pdf_parser_standalone.py
python pdf_parser_standalone.py --force-reparse
python pdf_parser_standalone.py --document 16000520

# LLM-based PDF parsing
python -m src.llm_pdf_parser --model cost-optimized
python -m src.llm_pdf_parser --document 16000520 --model flagship
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
4. **Parse** → Extract 47 structured fields from PDFs using PyMuPDF regex patterns or LLM structured outputs

### Key Components
- **src/pipeline.py**: Main orchestrator with CNPJ support
- **src/cnpj_loader.py**: XLSX CNPJ loader and validator
- **src/scrape_list.py**: Company search (seeds + CNPJs)
- **cert_mov_direct_downloader.py**: Primary PDF downloader
- **interactive_pdf_downloader.py**: Fallback downloader
- **pdf_parser_standalone.py**: Enhanced regex PDF parser (47 fields)
- **src/llm_pdf_parser.py**: LLM-based PDF parser with structured outputs
- **src/schemas.py**: Pydantic schemas for structured data validation
- **src/store_csv.py**: CSV persistence with deduplication

## Data Output

### empresas.csv (Company Data)
- `cnpj`, `razao_social`, `municipio`, `uf`
- `numero_cadastro_cetesb`, `descricao_atividade`

### cadri_itens.csv (Technical Data - 47 Fields)
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

**Entity Data (27+ additional fields):**
- Complete generator entity data (name, address, CETESB registration, etc.)
- Complete destination entity data (name, address, license info, etc.)
- Document metadata (process number, certificate, version, dates)

## PDF Parser Methods

### Available Parser Options
Use `--parser-method` to select extraction method:

- **`llm`** - LLM structured output parser (highest accuracy)
- **`regex`** - Traditional PyMuPDF regex parser (fastest)
- **`docling`** - Enhanced Docling parser (balance of speed/accuracy)
- **`hybrid`** - LLM with automatic fallback to regex
- **`auto`** - Intelligent selection based on availability

### Parser Comparison
| Method | Accuracy | Speed | Layout Flexibility | Cost |
|--------|----------|-------|-------------------|------|
| LLM | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ | Medium |
| Regex | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | Free |
| Docling | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Free |
| Hybrid | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Low |

### Usage Examples
```bash
# Use LLM for maximum accuracy
python -m src.pipeline --stage parse --parser-method llm

# Use hybrid approach (recommended)
python -m src.pipeline --stage all --parser-method hybrid

# Use traditional regex for speed
python -m src.pipeline --stage parse --parser-method regex
```

## Key Features

- **Idempotent operations**: Safe resume from interruption
- **47-field structured extraction**: Complete technical data from PDFs
- **Multiple parser methods**: LLM, regex, docling, and hybrid options
- **LLM structured outputs**: High accuracy with complex PDF layouts
- **Direct PDF download**: Optimized URL pattern discovery
- **Rate limiting**: Respects server limits
- **CSV persistence**: Ready-to-analyze structured data

## Configuration

Environment variables in `.env`:
- Rate limiting: `RATE_MIN`, `RATE_MAX`
- Browser: `HEADLESS`, `BROWSER_TIMEOUT`
- Limits: `MAX_PAGES`, `MAX_RETRIES`
- Paths: `DATA_DIR`, `CSV_DIR`, `PDF_DIR`
- LLM Parser: `LLM_PARSER_ENABLED`, `LLM_DEFAULT_MODEL`, `OPENROUTER_API_KEY`

## Important Notes

- **CNPJ search support**: Direct CNPJ search via XLSX input files
- **XLSX format**: Requires "cnpj" column with 14-digit CNPJs
- **Corporate stopwords blocked**: "LTDA", "ME", "EPP" don't return results in text search
- **LLM parser flexibility**: Can handle varied PDF layouts and structures
- **Structured outputs**: LLM ensures valid JSON schema compliance
- **Automatic fallback**: Hybrid mode falls back to regex if LLM fails
- **Rate limiting required**: Must respect server limits to avoid blocks

## CNPJ File Format

Create XLSX file with structure:
```
cnpj
11222333000181
44555666000199
77888999000155
```

Usage: `python -m src.pipeline --stage all --cnpj-file empresas.xlsx`