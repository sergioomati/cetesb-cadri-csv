# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=src --cov-report=html

# Run specific test
pytest tests/test_seeds.py -v

# Run by marker
pytest -m unit
pytest -m integration
pytest -m slow
```

### Code Quality
```bash
# Format code
black src/ tests/

# Lint
flake8 src/ tests/

# Type checking
mypy src/
```

### Pipeline Execution
```bash
# Complete pipeline with all stages (list → detail → pdf → parse)
python -m src.pipeline --stage all

# Individual stages
python -m src.pipeline --stage list --seeds CEM
python -m src.pipeline --stage detail
python -m src.pipeline --stage pdf
python -m src.pipeline --stage parse

# Pipeline control
python -m src.pipeline --reset-seeds
python -m src.pipeline --no-resume
python -m src.pipeline --log-level DEBUG
python -m src.pipeline --max-iterations 3
```

### PDF Management
```bash
# Direct download (CERT MOV RESIDUOS INT AMB only)
python cert_mov_direct_downloader.py
python cert_mov_direct_downloader.py --test  # Test mode (5 docs only)

# Interactive download (fallback for failures)
python interactive_pdf_downloader.py
python interactive_pdf_downloader.py --type "CERT MOV RESIDUOS INT AMB"
python interactive_pdf_downloader.py --retry-failed

# PDF parsing (extração aprimorada com todos os campos)
python pdf_parser_standalone.py
python pdf_parser_standalone.py --force-reparse
python pdf_parser_standalone.py --document 16000520
python pdf_parser_standalone.py --file specific_file.pdf
python pdf_parser_standalone.py --type "CERT MOV RESIDUOS INT AMB"

# Direct PDF download with retry management
python pdf_direct_downloader.py
```

### Monitoring and Utilities
```bash
# Progress monitoring
python monitor_progress.py
python monitor_progress.py --doc-type "CERT MOV RESIDUOS INT AMB"
python monitor_progress.py --save report.txt
python monitor_progress.py --missing-only

# Utility commands
python cadri_utils.py list-types
python cadri_utils.py count --type "CERT MOV RESIDUOS INT AMB"
python cadri_utils.py reset --type "CERT MOV RESIDUOS INT AMB" --status not_found
python cadri_utils.py cleanup --min-size 10
python cadri_utils.py export-failed --output failed.csv
python cadri_utils.py validate
python cadri_utils.py quick-download --limit 5
```

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Setup environment
cp .env.example .env
```

## Architecture

### Pipeline Flow
The system follows a 4-stage ETL pipeline:
1. **List** → Search companies via Playwright (headless browser)
2. **Detail** → Extract CADRI documents via httpx/BeautifulSoup
3. **PDF** → Download PDFs using direct method + interactive fallback
4. **Parse** → Extract waste data with PyMuPDF

### Core Components

- **pipeline.py**: Main orchestrator with signal handling and checkpoints
- **seeds.py**: Adaptive search strategy with 3→4 character refinement
- **scrape_list.py**: Company search using Playwright
- **scrape_detail.py**: Document extraction with httpx/BeautifulSoup
- **cert_mov_direct_downloader.py**: Direct PDF downloads using discovered URL pattern
- **interactive_pdf_downloader.py**: Playwright-based fallback for failed downloads
- **pdf_parser_standalone.py**: PDF text extraction and waste parsing with PyMuPDF
- **monitor_progress.py**: Progress monitoring and reporting
- **cadri_utils.py**: Utility commands for data management
- **store_csv.py**: CSV persistence with deduplication
- **config.py**: Centralized configuration from .env

### Data Storage

Three main CSV outputs:
- **empresas.csv**: Company data (CNPJ as primary key)
- **cadri_documentos.csv**: Document metadata (numero_documento as primary key)
- **cadri_itens.csv**: Waste item details extracted from PDFs with complete field extraction:
  - Número e descrição do resíduo
  - Classe, estado físico, OII, quantidade/unidade
  - Composição aproximada, método utilizado
  - Cor/cheiro/aspecto, acondicionamento, destino

### Resilience Features

- **Idempotent operations**: Can safely resume from interruption
- **Checkpoint system**: Saves state every 100 operations
- **Rate limiting**: Configurable delays with jitter
- **Adaptive search**: Automatically refines search terms based on results
- **Error handling**: Comprehensive logging and retry mechanisms

## Configuration

Environment variables in `.env`:
- Rate limiting: `RATE_MIN`, `RATE_MAX`
- Browser: `HEADLESS`, `BROWSER_TIMEOUT`
- Limits: `MAX_PAGES`, `MAX_RETRIES`
- Paths: `DATA_DIR`, `CSV_DIR`, `PDF_DIR`
- Pipeline: `RESUME_ENABLED`, `CHECKPOINT_INTERVAL`

## Key Constraints

- **No CNPJ substring search**: System requires exact CNPJ matches
- **Corporate stopwords blocked**: "LTDA", "ME", "EPP", "S/A" don't return results
- **PDF format variations**: Parsing relies on consistent PDF layouts
- **Rate limiting required**: Must respect server limits to avoid blocks
- **Headless browser dependency**: Playwright needed for JavaScript-heavy pages