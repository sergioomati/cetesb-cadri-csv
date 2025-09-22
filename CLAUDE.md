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
# Complete pipeline with all stages
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
The system follows a 6-stage ETL pipeline:
1. **Seeds** → Generate search terms (avoiding corporate stopwords)
2. **List** → Search companies via Playwright (headless browser)
3. **Detail** → Extract CADRI documents via httpx/BeautifulSoup
4. **PDF** → Download PDFs from authenticity portal
5. **Parse** → Extract waste data with pymupdf
6. **CSV** → Store with pandas (idempotent upserts)

### Core Components

- **pipeline.py**: Main orchestrator with signal handling and checkpoints
- **seeds.py**: Adaptive search strategy with 3→4 character refinement
- **scrape_list.py**: Company search using Playwright
- **scrape_detail.py**: Document extraction with httpx/BeautifulSoup
- **download_pdf.py**: Async PDF downloads with validation
- **parse_pdf.py**: PDF text extraction and waste parsing
- **store_csv.py**: CSV persistence with deduplication
- **config.py**: Centralized configuration from .env

### Data Storage

Three main CSV outputs:
- **empresas.csv**: Company data (CNPJ as primary key)
- **cadri_documentos.csv**: Document metadata (numero_documento as primary key)
- **cadri_itens.csv**: Waste item details extracted from PDFs

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