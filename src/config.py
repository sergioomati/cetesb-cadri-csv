import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Rate limiting
RATE_MIN = float(os.getenv("RATE_MIN", "0.6"))
RATE_MAX = float(os.getenv("RATE_MAX", "1.4"))

# Browser settings
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
BROWSER_TIMEOUT = int(os.getenv("BROWSER_TIMEOUT", "30000"))

# Scraping limits
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# User agent
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (compatible; CetesbScraper/1.0; +contato@example.com)"
)

# Base URLs
BASE_URL_LICENCIAMENTO = "https://licenciamento.cetesb.sp.gov.br/cetesb"
BASE_URL_AUTENTICIDADE = "https://autenticidade.cetesb.sp.gov.br"

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / os.getenv("DATA_DIR", "data")
PDF_DIR = DATA_DIR / "pdfs"
CSV_DIR = DATA_DIR / "csv"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(exist_ok=True)
CSV_DIR.mkdir(exist_ok=True)

# CSV files
CSV_EMPRESAS = CSV_DIR / "empresas.csv"
CSV_CADRI_DOCS = CSV_DIR / "cadri_documentos.csv"
CSV_CADRI_ITEMS = CSV_DIR / "cadri_itens.csv"

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = DATA_DIR / "scraper.log"

# PDF parsing
PDF_TIMEOUT = int(os.getenv("PDF_TIMEOUT", "60"))

# Pipeline
RESUME_ENABLED = os.getenv("RESUME_ENABLED", "true").lower() == "true"
CHECKPOINT_INTERVAL = int(os.getenv("CHECKPOINT_INTERVAL", "100"))

# Document type to filter
TARGET_DOC_TYPE = "CERT MOV RESIDUOS INT AMB"

# LLM Parser Configuration
LLM_PARSER_ENABLED = os.getenv("LLM_PARSER_ENABLED", "true").lower() == "true"
LLM_DEFAULT_MODEL = os.getenv("LLM_DEFAULT_MODEL", "gpt-5-mini")
LLM_MAX_TEXT_LENGTH = int(os.getenv("LLM_MAX_TEXT_LENGTH", "15000"))
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.1"))
LLM_BATCH_SIZE = int(os.getenv("LLM_BATCH_SIZE", "100"))

# OpenRouter API Configuration (inherited from OpenRouterController)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
if not OPENROUTER_API_KEY and LLM_PARSER_ENABLED:
    import warnings
    warnings.warn("LLM_PARSER_ENABLED=true but OPENROUTER_API_KEY not found. LLM parsing will not work.")