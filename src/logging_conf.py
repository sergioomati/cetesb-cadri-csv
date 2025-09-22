import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import LOG_LEVEL, LOG_FILE


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for terminal output"""

    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        if sys.stdout.isatty():
            levelname = record.levelname
            record.levelname = f"{self.COLORS.get(levelname, '')}{levelname}{self.RESET}"
        return super().format(record)


class MetricsLogger:
    """Simple metrics tracking for scraping operations"""

    def __init__(self):
        self.start_time = datetime.now()
        self.counters = {
            'searches': 0,
            'details_scraped': 0,
            'pdfs_downloaded': 0,
            'pdfs_parsed': 0,
            'errors': 0,
        }

    def increment(self, metric: str, count: int = 1):
        """Increment a counter"""
        if metric in self.counters:
            self.counters[metric] += count

    def get_elapsed(self) -> float:
        """Get elapsed time in seconds"""
        return (datetime.now() - self.start_time).total_seconds()

    def get_rate(self, metric: str) -> float:
        """Get rate per hour for a metric"""
        elapsed_hours = self.get_elapsed() / 3600
        if elapsed_hours > 0:
            return self.counters.get(metric, 0) / elapsed_hours
        return 0

    def get_summary(self) -> str:
        """Get summary of metrics"""
        elapsed = self.get_elapsed()
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)

        return (
            f"Runtime: {hours}h {minutes}m | "
            f"Searches: {self.counters['searches']} | "
            f"Details: {self.counters['details_scraped']} | "
            f"PDFs: {self.counters['pdfs_downloaded']} | "
            f"Parsed: {self.counters['pdfs_parsed']} | "
            f"Errors: {self.counters['errors']} | "
            f"Rate: {self.get_rate('details_scraped'):.1f} details/h"
        )


def setup_logging(
    name: Optional[str] = None,
    level: Optional[str] = None,
    log_file: bool = True
) -> tuple[logging.Logger, MetricsLogger]:
    """
    Setup logging configuration

    Returns:
        Tuple of (logger, metrics)
    """
    if name is None:
        name = "cetesb_scraper"

    if level is None:
        level = LOG_LEVEL

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    logger.handlers.clear()

    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    )
    logger.addHandler(console_handler)

    # File handler
    if log_file:
        LOG_FILE.parent.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
        file_handler.setFormatter(
            logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        )
        logger.addHandler(file_handler)

    # Create metrics instance
    metrics = MetricsLogger()

    return logger, metrics


# Global instances
logger, metrics = setup_logging()