import httpx
import asyncio
from pathlib import Path
from typing import Optional, List, Dict
import time
import hashlib

from config import PDF_DIR, USER_AGENT, RATE_MIN, RATE_MAX, MAX_RETRIES
from store_csv import get_pending_pdfs, mark_pdf_status, hash_file
from logging_conf import logger, metrics
from browser import RetryHelper


class PDFDownloader:
    """Download PDFs from CETESB authenticity portal"""

    def __init__(self):
        self.client = httpx.AsyncClient(
            headers={'User-Agent': USER_AGENT},
            timeout=60.0,
            follow_redirects=True
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def download_pdf(self, url: str, numero_documento: str) -> bool:
        """
        Download a single PDF

        Args:
            url: PDF URL
            numero_documento: Document number for filename

        Returns:
            True if successful
        """
        pdf_path = PDF_DIR / f"{numero_documento}.pdf"

        # Skip if already exists
        if pdf_path.exists():
            logger.debug(f"PDF already exists: {numero_documento}")
            # Update status and hash
            pdf_hash = hash_file(pdf_path)
            mark_pdf_status(numero_documento, 'downloaded', pdf_hash)
            return True

        try:
            # Download with retry
            response = await RetryHelper.retry_async(
                lambda: self.client.get(url),
                max_retries=MAX_RETRIES,
                exceptions=(httpx.HTTPError, httpx.TimeoutException)
            )

            response.raise_for_status()

            # Check if response is PDF
            content_type = response.headers.get('content-type', '')
            if 'pdf' not in content_type.lower() and len(response.content) < 1000:
                logger.warning(f"Response doesn't appear to be a PDF for {numero_documento}")
                mark_pdf_status(numero_documento, 'not_found')
                return False

            # Save PDF
            pdf_path.write_bytes(response.content)

            # Calculate hash
            pdf_hash = hashlib.sha256(response.content).hexdigest()

            # Update status
            mark_pdf_status(numero_documento, 'downloaded', pdf_hash)

            metrics.increment('pdfs_downloaded')
            logger.info(f"Downloaded PDF: {numero_documento} ({len(response.content)} bytes)")

            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"PDF not found: {numero_documento}")
                mark_pdf_status(numero_documento, 'not_found')
            else:
                logger.error(f"HTTP error downloading {numero_documento}: {e}")
                mark_pdf_status(numero_documento, 'error')
            metrics.increment('errors')
            return False

        except Exception as e:
            logger.error(f"Error downloading PDF {numero_documento}: {e}")
            mark_pdf_status(numero_documento, 'error')
            metrics.increment('errors')
            return False

    async def download_batch(self, pdf_list: List[Dict[str, str]]) -> int:
        """
        Download multiple PDFs with rate limiting

        Args:
            pdf_list: List of dicts with 'numero_documento' and 'url_pdf'

        Returns:
            Number of successful downloads
        """
        successful = 0

        for i, pdf_info in enumerate(pdf_list, 1):
            numero = pdf_info['numero_documento']
            url = pdf_info['url_pdf']

            logger.info(f"Downloading {i}/{len(pdf_list)}: {numero}")

            # Download
            success = await self.download_pdf(url, numero)
            if success:
                successful += 1

            # Rate limiting
            if i < len(pdf_list):
                delay = RATE_MIN + (RATE_MAX - RATE_MIN) * 0.5
                await asyncio.sleep(delay)

        logger.info(f"Downloaded {successful}/{len(pdf_list)} PDFs successfully")
        return successful

    async def download_all_pending(self) -> int:
        """Download all pending PDFs from CSV"""
        pending = get_pending_pdfs()

        if not pending:
            logger.info("No pending PDFs to download")
            return 0

        logger.info(f"Found {len(pending)} pending PDFs")
        return await self.download_batch(pending)


class PDFValidator:
    """Validate downloaded PDFs"""

    @staticmethod
    def validate_pdf(file_path: Path) -> bool:
        """Check if file is a valid PDF"""
        if not file_path.exists():
            return False

        try:
            # Check file size
            if file_path.stat().st_size < 100:
                return False

            # Check PDF header
            with open(file_path, 'rb') as f:
                header = f.read(5)
                return header == b'%PDF-'

        except Exception as e:
            logger.error(f"Error validating PDF {file_path}: {e}")
            return False

    @staticmethod
    def validate_all() -> Dict[str, int]:
        """Validate all downloaded PDFs"""
        stats = {'valid': 0, 'invalid': 0, 'total': 0}

        for pdf_file in PDF_DIR.glob("*.pdf"):
            stats['total'] += 1

            if PDFValidator.validate_pdf(pdf_file):
                stats['valid'] += 1
            else:
                stats['invalid'] += 1
                logger.warning(f"Invalid PDF: {pdf_file.name}")

        logger.info(f"PDF validation: {stats}")
        return stats


async def main():
    """Test PDF download"""
    # Test with a specific document
    test_pdf = {
        'numero_documento': '123456',
        'url_pdf': 'https://autenticidade.cetesb.sp.gov.br/autentica.php?idocmn=12&ndocmn=123456'
    }

    async with PDFDownloader() as downloader:
        success = await downloader.download_pdf(
            test_pdf['url_pdf'],
            test_pdf['numero_documento']
        )
        print(f"Download {'successful' if success else 'failed'}")

        # Download all pending
        count = await downloader.download_all_pending()
        print(f"Downloaded {count} pending PDFs")

    # Validate PDFs
    stats = PDFValidator.validate_all()
    print(f"Validation stats: {stats}")


if __name__ == "__main__":
    asyncio.run(main())