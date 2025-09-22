#!/usr/bin/env python3
"""
Direct PDF download using discovered URL pattern
"""

import asyncio
import sys
from pathlib import Path
import httpx
import random
import hashlib

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from logging_conf import logger
from store_csv import mark_pdf_status
from config import PDF_DIR, USER_AGENT


class DirectPDFDownloader:
    """Download PDFs directly using discovered URL pattern"""

    def __init__(self):
        self.pdf_dir = PDF_DIR
        self.pdf_dir.mkdir(exist_ok=True)
        self.client = httpx.AsyncClient(
            headers={'User-Agent': USER_AGENT},
            timeout=60.0,
            follow_redirects=True
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    def construct_pdf_url(self, numero_documento: str, idocmn: str = "12") -> str:
        """
        Construct the direct PDF URL based on discovered pattern

        Args:
            numero_documento: Document number (e.g., "57000087")
            idocmn: Document type ID (usually "12" for CADRI)

        Returns:
            Direct URL to PDF
        """
        # Generate a random session ID (observed pattern)
        sid = random.random()

        # Construct URL based on discovered pattern
        url = f"https://autenticidade.cetesb.sp.gov.br/ajax/DocSipol.php?sid={sid}&idocmn={idocmn}&ndocmn={numero_documento}&method=post"

        return url

    async def download_pdf_direct(self, numero_documento: str, idocmn: str = "12") -> bool:
        """
        Download PDF directly using constructed URL

        Args:
            numero_documento: Document number
            idocmn: Document type ID

        Returns:
            True if successful
        """
        pdf_path = self.pdf_dir / f"{numero_documento}.pdf"

        # Skip if already exists
        if pdf_path.exists():
            logger.info(f"PDF already exists: {numero_documento}")
            pdf_hash = self._calculate_file_hash(pdf_path)
            mark_pdf_status(numero_documento, 'downloaded', pdf_hash)
            return True

        try:
            # Construct direct URL
            pdf_url = self.construct_pdf_url(numero_documento, idocmn)
            logger.info(f"Attempting direct download: {pdf_url}")

            # Download PDF
            response = await self.client.get(pdf_url)
            response.raise_for_status()

            # Check if response is PDF
            content_type = response.headers.get('content-type', '')
            if 'pdf' not in content_type.lower() and len(response.content) < 1000:
                logger.warning(f"Response doesn't appear to be a PDF for {numero_documento}")
                logger.warning(f"Content-Type: {content_type}")
                logger.warning(f"Response size: {len(response.content)} bytes")

                # Save response for debugging
                debug_path = self.pdf_dir / f"{numero_documento}_debug.html"
                debug_path.write_bytes(response.content)
                logger.info(f"Saved debug response to: {debug_path}")

                mark_pdf_status(numero_documento, 'not_pdf')
                return False

            # Save PDF
            pdf_path.write_bytes(response.content)

            # Calculate hash
            pdf_hash = hashlib.sha256(response.content).hexdigest()

            # Update status
            mark_pdf_status(numero_documento, 'downloaded', pdf_hash)

            logger.info(f"Downloaded PDF: {numero_documento} ({len(response.content)} bytes)")
            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"PDF not found: {numero_documento}")
                mark_pdf_status(numero_documento, 'not_found')
            else:
                logger.error(f"HTTP error downloading {numero_documento}: {e}")
                mark_pdf_status(numero_documento, 'error')
            return False

        except Exception as e:
            logger.error(f"Error downloading PDF {numero_documento}: {e}")
            mark_pdf_status(numero_documento, 'error')
            return False

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of a file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _validate_pdf(self, file_path: Path) -> bool:
        """Validate that file is a proper PDF"""
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


async def test_direct_download():
    """Test the direct download approach"""

    test_documento = "57000087"

    logger.info(f"Testing direct PDF download for document: {test_documento}")

    async with DirectPDFDownloader() as downloader:
        success = await downloader.download_pdf_direct(test_documento)

        if success:
            logger.info("SUCCESS: Direct PDF download worked!")

            # Validate the downloaded PDF
            pdf_path = downloader.pdf_dir / f"{test_documento}.pdf"
            if pdf_path.exists():
                is_valid = downloader._validate_pdf(pdf_path)
                file_size = pdf_path.stat().st_size
                logger.info(f"PDF path: {pdf_path}")
                logger.info(f"File size: {file_size} bytes")
                logger.info(f"PDF valid: {is_valid}")

                if is_valid and file_size > 1000:
                    logger.info("VALIDATION PASSED - File is a valid PDF")
                    return True
                else:
                    logger.warning("VALIDATION FAILED - File may be corrupted")
                    return False
            else:
                logger.error("PDF file not found after download")
                return False
        else:
            logger.error("FAILED: Direct download did not work")
            return False


async def main():
    """Main function"""
    try:
        success = await test_direct_download()

        if success:
            logger.info("Direct PDF download approach WORKS!")
            logger.info("This can be used to replace the JavaScript-based approach")
        else:
            logger.error("Direct PDF download approach FAILED")
            logger.info("May need to adjust URL construction or add authentication")

    except Exception as e:
        logger.error(f"Test failed with error: {e}")


if __name__ == "__main__":
    asyncio.run(main())