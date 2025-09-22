#!/usr/bin/env python3
"""
Test script for specific CETESB document download
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from test_interactive_download import InteractivePDFTester
from logging_conf import logger


async def test_specific_document():
    """Test downloading the specific document provided by user"""

    # Document provided by user
    test_url = "https://autenticidade.cetesb.sp.gov.br/autentica.php?idocmn=12&ndocmn=57000087"
    numero_documento = "57000087"

    logger.info(f"Testing download for document: {numero_documento}")
    logger.info(f"URL: {test_url}")

    tester = InteractivePDFTester()

    async with tester.browser_manager:
        # Test download
        success = await tester.download_pdf_interactive(test_url, numero_documento)

        if success:
            logger.info("SUCCESS: PDF downloaded successfully!")

            # Validate the downloaded PDF
            pdf_path = tester.pdf_dir / f"{numero_documento}.pdf"
            if pdf_path.exists():
                is_valid = tester._validate_pdf(pdf_path)
                file_size = pdf_path.stat().st_size
                logger.info(f"PDF path: {pdf_path}")
                logger.info(f"File size: {file_size} bytes")
                logger.info(f"PDF valid: {is_valid}")

                if is_valid and file_size > 1000:
                    logger.info("PDF validation PASSED - File appears to be a valid PDF")
                else:
                    logger.warning("PDF validation FAILED - File may be corrupted")
            else:
                logger.error("PDF file not found after download")
        else:
            logger.error("FAILED: Could not download PDF")


if __name__ == "__main__":
    asyncio.run(test_specific_document())