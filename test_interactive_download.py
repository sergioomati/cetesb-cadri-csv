#!/usr/bin/env python3
"""
Teste rápido para módulo de download interativo de PDFs

Este script testa a funcionalidade de download de PDFs do CETESB usando Playwright
para interação com as páginas que requerem JavaScript.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional, Dict
import time

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from browser import BrowserManager
from store_csv import get_pending_pdfs, mark_pdf_status, CSVStore
from config import PDF_DIR, RATE_MIN, RATE_MAX
from logging_conf import logger
import hashlib


class InteractivePDFTester:
    """Tester for interactive PDF download functionality"""

    def __init__(self):
        self.browser_manager = BrowserManager()
        self.pdf_dir = PDF_DIR
        self.pdf_dir.mkdir(exist_ok=True)
        self.download_timeout = 60000  # 60 seconds - increased for JavaScript downloads
        self.consultar_wait = 4000  # 4 seconds after clicking Consultar

    async def test_navigation(self, url: str) -> bool:
        """Test basic navigation to CETESB authenticity page"""
        page = None
        try:
            page = await self.browser_manager.new_page()

            logger.info(f"Testing navigation to: {url}")
            await page.goto(url, wait_until='networkidle')
            await page.wait_for_load_state('domcontentloaded')

            # Check if page loaded correctly
            title = await page.title()
            logger.info(f"Page title: {title}")

            # Look for key elements
            consultar_found = await self._find_consultar_button(page) is not None
            logger.info(f"Consultar button found: {consultar_found}")

            return consultar_found

        except Exception as e:
            logger.error(f"Navigation test failed: {e}")
            return False
        finally:
            if page:
                await page.close()

    async def _find_consultar_button(self, page):
        """Find Consultar button using multiple strategies"""
        selectors = [
            "input[name='btOK']",  # Specific button found in debug
            "input[type='button'][value='Consultar']",
            "input[type='button'][value*='Consultar']",
            "input[type='submit'][value*='Consulte']",
            "input[type='submit'][value*='Consultar']",
            "button:has-text('Consultar')",
            "input[value='Consulte ...']",
            "input[value*='Consulte']"
        ]

        for selector in selectors:
            try:
                button = await page.query_selector(selector)
                if button:
                    is_visible = await button.is_visible()
                    if is_visible:
                        logger.debug(f"Found Consultar button with selector: {selector}")
                        return button
            except Exception as e:
                logger.debug(f"Selector '{selector}' failed: {e}")
                continue

        # Try text-based search as fallback
        try:
            await page.wait_for_selector("text=Consulte", timeout=2000)
            button = await page.query_selector("text=Consulte")
            if button:
                logger.debug("Found Consultar button via text search")
                return button
        except:
            pass

        return None

    async def _find_visualize_link(self, page):
        """Find Visualize link for PDF download"""
        selectors = [
            "a:has-text('Visualize')",  # Main target
            "a[href*='VisualizacaoSipol']",  # JavaScript function
            "a[href*='Adobe']:has-text('Visualize')",
            "td:has-text('Imagem da Licença') ~ td a",
            "a[onclick*='window.open']",
            "a:has-text('PDF')",
            "a[href*='.pdf']"
        ]

        for selector in selectors:
            try:
                link = await page.query_selector(selector)
                if link:
                    is_visible = await link.is_visible()
                    href = await link.get_attribute('href')
                    text = await link.inner_text()
                    logger.debug(f"Found link with selector '{selector}': text='{text}', href='{href}', visible={is_visible}")

                    if is_visible and ('visualize' in text.lower() or 'visualizacaosipol' in str(href).lower()):
                        logger.debug(f"Selected Visualize link: {selector}")
                        return link
            except Exception as e:
                logger.debug(f"Selector '{selector}' failed: {e}")
                continue
        return None

    async def download_pdf_interactive(self, url: str, numero_documento: str) -> bool:
        """
        Download a PDF using interactive browser automation

        Args:
            url: CETESB authenticity URL
            numero_documento: Document number for filename

        Returns:
            True if successful
        """
        page = None
        try:
            # Create new page
            page = await self.browser_manager.new_page()

            # Check if PDF already exists
            pdf_path = self.pdf_dir / f"{numero_documento}.pdf"
            if pdf_path.exists():
                logger.info(f"PDF already exists: {numero_documento}")
                # Calculate hash and update status
                pdf_hash = self._calculate_file_hash(pdf_path)
                mark_pdf_status(numero_documento, 'downloaded', pdf_hash)
                return True

            # Navigate to URL
            logger.info(f"Navigating to: {url}")
            await page.goto(url, wait_until='networkidle')
            await page.wait_for_load_state('domcontentloaded')

            # Find and click Consultar button
            consultar_button = await self._find_consultar_button(page)
            if consultar_button:
                logger.info("Clicking 'Consultar' button...")
                await consultar_button.click()

                # Wait for processing
                logger.info(f"Waiting {self.consultar_wait}ms for processing...")
                await page.wait_for_timeout(self.consultar_wait)

                # Wait for "Obtenha uma cópia" section to appear
                try:
                    await page.wait_for_selector(
                        "text=Obtenha uma cópia",
                        timeout=10000
                    )
                    logger.info("'Obtenha uma cópia' section found")
                except:
                    logger.warning("'Obtenha uma cópia' section not found, continuing...")

                # Find Visualize link
                visualize_link = await self._find_visualize_link(page)
                if visualize_link:
                    logger.info("Clicking 'Visualize' link...")

                    # The Visualize button may open a popup or trigger download directly
                    # Let's try multiple approaches
                    try:
                        # First approach: expect direct download
                        async with page.expect_download(timeout=self.download_timeout) as download_info:
                            await visualize_link.click()

                        # Process download
                        download = await download_info.value
                        await download.save_as(pdf_path)

                    except Exception as direct_download_error:
                        logger.warning(f"Direct download failed: {direct_download_error}")
                        logger.info("Trying popup approach...")

                        # Second approach: handle popup window
                        try:
                            # Wait for popup
                            async with page.context.expect_page() as popup_info:
                                await visualize_link.click()

                            popup = await popup_info.value
                            logger.info(f"Popup opened: {popup.url}")

                            # Wait for popup to load
                            await popup.wait_for_load_state('domcontentloaded', timeout=10000)

                            # The popup should auto-start download, but if not, look for "clique aqui" link
                            try:
                                # First, wait a bit for auto-download
                                async with popup.expect_download(timeout=15000) as download_info:
                                    logger.info("Waiting for automatic download from popup...")
                                    await popup.wait_for_timeout(2000)  # Give it time to auto-start

                                download = await download_info.value
                                await download.save_as(pdf_path)
                                logger.info("Automatic download from popup successful")

                            except Exception as auto_download_error:
                                logger.warning(f"Auto-download failed: {auto_download_error}")
                                logger.info("Looking for 'clique aqui' fallback link...")

                                # Look for the fallback "clique aqui" link
                                fallback_link = await popup.query_selector("a:has-text('clique aqui')")
                                if fallback_link:
                                    logger.info("Found 'clique aqui' link, clicking...")

                                    async with popup.expect_download(timeout=self.download_timeout) as download_info:
                                        await fallback_link.click()

                                    download = await download_info.value
                                    await download.save_as(pdf_path)
                                    logger.info("Fallback download successful")
                                else:
                                    logger.error("No fallback link found in popup")
                                    raise Exception("Could not find download mechanism in popup")

                            await popup.close()

                        except Exception as popup_error:
                            logger.error(f"Popup approach also failed: {popup_error}")
                            raise popup_error

                    # Validate download
                    if pdf_path.exists() and pdf_path.stat().st_size > 100:
                        pdf_hash = self._calculate_file_hash(pdf_path)
                        mark_pdf_status(numero_documento, 'downloaded', pdf_hash)
                        logger.info(f"PDF downloaded successfully: {numero_documento} ({pdf_path.stat().st_size} bytes)")
                        return True
                    else:
                        logger.error(f"Downloaded file is invalid: {numero_documento}")
                        mark_pdf_status(numero_documento, 'error')
                        return False
                else:
                    logger.error("'Visualize' link not found")
                    mark_pdf_status(numero_documento, 'no_visualize_button')
                    return False
            else:
                logger.error("'Consultar' button not found")
                mark_pdf_status(numero_documento, 'no_consultar_button')
                return False

        except Exception as e:
            logger.error(f"Error downloading PDF {numero_documento}: {e}")
            mark_pdf_status(numero_documento, 'error')
            return False
        finally:
            if page:
                await page.close()

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


async def test_single_download():
    """Test downloading a single PDF from CSV data"""
    logger.info("Starting interactive PDF download test")

    # Get pending PDFs
    pending_pdfs = get_pending_pdfs()

    if not pending_pdfs:
        logger.warning("No pending PDFs found for testing")
        return

    # Take first pending PDF for testing
    test_pdf = pending_pdfs[0]
    numero_documento = test_pdf['numero_documento']
    url_pdf = test_pdf['url_pdf']

    logger.info(f"Testing with document: {numero_documento}")
    logger.info(f"URL: {url_pdf}")

    tester = InteractivePDFTester()

    async with tester.browser_manager:
        # Test navigation first
        navigation_ok = await tester.test_navigation(url_pdf)

        if navigation_ok:
            logger.info("Navigation test passed")

            # Test download
            download_ok = await tester.download_pdf_interactive(url_pdf, numero_documento)

            if download_ok:
                logger.info("Download test passed")

                # Validate PDF
                pdf_path = PDF_DIR / f"{numero_documento}.pdf"
                if tester._validate_pdf(pdf_path):
                    logger.info("PDF validation passed")
                    logger.info(f"PDF saved: {pdf_path}")
                    logger.info(f"File size: {pdf_path.stat().st_size} bytes")
                else:
                    logger.error("PDF validation failed")
            else:
                logger.error("Download test failed")
        else:
            logger.error("Navigation test failed")


async def test_multiple_downloads(max_count: int = 3):
    """Test downloading multiple PDFs"""
    logger.info(f"Testing multiple PDF downloads (max: {max_count})")

    pending_pdfs = get_pending_pdfs()[:max_count]

    if not pending_pdfs:
        logger.warning("No pending PDFs found for testing")
        return

    tester = InteractivePDFTester()
    stats = {'success': 0, 'failed': 0, 'total': len(pending_pdfs)}

    async with tester.browser_manager:
        for i, pdf_info in enumerate(pending_pdfs, 1):
            numero = pdf_info['numero_documento']
            url = pdf_info['url_pdf']

            logger.info(f"[{i}/{len(pending_pdfs)}] Testing: {numero}")

            success = await tester.download_pdf_interactive(url, numero)

            if success:
                stats['success'] += 1
            else:
                stats['failed'] += 1

            # Rate limiting between downloads
            if i < len(pending_pdfs):
                delay = RATE_MIN + (RATE_MAX - RATE_MIN) * 0.5
                logger.info(f"Waiting {delay:.1f}s before next download...")
                await asyncio.sleep(delay)

    logger.info(f"""
    Test Results:
       Total: {stats['total']}
       Success: {stats['success']}
       Failed: {stats['failed']}
       Success Rate: {stats['success']/stats['total']*100:.1f}%
    """)


async def main():
    """Main test function"""
    import argparse

    parser = argparse.ArgumentParser(description='Test interactive PDF download')
    parser.add_argument('--mode', choices=['single', 'multiple'], default='single',
                       help='Test mode: single PDF or multiple PDFs')
    parser.add_argument('--count', type=int, default=3,
                       help='Number of PDFs to test in multiple mode')

    args = parser.parse_args()

    try:
        if args.mode == 'single':
            await test_single_download()
        else:
            await test_multiple_downloads(args.count)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())