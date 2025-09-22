#!/usr/bin/env python3
"""
Debug the JavaScript download functionality
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from browser import BrowserManager
from logging_conf import logger


async def debug_javascript_download():
    """Debug how the JavaScript download works"""

    test_url = "https://autenticidade.cetesb.sp.gov.br/autentica.php?idocmn=12&ndocmn=57000087"
    numero_documento = "57000087"

    browser_manager = BrowserManager()

    async with browser_manager:
        page = await browser_manager.new_page()

        try:
            # Navigate and click consultar
            await page.goto(test_url, wait_until='networkidle')
            await page.wait_for_load_state('domcontentloaded')

            # Click consultar
            consultar_button = await page.query_selector("input[name='btOK']")
            if consultar_button:
                await consultar_button.click()
                await page.wait_for_timeout(4000)

                # Find the visualize link
                visualize_link = await page.query_selector("a:has-text('Visualize')")
                if visualize_link:
                    href = await visualize_link.get_attribute('href')
                    logger.info(f"Visualize link href: {href}")

                    # Let's see what happens when we click it (but handle it differently)
                    logger.info("Setting up listeners for new pages/downloads...")

                    # Listen for new pages (popups)
                    popup_promise = page.context.wait_for_event("page")

                    # Click the link
                    logger.info("Clicking Visualize link...")
                    await visualize_link.click()

                    # Wait for popup
                    try:
                        popup = await asyncio.wait_for(popup_promise, timeout=10.0)
                        logger.info(f"New page opened: {popup.url}")

                        # Wait for popup to load
                        await popup.wait_for_load_state('networkidle', timeout=10000)

                        # Save popup content
                        popup_content = await popup.content()
                        with open("data/debug/popup_content.html", "w", encoding="utf-8") as f:
                            f.write(popup_content)

                        logger.info(f"Popup title: {await popup.title()}")

                        # Look for download links in popup
                        download_links = await popup.query_selector_all("a")
                        for i, link in enumerate(download_links):
                            href = await link.get_attribute('href')
                            text = await link.inner_text()
                            if href:
                                logger.info(f"Popup link {i}: text='{text}', href='{href}'")

                        await popup.close()

                    except asyncio.TimeoutError:
                        logger.warning("No popup opened, trying alternative approach...")

                        # Maybe it's a direct download that we missed
                        # Let's inspect the page for any changes
                        await page.wait_for_timeout(2000)

                        # Check if there are any new elements or changes
                        page_content = await page.content()
                        with open("data/debug/page_after_click.html", "w", encoding="utf-8") as f:
                            f.write(page_content)

                        logger.info("Saved page content after clicking Visualize")

                else:
                    logger.error("Visualize link not found")
            else:
                logger.error("Consultar button not found")

        except Exception as e:
            logger.error(f"Error during debug: {e}")

        finally:
            await page.close()


if __name__ == "__main__":
    Path("data/debug").mkdir(exist_ok=True)
    asyncio.run(debug_javascript_download())