#!/usr/bin/env python3
"""
Test script with visible browser to debug popup behavior
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from logging_conf import logger
from playwright.async_api import async_playwright


async def test_with_visible_browser():
    """Test with visible browser to see what happens"""

    test_url = "https://autenticidade.cetesb.sp.gov.br/autentica.php?idocmn=12&ndocmn=57000087"
    numero_documento = "57000087"

    # Create browser instance directly with visible mode
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Force visible browser
        context = await browser.new_context()
        page = await context.new_page()

        try:
            logger.info(f"Opening {test_url} in visible browser...")
            await page.goto(test_url, wait_until='networkidle')
            await page.wait_for_load_state('domcontentloaded')

            # Click consultar
            logger.info("Looking for Consultar button...")
            consultar_button = await page.query_selector("input[name='btOK']")
            if consultar_button:
                logger.info("Clicking Consultar button...")
                await consultar_button.click()
                await page.wait_for_timeout(4000)

                # Look for visualize link
                logger.info("Looking for Visualize link...")
                visualize_link = await page.query_selector("a:has-text('Visualize')")
                if visualize_link:
                    logger.info("Found Visualize link!")
                    href = await visualize_link.get_attribute('href')
                    logger.info(f"Visualize link href: {href}")

                    logger.info("Clicking Visualize link... (watch the browser)")

                    # Just click and wait to see what happens visually
                    await visualize_link.click()

                    logger.info("Clicked! Waiting 10 seconds for you to observe what happens...")
                    await page.wait_for_timeout(10000)

                    logger.info("Done observing. Check if popup opened or download started.")

                else:
                    logger.error("Visualize link not found")
            else:
                logger.error("Consultar button not found")

        except Exception as e:
            logger.error(f"Error: {e}")

        finally:
            logger.info("Closing browser in 5 seconds...")
            await page.wait_for_timeout(5000)
            await browser.close()


if __name__ == "__main__":
    asyncio.run(test_with_visible_browser())