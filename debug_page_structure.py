#!/usr/bin/env python3
"""
Debug script to analyze CETESB page structure
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from browser import BrowserManager
from store_csv import get_pending_pdfs
from logging_conf import logger


async def debug_page_structure():
    """Debug the structure of CETESB authenticity page"""

    # Use specific document provided by user
    test_url = "https://autenticidade.cetesb.sp.gov.br/autentica.php?idocmn=12&ndocmn=57000087"
    numero_documento = "57000087"

    logger.info(f"Debugging page structure for: {numero_documento}")
    logger.info(f"URL: {test_url}")

    browser_manager = BrowserManager()

    async with browser_manager:
        page = await browser_manager.new_page()

        try:
            # Navigate to page
            logger.info("Navigating to page...")
            await page.goto(test_url, wait_until='networkidle')
            await page.wait_for_load_state('domcontentloaded')

            # Save initial page
            logger.info("Saving initial page...")
            await page.screenshot(path="data/debug/initial_page.png")
            initial_html = await page.content()
            with open("data/debug/initial_page.html", "w", encoding="utf-8") as f:
                f.write(initial_html)

            # Look for form elements
            logger.info("Analyzing form elements...")
            forms = await page.query_selector_all("form")
            logger.info(f"Found {len(forms)} forms")

            inputs = await page.query_selector_all("input")
            logger.info(f"Found {len(inputs)} input elements")

            for i, input_elem in enumerate(inputs):
                input_type = await input_elem.get_attribute("type")
                input_value = await input_elem.get_attribute("value")
                input_name = await input_elem.get_attribute("name")
                logger.info(f"Input {i}: type={input_type}, name={input_name}, value={input_value}")

            # Look for buttons
            buttons = await page.query_selector_all("button")
            logger.info(f"Found {len(buttons)} button elements")

            # Find submit button specifically
            submit_buttons = await page.query_selector_all("input[type='submit']")
            logger.info(f"Found {len(submit_buttons)} submit buttons")

            for i, btn in enumerate(submit_buttons):
                btn_value = await btn.get_attribute("value")
                btn_name = await btn.get_attribute("name")
                is_visible = await btn.is_visible()
                logger.info(f"Submit button {i}: value={btn_value}, name={btn_name}, visible={is_visible}")

            # Try to find the specific button
            consultar_button = None
            selectors_to_try = [
                "input[name='btOK']",  # Updated based on findings
                "input[type='button'][value='Consultar']",
                "input[type='submit'][value*='Consulte']",
                "input[type='submit'][value*='Consultar']",
                "input[value='Consulte ...']",
                "input[value*='Consulte']"
            ]

            for selector in selectors_to_try:
                try:
                    button = await page.query_selector(selector)
                    if button:
                        is_visible = await button.is_visible()
                        btn_value = await button.get_attribute("value")
                        logger.info(f"Found button with selector '{selector}': value={btn_value}, visible={is_visible}")
                        if is_visible:
                            consultar_button = button
                            break
                except Exception as e:
                    logger.debug(f"Selector '{selector}' failed: {e}")

            if consultar_button:
                logger.info("Clicking Consultar button...")
                await consultar_button.click()

                # Wait for page to change
                logger.info("Waiting for page to process...")
                await page.wait_for_timeout(5000)

                # Save page after clicking
                logger.info("Saving page after clicking...")
                await page.screenshot(path="data/debug/after_consultar.png")
                after_html = await page.content()
                with open("data/debug/after_consultar.html", "w", encoding="utf-8") as f:
                    f.write(after_html)

                # Look for download links
                logger.info("Looking for download/visualize links...")
                all_links = await page.query_selector_all("a")
                logger.info(f"Found {len(all_links)} links")

                for i, link in enumerate(all_links):
                    href = await link.get_attribute("href")
                    text = await link.inner_text()
                    is_visible = await link.is_visible()
                    if text and ("visual" in text.lower() or "pdf" in text.lower() or "download" in text.lower()):
                        logger.info(f"Interesting link {i}: text='{text}', href={href}, visible={is_visible}")

                # Look for specific text patterns
                page_text = await page.inner_text("body")
                if "obtenha uma cópia" in page_text.lower():
                    logger.info("Found 'Obtenha uma cópia' text in page")
                else:
                    logger.warning("'Obtenha uma cópia' text NOT found in page")

                if "visualize" in page_text.lower():
                    logger.info("Found 'visualize' text in page")
                else:
                    logger.warning("'visualize' text NOT found in page")

            else:
                logger.error("Could not find Consultar button")

        except Exception as e:
            logger.error(f"Error during debug: {e}")

        finally:
            await page.close()


if __name__ == "__main__":
    # Ensure debug directory exists
    Path("data/debug").mkdir(exist_ok=True)
    asyncio.run(debug_page_structure())