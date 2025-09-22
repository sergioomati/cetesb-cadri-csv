import asyncio
from typing import Optional, Dict, List, Any
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import random

from config import HEADLESS, BROWSER_TIMEOUT, USER_AGENT, MAX_RETRIES
from logging_conf import logger


class BrowserManager:
    """Manage Playwright browser instances"""

    def __init__(self):
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        """Start browser instance"""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=HEADLESS,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                ]
            )

            self.context = await self.browser.new_context(
                user_agent=USER_AGENT,
                viewport={'width': 1920, 'height': 1080},
                locale='pt-BR',
                timezone_id='America/Sao_Paulo',
            )

            # Set default timeout
            self.context.set_default_timeout(BROWSER_TIMEOUT)

            logger.info(f"Browser started (headless={HEADLESS})")

        except Exception as e:
            logger.error(f"Failed to start browser: {e}")
            raise

    async def close(self):
        """Close browser instance"""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            logger.info("Browser closed")
        except Exception as e:
            logger.error(f"Error closing browser: {e}")

    async def new_page(self) -> Page:
        """Create new page"""
        if not self.context:
            await self.start()
        page = await self.context.new_page()

        # Add stealth scripts
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        return page

    async def random_delay(self, min_sec: float = 0.5, max_sec: float = 2.0):
        """Add random delay to simulate human behavior"""
        delay = random.uniform(min_sec, max_sec)
        await asyncio.sleep(delay)


class FormHelper:
    """Helper for form interactions"""

    @staticmethod
    async def clear_and_type(page: Page, selector: str, text: str):
        """Clear field and type text"""
        await page.click(selector)
        await page.keyboard.press('Control+A')
        await page.keyboard.press('Delete')
        await page.type(selector, text)

    @staticmethod
    async def submit_form(
        page: Page,
        form_data: Dict[str, str],
        submit_selector: str,
        wait_for: Optional[str] = None
    ) -> bool:
        """
        Submit form with data

        Args:
            page: Page instance
            form_data: Dict of selector -> value
            submit_selector: Submit button selector
            wait_for: Optional selector to wait for after submit

        Returns:
            True if successful
        """
        try:
            # Fill form fields
            for selector, value in form_data.items():
                await FormHelper.clear_and_type(page, selector, value)
                await asyncio.sleep(0.2)  # Small delay between fields

            # Submit
            await page.click(submit_selector)

            # Wait for response
            if wait_for:
                await page.wait_for_selector(wait_for, timeout=10000)

            return True

        except Exception as e:
            logger.error(f"Form submission failed: {e}")
            return False

    @staticmethod
    async def handle_pagination(
        page: Page,
        next_button_selectors: List[str],
        max_pages: int,
        extract_func
    ) -> List[Any]:
        """
        Handle pagination and extract data from each page

        Args:
            page: Page instance
            next_button_selectors: List of possible next button selectors
            max_pages: Maximum pages to process
            extract_func: Async function to extract data from page

        Returns:
            List of extracted data from all pages
        """
        all_results = []
        page_num = 1

        while page_num <= max_pages:
            try:
                # Extract from current page
                results = await extract_func(page)
                all_results.extend(results)
                logger.debug(f"Extracted {len(results)} items from page {page_num}")

                # Try to find and click next button
                next_found = False
                for selector in next_button_selectors:
                    try:
                        next_btn = await page.query_selector(selector)
                        if next_btn and await next_btn.is_visible():
                            # Check if button is enabled
                            disabled = await next_btn.get_attribute('disabled')
                            if disabled:
                                logger.debug("Next button is disabled, reached last page")
                                break

                            await next_btn.click()
                            await page.wait_for_load_state('networkidle')
                            await asyncio.sleep(1)  # Wait for content to load
                            next_found = True
                            break
                    except:
                        continue

                if not next_found:
                    logger.debug("No more pages available")
                    break

                page_num += 1

            except Exception as e:
                logger.error(f"Error on page {page_num}: {e}")
                break

        logger.info(f"Processed {page_num} pages, extracted {len(all_results)} total items")
        return all_results


class RetryHelper:
    """Helper for retry logic"""

    @staticmethod
    async def retry_async(
        func,
        max_retries: int = MAX_RETRIES,
        delay: float = 1.0,
        backoff: float = 2.0,
        exceptions=(Exception,)
    ):
        """
        Retry async function with exponential backoff

        Args:
            func: Async function to retry
            max_retries: Maximum number of retries
            delay: Initial delay between retries
            backoff: Backoff multiplier
            exceptions: Tuple of exceptions to catch

        Returns:
            Function result
        """
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                return await func()

            except exceptions as e:
                last_exception = e

                if attempt < max_retries:
                    wait_time = delay * (backoff ** attempt)
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {wait_time:.1f}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"All {max_retries + 1} attempts failed")

        raise last_exception

    @staticmethod
    def retry_sync(
        func,
        max_retries: int = MAX_RETRIES,
        delay: float = 1.0,
        backoff: float = 2.0,
        exceptions=(Exception,)
    ):
        """
        Retry synchronous function with exponential backoff

        Args:
            func: Function to retry
            max_retries: Maximum number of retries
            delay: Initial delay between retries
            backoff: Backoff multiplier
            exceptions: Tuple of exceptions to catch

        Returns:
            Function result
        """
        import time

        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                return func()

            except exceptions as e:
                last_exception = e

                if attempt < max_retries:
                    wait_time = delay * (backoff ** attempt)
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"All {max_retries + 1} attempts failed")

        raise last_exception