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
            # DEBUG_CNPJ: Log início da submissão
            logger.debug(f"[DEBUG_CNPJ] FormHelper.submit_form iniciado com {len(form_data)} campos")

            # Fill form fields
            for selector, value in form_data.items():
                # DEBUG_CNPJ: Log preenchimento de cada campo
                logger.debug(f"[DEBUG_CNPJ] Preenchendo campo '{selector}' com valor '{value}'")

                await FormHelper.clear_and_type(page, selector, value)

                # DEBUG_CNPJ: Verificar se valor foi preenchido corretamente
                field_element = await page.query_selector(selector)
                if field_element:
                    actual_value = await field_element.input_value()
                    logger.debug(f"[DEBUG_CNPJ] Valor atual no campo '{selector}': '{actual_value}'")

                    # DEBUG_CNPJ: Disparar eventos JavaScript adicionais
                    await page.evaluate(f"""
                        () => {{
                            const field = document.querySelector('{selector}');
                            if (field) {{
                                // Simular interação humana completa
                                field.focus();

                                // Disparar eventos em sequência
                                ['focus', 'input', 'change', 'blur'].forEach(eventType => {{
                                    const event = new Event(eventType, {{
                                        bubbles: true,
                                        cancelable: true
                                    }});
                                    field.dispatchEvent(event);
                                }});

                                // Também tentar eventos de teclado
                                const keyEvents = ['keydown', 'keyup'];
                                keyEvents.forEach(eventType => {{
                                    const event = new KeyboardEvent(eventType, {{
                                        bubbles: true,
                                        cancelable: true,
                                        key: 'Tab'
                                    }});
                                    field.dispatchEvent(event);
                                }});
                            }}
                        }}
                    """)

                await asyncio.sleep(0.2)  # Small delay between fields

            # DEBUG_CNPJ: Verificar estado do formulário antes da submissão
            logger.debug(f"[DEBUG_CNPJ] Verificando botão submit: '{submit_selector}'")
            submit_element = await page.query_selector(submit_selector)
            if submit_element:
                is_visible = await submit_element.is_visible()
                is_enabled = await submit_element.is_enabled()
                logger.debug(f"[DEBUG_CNPJ] Botão submit - Visível: {is_visible}, Habilitado: {is_enabled}")

            # DEBUG_CNPJ: Aguardar um momento para garantir que JS processou
            await asyncio.sleep(0.5)

            # Submit
            logger.debug(f"[DEBUG_CNPJ] Clicando em submit...")
            await page.click(submit_selector)

            # DEBUG_CNPJ: Aguardar navegação/mudança de estado
            logger.debug(f"[DEBUG_CNPJ] Aguardando resposta do servidor...")

            # Wait for response
            if wait_for:
                await page.wait_for_selector(wait_for, timeout=10000)
                logger.debug(f"[DEBUG_CNPJ] Seletor '{wait_for}' encontrado - submissão bem-sucedida")

            return True

        except Exception as e:
            # DEBUG_CNPJ: Log de erro detalhado
            logger.error(f"[DEBUG_CNPJ] Form submission failed: {e}")

            # DEBUG_CNPJ: Capturar estado atual da página em caso de erro
            try:
                current_url = page.url
                logger.error(f"[DEBUG_CNPJ] URL atual: {current_url}")

                # Verificar se há erros JavaScript na página
                js_errors = await page.evaluate("""
                    () => {
                        // Capturar erros JavaScript se houver
                        const errors = window._jsErrors || [];
                        return errors;
                    }
                """)
                if js_errors:
                    logger.error(f"[DEBUG_CNPJ] Erros JavaScript detectados: {js_errors}")

            except Exception as debug_e:
                logger.error(f"[DEBUG_CNPJ] Erro durante debug: {debug_e}")

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