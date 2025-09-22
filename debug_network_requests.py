#!/usr/bin/env python3
"""
Debug script to intercept network requests and discover PDF download URLs
"""

import asyncio
import sys
from pathlib import Path
import json
from datetime import datetime

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from logging_conf import logger
from playwright.async_api import async_playwright


class NetworkRequestCapture:
    """Capture and analyze network requests"""

    def __init__(self):
        self.requests = []
        self.responses = []
        self.debug_dir = Path("data/debug")
        self.debug_dir.mkdir(exist_ok=True)

    def capture_request(self, request):
        """Capture outgoing requests"""
        request_info = {
            'timestamp': datetime.now().isoformat(),
            'url': request.url,
            'method': request.method,
            'headers': dict(request.headers),
            'post_data': request.post_data,
            'resource_type': request.resource_type
        }
        self.requests.append(request_info)

        # Log interesting requests
        if any(keyword in request.url.lower() for keyword in ['pdf', 'download', 'sipol', 'visualizacao']):
            logger.info(f"🔍 Interesting REQUEST: {request.method} {request.url}")

    def capture_response(self, response):
        """Capture responses"""
        response_info = {
            'timestamp': datetime.now().isoformat(),
            'url': response.url,
            'status': response.status,
            'headers': dict(response.headers),
            'ok': response.ok
        }
        self.responses.append(response_info)

        # Log interesting responses
        if any(keyword in response.url.lower() for keyword in ['pdf', 'download', 'sipol', 'visualizacao']):
            logger.info(f"📥 Interesting RESPONSE: {response.status} {response.url}")

        # Log PDF content types
        content_type = response.headers.get('content-type', '').lower()
        if 'pdf' in content_type:
            logger.info(f"🎯 PDF RESPONSE: {response.url} - {content_type}")

    def save_captured_data(self):
        """Save all captured data to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save requests
        requests_file = self.debug_dir / f"requests_{timestamp}.json"
        with open(requests_file, 'w', encoding='utf-8') as f:
            json.dump(self.requests, f, indent=2, ensure_ascii=False)

        # Save responses
        responses_file = self.debug_dir / f"responses_{timestamp}.json"
        with open(responses_file, 'w', encoding='utf-8') as f:
            json.dump(self.responses, f, indent=2, ensure_ascii=False)

        logger.info(f"📁 Saved {len(self.requests)} requests to {requests_file}")
        logger.info(f"📁 Saved {len(self.responses)} responses to {responses_file}")

    def analyze_pdf_patterns(self):
        """Analyze captured data for PDF download patterns"""
        logger.info("🔍 Analyzing captured data for PDF patterns...")

        # Look for PDF-related URLs
        pdf_requests = [r for r in self.requests if
                       'pdf' in r['url'].lower() or
                       'download' in r['url'].lower() or
                       'sipol' in r['url'].lower()]

        pdf_responses = [r for r in self.responses if
                        'pdf' in r['url'].lower() or
                        'application/pdf' in r['headers'].get('content-type', '').lower()]

        logger.info(f"📊 Found {len(pdf_requests)} PDF-related requests")
        logger.info(f"📊 Found {len(pdf_responses)} PDF-related responses")

        # Print PDF URLs found
        for req in pdf_requests:
            logger.info(f"🔗 PDF Request URL: {req['url']}")

        for resp in pdf_responses:
            logger.info(f"🔗 PDF Response URL: {resp['url']}")

        return pdf_requests, pdf_responses


async def debug_network_capture():
    """Main function to capture network requests during PDF download attempt"""

    test_url = "https://autenticidade.cetesb.sp.gov.br/autentica.php?idocmn=12&ndocmn=57000087"
    numero_documento = "57000087"

    capture = NetworkRequestCapture()

    logger.info(f"🚀 Starting network capture for document: {numero_documento}")

    async with async_playwright() as p:
        # Use visible browser to ensure JavaScript works
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # Setup network listeners
        page.on("request", capture.capture_request)
        page.on("response", capture.capture_response)

        try:
            logger.info("📱 Setting up network listeners...")

            # Navigate to page
            logger.info(f"🌐 Navigating to: {test_url}")
            await page.goto(test_url, wait_until='networkidle')
            await page.wait_for_load_state('domcontentloaded')

            # Click Consultar
            logger.info("🔘 Looking for Consultar button...")
            consultar_button = await page.query_selector("input[name='btOK']")
            if consultar_button:
                logger.info("▶️ Clicking Consultar button...")
                await consultar_button.click()
                await page.wait_for_timeout(4000)

                # Look for Visualize link
                logger.info("🔍 Looking for Visualize link...")
                visualize_link = await page.query_selector("a:has-text('Visualize')")
                if visualize_link:
                    href = await visualize_link.get_attribute('href')
                    logger.info(f"🔗 Found Visualize link: {href}")

                    logger.info("🎯 Clicking Visualize link and monitoring network traffic...")

                    # Clear previous captures to focus on this action
                    capture.requests.clear()
                    capture.responses.clear()

                    # Click and wait to see what network requests happen
                    await visualize_link.click()

                    # Wait and observe network activity
                    logger.info("⏳ Waiting 15 seconds to capture all network activity...")
                    await page.wait_for_timeout(15000)

                    # Check for any new windows/tabs
                    pages = context.pages
                    logger.info(f"📄 Total pages/tabs: {len(pages)}")

                    if len(pages) > 1:
                        logger.info("🆕 New page/popup detected!")
                        for i, p in enumerate(pages[1:], 1):
                            logger.info(f"🔗 Page {i} URL: {p.url}")

                            # Setup listeners on new page too
                            p.on("request", capture.capture_request)
                            p.on("response", capture.capture_response)

                            # Wait for content to load
                            await p.wait_for_load_state('networkidle', timeout=10000)
                            await p.wait_for_timeout(5000)

                else:
                    logger.error("❌ Visualize link not found")
            else:
                logger.error("❌ Consultar button not found")

        except Exception as e:
            logger.error(f"💥 Error during capture: {e}")

        finally:
            logger.info("💾 Saving captured network data...")
            capture.save_captured_data()

            logger.info("🔍 Analyzing PDF patterns...")
            pdf_requests, pdf_responses = capture.analyze_pdf_patterns()

            logger.info("🎯 Network capture complete!")
            logger.info("⏳ Keeping browser open for 10 seconds for manual inspection...")
            await page.wait_for_timeout(10000)

            await browser.close()

    return capture


async def main():
    """Main function"""
    try:
        capture = await debug_network_capture()

        # Print summary
        logger.info(f"""
        📊 CAPTURE SUMMARY:
        📨 Total Requests: {len(capture.requests)}
        📥 Total Responses: {len(capture.responses)}
        📁 Data saved to: data/debug/
        """)

    except Exception as e:
        logger.error(f"💥 Script failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())