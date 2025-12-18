import asyncio
import logging
import random
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from stealth_utils import apply_stealth

logger = logging.getLogger(__name__)

class URLScraper:
    """Scrape product URLs from category pages."""

    def __init__(self, db):
        self.db = db

    async def wait_for_network_idle(self, page):
        """Wait for network to be idle."""
        await page.wait_for_load_state('networkidle')

import asyncio
import logging
import random
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from stealth_utils import apply_stealth

logger = logging.getLogger(__name__)

class URLScraper:
    """Scrape product URLs from category pages."""

    def __init__(self, db):
        self.db = db

    async def wait_for_network_idle(self, page):
        """Wait for network to be idle."""
        await page.wait_for_load_state('networkidle')

    async def fetch_page(self, url: str) -> str:
        """Fetch page content with stealth and anti-bot measures."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ua = random.choice([
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            ])
            viewport = {'width': random.randint(1024, 1920), 'height': random.randint(768, 1080)}
            context = await browser.new_context(
                user_agent=ua,
                viewport=viewport
            )
            page = await context.new_page()
            await apply_stealth(page)
            
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(random.randint(3000, 8000))  # Randomized wait
            
            # Enhanced Cloudflare check
            title = await page.title()
            content = await page.content()
            if "Just a moment" in title or "cf-browser-verification" in content or await page.locator('div.cf-challenge').is_visible():
                await page.wait_for_timeout(random.randint(10000, 20000))
                title = await page.title()
                if "Just a moment" in title:
                    logger.warning(f"Challenge persistent for {url}, skipping")
                    await browser.close()
                    return []
            
            try:
                await self.wait_for_network_idle(page)
            except:
                pass  # Fallback if idle not reached
            
            # Wait for product links with retry
            try:
                await page.wait_for_selector('a.product-img', timeout=15000)
            except:
                logger.debug(f"Product links selector not found for {url}")
            
            content = await page.content()
            logger.info(f"Page title: {await page.title()}")
            # Extract URLs using Playwright
            links = page.locator('a')
            count = await links.count()
            urls = []
            for i in range(count):
                href = await links.nth(i).get_attribute('href')
                if href:
                    if href.startswith('http'):
                        full_url = href
                    elif href.startswith('/'):
                        full_url = f"https://www.prestigeflowers.co.uk{href}"
                    else:
                        continue
                    if '/christmas-plants/' in full_url and full_url != 'https://www.prestigeflowers.co.uk/christmas-plants/':
                        urls.append(full_url)
            urls = list(set(urls))
            logger.info(f"Found {len(urls)} links")
            await browser.close()
            return urls

    def extract_product_urls(self, html_content: str) -> list:
        """Extract product URLs using exact selectors."""
        soup = BeautifulSoup(html_content, 'html.parser')
        urls = []
        for a in soup.select('a[href^="/christmas-plants/"]'):
            href = a.get('href')
            if href and href != '/christmas-plants/':  # exclude the category itself
                full_url = f"https://www.prestigeflowers.co.uk{href}"
                urls.append(full_url)
        return list(set(urls))  # unique

    async def scrape_category_urls(self, category_url: str) -> list:
        """Scrape and save product URLs from category page."""
        logger.info(f"Scraping URLs from {category_url}")
        urls = await self.fetch_page(category_url)
        for url in urls:
            self.db.save_product_url(url)
        logger.info(f"Scraped {len(urls)} URLs")
        return urls