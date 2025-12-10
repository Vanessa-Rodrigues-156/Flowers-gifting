import json
import asyncio
import sqlite3
import re
import logging
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page
import nest_asyncio

# Apply nest_asyncio for compatibility
nest_asyncio.apply()

# ===== LOGGING CONFIGURATION =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('prestige_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== IMPROVED NETWORK IDLE WAITER =====
async def wait_for_network_idle(page: Page, timeout_ms: int = 30000, idle_time_ms: int = 1000) -> None:
    """
    Custom network idle waiter that properly manages event listeners.
    """
    pending_requests = set()

    def on_request(request):
        pending_requests.add(request.url)

    def on_request_done(request):
        pending_requests.discard(request.url)

    # Register event listeners
    page.on('request', on_request)
    page.on('requestfinished', on_request_done)
    page.on('requestfailed', on_request_done)

    try:
        start_time = asyncio.get_event_loop().time()
        idle_start = None

        while True:
            current_time = asyncio.get_event_loop().time()
            elapsed = (current_time - start_time) * 1000

            # Check total timeout
            if elapsed > timeout_ms:
                logger.debug(f"⚠️ Network idle timeout after {int(elapsed)}ms. Proceeding anyway.")
                break

            # Check if network is idle
            if len(pending_requests) == 0:
                if idle_start is None:
                    idle_start = current_time
                else:
                    idle_elapsed = (current_time - idle_start) * 1000
                    if idle_elapsed >= idle_time_ms:
                        logger.debug(f"✅ Network idle for {int(idle_elapsed)}ms")
                        break
            else:
                idle_start = None
                logger.debug(f"⏳ Pending requests: {len(pending_requests)}")

            await asyncio.sleep(0.1)

    finally:
        # CRITICAL: Use remove_listener (Python) not off() (JavaScript)
        try:
            page.remove_listener('request', on_request)
            page.remove_listener('requestfinished', on_request_done)
            page.remove_listener('requestfailed', on_request_done)
        except Exception as e:
            logger.warning(f"⚠️ Error removing event listeners: {e}")

# ===== DATABASE INITIALIZATION =====
class Database:
    """Handle all database operations for URL scraping."""

    def __init__(self, db_path: str = 'prestige_flowers.db'):
        self.db_path = db_path
        self.init_database()

    def init_database(self) -> None:
        """Initialize SQLite database with products table."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_url TEXT UNIQUE NOT NULL,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'pending'
            )
        ''')

        conn.commit()
        conn.close()
        logger.info(f"✅ Database initialized: {self.db_path}")

    def batch_insert_product_urls(self, urls: list) -> tuple:
        """Insert multiple product URLs into database in a single transaction."""
        conn = None
        inserted_count = 0
        skipped_count = 0
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()

            for url in urls:
                try:
                    cursor.execute('''
                        INSERT INTO products (product_url)
                        VALUES (?)
                    ''', (url,))
                    inserted_count += 1
                except sqlite3.IntegrityError:
                    skipped_count += 1
                    #logger.debug(f"Skipping duplicate URL: {url}")

            conn.commit()
            return inserted_count, skipped_count
        except Exception as e:
            logger.error(f"Error during batch insert operation: {e}")
            if conn:
                conn.rollback()
            return 0, len(urls)
        finally:
            if conn:
                conn.close()

# ===== URL SCRAPER =====
class URLScraper:
    """Scrape product URLs from category page."""

    def __init__(self, db: Database = None):
        self.db = db or Database()

    async def fetch_page(self, url: str) -> str:
        """Fetch the webpage using Playwright with improved error handling."""
        logger.info(f"🔍 Fetching category page: {url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                await page.set_extra_http_headers({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                })

                await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(2)

                try:
                    await page.wait_for_selector('a.product-img', timeout=10000)
                except:
                    logger.warning("⚠️ Product selector not found immediately, proceeding anyway...")

                content = await page.content()
                return content
            except Exception as e:
                logger.error(f"❌ Error fetching page: {e}")
                raise
            finally:
                try:
                    await page.close()
                except:
                    pass
                await browser.close()

    async def extract_urls(self, html_content: str, base_url: str) -> list:
        """Extract product URLs from HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        product_links = soup.find_all('a', class_='product-img')

        logger.info(f"🔎 Found {len(product_links)} product links")

        seen_urls = set()
        product_urls = []

        for link in product_links:
            href = link.get('href')
            if href:
                absolute_url = urljoin(base_url, href)
                clean_url = absolute_url.split('#')[0]

                if clean_url not in seen_urls:
                    seen_urls.add(clean_url)
                    product_urls.append(clean_url)

        return product_urls

    async def scrape_and_save_urls(self, category_url: str) -> dict:
        """Scrape URLs from category page and save to database."""
        try:
            html_content = await self.fetch_page(category_url)
            base_url = category_url.split('/christmas')[0] if '/christmas' in category_url else category_url
            urls = await self.extract_urls(html_content, base_url)
            inserted_count, skipped_count = self.db.batch_insert_product_urls(urls)

            logger.info(f"✅ Inserted {inserted_count} new product URLs")
            if skipped_count > 0:
                logger.info(f"⏭️ Skipped {skipped_count} duplicate URLs")

            return {'inserted': inserted_count, 'skipped': skipped_count, 'total': len(urls)}
        except Exception as e:
            logger.error(f"❌ Error in URL scraping: {e}")
            raise

# ===== MAIN FUNCTION =====
async def scrape_product_urls(category_url: str, db_path: str = 'prestige_flowers.db') -> dict:
    """Main function to scrape product URLs from a category page."""
    db = Database(db_path)
    scraper = URLScraper(db)
    return await scraper.scrape_and_save_urls(category_url)