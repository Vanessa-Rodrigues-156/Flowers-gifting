import json
import asyncio
import sqlite3
import re
import logging
import hashlib
from datetime import datetime
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

# ===== DATABASE INITIALIZATION =====
class Database:
    """Handle all database operations for detail scraping."""

    def __init__(self, db_path: str = 'prestige_flowers.db'):
        self.db_path = db_path
        self.init_database()

    def init_database(self) -> None:
        """Initialize SQLite database with product_details table."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        cursor = conn.cursor()

        # Ensure products table exists (in case this is run first)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_url TEXT UNIQUE NOT NULL,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'pending',
                last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                url_discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS product_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                Productid TEXT,
                SKU TEXT,
                name TEXT,
                Description TEXT,
                price_retail REAL,
                price_medium REAL,
                price_large REAL,
                imageurl TEXT,
                product_url TEXT NOT NULL,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_hash TEXT,
                structured_json TEXT,
                FOREIGN KEY(product_id) REFERENCES products(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scrape_errors (
                error_id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER,
                product_url TEXT,
                error_type TEXT,
                error_message TEXT,
                retry_count INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()
        logger.info(f"✅ Database initialized: {self.db_path}")

    def get_existing_product_details(self, product_id: int) -> dict:
        """Get existing product details for comparison."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM product_details WHERE product_id = ?', (product_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                'id': row[0],
                'product_id': row[1],
                'Productid': row[2],
                'SKU': row[3],
                'name': row[4],
                'Description': row[5],
                'price_retail': row[6],
                'price_medium': row[7],
                'price_large': row[8],
                'imageurl': row[9],
                'product_url': row[10],
                'scraped_at': row[11],
                'last_updated': row[12],
                'data_hash': row[13],
                'structured_json': row[14]
            }
        return None

    def generate_data_hash(self, details: dict) -> str:
        """Generate a hash of key product details to detect changes."""
        key_data = {
            'Productid': details.get('Productid', ''),
            'SKU': details.get('SKU', ''),
            'name': details.get('name', ''),
            'Description': details.get('Description', ''),
            'price_retail': details.get('price_retail'),
            'price_medium': details.get('price_medium'),
            'price_large': details.get('price_large'),
            'imageurl': details.get('imageurl', '')
        }
        data_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()

    def get_pending_products(self, limit: int = None, check_recent: bool = True) -> list:
        """Get all products pending detail scraping or needing re-check."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        cursor = conn.cursor()

        if check_recent:
            # Get products that are truly pending OR haven't been checked in the last 24 hours
            # This allows periodic re-checking of existing products for updates
            if limit:
                cursor.execute('''
                    SELECT id, product_url FROM products
                    WHERE status = "pending"
                       OR (status = "processed" AND last_checked < datetime('now', '-1 day'))
                    LIMIT ?
                ''', (limit,))
            else:
                cursor.execute('''
                    SELECT id, product_url FROM products
                    WHERE status = "pending"
                       OR (status = "processed" AND last_checked < datetime('now', '-1 day'))
                ''')
        else:
            # Original behavior - only pending products
            if limit:
                cursor.execute('SELECT id, product_url FROM products WHERE status = "pending" LIMIT ?', (limit,))
            else:
                cursor.execute('SELECT id, product_url FROM products WHERE status = "pending"')

        products = cursor.fetchall()
        conn.close()
        return products

    def save_product_details(self, product_id: int, product_url: str, details: dict) -> tuple:
        """Save or update product details, returning (is_inserted, has_changed)."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()
            structured_json = details.get('structured_json', {})

            # Generate hash of new data
            new_hash = self.generate_data_hash(details)

            # Check if product details already exist
            existing = self.get_existing_product_details(product_id)

            if existing:
                # Compare hashes to see if data changed
                if existing['data_hash'] == new_hash:
                    # Data hasn't changed, just update last_updated timestamp
                    cursor.execute('''
                        UPDATE product_details SET last_updated = CURRENT_TIMESTAMP
                        WHERE product_id = ?
                    ''', (product_id,))
                    conn.commit()
                    return False, False  # Not inserted, not changed

                # Data changed, update existing record
                cursor.execute('''
                    UPDATE product_details SET
                        Productid = ?, SKU = ?, name = ?, Description = ?,
                        price_retail = ?, price_medium = ?, price_large = ?,
                        imageurl = ?, product_url = ?, last_updated = CURRENT_TIMESTAMP,
                        data_hash = ?, structured_json = ?
                    WHERE product_id = ?
                ''', (
                    details.get('Productid'),
                    details.get('SKU'),
                    details.get('name'),
                    details.get('Description'),
                    details.get('price_retail'),
                    details.get('price_medium'),
                    details.get('price_large'),
                    details.get('imageurl'),
                    product_url,
                    new_hash,
                    json.dumps(structured_json, indent=2),
                    product_id
                ))
                conn.commit()
                return False, True  # Not inserted, but changed

            # New product details, insert
            cursor.execute('''
                INSERT INTO product_details
                (product_id, Productid, SKU, name, Description, price_retail, price_medium, price_large, imageurl, product_url, data_hash, structured_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                product_id,
                details.get('Productid'),
                details.get('SKU'),
                details.get('name'),
                details.get('Description'),
                details.get('price_retail'),
                details.get('price_medium'),
                details.get('price_large'),
                details.get('imageurl'),
                product_url,
                new_hash,
                json.dumps(structured_json, indent=2)
            ))

            cursor.execute('UPDATE products SET status = "processed" WHERE id = ?', (product_id,))
            conn.commit()
            return True, True  # Inserted and changed (new data)

        except Exception as e:
            logger.error(f"Error saving product details for product_id {product_id}: {e}")
            return False, False
        finally:
            if conn:
                conn.close()

    def log_scrape_error(self, product_id: int, product_url: str, error_type: str, error_message: str, retry_count: int = 0) -> None:
        """Log scraping errors for debugging."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO scrape_errors (product_id, product_url, error_type, error_message, retry_count)
                VALUES (?, ?, ?, ?, ?)
            ''', (product_id, product_url, error_type, str(error_message)[:500], retry_count))
            conn.commit()
        except Exception as e:
            logger.warning(f"Could not log error for {product_url}: {e}")
        finally:
            if conn:
                conn.close()

# ===== DETAIL SCRAPER =====
class DetailScraper:
    """Scrape detailed product information from individual product pages."""

    def __init__(self, db: Database = None):
        self.db = db or Database()

    async def fetch_product_page(self, page: Page, url: str) -> str:
        """Fetch individual product page with improved error handling."""
        try:
            await page.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            })

            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(1)

            try:
                await page.wait_for_selector('script[type="application/ld+json"]', timeout=5000)
            except:
                logger.debug(f"⚠️ JSON-LD not found immediately for {url}")

            content = await page.content()
            return content
        except Exception as e:
            logger.error(f"❌ Error fetching {url}: {e}")
            return None

    def extract_structured_json(self, html_content: str) -> dict:
        """Extract JSON-LD structured data."""
        soup = BeautifulSoup(html_content, 'html.parser')
        scripts = soup.find_all('script', type='application/ld+json')

        for script in scripts:
            try:
                data = json.loads(script.string)

                if isinstance(data, list):
                    for item in data:
                        if item.get('@type') == 'Product':
                            return item
                elif data.get('@type') == 'Product':
                    return data
            except json.JSONDecodeError:
                continue

        return {}

    def extract_og_description(self, html_content: str) -> str:
        """Extract description from meta tags."""
        soup = BeautifulSoup(html_content, 'html.parser')

        meta_tag = soup.find('meta', property='og:description')
        if meta_tag:
            return meta_tag.get('content', '')

        meta_tag = soup.find('meta', attrs={'name': 'description'})
        if meta_tag:
            return meta_tag.get('content', '')

        return ''

    def extract_prices(self, html_content: str) -> dict:
        """Extract pricing information."""
        soup = BeautifulSoup(html_content, 'html.parser')

        prices = {
            'price_retail': None,
            'price_medium': None,
            'price_large': None,
            'currency': None
        }

        retail_span = soup.find('span', class_='price-retail')
        if retail_span:
            price_text = retail_span.get_text(strip=True)
            match = re.search(r'[£$€]?\s*(\d+\.?\d*)', price_text)
            if match:
                prices['price_retail'] = float(match.group(1))

            currency_match = re.search(r'[£$€]', price_text)
            if currency_match:
                prices['currency'] = currency_match.group(0)

        size_costs = soup.find_all('span', class_='size-cost')
        for idx, span in enumerate(size_costs):
            cost_text = span.get_text(strip=True).replace('+', '+')
            match = re.search(r'[+\-]?\s*[£$€]?\s*(\d+\.?\d*)', cost_text)

            if match and prices['price_retail']:
                price_value = float(match.group(1))

                if idx == 0:
                    prices['price_medium'] = prices['price_retail'] + price_value
                elif idx == 1:
                    prices['price_large'] = prices['price_retail'] + price_value

        return prices

    def extract_product_id(self, html_content: str) -> str:
        """Extract product ID from hidden input field."""
        soup = BeautifulSoup(html_content, 'html.parser')
        productid_input = soup.find('input', {'id': 'productid', 'name': 'productid'})
        if productid_input:
            return productid_input.get('value', '')
        return ''

    async def scrape_product_details(self, page: Page, product_id: int, product_url: str) -> dict:
        """Scrape all details from a single product page."""
        logger.info(f"🔄 Scraping details: {product_url}")

        html_content = await self.fetch_product_page(page, product_url)
        if not html_content:
            self.db.log_scrape_error(product_id, product_url, 'FetchError', 'Could not fetch page content', 0)
            return {}

        try:
            structured_json = self.extract_structured_json(html_content)
            description = self.extract_og_description(html_content)
            prices = self.extract_prices(html_content)
            product_id_value = self.extract_product_id(html_content)  # Extract from hidden input

            details = {
                'Productid': product_id_value,  # Use the hidden input value
                'SKU': structured_json.get('sku', ''),
                'name': structured_json.get('name', ''),
                'Description': description,
                'price_retail': prices['price_retail'],
                'price_medium': prices['price_medium'],
                'price_large': prices['price_large'],
                'imageurl': structured_json.get('image', [None])[0] if isinstance(structured_json.get('image'), list) else structured_json.get('image'),
                'structured_json': structured_json
            }

            is_inserted, has_changed = self.db.save_product_details(product_id, product_url, details)

            if is_inserted:
                logger.info(f"✅ NEW: {details['name']} | {prices['currency']}{prices['price_retail']}")
            elif has_changed:
                logger.info(f"🔄 UPDATED: {details['name']} | {prices['currency']}{prices['price_retail']}")
            else:
                logger.info(f"⏭️ NO CHANGE: {details['name']} | {prices['currency']}{prices['price_retail']}")

            return details
        except Exception as e:
            self.db.log_scrape_error(product_id, product_url, type(e).__name__, str(e), 0)
            logger.error(f"❌ Error extracting details from {product_url}: {e}")
            return {}

    async def scrape_all_pending_products(self, limit: int = None, check_recent: bool = True) -> dict:
        """Scrape all pending products with improved resource management."""
        products = self.db.get_pending_products(limit, check_recent)

        logger.info(f"\n{'='*100}")
        logger.info(f"🌸 PRESTIGE FLOWERS - DETAILED PRODUCT SCRAPER")
        logger.info(f"{'='*100}")
        logger.info(f"📋 Starting scrape of {len(products)} products...\n")

        successful = 0
        failed = 0

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()

                for idx, (product_id, product_url) in enumerate(products, 1):
                    logger.info(f"[{idx}/{len(products)}] Processing: {product_url}")

                    # Update last_checked timestamp
                    conn = sqlite3.connect(self.db.db_path, timeout=10)
                    cursor = conn.cursor()
                    cursor.execute('UPDATE products SET last_checked = CURRENT_TIMESTAMP WHERE id = ?', (product_id,))
                    conn.commit()
                    conn.close()

                    try:
                        details = await self.scrape_product_details(page, product_id, product_url)
                        if details:
                            successful += 1
                        else:
                            failed += 1
                    except Exception as e:
                        logger.error(f"❌ Error: {e}")
                        self.db.log_scrape_error(product_id, product_url, type(e).__name__, str(e), 0)
                        failed += 1

                    await asyncio.sleep(2)  # Rate limiting

                try:
                    await page.close()
                except:
                    pass
                await browser.close()
        except Exception as e:
            logger.error(f"❌ Browser error: {e}")
            failed = len(products) - successful

        logger.info(f"\n{'='*100}")
        logger.info(f"📊 SCRAPING COMPLETE:")
        logger.info(f"{'='*100}")
        logger.info(f"✅ Successfully scraped: {successful}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"📊 Total: {len(products)}\n")
        return {
            'total': len(products),
            'successful': successful,
            'failed': failed
        }

# ===== MAIN FUNCTION =====
async def scrape_product_details(limit: int = None, db_path: str = 'prestige_flowers.db', check_recent: bool = True) -> dict:
    """Main function to scrape product details for pending products."""
    db = Database(db_path)
    scraper = DetailScraper(db)
    return await scraper.scrape_all_pending_products(limit, check_recent)