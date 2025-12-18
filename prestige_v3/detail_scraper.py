import asyncio
import logging
import json
import random
import re
import sqlite3
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from stealth_utils import apply_stealth

logger = logging.getLogger(__name__)

class DetailScraper:
    """Scrape detailed product information from individual pages."""

    def __init__(self, db):
        self.db = db

    async def wait_for_network_idle(self, page):
        """Wait for network to be idle."""
        await page.wait_for_load_state('networkidle')

    async def fetch_product_page(self, page, url: str) -> str:
        """Fetch product page with stealth."""
        ua = random.choice([
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        ])
        await page.set_extra_http_headers({
            'User-Agent': ua,
        })

        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(random.randint(3000, 8000))  # Randomized wait

        # Check for JSON-LD
        try:
            await page.wait_for_selector('script[type="application/ld+json"]', timeout=5000)
        except:
            logger.debug(f"JSON-LD not found for {url}")

        # Enhanced Cloudflare check
        title = await page.title()
        content = await page.content()
        if "just a moment" in title.lower() or "cf-browser-verification" in content or await page.locator('div.cf-challenge').is_visible():
            await page.wait_for_timeout(random.randint(10000, 20000))
            title = await page.title()
            if "just a moment" in title.lower():
                try:
                    await page.wait_for_selector('h1.products-name', timeout=30000)
                    logger.info(f"Page loaded after waiting for h1: {url}")
                except:
                    logger.warning(f"Could not wait for h1 on {url}, continuing anyway")

        content = await page.content()
        title = await page.title()
        if "just a moment" in title.lower():
            logger.warning(f"Page still on challenge for {url}, skipping")
            return None
        return content

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

    def extract_prices(self, soup: BeautifulSoup) -> dict:
        """Extract pricing using exact selectors."""
        prices = {'price_retail': None, 'price_medium': None, 'price_large': None}
        
        # Exact selector for retail price
        retail_elem = soup.select_one('span.price-retail')
        if retail_elem:
            match = re.search(r'£(\d+(?:\.\d{2})?)', retail_elem.get_text())
            if match:
                prices['price_retail'] = float(match.group(1))
        
        # Exact selectors for size costs
        size_costs = soup.select('span.size-cost')
        for i, cost_elem in enumerate(size_costs[:2]):  # Limit to medium/large
            match = re.search(r'£(\d+(?:\.\d{2})?)', cost_elem.get_text())
            if match and prices['price_retail']:
                cost = float(match.group(1))
                if i == 0:
                    prices['price_medium'] = prices['price_retail'] + cost
                elif i == 1:
                    prices['price_large'] = prices['price_retail'] + cost
        
        return prices

    def extract_product_id(self, soup: BeautifulSoup) -> str:
        """Extract product ID using exact selector."""
        productid_input = soup.select_one('input#productid')
        if productid_input:
            return productid_input.get('value', '')
        return ''

    def extract_name(self, soup: BeautifulSoup, structured_json: dict) -> str:
        """Extract product name with fallbacks."""
        name = structured_json.get('name', '')
        if not name:
            h1 = soup.select_one('h1.products-name')
            if h1:
                name = h1.get_text().strip()
            else:
                title = soup.find('title')
                if title:
                    name = title.get_text().strip()
        return name

    def extract_sku(self, soup: BeautifulSoup, structured_json: dict) -> str:
        """Extract SKU with fallbacks."""
        sku = structured_json.get('sku', '')
        if not sku:
            # Search for SKU or Product Code
            text = soup.get_text()
            match = re.search(r'(?:SKU|Product Code)[:\s]*([A-Z0-9\-]+)', text, re.I)
            if match:
                sku = match.group(1)
        return sku

    def extract_image_url(self, structured_json: dict) -> str:
        """Extract image URL from JSON-LD."""
        image = structured_json.get('image', [None])[0] if isinstance(structured_json.get('image'), list) else structured_json.get('image')
        return image or ''

    def extract_rating(self, structured_json: dict) -> float:
        """Extract rating from JSON-LD."""
        aggregate_rating = structured_json.get('aggregateRating', {})
        if isinstance(aggregate_rating, dict):
            return aggregate_rating.get('ratingValue')
        return None

    def extract_availability(self, structured_json: dict) -> str:
        """Extract availability from JSON-LD."""
        offers = structured_json.get('offers', {})
        if isinstance(offers, dict):
            return offers.get('availability')
        elif isinstance(offers, list) and offers:
            return offers[0].get('availability')
        return None

    def extract_delivery_info(self, soup: BeautifulSoup) -> str:
        """Extract delivery info from page content."""
        # Look for common delivery selectors
        delivery_div = soup.select_one('div.delivery-info, .shipping-info, .delivery-details')
        if delivery_div:
            return delivery_div.get_text(strip=True)
        # Fallback to text search
        text = soup.get_text()
        match = re.search(r'(?:Delivery|Shipping)[:\s]*(.+?)(?:\n|$)', text, re.I)
        if match:
            return match.group(1).strip()
        return ''

    async def scrape_product_details(self, page, product_id: int, product_url: str) -> dict:
        """Scrape details from a product page."""
        logger.info(f"Scraping details: {product_url}")

        for attempt in range(3):
            try:
                html_content = await self.fetch_product_page(page, product_url)
                if html_content:
                    break
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                logger.error(f"Attempt {attempt+1} failed for {product_url}: {e}")
        else:
            self.db.log_scrape_error(product_id, product_url, 'MaxRetries', 'Failed after 3 attempts')
            return {}

        if not html_content:
            self.db.log_scrape_error(product_id, product_url, 'FetchError', 'Could not fetch page')
            return {}

        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            structured_json = self.extract_structured_json(html_content)
            description = self.extract_og_description(html_content)
            prices = self.extract_prices(soup)
            vendor_code = self.extract_product_id(soup)
            name = self.extract_name(soup, structured_json)
            sku = self.extract_sku(soup, structured_json)
            image_url = self.extract_image_url(structured_json)
            rating = self.extract_rating(structured_json)
            availability = self.extract_availability(structured_json)
            delivery_info = self.extract_delivery_info(soup)

            details = {
                'vendor_code': vendor_code,
                'sku': sku,
                'name': name,
                'description': description,
                'price_retail': prices['price_retail'],
                'price_medium': prices['price_medium'],
                'price_large': prices['price_large'],
                'image_url': image_url,
                'rating': rating,
                'availability': availability,
                'delivery_info': delivery_info,
                'structured_json': structured_json
            }

            is_inserted, has_changed = self.db.save_product_details(product_id, product_url, details)

            if is_inserted:
                logger.info(f"NEW: {name} | £{prices['price_retail']}")
            elif has_changed:
                logger.info(f"UPDATED: {name} | £{prices['price_retail']}")
            else:
                logger.info(f"NO CHANGE: {name}")

            return details
        except Exception as e:
            self.db.log_scrape_error(product_id, product_url, type(e).__name__, str(e))
            logger.error(f"Error extracting from {product_url}: {e}")
            return {}

    async def scrape_all_pending_products(self, limit: int = None) -> dict:
        """Scrape all pending products."""
        products = self.db.get_pending_products(limit)

        logger.info(f"Starting scrape of {len(products)} products")

        successful = 0
        failed = 0

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

            for idx, (product_id, product_url) in enumerate(products, 1):
                logger.info(f"[{idx}/{len(products)}] Processing: {product_url}")

                # Update last_checked
                conn = sqlite3.connect(self.db.db_path, timeout=10)
                cursor = conn.cursor()
                cursor.execute('UPDATE vendor_country_url_msts SET last_crawl_time = CURRENT_TIMESTAMP WHERE id = ?', (product_id,))
                conn.commit()
                conn.close()

                try:
                    details = await self.scrape_product_details(page, product_id, product_url)
                    if details:
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"Error: {e}")
                    self.db.log_scrape_error(product_id, product_url, type(e).__name__, str(e))
                    failed += 1

                await asyncio.sleep(random.uniform(1.5, 4.0))  # Randomized rate limiting

            await browser.close()

        logger.info(f"Scrape complete: {successful} successful, {failed} failed")
        return {'total': len(products), 'successful': successful, 'failed': failed}