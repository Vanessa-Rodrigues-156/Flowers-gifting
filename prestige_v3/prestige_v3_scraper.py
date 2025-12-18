import asyncio
import os
import logging
import nest_asyncio
from database import Database
from url_scraper import URLScraper
from detail_scraper import DetailScraper

# Apply nest_asyncio
nest_asyncio.apply()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('prestige_v3_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Environment variables
DB_PATH = os.getenv('SCRAPER_DB_PATH', 'prestige_flowers_v3.db')
CATEGORY_URL = os.getenv('SCRAPER_CATEGORY_URL', 'https://www.prestigeflowers.co.uk/christmas-plants')
STEALTH_ENABLED = os.getenv('SCRAPER_STEALTH_ENABLED', 'true').lower() == 'true'
RATE_LIMIT = int(os.getenv('SCRAPER_RATE_LIMIT', '2'))

async def main():
    """Main scraper function."""
    logger.info("Starting Prestige V3 Scraper")
    logger.info(f"DB Path: {DB_PATH}")
    logger.info(f"Category URL: {CATEGORY_URL}")
    logger.info(f"Stealth Enabled: {STEALTH_ENABLED}")
    logger.info(f"Rate Limit: {RATE_LIMIT}s")

    # Initialize database
    db = Database(DB_PATH)

    # Step 1: Scrape product URLs from category
    url_scraper = URLScraper(db)
    urls = await url_scraper.scrape_category_urls(CATEGORY_URL)
    logger.info(f"Scraped {len(urls)} URLs")

    # Step 2: Scrape product details
    detail_scraper = DetailScraper(db)
    results = await detail_scraper.scrape_all_pending_products()

    logger.info("Scraping complete")
    logger.info(f"Results: {results}")

if __name__ == "__main__":
    asyncio.run(main())