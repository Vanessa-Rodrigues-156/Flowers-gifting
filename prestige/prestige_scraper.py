import asyncio
import logging
import csv
import sqlite3
import os
from product_url_scraper import scrape_product_urls
from product_details_scraper import scrape_product_details

# ===== CONFIGURATION =====
# Environment Variables (all optional with sensible defaults):
# SCRAPER_DB_PATH: Database file path (default: 'prestige_flowers.db')
# SCRAPER_CATEGORY_URL: Category page URL to scrape (default: 'https://www.prestigeflowers.co.uk/christmas-plants')
# SCRAPER_DETAILS_LIMIT: Max products to scrape ('None' for unlimited, default: 'None')
# SCRAPER_CHECK_RECENT: Whether to re-check recently processed products (default: 'True')
# SCRAPER_CSV_FILENAME: Output CSV filename (default: 'prestige_flowers_combined.csv')
#
# Usage examples:
# export SCRAPER_CATEGORY_URL=https://www.prestigeflowers.co.uk/christmas-flowers
# export SCRAPER_DETAILS_LIMIT=5
# python prestige_scraper.py

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

# ===== DATABASE STATS =====
def get_database_stats(db_path: str) -> dict:
    """Get database statistics."""
    conn = sqlite3.connect(db_path, timeout=10)
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM products')
    total_urls = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM products WHERE status = "pending"')
    pending = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM products WHERE status = "processed"')
    processed = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM product_details')
    total_details = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM scrape_errors')
    total_errors = cursor.fetchone()[0]

    # Get recent activity (last 24 hours)
    cursor.execute('''
        SELECT COUNT(*) FROM product_details
        WHERE last_updated >= datetime('now', '-1 day')
    ''')
    recently_updated = cursor.fetchone()[0]

    cursor.execute('''
        SELECT COUNT(*) FROM products
        WHERE url_discovered_at >= datetime('now', '-1 day')
    ''')
    recently_discovered_urls = cursor.fetchone()[0]

    cursor.execute('SELECT error_type, COUNT(*) as count FROM scrape_errors GROUP BY error_type')
    error_breakdown = cursor.fetchall()

    conn.close()
    return {
        'total_urls': total_urls,
        'pending': pending,
        'processed': processed,
        'total_details': total_details,
        'total_errors': total_errors,
        'recently_updated': recently_updated,
        'recently_discovered_urls': recently_discovered_urls,
        'error_breakdown': error_breakdown
    }

def export_to_csv(db_path: str, filename: str = 'prestige_flowers_combined.csv') -> None:
    """Export all scraped data to CSV."""
    conn = sqlite3.connect(db_path, timeout=10)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            p.id, p.product_url, p.scraped_at as url_scraped_at, p.status,
            pd.Productid, pd.SKU, pd.name, pd.Description,
            pd.price_retail, pd.price_medium, pd.price_large,
            pd.imageurl, pd.product_url as detail_url, pd.scraped_at as detail_scraped_at
        FROM products p
        LEFT JOIN product_details pd ON p.id = pd.product_id
        ORDER BY p.id
    ''')

    rows = cursor.fetchall()
    conn.close()

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Product ID', 'Product URL', 'URL Scraped At', 'Status',
            'Productid', 'SKU', 'Name', 'Description',
            'Price (Retail)', 'Price (Medium)', 'Price (Large)',
            'Image URL', 'Detail URL', 'Detail Scraped At'
        ])
        writer.writerows(rows)

    logger.info(f"✅ Exported {len(rows)} products to {filename}")

# ===== MAIN ORCHESTRATOR =====
async def main():
    """Main orchestrator function."""
    logger.info("="*100)
    logger.info("🌸 PRESTIGE FLOWERS - MODULAR SCRAPER ORCHESTRATOR")
    logger.info("="*100)

    # Configuration from environment variables with defaults
    db_path = os.getenv('SCRAPER_DB_PATH', 'prestige_flowers.db')
    category_url = os.getenv('SCRAPER_CATEGORY_URL', 'https://www.prestigeflowers.co.uk/christmas-plants')
    details_limit_str = os.getenv('SCRAPER_DETAILS_LIMIT', 'None')
    check_recent_str = os.getenv('SCRAPER_CHECK_RECENT', 'True')
    csv_filename = os.getenv('SCRAPER_CSV_FILENAME', 'prestige_flowers_combined.csv')

    # Parse details_limit (None means no limit)
    details_limit = None if details_limit_str.lower() == 'none' else int(details_limit_str)

    # Parse check_recent boolean
    check_recent = check_recent_str.lower() in ('true', '1', 'yes', 'on')

    logger.info(f"📋 Configuration:")
    logger.info(f"   Database: {db_path}")
    logger.info(f"   Category URL: {category_url}")
    logger.info(f"   Details Limit: {details_limit}")
    logger.info(f"   Check Recent: {check_recent}")
    logger.info(f"   CSV Filename: {csv_filename}")
    logger.info("-"*100)

    # STAGE 1: Scrape Product URLs
    logger.info("\n📍 STAGE 1: Scraping Product URLs")
    logger.info("-"*100)
    try:
        url_result = await scrape_product_urls(category_url, db_path)
        logger.info(f"📊 URLs Result: Inserted {url_result['inserted']}, Updated {url_result.get('updated', 0)}")
    except Exception as e:
        logger.error(f"❌ Stage 1 failed: {e}")
        return

    # STAGE 2: Scrape Product Details
    logger.info("\n📍 STAGE 2: Scraping Product Details")
    logger.info("-"*100)
    try:
        detail_result = await scrape_product_details(details_limit, db_path, check_recent)
    except Exception as e:
        logger.error(f"❌ Stage 2 failed: {e}")
        return

    # Final Summary with Error Breakdown
    logger.info("\n📍 FINAL SUMMARY")
    logger.info("-"*100)
    stats = get_database_stats(db_path)
    logger.info(f"📊 Database Statistics:")
    logger.info(f"   Total URLs: {stats['total_urls']}")
    logger.info(f"   Pending: {stats['pending']}")
    logger.info(f"   Processed: {stats['processed']}")
    logger.info(f"   Total Details: {stats['total_details']}")
    logger.info(f"   Recently Updated (24h): {stats['recently_updated']}")
    logger.info(f"   Recently Discovered URLs (24h): {stats['recently_discovered_urls']}")
    logger.info(f"   Total Errors: {stats['total_errors']}")

    if stats['error_breakdown']:
        logger.info(f"\n📋 Error Breakdown:")
        for error_type, count in stats['error_breakdown']:
            logger.info(f"   {error_type}: {count}")

    export_to_csv(db_path, csv_filename)
    logger.info(f"\n✅ Orchestrator completed successfully!")
    logger.info(f"{'='*100}\n")


if __name__ == "__main__":
    asyncio.run(main())