import asyncio
import logging
import csv
import sqlite3
from product_url_scraper import scrape_product_urls
from product_details_scraper import scrape_product_details

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
def get_database_stats(db_path: str = 'prestige_flowers.db') -> dict:
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

    cursor.execute('SELECT error_type, COUNT(*) as count FROM scrape_errors GROUP BY error_type')
    error_breakdown = cursor.fetchall()

    conn.close()
    return {
        'total_urls': total_urls,
        'pending': pending,
        'processed': processed,
        'total_details': total_details,
        'total_errors': total_errors,
        'error_breakdown': error_breakdown
    }

def export_to_csv(db_path: str = 'prestige_flowers.db', filename: str = 'prestige_flowers_combined.csv') -> None:
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

    db_path = 'prestige_flowers.db'
    category_url = "https://www.prestigeflowers.co.uk/christmas-flowers"
    details_limit = 2  # Limit for testing, can be None for all

    # STAGE 1: Scrape Product URLs
    logger.info("\n📍 STAGE 1: Scraping Product URLs")
    logger.info("-"*100)
    try:
        url_result = await scrape_product_urls(category_url, db_path)
        logger.info(f"📊 URLs Result: Inserted {url_result['inserted']}, Skipped {url_result['skipped']}")
    except Exception as e:
        logger.error(f"❌ Stage 1 failed: {e}")
        return

    # STAGE 2: Scrape Product Details
    logger.info("\n📍 STAGE 2: Scraping Product Details")
    logger.info("-"*100)
    try:
        detail_result = await scrape_product_details(details_limit, db_path)
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
    logger.info(f"   Total Errors: {stats['total_errors']}")

    if stats['error_breakdown']:
        logger.info(f"\n📋 Error Breakdown:")
        for error_type, count in stats['error_breakdown']:
            logger.info(f"   {error_type}: {count}")

    export_to_csv(db_path)
    logger.info(f"\n✅ Orchestrator completed successfully!")
    logger.info(f"{'='*100}\n")


if __name__ == "__main__":
    asyncio.run(main())