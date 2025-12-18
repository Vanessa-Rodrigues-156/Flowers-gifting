# Prestige V3 Scraper

A robust web scraper for Prestige Flowers product data, combining the best features from previous versions with anti-bot measures.

## Features

- **Modular Design**: Separate classes for database, URL scraping, and detail scraping.
- **Anti-Bot Protection**: Custom stealth measures to bypass Cloudflare challenges.
- **Change Detection**: Hash-based comparison to avoid re-scraping unchanged data.
- **Error Handling**: Comprehensive logging and error recovery.
- **Database Storage**: SQLite with tables for products and details.
- **Configurable**: Environment variables for customization.

## Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Install Playwright: `playwright install chromium`
3. Run database setup: `python db_setup.py`
4. Run scraper: `python prestige_v3_scraper.py`

## Configuration

- `SCRAPER_DB_PATH`: Database file path (default: prestige_flowers_v3.db)
- `SCRAPER_CATEGORY_URL`: Category page URL (default: https://www.prestigeflowers.co.uk/christmas-plants)
- `SCRAPER_STEALTH_ENABLED`: Enable stealth (default: true)
- `SCRAPER_RATE_LIMIT`: Delay between requests in seconds (default: 2)

## Output

- SQLite database with product URLs and details (including name, description, prices, SKU, image, rating, availability, delivery info).
- Console logs with scraping progress.

## Improvements Over Previous Versions

- Better modularity and maintainability.
- Enhanced anti-bot evasion with stealth.
- Improved error handling and challenge detection.
- Successful extraction from protected pages (partial success rate).