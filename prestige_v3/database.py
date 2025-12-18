import sqlite3
import logging
from datetime import datetime
import hashlib
import json

logger = logging.getLogger(__name__)

class Database:
    """Handle all database operations for scraping."""

    def __init__(self, db_path: str = 'prestige_flowers_v3.db'):
        self.db_path = db_path
        self.init_database()

    def init_database(self) -> None:
        """Initialize SQLite database with tables."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        cursor = conn.cursor()

        # Drop old tables if exist
        cursor.execute('DROP TABLE IF EXISTS products')
        cursor.execute('DROP TABLE IF EXISTS product_details')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vendor_country_url_msts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vid INTEGER DEFAULT 1,
                cid INTEGER DEFAULT 1,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                url TEXT UNIQUE NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_crawl_time TIMESTAMP,
                cron_run_at TIMESTAMP,
                currency_conv TEXT,
                cron_status INTEGER DEFAULT 0,
                cron_run_status TEXT DEFAULT 'inactive',
                prod_cnt TEXT,
                "from" TEXT,
                "start of the counter" TEXT,
                totcnt TEXT,
                "Total Num" TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vendor_product_management (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pid TEXT,
                fg_vendor_price TEXT,
                cid INTEGER DEFAULT 1,
                vid INTEGER DEFAULT 1,
                vendor_price TEXT,
                product_name TEXT,
                vendor_code TEXT,
                vendor_product_sku TEXT,
                vendor_img TEXT,
                vendor_product_url TEXT,
                prod_description TEXT,
                vendor_price2 TEXT,
                vendor_price3 TEXT,
                vendor_price1_desc TEXT,
                vendor_price2_desc TEXT,
                vendor_price3_desc TEXT,
                action TEXT,
                flag TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scrape_errors (
                error_id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER,
                product_url TEXT,
                error_type TEXT,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()
        logger.info(f"✅ Database initialized: {self.db_path}")

    def save_product_url(self, url: str) -> None:
        """Save a product URL if not exists."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        cursor = conn.cursor()
        try:
            cursor.execute('INSERT OR IGNORE INTO vendor_country_url_msts (url, status) VALUES (?, ?)', (url, 'active'))
            conn.commit()
        except Exception as e:
            logger.error(f"Error saving URL {url}: {e}")
        finally:
            conn.close()

    def get_pending_products(self, limit: int = None) -> list:
        """Get pending products."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        cursor = conn.cursor()
        if limit:
            cursor.execute('SELECT id, url FROM vendor_country_url_msts WHERE status = "active" LIMIT ?', (limit,))
        else:
            cursor.execute('SELECT id, url FROM vendor_country_url_msts WHERE status = "active"')
        products = cursor.fetchall()
        conn.close()
        return products

    def get_existing_product_details(self, product_id: int) -> dict:
        """Get existing product details for comparison."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM vendor_product_management WHERE pid = ?', (product_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                'id': row[0],
                'pid': row[1],
                'fg_vendor_price': row[2],
                'cid': row[3],
                'vid': row[4],
                'vendor_price': row[5],
                'product_name': row[6],
                'vendor_code': row[7],
                'vendor_product_sku': row[8],
                'vendor_img': row[9],
                'vendor_product_url': row[10],
                'prod_description': row[11],
                'vendor_price2': row[12],
                'vendor_price3': row[13],
                'vendor_price1_desc': row[14],
                'vendor_price2_desc': row[15],
                'vendor_price3_desc': row[16],
                'action': row[17],
                'flag': row[18],
                'created_at': row[19],
                'updated_at': row[20],
                # Map to old keys for compatibility
                'vendor_code': row[7],
                'sku': row[8],
                'name': row[6],
                'description': row[11],
                'price_retail': row[5],
                'price_medium': row[12],
                'price_large': row[13],
                'image_url': row[9],
                'product_url': row[10],
                'rating': None,
                'availability': None,
                'delivery_info': None,
                'structured_json': None,
                'data_hash': None
            }
        return None

    def generate_data_hash(self, details: dict) -> str:
        """Generate a hash of key product details to detect changes."""
        key_data = {
            'vendor_code': details.get('vendor_code', ''),
            'sku': details.get('sku', ''),
            'name': details.get('name', ''),
            'description': details.get('description', ''),
            'price_retail': details.get('price_retail'),
            'price_medium': details.get('price_medium'),
            'price_large': details.get('price_large'),
            'image_url': details.get('image_url', ''),
            'rating': details.get('rating'),
            'availability': details.get('availability', ''),
            'delivery_info': details.get('delivery_info', '')
        }
        data_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()

    def save_product_details(self, product_id: int, product_url: str, details: dict) -> tuple:
        """Save or update product details, returning (is_inserted, has_changed)."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            cursor = conn.cursor()

            existing = self.get_existing_product_details(product_id)

            if existing:
                cursor.execute('''
                    UPDATE vendor_product_management SET
                        vendor_price = ?, product_name = ?, vendor_code = ?,
                        vendor_product_sku = ?, vendor_img = ?, vendor_product_url = ?, prod_description = ?,
                        vendor_price2 = ?, vendor_price3 = ?, vendor_price1_desc = ?, vendor_price2_desc = ?, vendor_price3_desc = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE pid = ?
                ''', (
                    details.get('price_retail'),
                    details.get('name'),
                    details.get('vendor_code'),
                    details.get('sku'),
                    details.get('image_url'),
                    product_url,
                    details.get('description'),
                    details.get('price_medium'),
                    details.get('price_large'),
                    'Retail',
                    'Medium',
                    'Large',
                    product_id
                ))
                conn.commit()
                return False, True

            cursor.execute('''
                INSERT INTO vendor_product_management
                (pid, fg_vendor_price, cid, vid, vendor_price, product_name, vendor_code, vendor_product_sku, vendor_img, vendor_product_url, prod_description, vendor_price2, vendor_price3, vendor_price1_desc, vendor_price2_desc, vendor_price3_desc, action, flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                product_id,
                '',
                1,
                1,
                details.get('price_retail'),
                details.get('name'),
                details.get('vendor_code'),
                details.get('sku'),
                details.get('image_url'),
                product_url,
                details.get('description'),
                details.get('price_medium'),
                details.get('price_large'),
                'Retail',
                'Medium',
                'Large',
                '',
                ''
            ))

            cursor.execute('UPDATE vendor_country_url_msts SET status = "inactive" WHERE id = ?', (product_id,))
            conn.commit()
            return True, True

        except Exception as e:
            logger.error(f"Error saving product details for product_id {product_id}: {e}")
            return False, False
        finally:
            if conn:
                conn.close()

    def log_scrape_error(self, product_id: int, product_url: str, error_type: str, error_message: str, retry_count: int = 0) -> None:
        """Log scraping errors."""
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