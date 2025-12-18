import random
from playwright_stealth import Stealth

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/120.0',
]

async def apply_stealth(page):
    """Apply enhanced stealth plugins to evade detection."""
    stealth = Stealth()
    await stealth.apply_stealth_async(page)  # Use playwright-stealth for comprehensive evasion
    
    # Randomize user agent and viewport
    ua = random.choice(user_agents)
    await page.set_extra_http_headers({'User-Agent': ua})
    viewport = {'width': random.randint(1024, 1920), 'height': random.randint(768, 1080)}
    await page.set_viewport_size(viewport)
    
    # Additional custom stealth
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [{}]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        // Simulate mouse movement
        document.addEventListener('DOMContentLoaded', () => {
            const event = new MouseEvent('mousemove', {clientX: Math.random() * window.innerWidth, clientY: Math.random() * window.innerHeight});
            document.dispatchEvent(event);
        });
    """)
    
    # Random scroll to simulate browsing
    await page.evaluate("window.scrollTo(0, Math.random() * document.body.scrollHeight);")