from pathlib import Path

# ================= CONFIG =================
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data/products-0-200000.csv"   # cột: id
OUTPUT_DIR = BASE_DIR / "tiki_products"
BATCH_SIZE = 1000
TIMEOUT = 60
MAX_DESC_LENGTH = 2000
CONCURRENT_REQUESTS = 10  # Optimal safe limit for Semaphore
MAX_RETRIES = 5
MAX_NON_JSON_RETRIES = 2
BASE_DELAY = 1.0
# =========================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
]

OUTPUT_DIR.mkdir(exist_ok=True)