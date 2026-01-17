import asyncio
import aiohttp
import pandas as pd
import json
from pathlib import Path
from bs4 import BeautifulSoup
import unicodedata
import re
from tqdm import tqdm
import random

# ================= CONFIG =================
INPUT_FILE = "data/products-0-200000.csv"   # cột: id
OUTPUT_DIR = Path("tiki_products")
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

# ---------- Description normalization ----------
def normalize_description(desc: str) -> str:
    if not desc:
        return ""

    soup = BeautifulSoup(desc, "html.parser")
    text = soup.get_text(separator=" ")

    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text[:MAX_DESC_LENGTH]


# ---------- Fetch one product ----------
async def fetch_product(semaphore, session, product_id):
    async with semaphore:
        url = f"https://tiki.vn/api/v2/products/{product_id}"

        non_json_attempts = 0

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Random delay before request to avoid bursts (lighter delay now)
                await asyncio.sleep(random.uniform(1.0, 2.0))
                
                # Rotate User-Agent per request
                headers = {
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "application/json",
                    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
                    "Referer": "https://tiki.vn/",
                    "x-guest-token": "8jWFF5HZAQF7d5h73J2c029199", # Optional: sometimes helps
                }

                async with session.get(
                    url,
                    headers=headers,
                    timeout=TIMEOUT,
                    allow_redirects=False
                ) as resp:
                    content_type = resp.headers.get("Content-Type", "")

                    # ---------- HTTP status != 200 ----------
                    if resp.status != 200:
                        # Handle 302 Redirects (Rate Limit / Maintenance)
                        if resp.status == 302:
                            location = resp.headers.get("Location", "")
                            # If redirect to maintenance or login, it's a block/rate limit
                            if "maintenance" in location or "login" in location or "challenge" in location:
                                print(f" [WARN] Rate Limit/Maintenance detected for {product_id}. Sleeping 30s...")
                                await asyncio.sleep(30) # Backoff
                                continue
                        
                        # retry cho 5xx / 429
                        if resp.status in (429, 500, 502, 503, 504, 302) and attempt < MAX_RETRIES:
                            await asyncio.sleep(BASE_DELAY * attempt)
                            continue

                        return {
                            "product_id": product_id,
                            "success": False,
                            "http_status": resp.status,
                            "error_type": "http_error",
                            "error_message": f"{resp.reason} (Location: {resp.headers.get('Location')})",
                        }

                    # ---------- 200 but not JSON ----------
                    if "application/json" not in content_type:
                        non_json_attempts += 1

                        if non_json_attempts <= MAX_NON_JSON_RETRIES:
                            await asyncio.sleep(BASE_DELAY * non_json_attempts)
                            continue

                        text = await resp.text()
                        return {
                            "product_id": product_id,
                            "success": False,
                            "http_status": resp.status,
                            "error_type": "invalid_content_type",
                            "error_message": f"Expected JSON, got {content_type}",
                            "raw_response": text[:300],
                        }

                    # ---------- JSON OK ----------
                    data = await resp.json()
                    return {
                        "product_id": product_id,
                        "success": True,
                        "data": {
                            "id": data.get("id"),
                            "name": data.get("name"),
                            "url_key": data.get("url_key"),
                            "price": data.get("price"),
                            "description": normalize_description(data.get("description")),
                            "images": [
                                img.get("base_url")
                                for img in data.get("images", [])
                                if img.get("base_url")
                            ],
                        },
                    }

            # ---------- Timeout (NO retry) ----------
            except asyncio.TimeoutError:
                return {
                    "product_id": product_id,
                    "success": False,
                    "http_status": None,
                    "error_type": "timeout",
                    "error_message": f"Timeout > {TIMEOUT}s",
                }

            # ---------- Server disconnected (NO retry) ----------
            except aiohttp.ServerDisconnectedError:
                return {
                    "product_id": product_id,
                    "success": False,
                    "http_status": 0,
                    "error_type": "server_disconnected",
                    "error_message": "Server disconnected",
                }

            # ---------- Connection error (NO retry) ----------
            except aiohttp.ClientConnectionError:
                return {
                    "product_id": product_id,
                    "success": False,
                    "http_status": 0,
                    "error_type": "connection_error",
                    "error_message": "Connection reset by server",
                }

            # ---------- Other ClientError ----------
            except aiohttp.ClientError as e:
                return {
                    "product_id": product_id,
                    "success": False,
                    "http_status": 0,
                    "error_type": "client_error",
                    "error_message": repr(e),
                }

            # ---------- Unknown ----------
            except Exception as e:
                return {
                    "product_id": product_id,
                    "success": False,
                    "http_status": None,
                    "error_type": "unknown_error",
                    "error_message": repr(e),
                }

        return {
            "product_id": product_id,
            "success": False,
            "http_status": None,
            "error_type": "Max attempts exceeded",
            "error_message": "Max attempts exceeded",
        }
# ---------- Process batch ----------
async def process_batch(product_ids, batch_index):
    # Use explicit Semaphore for task concurrency
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    # TCPConnector limit can be higher or same, but Semaphore controls active processing
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS + 5) 

    async with aiohttp.ClientSession(
        connector=connector,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)", # Default, overridden by request headers
            "Accept": "application/json",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
            "Referer": "https://tiki.vn/",
        }
    ) as session:

        tasks = [
            fetch_product(semaphore, session, pid)
            for pid in product_ids
        ]

        results = []
        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            res = await coro
            results.append(res)

        start = batch_index * BATCH_SIZE + 1
        end = start + len(product_ids) - 1

        out_file = OUTPUT_DIR / f"products_{start:05d}_{end:05d}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)


# ---------- Main ----------
async def main():
    df = pd.read_csv(INPUT_FILE)
    # UNCOMMENT LINE BELOW TO TEST WITH FEWER ITEMS
    product_ids = df["id"].head(1000).tolist()
    # product_ids = df["id"].tolist()

    batches = [
        product_ids[i:i + BATCH_SIZE]
        for i in range(0, len(product_ids), BATCH_SIZE)
    ]

    for i, batch in enumerate(batches):
        print(f"\n📦 Processing batch {i+1}/{len(batches)}")
        await process_batch(batch, i)


if __name__ == "__main__":
    asyncio.run(main())