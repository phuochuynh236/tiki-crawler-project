import asyncio
import aiohttp
import pandas as pd
import json
from pathlib import Path
from bs4 import BeautifulSoup
import unicodedata
import re
from tqdm import tqdm

# ================= CONFIG =================
INPUT_FILE = "data/products-0-200000.csv"   # cột: id
OUTPUT_DIR = Path("tiki_products")
BATCH_SIZE = 1000
TIMEOUT = 60
MAX_DESC_LENGTH = 2000
CONCURRENT_REQUESTS = 15
MAX_RETRIES = 10
MAX_NON_JSON_RETRIES = 2
BASE_DELAY = 1.0
# =========================================

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
async def fetch_product(session, product_id):
    url = f"https://tiki.vn/api/v2/products/{product_id}"

    non_json_attempts = 0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(
                url,
                timeout=TIMEOUT,
                allow_redirects=False
            ) as resp:
                content_type = resp.headers.get("Content-Type", "")

                # ---------- HTTP status != 200 ----------
                if resp.status != 200:
                    # retry cho 5xx / 429
                    if resp.status in (429, 500, 502, 503, 504, 302) and attempt < MAX_RETRIES:
                        await asyncio.sleep(BASE_DELAY * attempt)
                        continue

                    return {
                        "product_id": product_id,
                        "success": False,
                        "http_status": resp.status,
                        "error_type": "http_error",
                        "error_message": resp.reason,
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
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(
        connector=connector,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
            "Accept": "application/json",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
            "Referer": "https://tiki.vn/",
        }
    ) as session:

        tasks = [
            fetch_product(session, pid)
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
    product_ids = df["id"].head(100).tolist()

    batches = [
        product_ids[i:i + BATCH_SIZE]
        for i in range(0, len(product_ids), BATCH_SIZE)
    ]

    for i, batch in enumerate(batches):
        print(f"\n📦 Processing batch {i+1}/{len(batches)}")
        await process_batch(batch, i)


if __name__ == "__main__":
    asyncio.run(main())