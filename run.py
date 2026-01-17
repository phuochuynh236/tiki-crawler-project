import asyncio
import aiohttp
import pandas as pd
import json
from pathlib import Path
from bs4 import BeautifulSoup
import unicodedata
import re
from typing import List

INPUT_CSV = "data/products-0-200000.csv"
OUTPUT_DIR = Path("tiki_products/tiki_products")
BATCH_SIZE = 1000
CONCURRENT_REQUESTS = 50
TIMEOUT = 15

OUTPUT_DIR.mkdir(exist_ok=True)


# -------------------------
# Normalize description
# -------------------------
def normalize_description(text: str | None) -> str:
    if not text:
        return ""

    soup = BeautifulSoup(text, "html.parser")
    clean_text = soup.get_text(separator=" ")
    clean_text = unicodedata.normalize("NFKC", clean_text)
    clean_text = re.sub(r"\s+", " ", clean_text).strip()
    return clean_text


# -------------------------
# Fetch one product
# -------------------------
async def fetch_product(session, product_id):
    url = f"https://tiki.vn/api/v2/products/{product_id}"

    try:
        async with session.get(url, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                return None

            data = await resp.json()

            return {
                "id": data.get("id"),
                "name": data.get("name"),
                "url_key": data.get("url_key"),
                "price": data.get("price"),
                "description": normalize_description(data.get("description")),
                "images": [img.get("base_url") for img in data.get("images", [])]
            }

    except Exception:
        return None


# -------------------------
# Fetch batch
# -------------------------
async def fetch_batch(product_ids: List[int], batch_idx: int):
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [fetch_product(session, pid) for pid in product_ids]
        results = await asyncio.gather(*tasks)

    products = [p for p in results if p]

    output_file = OUTPUT_DIR / f"products_batch_{batch_idx:04d}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(products)} products → {output_file}")


# -------------------------
# Main
# -------------------------
async def main():
    df = pd.read_csv(INPUT_CSV)
    product_ids = df["id"].head(1000).tolist()

    batches = [
        product_ids[i:i + BATCH_SIZE]
        for i in range(0, len(product_ids), BATCH_SIZE)
    ]

    for idx, batch in enumerate(batches):
        await fetch_batch(batch, idx)
        await asyncio.sleep(0.5)  # tránh bị rate-limit


if __name__ == "__main__":
    asyncio.run(main())
