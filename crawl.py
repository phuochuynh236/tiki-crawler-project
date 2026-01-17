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
INPUT_FILE = "data/products-0-200000.csv"   # cột: product_id
OUTPUT_DIR = Path("tiki_products")
BATCH_SIZE = 1000
CONCURRENT_REQUESTS = 15
TIMEOUT = 20
MAX_DESC_LENGTH = 2000

MAX_RETRIES = 2
RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
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
async def fetch_product(session, product_id, semaphore):
    url = f"https://tiki.vn/api/v2/products/{product_id}"

    for attempt in range(MAX_RETRIES + 1):
        async with semaphore:
            try:
                async with session.get(url) as resp:
                    status = resp.status

                    if status != 200:
                        # retry được
                        if status in RETRYABLE_HTTP_STATUS and attempt < MAX_RETRIES:
                            await asyncio.sleep(0.5 * (attempt + 1))
                            continue

                        return {
                            "product_id": product_id,
                            "success": False,
                            "http_status": status,
                            "error_type": "http_error"
                        }

                    # tránh response bị truncate
                    text = await resp.text(errors="ignore")
                    data = json.loads(text)

                    return {
                        "product_id": product_id,
                        "success": True,
                        "http_status": 200,
                        "data": {
                            "id": data.get("id"),
                            "name": data.get("name"),
                            "url_key": data.get("url_key"),
                            "price": data.get("price"),
                            "description": normalize_description(
                                data.get("description")
                            ),
                            "images": [
                                img.get("base_url")
                                for img in data.get("images", [])
                            ]
                        }
                    }

            except asyncio.TimeoutError:
                return {
                    "product_id": product_id,
                    "success": False,
                    "http_status": None,
                    "error_type": "timeout"
                }

            except aiohttp.ClientConnectorError as e:
                error_type = "connection_error"
                return {
                    "product_id": product_id,
                    "success": False,
                    "http_status": None,
                    "error_type": error_type,
                    "error_message": str(e)
                }
            except aiohttp.ClientResponseError as e:
                # retry cho response_error
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue

                error_type = "response_error"
                return {
                    "product_id": product_id,
                    "success": False,
                    "http_status": e.status,
                    "error_type": error_type,
                    "error_message": str(e)
                }

            except aiohttp.ClientError as e:
                error_type = "client_error"
                return {
                    "product_id": product_id,
                    "success": False,
                    "http_status": None,
                    "error_type": error_type,
                    "error_message": str(e)
                }

            except Exception as e:
                return {
                    "product_id": product_id,
                    "success": False,
                    "http_status": None,
                    "error_type": "unknown_error",
                    "error_message": str(e)
                }

# ---------- Process batch ----------
async def process_batch(session, product_ids, batch_index, semaphore):
    # semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    # async with aiohttp.ClientSession(
    #     headers={
    #         "User-Agent": "Mozilla/5.0",
    #         "Accept": "application/json"
    #     }
    # ) as session:

    tasks = [
            fetch_product(session, pid, semaphore)
            for pid in product_ids
        ]

    results = []
    for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        res = await coro
        if res:
            results.append(res)

    start = batch_index * BATCH_SIZE + 1
    end = start + len(product_ids) - 1

    out_file = OUTPUT_DIR / f"products_{start:05d}_{end:05d}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)


# ---------- Main ----------
async def main():
    df = pd.read_csv(INPUT_FILE)
    product_ids = df["id"].head(2000).tolist() # Gioi han so luong product_id

    batches = [
        product_ids[i:i + BATCH_SIZE]
        for i in range(0, len(product_ids), BATCH_SIZE)
    ]

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    connector = aiohttp.TCPConnector(
        limit=CONCURRENT_REQUESTS,
        force_close=True,
        enable_cleanup_closed=True
    )

    timeout = aiohttp.ClientTimeout(total=TIMEOUT)

    async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
    ) as session:
        for i, batch in enumerate(batches):
            print(f"\n📦 Processing batch {i+1}/{len(batches)}")
            await process_batch(session, batch, i, semaphore)


if __name__ == "__main__":
    asyncio.run(main())

    # ===== SUMMARY =====
    import json, glob
    from collections import Counter

    DATA_DIR = "tiki_products"

    total_requests = 0
    success_count = 0
    error_counter = Counter()

    for file in glob.glob(f"{DATA_DIR}/*.json"):
        with open(file, encoding="utf-8") as f:
            records = json.load(f)

        for r in records:
            total_requests += 1
            if r.get("success"):
                success_count += 1
            else:
                error_counter[
                    (r.get("error_type"), r.get("http_status"))
                ] += 1

    print("\n===== REQUEST SUMMARY =====")
    print(f"Total requests   : {total_requests}")
    print(f"Success requests : {success_count}")
    print(f"Failed requests  : {total_requests - success_count}")
    print(f"Success rate     : {success_count / total_requests:.2%}")

    print("\n===== ERROR BREAKDOWN =====")
    for (err_type, status), cnt in error_counter.most_common():
        print(f"{err_type:15s} | status={status} | {cnt}")