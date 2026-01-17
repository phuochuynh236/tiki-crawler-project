import asyncio
import aiohttp
from tqdm import tqdm
from config.config import *
from src.fetch_product import fetch_product
import json

# ---------- Process batch ----------
async def process_batch(product_ids, batch_index):
    # Use explicit Semaphore for task concurrency
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    # TCPConnector limit can be higher or same, but Semaphore controls active processing
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS + 5)

    async with aiohttp.ClientSession(
            connector=connector,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",  # Default, overridden by request headers
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