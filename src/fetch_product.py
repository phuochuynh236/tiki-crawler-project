import asyncio
import aiohttp
import random
from config.config import *
from src.normalize_description import normalize_description

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
                    "x-guest-token": "8jWFF5HZAQF7d5h73J2c029199",  # Optional: sometimes helps
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
                                await asyncio.sleep(30)  # Backoff
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