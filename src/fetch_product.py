import requests


def fetch_tiki_product(product_id, timeout=10):
    """
    Lấy thông tin sản phẩm từ Tiki API theo product_id

    :param product_id: int | str – ID sản phẩm Tiki
    :param timeout: int – thời gian timeout (giây)
    :return: dict | None
    """
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] product_id={product_id} | {e}")
        return None