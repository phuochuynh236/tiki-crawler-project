from bs4 import BeautifulSoup
import unicodedata
import re
from config.config import *

# ---------- Description normalization ----------
def normalize_description(desc: str) -> str:
    if not desc:
        return ""

    soup = BeautifulSoup(desc, "html.parser")
    text = soup.get_text()

    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text[:MAX_DESC_LENGTH]