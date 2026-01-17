import pandas as pd
from config.config import *
from src.process_batch import process_batch
# ---------- Main ----------
async def main():
    df = pd.read_csv(INPUT_FILE)
    # UNCOMMENT LINE BELOW TO TEST WITH FEWER ITEMS
    product_ids = df["id"].head(100).tolist()
    # product_ids = df["id"].tolist()

    batches = [
        product_ids[i:i + BATCH_SIZE]
        for i in range(0, len(product_ids), BATCH_SIZE)
    ]

    for i, batch in enumerate(batches):
        print(f"\n📦 Processing batch {i+1}/{len(batches)}")
        await process_batch(batch, i)