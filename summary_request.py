import json
import glob
from collections import Counter

DATA_DIR = "tiki_products"

total_requests = 0
success_count = 0
error_counter = Counter()

# đọc tất cả file batch
for file in glob.glob(f"{DATA_DIR}/*.json"):
    with open(file, encoding="utf-8") as f:
        records = json.load(f)

    for r in records:
        total_requests += 1

        if r.get("success") is True:
            success_count += 1
        else:
            key = (
                r.get("error_type"),
                r.get("http_status")
            )
            error_counter[key] += 1

# ---------- In kết quả ----------
failed_requests = total_requests - success_count

print("\n===== REQUEST SUMMARY =====")
print(f"Total requests   : {total_requests}")
print(f"Success requests : {success_count}")
print(f"Failed requests  : {failed_requests}")
print(f"Success rate     : {success_count / total_requests:.2%}")

print("\n===== ERROR BREAKDOWN =====")
for (err_type, status), cnt in error_counter.most_common():
    print(f"{err_type:15s} | status={status} | {cnt}")