import json
file = "tiki_products/products_00001_00100.json"
with open(file, encoding="utf-8") as f:
    data = json.load(f)
    error_list = [p for p in data if not p["success"] and p["error_type"] == "client_error"]
    for error_pid in error_list:
        # print(error_pid.get("error_message"))
        print(error_pid)