import json
import os
import glob
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from configs.stock_pool import TRACKED_SYMBOLS

tracked_symbols = set(TRACKED_SYMBOLS)

# 合并所有以 daily_price 开头的 json，逐文件一行写入 merged.jsonl
current_dir = os.path.dirname(__file__)
pattern = os.path.join(current_dir, 'daily_price*.json')
files = sorted(glob.glob(pattern))

output_file = os.path.join(current_dir, 'merged.jsonl')

with open(output_file, 'w', encoding='utf-8') as fout:
    for fp in files:
        basename = os.path.basename(fp)
        # 仅当文件名包含股票池内的代码时才写入
        if not any(symbol in basename for symbol in tracked_symbols):
            continue
        with open(fp, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # 统一重命名："1. open" -> "1. buy price"；"4. close" -> "4. sell price"
        # 对于最新的一天，只保留并写入 "1. buy price"
        try:
            # 查找所有以 "Time Series" 开头的键
            series = None
            for key, value in data.items():
                if key.startswith("Time Series"):
                    series = value
                    break
            if isinstance(series, dict) and series:
                # 先对所有日期做键名重命名
                desired_order = [
                    "1. buy price",
                    "2. high",
                    "3. low",
                    "4. sell price",
                    "5. volume",
                ]

                for d, bar in list(series.items()):
                    if not isinstance(bar, dict):
                        continue
                    if "1. open" in bar:
                        bar["1. buy price"] = bar.pop("1. open")
                    if "4. close" in bar:
                        bar["4. sell price"] = bar.pop("4. close")

                    ordered_bar = {}
                    for key in desired_order:
                        if key in bar:
                            ordered_bar[key] = bar[key]
                    for key, value in bar.items():
                        if key not in ordered_bar:
                            ordered_bar[key] = value
                    series[d] = ordered_bar
                # 再处理最新日期，仅保留买入价
                latest_date = max(series.keys())
                latest_bar = series.get(latest_date, {})
                if isinstance(latest_bar, dict):
                    buy_val = latest_bar.get("1. buy price")
                    series[latest_date] = {"1. buy price": buy_val} if buy_val is not None else {}
                # 更新 Meta Data 描述
                meta = data.get("Meta Data", {})
                if isinstance(meta, dict):
                    meta["1. Information"] = "Daily Prices (buy price, high, low, sell price) and Volumes"
        except Exception:
            # 若结构异常则原样写入
            pass

        fout.write(json.dumps(data, ensure_ascii=False) + "\n")
