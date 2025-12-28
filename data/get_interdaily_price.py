import requests
import os
from dotenv import load_dotenv
load_dotenv()
import json
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from configs.stock_pool import TRACKED_SYMBOLS

tracked_symbols = TRACKED_SYMBOLS

def get_daily_price(SYMBOL: str):
    # FUNCTION = "TIME_SERIES_DAILY"
    FUNCTION = "TIME_SERIES_INTRADAY"
    INTERVAL = "60min"
    OUTPUTSIZE = 'compact'
    APIKEY = os.getenv("ALPHAADVANTAGE_API_KEY")
    url = f'https://www.alphavantage.co/query?function={FUNCTION}&symbol={SYMBOL}&interval={INTERVAL}&outputsize={OUTPUTSIZE}&entitlement=delayed&apikey={APIKEY}'
    r = requests.get(url)
    data = r.json()
    print(data)
    if data.get('Note') is not None or data.get('Information') is not None:
        print(f"Error")
        return
    with open(f'./daily_prices_{SYMBOL}.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    if SYMBOL == "QQQ":
        with open(f'./Adaily_prices_{SYMBOL}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)



if __name__ == "__main__":
    for symbol in tracked_symbols:
        get_daily_price(symbol)

    get_daily_price("QQQ")
