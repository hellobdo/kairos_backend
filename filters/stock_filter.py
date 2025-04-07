from filters.get_stocks_polygon import get_polygon_tickers_data
from datetime import datetime, timedelta
import pandas as pd
import time
import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
polygon_api_key = os.getenv("POLYGON_API_KEY")

def get_aggregates(ticker, start_date, end_date, multiplier=1, timespan='day'):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}"
    params = {
        "apiKey": polygon_api_key,
        "adjusted": "true",
        "sort": "desc",
        "limit": 50000
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        return None
    data = response.json()
    return data.get("results", [])


if __name__ == "__main__":
    data = get_polygon_tickers_data()
    print(data)