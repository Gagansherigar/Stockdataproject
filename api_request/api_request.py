import os
import requests

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

def fetch_data(symbol):
    symbol = symbol.strip().upper()  
    print(f"Fetching stock market data for {symbol}...")

    if not API_KEY:
        raise RuntimeError("ALPHA_VANTAGE_KEY is not set!")

    api_url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_INTRADAY"
        f"&symbol={symbol}"
        f"&interval=60min"
        f"&apikey={API_KEY}"
    )

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        print(f"API response received for {symbol}")

        data = response.json()

        
        print(f"Top keys for {symbol}: {list(data.keys())}")

        if "Error Message" in data:
            print(f"ALPHA ERROR for {symbol}: {data['Error Message']}")
        if "Note" in data:
            print(f"RATE LIMIT for {symbol}: {data['Note']}")

        return data

    except requests.exceptions.RequestException as e:
        print(f"Request failed for {symbol}: {e}")
        raise
