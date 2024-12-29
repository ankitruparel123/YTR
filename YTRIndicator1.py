import yfinance as yf
import pandas as pd
from datetime import datetime
from time import sleep

# Function to fetch weekly data with `max` as the start date
def get_weekly_data(symbol, retries=3):
    for attempt in range(retries):
        try:
            print(f"Attempting to fetch data for {symbol} (Attempt {attempt + 1})...")
            ticker = yf.Ticker(symbol)
            # Use max to fetch the maximum available history
            weekly_data = ticker.history(interval='1wk')
            if weekly_data.empty:
                print(f"Warning: No data for {symbol}.")
                return None
            return weekly_data
        except Exception as e:
            print(f"Error fetching data for {symbol} (Attempt {attempt + 1}): {e}")
            sleep(2)  # Retry delay
    return None

# Test the function for AADHARHFC.NS
if __name__ == "__main__":
    symbol = "AADHARHFC.NS"

    print(f"Fetching weekly data for {symbol} from maximum available history...")
    data = get_weekly_data(symbol)
    
    if data is not None:
        print(f"Successfully fetched data for {symbol}")
        print(data.head())  # Show first few rows
        # Save to CSV for inspection
        data.to_csv(f"{symbol}_weekly_data.csv", index=True)
        print(f"Data saved to {symbol}_weekly_data.csv")
    else:
        print(f"Failed to fetch data for {symbol}.")
