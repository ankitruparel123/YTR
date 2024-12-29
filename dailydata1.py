import yfinance as yf
import pandas as pd
import os
import time

# Load the YTR list
file_path = '/Users/ankitruparel/YTR/MasterList/YTR_lists.csv'
ytr_list = pd.read_csv(file_path)

# Convert the 'Listing Date' column to datetime
ytr_list['Listing Date'] = pd.to_datetime(ytr_list['Listing Date'], format='%b %d, %Y', errors='coerce')

# Extract the "Formatted Symbol" column
symbols = ytr_list['Formatted Symbol'].dropna().tolist()

# Output consolidated file
output_file = 'ytr_daily_data.csv'
temp_folder = 'temp_ytr_data_daily'  # Temporary folder for individual symbol data

# Create the temp folder if it doesn't exist
if not os.path.exists(temp_folder):
    os.makedirs(temp_folder)

# Function to fetch daily data for a single stock
def fetch_daily_data(symbol):
    print(f"Fetching daily data for {symbol} from maximum available history...")
    try:
        # Fetch daily data
        data = yf.download(symbol, interval='1d')
        if isinstance(data.index, pd.MultiIndex):
            data.reset_index(level=0, inplace=True)
        if not data.empty:
            data.reset_index(inplace=True)  # Ensure 'Date' is a column
            data.rename(columns={'index': 'Date'}, inplace=True)
            data['symbol'] = symbol  # Add symbol column
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

# Process each symbol
failed_symbols = []
for symbol in symbols:
    temp_file = os.path.join(temp_folder, f"{symbol}.csv")

    # Skip symbols already processed
    if os.path.exists(temp_file):
        print(f"{symbol} already processed. Skipping.")
        continue

    # Fetch data
    data = fetch_daily_data(symbol)
    if not data.empty:
        # Save individual symbol data
        data.to_csv(temp_file, index=False)
        print(f"Data for {symbol} saved to {temp_file}.")
    else:
        failed_symbols.append(symbol)
        print(f"No data fetched for {symbol}.")

    time.sleep(1)  # Avoid hitting API rate limits

# Consolidate all temp files into one
print("Consolidating all temporary files into a single CSV...")
all_data = pd.DataFrame()
for file in os.listdir(temp_folder):
    file_path = os.path.join(temp_folder, file)
    symbol_data = pd.read_csv(file_path)
    all_data = pd.concat([all_data, symbol_data], ignore_index=True)

# Save consolidated file
all_data.to_csv(output_file, index=False)
print(f"Consolidated data saved to {output_file}.")

# Log failed symbols
if failed_symbols:
    with open('failed_symbols_daily.txt', 'w') as f:
        f.write('\n'.join(failed_symbols))
    print(f"Failed symbols logged to 'failed_symbols_daily.txt'.")
