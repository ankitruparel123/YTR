import pandas as pd
import numpy as np
from datetime import datetime

# File paths
weekly_data_path = "/Users/ankitruparel/YTR/ytr_full_data.csv"
output_path = "/Users/ankitruparel/YTR/trade_logs_with_profit.csv"

# Function to calculate PROFITHIGH1 for all rows
def calculate_profithigh1(df):
    df['Current PROFITHIGH1'] = np.nan  # Initialize PROFITHIGH1 column
    for year in df.index.year.unique():
        if year == df.index.year.min():  # Skip the first year (no prior year data)
            continue

        current_year_data = df[df.index.year == year]
        if current_year_data.empty:
            continue

        yearly_open = current_year_data['Open'].iloc[0]
        prev_year_data = df[df.index.year == year - 1]

        if not prev_year_data.empty:
            prev_high = prev_year_data['High'].max()
            prev_low = prev_year_data['Low'].min()
            price_range = prev_high - prev_low
            profithigh1 = yearly_open + (price_range * 0.792)
            df.loc[df.index.year == year, 'Current PROFITHIGH1'] = profithigh1
    return df

# Load weekly data
weekly_data = pd.read_csv(weekly_data_path)

# Ensure correct data types
numeric_columns = ['Open', 'Close', 'High', 'Low', 'Volume']
for col in numeric_columns:
    if col in weekly_data.columns:
        weekly_data[col] = pd.to_numeric(weekly_data[col], errors='coerce')

# Check for the `date` column
if 'date' not in weekly_data.columns:
    print("The 'date' column is missing. Attempting to reconstruct...")
    possible_date_columns = ['Date', 'DATE', 'timestamp']
    for col in possible_date_columns:
        if col in weekly_data.columns:
            weekly_data.rename(columns={col: 'date'}, inplace=True)
            break
    else:
        raise ValueError("'date' column is missing from the dataset.")

# Convert 'date' column to datetime format
weekly_data['date'] = pd.to_datetime(weekly_data['date'], errors='coerce')
weekly_data = weekly_data.dropna(subset=['date'])  # Drop rows where 'date' is invalid
weekly_data.set_index('date', inplace=True)  # Set 'date' as index

# Prepare data for iteration
stocks = weekly_data['symbol'].unique()  # Get unique stock symbols

# Initialize trade logs
trade_logs = []

# Process each stock
for stock in stocks:
    print(f"Processing {stock}...")
    stock_data = weekly_data[weekly_data['symbol'] == stock].copy()
    
    try:
        # Calculate PROFITHIGH1
        stock_data = calculate_profithigh1(stock_data)

        # Identify the first buy signal
        buy_signal = stock_data[stock_data['Close'] > stock_data['Current PROFITHIGH1']]
        if not buy_signal.empty:
            first_signal = buy_signal.iloc[0]
            buy_date = first_signal.name
            buy_price = first_signal['Close']
            profithigh1 = first_signal['Current PROFITHIGH1']
            latest_price = stock_data['Close'].iloc[-1]
            profit = latest_price - buy_price
            profit_pct = (profit / buy_price) * 100

            # Add to trade logs
            trade_logs.append({
                "Stock Name": stock,
                "Buy Date": buy_date,
                "Buy Price": buy_price,
                "PROFITHIGH1": profithigh1,
                "Buy Signal": True,
                "Latest Price": latest_price,
                "Profit (₹)": profit,
                "Profit (%)": profit_pct
            })
        else:
            print(f"No buy signal for {stock}")
    except Exception as e:
        print(f"Error processing {stock}: {e}")

# Convert trade logs to DataFrame and save
trade_logs_df = pd.DataFrame(trade_logs)
trade_logs_df.to_csv(output_path, index=False)
print(f"Trade logs saved to {output_path}")
