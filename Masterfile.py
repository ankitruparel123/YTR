import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
from time import sleep

# Format symbols for Yahoo Finance
def format_symbol(symbol):
    if not symbol.endswith(".NS"):
        return symbol + ".NS"
    return symbol

# Validate and format dates
def validate_date(date):
    if pd.isna(date):
        return None
    try:
        for fmt in ['%b %d, %Y', '%Y-%m-%d']:
            try:
                return datetime.strptime(date, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        raise ValueError(f"Date format not recognized: {date}")
    except Exception as e:
        print(f"Invalid date: {date}. Error: {e}")
        return None

# Fetch weekly data
def get_weekly_data(symbol, start_date="2022-01-01", retries=3):
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(symbol)
            weekly_data = ticker.history(start=start_date, interval='1wk')
            if weekly_data.empty:
                print(f"Warning: No data for {symbol}.")
                return None
            return weekly_data
        except Exception as e:
            print(f"Error fetching data for {symbol} (Attempt {attempt + 1}): {e}")
            sleep(2)
    return None

# Calculate PROFITHIGH1
def calculate_profithigh1(df):
    df['PROFITHIGH1'] = np.nan
    for year in df.index.year.unique():
        if year == df.index.year.min():
            continue
        yearly_open = df[df.index.year == year]['Open'].iloc[0]
        prev_year_data = df[df.index.year == year - 1]
        if not prev_year_data.empty:
            prev_high = prev_year_data['High'].max()
            prev_low = prev_year_data['Low'].min()
            price_range = prev_high - prev_low
            profithigh1 = yearly_open + (price_range * 0.792)
            df.loc[df.index.year == year, 'PROFITHIGH1'] = profithigh1
    return df

# Signal generation
def generate_signals(ytr_list, threshold=0.05):
    crossed_stocks = []
    not_crossed_stocks = []

    for _, row in ytr_list.iterrows():
        symbol = row['Formatted Symbol']
        start_date = row['Valid Start Date']
        if pd.isna(symbol) or pd.isna(start_date):
            print(f"Skipping row: {row}")
            continue

        weekly_data = get_weekly_data(symbol, start_date=start_date)
        if weekly_data is None:
            continue

        data_with_indicators = calculate_profithigh1(weekly_data)
        data_with_indicators['Crossed'] = data_with_indicators['Close'] >= data_with_indicators['PROFITHIGH1']

        # Check if the stock has crossed PROFITHIGH1
        first_cross = data_with_indicators[data_with_indicators['Crossed']].head(1)
        if not first_cross.empty:
            crossed_stocks.append({
                'Symbol': symbol,
                'First Crossing Date': first_cross.index[0],
                'Close at Crossing': first_cross['Close'].iloc[0],
                'PROFITHIGH1': first_cross['PROFITHIGH1'].iloc[0],
            })
        else:
            # Track stocks that have not crossed or are approaching
            last_close = data_with_indicators['Close'].iloc[-1] if not data_with_indicators.empty else None
            current_profithigh1 = data_with_indicators['PROFITHIGH1'].iloc[-1] if not data_with_indicators.empty else None
            if last_close and current_profithigh1:
                approaching = abs(last_close - current_profithigh1) <= (threshold * current_profithigh1)
                not_crossed_stocks.append({
                    'Symbol': symbol,
                    'Last Close': last_close,
                    'Current PROFITHIGH1': current_profithigh1,
                    'Approaching': approaching
                })

    return pd.DataFrame(crossed_stocks), pd.DataFrame(not_crossed_stocks)

# Main Execution
if __name__ == "__main__":
    input_file = "/Users/ankitruparel/YTR/MasterList/YTR_lists.csv"
    crossed_output_file = "/Users/ankitruparel/YTR/MasterList/Crossed_Signals.csv"
    not_crossed_output_file = "/Users/ankitruparel/YTR/MasterList/NotCrossed_Signals.csv"

    # Load the YTR list
    ytr_list = pd.read_csv(input_file)

    # Format symbols and validate dates
    ytr_list['Formatted Symbol'] = ytr_list['Updated Symbol'].apply(format_symbol)
    ytr_list['Open Date'] = ytr_list['Open Date'].apply(validate_date)
    ytr_list['Listing Date'] = ytr_list['Listing Date'].apply(validate_date)

    # Handle missing dates
    ytr_list['Valid Start Date'] = ytr_list.apply(
        lambda row: row['Open Date'] if pd.notna(row['Open Date']) else row['Listing Date'], axis=1
    )
    ytr_list['Valid Start Date'] = ytr_list['Valid Start Date'].fillna('2022-01-01')

    # Generate signals
    crossed_df, not_crossed_df = generate_signals(ytr_list)

    # Save the signals to CSVs
    crossed_df.to_csv(crossed_output_file, index=False)
    not_crossed_df.to_csv(not_crossed_output_file, index=False)

    print(f"Crossed signals saved to {crossed_output_file}")
    print(f"Not crossed signals saved to {not_crossed_output_file}")
