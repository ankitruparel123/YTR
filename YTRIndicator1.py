import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
from time import sleep
import os

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

# Calculate PROFITHIGH1 for all rows
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

# Determine if a stock is approaching PROFITHIGH1
def calculate_approaching(last_close, profithigh1_current, threshold=0.05):
    if pd.notna(last_close) and pd.notna(profithigh1_current):
        return abs(last_close - profithigh1_current) <= (profithigh1_current * threshold)
    return False  # Default to False if any value is missing

# Generate combined signals
def generate_combined_signals(ytr_list, threshold=0.05):
    combined_signals = []

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
        data_with_indicators['Crossed'] = data_with_indicators['Close'] >= data_with_indicators['Current PROFITHIGH1']

        # Check if the stock has crossed PROFITHIGH1
        first_cross = data_with_indicators[data_with_indicators['Crossed']].head(1)
        if not first_cross.empty:
            combined_signals.append({
                'Symbol': symbol,
                'Status': 'Crossed',
                'First Crossing Date': first_cross.index[0].replace(tzinfo=None),
                'Close at Crossing': first_cross['Close'].iloc[0],
                'PROFITHIGH1': first_cross['Current PROFITHIGH1'].iloc[0],
                'Last Close': None,
                'Current PROFITHIGH1': None,
                'Approaching': None
            })
        else:
            last_close = data_with_indicators['Close'].iloc[-1] if not data_with_indicators.empty else None
            current_profithigh1 = data_with_indicators['Current PROFITHIGH1'].iloc[-1] if not data_with_indicators.empty else None
            approaching = calculate_approaching(last_close, current_profithigh1, threshold)

            combined_signals.append({
                'Symbol': symbol,
                'Status': 'Not Crossed',
                'First Crossing Date': None,
                'Close at Crossing': None,
                'PROFITHIGH1': None,
                'Last Close': last_close,
                'Current PROFITHIGH1': current_profithigh1 if pd.notna(current_profithigh1) else "Profithigh1 not yet formed",
                'Approaching': approaching
            })

    return pd.DataFrame(combined_signals)

# Sort signals for better prioritization
def sort_signals(combined_df):
    """
    Sort the combined DataFrame with the following priority:
    1. Stocks where 'Approaching' is True.
    2. Stocks where 'Current PROFITHIGH1' is valid and not "Profithigh1 not yet formed".
    3. Stocks where 'Current PROFITHIGH1' is "Profithigh1 not yet formed".
    4. Stocks with 'Status' == "Crossed".
    """
    combined_df['Sort Key'] = combined_df.apply(
        lambda row: (
            0 if row['Approaching'] else 
            1 if pd.notna(row['Current PROFITHIGH1']) and row['Current PROFITHIGH1'] != "Profithigh1 not yet formed" else 
            2 if row['Current PROFITHIGH1'] == "Profithigh1 not yet formed" else 
            3,  # Assign priority for crossed stocks
            row['First Crossing Date'] if row['Status'] == 'Crossed' else pd.Timestamp.max  # Secondary sort by date for 'Crossed'
        ),
        axis=1
    )
    combined_df.sort_values(by='Sort Key', inplace=True)
    combined_df.drop(columns=['Sort Key'], inplace=True)  # Remove temporary sort key
    return combined_df

# Save to Excel
def save_to_excel(combined_df, file_path):
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        combined_df.to_excel(writer, sheet_name='Signals', index=False)
    print(f"Excel file saved to {file_path}")

# Main Execution
if __name__ == "__main__":
    input_file = "/Users/ankitruparel/YTR/MasterList/YTR_lists.csv"
    output_file = "/Users/ankitruparel/YTR/MasterList/Sorted_Combined_Signals.xlsx"

    if not os.path.exists(input_file):
        print(f"Error: Input file not found at {input_file}")
        exit(1)

    ytr_list = pd.read_csv(input_file)
    ytr_list['Formatted Symbol'] = ytr_list['Updated Symbol'].apply(format_symbol)
    ytr_list['Open Date'] = ytr_list['Open Date'].apply(validate_date)
    ytr_list['Listing Date'] = ytr_list['Listing Date'].apply(validate_date)

    ytr_list['Valid Start Date'] = ytr_list.apply(
        lambda row: row['Open Date'] if pd.notna(row['Open Date']) else row['Listing Date'], axis=1
    ).fillna('2022-01-01')

    combined_df = generate_combined_signals(ytr_list)
    combined_df = sort_signals(combined_df)
    save_to_excel(combined_df, output_file)
