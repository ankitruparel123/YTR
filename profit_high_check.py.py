import yfinance as yf
import pandas as pd

# Define the stock symbol and listing date
stock_symbol = "BIKAJI.NS"
listing_date = "2022-11-16"  # Adjust to the actual listing date

# Fetch weekly historical price data from Yahoo Finance
data = yf.download(stock_symbol, start=listing_date, interval="1wk")

# Handle MultiIndex columns if they exist
if isinstance(data.columns, pd.MultiIndex):
    data.columns = data.columns.get_level_values(0)  # Flatten the MultiIndex to keep only the price types

# Ensure data has the necessary columns
required_columns = {"Open", "High", "Low", "Close"}
if required_columns.issubset(data.columns):
    # Calculate the price range (PR)
    data['PR'] = data['High'] - data['Low']
    
    # Apply the reverse-engineered formula for ProfitHigh1
    data['Calculated_ProfitHigh1'] = data['Open'] + (data['PR'] * 1.305)  # Hypothesized multiplier

    # Save the results for further analysis
    output_file = "INDIGO_ProfitHigh1_Comparison.csv"
    comparison = data[['Open', 'High', 'Low', 'Close', 'Calculated_ProfitHigh1']]
    comparison.to_csv(output_file, index=True)
    
    # Print confirmation and preview
    print(f"Comparison of Calculated ProfitHigh1 saved to {output_file}")
    print(comparison.head())  # Print first few rows for verification

else:
    print("The required OHLC columns are missing from the data.")
