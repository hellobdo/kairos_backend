import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
from datetime import datetime, timedelta
import yfinance as yf

# Example: Download data
symbol = "QQQ"
df = yf.download(symbol, start=datetime.now() - timedelta(days=365), end=datetime.now())

print("DataFrame columns:")
print(df.columns)

# Handle the MultiIndex correctly
if isinstance(df.columns, pd.MultiIndex):
    print("Detected MultiIndex, fixing column structure")
    # Get the correct columns - we need to use level 0 names (Price) instead of dropping them
    # Create a new DataFrame with proper column names
    new_df = pd.DataFrame()
    new_df['Open'] = df[('Open', symbol)]
    new_df['High'] = df[('High', symbol)]
    new_df['Low'] = df[('Low', symbol)]
    new_df['Close'] = df[('Close', symbol)]
    new_df['Volume'] = df[('Volume', symbol)]
    # Set the index to match the original dataframe
    new_df.index = df.index
    df = new_df
    print("New columns:")
    print(df.columns)

# Define and calculate T-shaped condition
tight_threshold = 0.005

# Safe calculation of condition 3 (using numpy where)
# This avoids the indexing issues with masks
abs_open_close = abs(df['Open'] - df['Close'])
abs_high_open = abs(df['High'] - df['Open'])
abs_low_open = abs(df['Low'] - df['Open'])

# Calculate condition3 using numpy.where to handle division by zero
condition3_values = np.where(
    abs_high_open != 0,  # Condition to check
    abs_low_open / abs_high_open > 2.5,  # True case
    False  # False case (when denominator is 0)
)

# Apply all conditions
df['is_t_shaped'] = (
    (abs_open_close / df['Open'] < tight_threshold) &  # Condition 1
    (df['Low'] < df['Open']) &  # Condition 2
    condition3_values  # Condition 3
)

# Print the number of T-shaped candles found
t_shaped_count = df['is_t_shaped'].sum()
print(f"Found {t_shaped_count} T-shaped candles out of {len(df)} total candles")

# Filter to get only the T-shaped candles
t_shaped_df = df[df['is_t_shaped']]
print(t_shaped_df)

'''
# Visualize the results
# Option 1: Plot all candles and highlight T-shaped ones
plt.figure(figsize=(15, 7))

# Convert to list dates for mplfinance
# Use the addplot to mark T-shaped candles instead of vlines
apdict = mpf.make_addplot(df['is_t_shaped'].astype(int), scatter=True, 
                         markersize=50, marker='^', color='red', panel=0)

mpf.plot(df, type='candle', style='yahoo', 
         title=f"{symbol} with T-shaped Candles Highlighted",
         addplot=apdict)
'''

# Option 2: Plot only the T-shaped candles with surrounding context
# For each T-shaped candle, show a window of 10 days around it
for idx in t_shaped_df.index:
    try:
        # Get 5 days before and 5 days after
        start_idx = df.index.get_loc(idx) - 5
        end_idx = df.index.get_loc(idx) + 5
        
        if start_idx < 0:
            start_idx = 0
        if end_idx >= len(df):
            end_idx = len(df) - 1
            
        window_df = df.iloc[start_idx:end_idx+1]
        
        # Highlight the T-shaped candle
        highlight = [idx]
        
        plt.figure(figsize=(10, 5))
        mpf.plot(window_df, type='candle', style='yahoo',
                title=f"T-shaped Candle on {idx.date()}",
                vlines=dict(vlines=highlight, colors=['r'], linewidths=2))
    except Exception as e:
        print(f"Error plotting candle at {idx}: {e}")


'''
Option 3: Create a custom marker for T-shaped candles and plot all data
apdict = mpf.make_addplot(df['is_t_shaped'].astype(int), scatter=True, markersize=100, 
                          marker='^', color='red', panel=0)

plt.figure(figsize=(15, 7))
mpf.plot(df, type='candle', style='yahoo', addplot=apdict,
         title=f"{symbol} Price Chart with T-shaped Candles Marked")

plt.show()
'''