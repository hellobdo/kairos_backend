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

def save_t_shaped_candle(df, idx, output_dir='indicators/examples'):
    """
    Save a single T-shaped candle plot as PNG.
    Shows a window of 10 days around the T-shaped candle.
    
    Args:
        df: Full DataFrame with all candles
        idx: Index of the T-shaped candle
        output_dir: Directory to save the PNG files
    """
    try:
        # Get 5 days before and 5 days after
        start_idx = df.index.get_loc(idx) - 5
        end_idx = df.index.get_loc(idx) + 5
        
        if start_idx < 0:
            start_idx = 0
        if end_idx >= len(df):
            end_idx = len(df) - 1
            
        window_df = df.iloc[start_idx:end_idx+1]
        
        # Format date for filename
        date_str = idx.strftime('%Y-%m-%d')
        filename = f"{output_dir}/t_shaped_{date_str}.png"
        
        # Calculate price range for annotation placement
        price_range = window_df['High'].max() - window_df['Low'].min()
        annotation_y = window_df.loc[idx, 'High'] + price_range * 0.02  # Place slightly above the candle
        
        # Create the plot
        fig, axlist = mpf.plot(window_df, type='candle', style='yahoo',
                title=f"T-shaped Candle on {date_str}",
                volume=True,
                returnfig=True)  # Return figure to add annotation
        
        # Add annotation
        ax = axlist[0]  # Main price axis
        t_shaped_idx = window_df.index.get_loc(idx)
        
        # Add red background for the T-shaped candle
        ax.axvspan(t_shaped_idx - 0.4, t_shaped_idx + 0.4, color='red', alpha=0.1)
        
        # Add 'T' annotation with arrow
        ax.annotate('T', xy=(t_shaped_idx, annotation_y),
                   xytext=(t_shaped_idx, annotation_y + price_range * 0.05),
                   color='red',
                   fontsize=12,
                   fontweight='bold',
                   ha='center',
                   va='bottom',
                   arrowprops=dict(arrowstyle='->',
                                 connectionstyle='arc3',
                                 color='red'))
        
        # Save the figure
        fig.savefig(filename, bbox_inches='tight')
        
        # Close all figures to prevent memory issues
        plt.close('all')
        
        print(f"Saved {filename}")
        
    except Exception as e:
        print(f"Error plotting candle at {idx}: {e}")

# Save individual PNGs for each T-shaped candle (limit to 20)
print(f"\nSaving individual candle plots to indicators/examples/...")
max_examples = 20

# Get the most recent 20 T-shaped candles
t_shaped_df_limited = t_shaped_df.tail(max_examples)
print(f"Saving {len(t_shaped_df_limited)} most recent examples (max {max_examples})")

for idx in t_shaped_df_limited.index:
    save_t_shaped_candle(df, idx)

print("\nDone saving plots!")