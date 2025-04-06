#!/usr/bin/env python3
"""
Plot Helper

This module provides helper functions for plotting candlestick charts and indicators.
It handles all visualization logic, making it reusable across different indicators.
"""

import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
import importlib.util
import yfinance as yf

def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame by fixing column structure if needed.
    Handles both MultiIndex columns and case-insensitive column mapping.
    
    Args:
        df: Input DataFrame with OHLCV data
        
    Returns:
        DataFrame with standard column names
    """
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Handle the MultiIndex correctly
    if isinstance(df.columns, pd.MultiIndex):
        print("Detected MultiIndex, fixing column structure")
        # Get the symbol from the second level if available
        symbol = df.columns.levels[1][0] if len(df.columns.levels) > 1 else None
        
        # Create a new DataFrame with proper column names
        new_df = pd.DataFrame()
        new_df['open'] = df[('open', symbol)] if symbol else df['open']
        new_df['high'] = df[('high', symbol)] if symbol else df['high']
        new_df['low'] = df[('low', symbol)] if symbol else df['low']
        new_df['close'] = df[('close', symbol)] if symbol else df['close']
        new_df['volume'] = df[('volume', symbol)] if symbol else df['volume']
        # Set the index to match the original dataframe
        new_df.index = df.index
        return new_df
    
    # Create case-insensitive column mapping
    column_mapping = {}
    expected_columns = {'open', 'high', 'low', 'close', 'volume'}
    
    # Create mapping for both lowercase and original columns
    for col in df.columns:
        col_upper = str(col).upper()
        if col_upper in {c.upper() for c in expected_columns}:
            # Map to the properly capitalized version
            proper_name = next(c for c in expected_columns if c.upper() == col_upper)
            column_mapping[col] = proper_name
    
    # Rename columns if needed
    if column_mapping:
        df.rename(columns=column_mapping, inplace=True)
    
    return df

def plot_candlestick_window(
    df: pd.DataFrame,
    target_idx,
    output_dir: str,
    filename: str,
    title: str = None,
    window_size: int = 5,
    highlight_candle: bool = True,
    show_volume: bool = True,
    annotation: str = None,
    highlight_color: str = 'red',
    highlight_alpha: float = 0.1
) -> str:
    """
    Plot a candlestick chart centered around a specific candle with customizable options.
    
    Args:
        df: DataFrame with OHLCV data
        target_idx: Index of the target candle to center the window on
        output_dir: Directory to save the plot
        filename: Name of the output file (without extension)
        title: Title for the plot (default: None)
        window_size: Number of candles to show before and after target (default: 5)
        highlight_candle: Whether to highlight the target candle (default: True)
        show_volume: Whether to show volume bars (default: True)
        annotation: Text to annotate above the target candle (default: None)
        highlight_color: Color for highlighting the target candle (default: 'red')
        highlight_alpha: Transparency of the highlight (default: 0.1)
    
    Returns:
        str: Path to the saved plot file
    """
    try:
        # Get window around target candle
        start_idx = df.index.get_loc(target_idx) - window_size
        end_idx = df.index.get_loc(target_idx) + window_size
        
        # Adjust window if it goes beyond data bounds
        if start_idx < 0:
            start_idx = 0
        if end_idx >= len(df):
            end_idx = len(df) - 1
            
        window_df = df.iloc[start_idx:end_idx+1].copy()
        
        # Handle MultiIndex columns by selecting the first level
        if isinstance(window_df.columns, pd.MultiIndex):
            # Create a mapping of OHLCV columns
            col_map = {
                'open': ('open', window_df.columns.levels[1][0]),
                'high': ('high', window_df.columns.levels[1][0]),
                'low': ('low', window_df.columns.levels[1][0]),
                'close': ('close', window_df.columns.levels[1][0]),
                'volume': ('volume', window_df.columns.levels[1][0])
            }
            
            # Create a new DataFrame with flattened columns
            plot_df = pd.DataFrame(index=window_df.index)
            for col, multi_col in col_map.items():
                plot_df[col] = pd.to_numeric(window_df[multi_col], errors='coerce')
        else:
            # Create a new DataFrame with numeric columns
            plot_df = pd.DataFrame(index=window_df.index)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                plot_df[col] = pd.to_numeric(window_df[col], errors='coerce')
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Set up the plot
        filename = f"{output_dir}/{filename}.png"
        
        # Create the plot with explicit kwargs
        kwargs = {
            'type': 'candle',
            'style': 'yahoo',
            'title': title,
            'volume': show_volume,
            'returnfig': True,
            'datetime_format': '%Y-%m-%d'
        }
        
        fig, axlist = mpf.plot(
            plot_df,
            **kwargs
        )
        
        # Get the main price axis
        ax = axlist[0]
        
        if highlight_candle:
            # Add highlight background for the target candle
            target_loc = window_df.index.get_loc(target_idx)
            ax.axvspan(
                target_loc - 0.4,
                target_loc + 0.4,
                color=highlight_color,
                alpha=highlight_alpha
            )
        
        if annotation:
            # Calculate position for annotation
            price_range = plot_df['high'].max() - plot_df['low'].min()
            annotation_y = plot_df.loc[target_idx, 'high'] + price_range * 0.02
            
            # Add annotation with arrow
            ax.annotate(
                annotation,
                xy=(target_loc, annotation_y),
                xytext=(target_loc, annotation_y + price_range * 0.05),
                color=highlight_color,
                fontsize=12,
                fontweight='bold',
                ha='center',
                va='bottom',
                arrowprops=dict(
                    arrowstyle='->',
                    connectionstyle='arc3',
                    color=highlight_color
                )
            )
        
        # Save the plot
        fig.savefig(filename, bbox_inches='tight')
        plt.close('all')
        
        return filename
        
    except Exception as e:
        print(f"Error plotting candlestick chart: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
        return None

def plot_examples(df: pd.DataFrame, indicator_column: str, max_examples: int = 20, **kwargs) -> None:
    """
    Plot examples from a DataFrame where the indicator column is True.
    
    Args:
        df: DataFrame with OHLCV data and indicator column
        indicator_column: Name of the column containing True/False for the indicator
        max_examples: Maximum number of examples to plot (default: 20)
        **kwargs: Additional arguments to pass to plot_candlestick_window
    """
    # Get rows where indicator is True (create a copy to avoid warnings)
    examples_df = df[df[indicator_column]].tail(max_examples).copy()
    print(f"\nPlotting {len(examples_df)} examples (max {max_examples})")
    
    for idx in examples_df.index:
        date_str = idx.strftime('%Y-%m-%d')
        filename = f"{indicator_column}_{date_str}"
        
        # Plot with default or overridden parameters
        plot_candlestick_window(
            df=df,
            target_idx=idx,
            output_dir='indicators/examples',
            filename=filename,
            title=f"{indicator_column} on {date_str}",
            annotation=indicator_column.replace('is_', '').replace('_', ' ').title(),
            **kwargs
        )
        
        print(f"Saved {filename}.png")

def load_indicator(indicator_file: str):
    """
    Load an indicator module and get its calculate_indicator function.
    
    Args:
        indicator_file: Path to the indicator Python file
        
    Returns:
        calculate_indicator function from the module
    """
    print(f"Loading indicator from {indicator_file}...")
    
    # Add the indicators directory to Python path
    indicators_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if indicators_dir not in sys.path:
        sys.path.append(indicators_dir)
        
    # Import the indicator module
    spec = importlib.util.spec_from_file_location("indicator", indicator_file)
    indicator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(indicator)
    
    if not hasattr(indicator, 'calculate_indicator'):
        raise ValueError("Indicator module must have a calculate_indicator function")
    
    return indicator.calculate_indicator

def run_plot_helper(indicator_file: str = None) -> None:
    """
    Run example plotting from an indicator file.
    
    Args:
        indicator_file: Path to a Python file containing indicator logic
    """
    if not indicator_file:
        print("Error: No indicator file provided")
        print("Usage: python plot_helper.py <indicator_file>")
        return
        
    # Load data from CSV
    print("Loading market data from CSV...")
    csv_path = "./data/csv/QQQ.csv"
    df = pd.read_csv(csv_path)
    
    # Convert datetime column to index
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    
    # Prepare data
    df = prepare_data(df)
    
    # Load and apply indicator
    calculate_indicator = load_indicator(indicator_file)
    df = calculate_indicator(df)
    
    # Find indicator columns (those starting with 'is_')
    indicator_cols = [col for col in df.columns if str(col).startswith('is_')]
    if not indicator_cols:
        print("No indicator columns found in the DataFrame (columns starting with 'is_')")
        return
    
    # Plot examples for each indicator
    for col in indicator_cols:
        plot_examples(df, indicator_column=col)

if __name__ == "__main__":
    # Check if an indicator file was provided
    indicator_file = sys.argv[1] if len(sys.argv) > 1 else None
    run_plot_helper(indicator_file)
    print("\nDone!") 