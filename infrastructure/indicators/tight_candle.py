import pandas as pd
import numpy as np
import plotly.graph_objects as go
from typing import Dict, Optional, List, Union, Tuple

class TightCandle:
    def __init__(self, 
                 tightness_threshold: float = 0.1, 
                 context_bars: int = 20,
                 wick_ratio_threshold: float = 2.0):  # minimum ratio between larger and smaller wick
        """
        Initialize the TightCandle indicator.
        
        Args:
            tightness_threshold: Maximum ratio of body size to total size for a candle
                               to be considered tight (default 0.1 or 10%)
            context_bars: Number of bars to show before and after the signal (default 20)
            wick_ratio_threshold: Minimum ratio between larger and smaller wick for T-shape
                                (default 2.0, meaning larger wick should be at least 2x smaller wick)
        """
        self.tightness_threshold = tightness_threshold
        self.context_bars = context_bars
        self.wick_ratio_threshold = wick_ratio_threshold
    
    def calculate(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate tight candle signals for the given OHLCV data.
        
        Args:
            ohlcv: DataFrame with columns ['open', 'high', 'low', 'close']
            
        Returns:
            DataFrame with columns:
            - body_size: Absolute size of the candle body
            - total_size: Total size of the candle (high - low)
            - upper_wick: Size of the upper wick
            - lower_wick: Size of the lower wick
            - tightness: Ratio of body size to total size
            - is_tight: Boolean indicating if the candle is tight
            - wick_ratio: Ratio of larger wick to smaller wick
            - trend: 'bullish' (T-shape), 'bearish' (inverted T-shape), or 'neutral'
        """
        # Calculate basic candle metrics
        df = pd.DataFrame(index=ohlcv.index)
        df['body_size'] = abs(ohlcv['close'] - ohlcv['open'])
        df['total_size'] = ohlcv['high'] - ohlcv['low']
        
        # Calculate wick sizes
        df['upper_wick'] = ohlcv['high'] - ohlcv[['open', 'close']].max(axis=1)
        df['lower_wick'] = ohlcv[['open', 'close']].min(axis=1) - ohlcv['low']
        
        # Calculate tightness ratio
        df['tightness'] = df['body_size'] / df['total_size'].where(df['total_size'] != 0, np.inf)
        
        # Identify tight candles
        df['is_tight'] = df['tightness'] < self.tightness_threshold
        
        # Calculate wick ratio (larger wick to smaller wick)
        df['wick_ratio'] = np.maximum(
            df['upper_wick'] / df['lower_wick'].where(df['lower_wick'] != 0, np.inf),
            df['lower_wick'] / df['upper_wick'].where(df['upper_wick'] != 0, np.inf)
        )
        
        # Determine trend based on T-shape analysis
        df['trend'] = 'neutral'
        
        # T-shape: big lower wick, small upper wick
        bullish_mask = (
            (df['lower_wick'] > df['upper_wick']) & 
            (df['lower_wick'] / df['upper_wick'].where(df['upper_wick'] != 0, np.inf) >= self.wick_ratio_threshold)
        )
        df.loc[bullish_mask, 'trend'] = 'bullish'
        
        # Inverted T-shape: big upper wick, small lower wick
        bearish_mask = (
            (df['upper_wick'] > df['lower_wick']) & 
            (df['upper_wick'] / df['lower_wick'].where(df['lower_wick'] != 0, np.inf) >= self.wick_ratio_threshold)
        )
        df.loc[bearish_mask, 'trend'] = 'bearish'
        
        return df
    
    def plot_signal_detail(self, 
                          ohlcv: pd.DataFrame, 
                          signals: pd.DataFrame,
                          signal_idx: int,
                          title: str = "Tight Candle Detail") -> go.Figure:
        """
        Create a detailed plot of a specific signal with context.
        
        Args:
            ohlcv: DataFrame with OHLCV data
            signals: DataFrame with signal data from calculate()
            signal_idx: Index of the signal to plot
            title: Plot title
            
        Returns:
            Plotly figure object
        """
        # Get context window
        start_idx = max(0, signal_idx - self.context_bars)
        end_idx = min(len(ohlcv), signal_idx + self.context_bars + 1)
        
        context_data = ohlcv.iloc[start_idx:end_idx]
        context_signals = signals.iloc[start_idx:end_idx]
        
        # Create figure
        fig = go.Figure()
        
        # Add candlestick chart
        fig.add_trace(go.Candlestick(
            x=context_data.index,
            open=context_data['open'],
            high=context_data['high'],
            low=context_data['low'],
            close=context_data['close'],
            name='Price'
        ))
        
        # Highlight the signal candle
        signal_data = ohlcv.iloc[signal_idx]
        signal_info = signals.iloc[signal_idx]
        
        color = 'green' if signal_info['trend'] == 'bullish' else 'red'
        marker_y = (signal_data['low'] - (signal_data['high'] - signal_data['low']) * 0.1 
                   if signal_info['trend'] == 'bullish' 
                   else signal_data['high'] + (signal_data['high'] - signal_data['low']) * 0.1)
        
        fig.add_trace(go.Scatter(
            x=[signal_data.name],
            y=[marker_y],
            mode='markers',
            marker=dict(
                symbol='triangle-up' if signal_info['trend'] == 'bullish' else 'triangle-down',
                size=15,
                color=color
            ),
            name=f"{'Bullish' if signal_info['trend'] == 'bullish' else 'Bearish'} Signal"
        ))
        
        # Add annotations
        fig.add_annotation(
            x=signal_data.name,
            y=context_data['high'].max(),
            text=f"Tightness: {signal_info['tightness']:.2%}<br>"
                 f"Body/Total: {signal_info['body_size']:.4f}/{signal_info['total_size']:.4f}<br>"
                 f"Upper Wick: {signal_info['upper_wick']:.4f}<br>"
                 f"Lower Wick: {signal_info['lower_wick']:.4f}",
            showarrow=True,
            arrowhead=1,
            bgcolor='white',
            bordercolor=color
        )
        
        # Update layout
        fig.update_layout(
            title=title,
            yaxis_title='Price',
            xaxis_title='Date',
            template='plotly_dark',
            showlegend=True
        )
        
        # Add vertical line at signal
        fig.add_vline(
            x=signal_data.name,
            line_width=1,
            line_dash="dash",
            line_color=color
        )
        
        return fig
    
    def plot(self, ohlcv: pd.DataFrame, signals: pd.DataFrame, title: str = "Tight Candle Analysis") -> go.Figure:
        """
        Create an interactive plot showing candlesticks and tight candle signals.
        
        Args:
            ohlcv: DataFrame with OHLCV data
            signals: DataFrame with signal data from calculate()
            title: Plot title
            
        Returns:
            Plotly figure object
        """
        # Create candlestick chart
        fig = go.Figure()
        
        # Add candlestick trace
        fig.add_trace(go.Candlestick(
            x=ohlcv.index,
            open=ohlcv['open'],
            high=ohlcv['high'],
            low=ohlcv['low'],
            close=ohlcv['close'],
            name='Price'
        ))
        
        # Add tight candle markers
        tight_candles = signals[signals['is_tight']]
        
        # Bullish tight candles
        bullish = tight_candles[tight_candles['trend'] == 'bullish']
        if len(bullish) > 0:
            fig.add_trace(go.Scatter(
                x=bullish.index,
                y=ohlcv.loc[bullish.index, 'low'] - (ohlcv.loc[bullish.index, 'high'] - ohlcv.loc[bullish.index, 'low']) * 0.1,
                mode='markers',
                marker=dict(symbol='triangle-up', size=10, color='green'),
                name='Bullish Tight Candle'
            ))
        
        # Bearish tight candles
        bearish = tight_candles[tight_candles['trend'] == 'bearish']
        if len(bearish) > 0:
            fig.add_trace(go.Scatter(
                x=bearish.index,
                y=ohlcv.loc[bearish.index, 'high'] + (ohlcv.loc[bearish.index, 'high'] - ohlcv.loc[bearish.index, 'low']) * 0.1,
                mode='markers',
                marker=dict(symbol='triangle-down', size=10, color='red'),
                name='Bearish Tight Candle'
            ))
        
        # Update layout
        fig.update_layout(
            title=title,
            yaxis_title='Price',
            xaxis_title='Date',
            template='plotly_dark'
        )
        
        return fig
    
    def find_entry_signals(self, ohlcv: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """
        Find entry signals based on tight candles.
        
        Args:
            ohlcv: DataFrame with OHLCV data
            
        Returns:
            Tuple of (long_entries, short_entries) as boolean Series
        """
        signals = self.calculate(ohlcv)
        
        # Generate entry signals
        long_entries = (signals['is_tight'] & (signals['trend'] == 'bullish'))
        short_entries = (signals['is_tight'] & (signals['trend'] == 'bearish'))
        
        return long_entries, short_entries
    
    def calculate_stop_price(self, ohlcv: pd.DataFrame, entry_signal: pd.Series, direction: str) -> pd.Series:
        """
        Calculate stop price for entry signals.
        
        Args:
            ohlcv: DataFrame with OHLCV data
            entry_signal: Boolean series indicating entry points
            direction: 'long' or 'short'
            
        Returns:
            Series with stop prices for each entry signal
        """
        stop_prices = pd.Series(index=ohlcv.index, dtype=float)
        
        if direction == 'long':
            # For long trades, stop is below the low of the tight candle
            stop_prices[entry_signal] = ohlcv.loc[entry_signal, 'low']
        else:
            # For short trades, stop is above the high of the tight candle
            stop_prices[entry_signal] = ohlcv.loc[entry_signal, 'high']
            
        return stop_prices 