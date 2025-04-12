import pandas as pd
from datetime import datetime
import os
import sys
from backtests.utils.backtest_functions import BaseStrategy

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Define backtest dates
backtesting_start = datetime.strptime(os.getenv("BACKTESTING_START"), "%Y-%m-%d")
backtesting_end = datetime.strptime(os.getenv("BACKTESTING_END"), "%Y-%m-%d")
tickers = ["SPY"]

class Strategy(BaseStrategy):    
    # Define strategy parameters that can be adjusted by the user
    parameters = {
        "symbols": tickers,
        "risk_reward": None,                  # risk reward multiplier, meaning the take profit price is risk*risk_reward
        "side": "buy",
        "risk_per_trade": None,
        "bar_signals_length": "1 day",
        "backtesting_start": backtesting_start.strftime("%Y-%m-%d"),
        "backtesting_end": backtesting_end.strftime("%Y-%m-%d"),
        "sleeptime": "1D",
        "indicators": None,
        "stop_loss_rules": None,
        "margin": False,
    }

    def initialize(self):
        # Initialize the strategy with common parameters and indicators
        self.initialize_strategy()
            
    def on_trading_iteration(self):
        # Use the base strategy's trading iteration handler
        self._handle_trading_iteration()

    def on_filled_order(self, position, order, price, quantity, multiplier):
        """Call the base class implementation to log trade information"""
        self._on_filled_order(position, order, price, quantity, multiplier)

    def after_market_closes(self):
        self._save_trades_at_end()

if __name__ == "__main__":
    result = Strategy.run_strategy()
    Strategy.rename_custom_logs()