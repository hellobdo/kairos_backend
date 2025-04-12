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
tickers = ["QQQ"]

class Strategy(BaseStrategy):    
    # Define strategy parameters that can be adjusted by the user
    parameters = {
        "symbols": tickers,
        "risk_reward": 3,                  # risk reward multiplier, meaning the take profit price is risk*risk_reward
        "side": "buy",
        "risk_per_trade": 0.005,
        "bar_signals_length": "30minute",
        "backtesting_start": backtesting_start.strftime("%Y-%m-%d"),
        "backtesting_end": backtesting_end.strftime("%Y-%m-%d"),
        "sleeptime": "1M",
        "indicators": ["t-shaped"],
        "stop_loss_rules": [
            {"price_above": 150, "amount": 1.00}
        ],
        "margin": True,
    }

    def initialize(self):
        # Initialize the strategy with common parameters and indicators
        self.initialize_strategy()
        
        # Initiate daily loss count
        self.vars.daily_loss_count = 0

    def before_starting_trading(self):
        self.vars.daily_loss_count = 0
            
    def on_trading_iteration(self):
        # Check if we're at the right time to trade
        current_time = self.get_datetime()
        if current_time.minute != 0 and current_time.minute != 30:
            return False
        
        # Check if max daily losses reached or position limit reached
        open_positions = self.get_positions()
        
        # Skip if any positions already exist for our symbols
        for position in open_positions:
            if position.symbol in self.symbols:
                return False

        max_loss_positions = 2
        if len(open_positions) >= max_loss_positions:
            return False
        
        # Use the base strategy's trading iteration handler
        self._handle_trading_iteration()

    def on_trade_close(self, trade):
        # Check if the trade closed with a loss
        if trade.realized_pnl < 0:
            self.vars.daily_loss_count += 1
            self.log_message(f"Loss trade closed. Daily loss count: {self.vars.daily_loss_count}")

    def before_market_closes(self):
        self.sell_all()

    def after_market_closes(self):
        self._save_trades_at_end()

    def on_filled_order(self, position, order, price, quantity, multiplier):
        """Call the base class implementation to log trade information"""
        self._on_filled_order(position, order, price, quantity, multiplier)

        # Check for a sell order; if it's a sell, compare the sell price with the recorded buy price
        if order.side == order.OrderSide.SELL:
            # Use the stored last buy price for comparison
            buy_price = self.vars.last_buy_price
            sell_price = price
            self.log_message(f'Order filled: SELL at {sell_price}. Last BUY was at {buy_price}.')
            # If the sell price is lower than the buy price, count it as a loss
            if buy_price is not None and sell_price < buy_price:
                self.vars.daily_loss_count += 1
                self.log_message(f'Loss trade detected. Daily loss count is now {self.vars.daily_loss_count}.' , color='red')
            else:
                self.log_message('Trade was not at a loss.')

if __name__ == "__main__":
    result = Strategy.run_strategy()
    Strategy.rename_custom_logs()