import pandas as pd
from datetime import datetime
import os
import sys
from pathlib import Path
from backtests.utils.backtest_data_to_db import get_latest_settings_file

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lumibot.entities import Order, Asset, Data
from lumibot.backtesting import PolygonDataBacktesting, PandasDataBacktesting
from indicators import load_indicators
from backtests.utils.backtest_functions import BaseStrategy

# Define backtest dates
backtesting_start = datetime.strptime(os.getenv("BACKTESTING_START"), "%Y-%m-%d")
backtesting_end = datetime.strptime(os.getenv("BACKTESTING_END"), "%Y-%m-%d")
tickers = ["QQQ"]
data_source = "polygon"

class Strategy(BaseStrategy):    
    # Define strategy parameters that can be adjusted by the user
    parameters = {
        "symbols": tickers,
        "risk_reward": 3,                  # risk reward multiplier, meaning the profit target is risk*4
        "side": "buy",
        "risk_per_trade": 0.005,
        "max_loss_positions": 2,
        "bar_signals_length": "30minute",
        "backtesting_start": backtesting_start.strftime("%Y-%m-%d"),
        "backtesting_end": backtesting_end.strftime("%Y-%m-%d"),
        "sleeptime": "1M",
        "indicators": ["t-shaped"],
        "data_source": data_source,
        "stop_loss_rules": [
            {"price_below": 150, "amount": 0.30},
            {"price_above": 150, "amount": 1.00}
        ]
    }

    def initialize(self):
        self.sleeptime = self.parameters.get("sleeptime")

        if not hasattr(self.vars, 'daily_loss_count'):
            self.vars.daily_loss_count = 0
        
        if not hasattr(self.vars, 'trade_log'):
            self.vars.trade_log = []
        
        # Load indicator calculation functions
        self.indicators = self.parameters.get("indicators")
        self.calculate_indicators = self._load_indicators(self.indicators, load_indicators)
        
        self.minutes_before_closing = 0.1 # close positions before market close, see below def before_market_closes()
            
    def on_trading_iteration(self):
        symbols = self.parameters.get("symbols", [])
        bar_signals_length = self.parameters.get("bar_signals_length")
        side = self.parameters.get("side")
        risk_reward = self.parameters.get("risk_reward")
        risk_per_trade = self.parameters.get("risk_per_trade")
        stop_loss_rules = self.parameters.get("stop_loss_rules")
        current_time = self.get_datetime()

        # Check if max daily losses reached or position limit reached
        if self._check_position_limits():
            return

        # Check if we're at the right time to trade
        if not self._check_time_conditions(current_time):
            return

        # Loop through each symbol to check if the entry conditions are met
        for symbol in symbols:
            # Skip if there is already a position in this asset
            if self.get_position(symbol) is not None:
                continue

            bars = self.get_historical_prices(symbol, length=1, timestep=bar_signals_length)
            if bars is None or bars.df.empty:
                continue

            # Apply indicators and check if all signals are valid
            signal_valid, df = self._apply_indicators(bars.df.copy(), self.calculate_indicators)
            if not signal_valid:
                continue
            
            # Process valid signal
            entry_price = self.get_last_price(symbol)
            if entry_price is None:
                continue

            # Determine stop loss amount
            price = df['close'].iloc[-1]
            stop_loss_amount = self._determine_stop_loss(price, stop_loss_rules)
            if stop_loss_amount is None:
                continue  # No matching rule found

            stop_loss_price, take_profit_price = self._calculate_price_levels(entry_price, stop_loss_amount, side, risk_reward)
            quantity = self._calculate_qty_based_on_risk_per_trade(stop_loss_amount, risk_per_trade)
            
            # Create a market order with attached stop loss and take profit orders
            # Trading on margin by passing custom parameter 'margin': True
            entry_order = self.create_order(
                symbol,
                quantity,
                side=side,
                type="bracket",  # This makes it a bracket order
                stop_loss_price=stop_loss_price,  # Exit stop loss price
                take_profit_price=take_profit_price,  # Exit take profit price
                custom_params={
                    "margin": True,
                    "stop_loss_price": stop_loss_price,
                    "take_profit_price": take_profit_price
                },
                time_in_force="day"
            )
            self.submit_order(entry_order)

    def on_filled_order(self, position, order, price, quantity, multiplier):

        stop_loss = None
        take_profit = None
        if order.custom_params:
            stop_loss = order.custom_params.get('stop_loss_price')
            take_profit = order.custom_params.get('take_profit_price')

        trade_info = {
            "name": self.name,
            "order_id": order.identifier,
            "symbol": order.asset if isinstance(order.asset, str) else order.asset.symbol,
            "price": price,
            "quantity": quantity,
            "side": order.side,
            "timestamp": self.get_datetime(),  # The time this fill was processed
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "status": order.status,
            "type": order.order_type,
            "risk_per_trade": self.parameters.get("risk_per_trade")
        }

        self.vars.trade_log.append(trade_info)

    def before_market_closes(self): 
        self.cancel_open_orders()
        positions = self.get_positions()
        if len(positions) > 0:
            self.sell_all()

    def after_market_closes(self):
        self.vars.daily_loss_count = 0
        current_time = self.get_datetime()
        self._save_trades_at_end(current_time, backtesting_end)

def run_strategy():
    """Run the backtest strategy and return the result."""
    if data_source == "polygon":
        polygon_api_key = os.getenv("POLYGON_API_KEY")
        return Strategy.run_backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            parameters=Strategy.parameters,
            quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
            polygon_api_key=polygon_api_key,
            show_plot=False,
            show_tearsheet=False
        ) 
    elif data_source == "csv":
        df = pd.read_csv(f'data/csv/{tickers[0]}.csv')
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        asset = Asset(tickers[0], asset_type=Asset.AssetType.STOCK)
        pandas_data = {
            asset: Data(asset, df, timestep="minute"),
        }

        return Strategy.run_backtest(
            PandasDataBacktesting,
            backtesting_start,
            backtesting_end,
            parameters=Strategy.parameters,
            pandas_data=pandas_data
        )

if __name__ == "__main__":
    result = run_strategy()
    
    # Get the identifier from latest files
    settings_file = str(get_latest_settings_file())
    print(f"\nLooking for files to rename:")
    print(f"Settings file found: {settings_file}")
    
    if settings_file:
        # Extract identifier and timestamp from settings file name
        parts = settings_file.split('_')
        identifier = parts[-2]
        timestamp = f"{parts[1]}_{parts[2]}"  # Gets YYYY-MM-DD_HH-MM
        print(f"Extracted timestamp: {timestamp}")
        print(f"Extracted identifier: {identifier}")
        
        # Find and rename files from current run containing 'id'
        logs_dir = Path("logs")
        if logs_dir.exists():
            pattern = f"*_{timestamp}_id_*"
            print(f"Looking for files matching pattern: {pattern}")
            for file in logs_dir.glob(pattern):
                print(f"Found file to rename: {file}")
                new_name = str(file).replace("_id_", f"_{identifier}_")
                os.rename(file, new_name)
                print(f"Renamed to: {new_name}")
    else:
        identifier = "id"
        print(f"Identifier not replaced")