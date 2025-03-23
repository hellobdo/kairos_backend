# Add the project root directory to Python's path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime
import os
import subprocess
import sys
from analytics.backtest_executions import process_backtest_executions
from lumibot.strategies.strategy import Strategy
from lumibot.entities import Order, Asset
from lumibot.backtesting import PolygonDataBacktesting
from indicators import load_indicators
from utils.get_latest_trade_report import get_latest_trade_report

indicator1 = load_indicators('t-shaped.py')

# Define backtest dates
backtesting_start = datetime.strptime(os.getenv("BACKTESTING_START"), "%Y-%m-%d")
backtesting_end = datetime.strptime(os.getenv("BACKTESTING_END"), "%Y-%m-%d")
polygon_api_key = os.getenv("POLYGON_API_KEY")

class Strategy(Strategy):
    # Define strategy parameters that can be adjusted by the user
    parameters = {
        "symbols": ["NVDA"],
        "risk_reward": 3,                  # risk reward multiplier, meaning the profit target is risk*4
        "side": "buy",
        "risk_per_trade": 0.005,
        "max_loss_positions": 2,
        "bar_signals_length": "30minute",
        "backtesting_start": backtesting_start.strftime("%Y-%m-%d"),
        "backtesting_end": backtesting_end.strftime("%Y-%m-%d"),
        "sleeptime": "1M",
        "indicators": "t-shaped",
        "data_source": "polygon",
        "stop_loss_rules": [
            {"price_below": 150, "amount": 0.30},
            {"price_above": 150, "amount": 1.00}
        ]
    }

    def initialize(self):
        # watches every minute
        self.sleeptime = self.parameters.get("sleeptime")
        # Initialize persistent variables for risk management and trade logging
        if not hasattr(self.vars, 'daily_loss_count'):
            self.vars.daily_loss_count = 0  # track consecutive losses in the day
        # Initialize persistent variable for storing filled trade details
        if not hasattr(self.vars, 'trade_log'):
            self.vars.trade_log = []
        
        self.minutes_before_closing = 0.1 # close positions before market close, see below def before_market_closes()
            
    def on_trading_iteration(self):
        calculate_indicator = indicator1.calculate_indicator
        symbols = self.parameters.get("symbols", [])
        risk_reward = self.parameters.get("risk_reward")
        side = self.parameters.get("side")
        risk_per_trade = self.parameters.get("risk_per_trade")
        max_loss_positions = self.parameters.get("max_loss_positions")
        bar_signals_length = self.parameters.get("bar_signals_length")
        stop_loss_rules = self.parameters.get("stop_loss_rules", [])

        # Check if max daily losses reached (2 consecutive losses) to prevent further trading
        if self.vars.daily_loss_count >= max_loss_positions:
            return

        current_time = self.get_datetime()
        if not (current_time.minute == 0 or current_time.minute == 30):
            return

        # Check current open positions (exclude USD cash position)
        open_positions = [p for p in self.get_positions() if not (p.asset.symbol == "USD" and p.asset.asset_type == Asset.AssetType.FOREX)]
        if len(open_positions) >= max_loss_positions:
            return
        
        # Calculate risk size: 0.5% of available cash
        risk_size = 30000 * risk_per_trade

        # Loop through each symbol to check if the entry conditions are met
        for symbol in symbols:
            # Skip if there is already a position in this asset
            if self.get_position(symbol) is not None:
                continue

            # Obtain 30-minute historical prices
            bars = self.get_historical_prices(symbol, length=1, timestep=bar_signals_length)
            if bars is None or bars.df.empty:
                continue

            # Calculate T-shaped indicator
            df = bars.df.copy()
            df = calculate_indicator(df)
            
            # Check if the latest candle is t-shaped
            latest_candle = df.iloc[-1]
            if latest_candle['is_indicator']:
                entry_price = self.get_last_price(symbol)
                if entry_price is None:
                    continue

                # Get stop loss amount based on current price using the rules
                price = latest_candle['close']
                stop_loss_amount = None
                for rule in stop_loss_rules:
                    if "price_below" in rule and price < rule["price_below"]:
                        stop_loss_amount = rule["amount"]
                        break
                    elif "price_above" in rule and price >= rule["price_above"]:
                        stop_loss_amount = rule["amount"]
                        break

                # Determine trade quantity based on risk size divided by stop loss per share
                computed_quantity = int(risk_size // stop_loss_amount)
                trade_quantity = computed_quantity

                # Calculate stop loss and take profit levels
                if side == "buy":
                    stop_loss_price = entry_price - stop_loss_amount # stop loss price below entry price
                    take_profit_price = entry_price + (stop_loss_amount * risk_reward) # take profit price above entry price
                else:
                    stop_loss_price = entry_price + stop_loss_amount # stop loss price above entry price
                    take_profit_price = entry_price - (stop_loss_amount * risk_reward) # take profit price below entry price

                # Create a market order with attached stop loss and take profit orders
                # Trading on margin by passing custom parameter 'margin': True
                entry_order = self.create_order(
                    symbol,
                    trade_quantity,
                    side=side,
                    type="bracket",  # This makes it a bracket order
                    stop_loss_price=stop_loss_price,  # Exit stop loss price
                    take_profit_price=take_profit_price,  # Exit take profit price
                    custom_params={"margin": True},
                    time_in_force="day"
                )
                self.submit_order(entry_order)

    def on_filled_order(self, position, order, price, quantity, multiplier):
        trade_info = {
            "order_id": order.identifier,
            "symbol": order.asset if isinstance(order.asset, str) else order.asset.symbol,
            "filled_price": price,
            "quantity": quantity,
            "side": order.side,
            "timestamp": self.get_datetime(),  # The time this fill was processed
            "limit_price": getattr(order, 'limit_price', None),
            "stop_price": getattr(order, 'stop_price', None),
            "take_profit_price": getattr(order, 'take_profit_price', None),
            "custom_params": order.custom_params,
            "order_type": getattr(order, 'type', None),
            "date_created": getattr(order, 'date_created', None),
        }

        self.vars.trade_log.append(trade_info)

    def before_market_closes(self): 
        self.cancel_open_orders()

        # close positions before market close
        # checks if there are any positions to close
        positions = self.get_positions()
        if len(positions) > 0:
            self.sell_all()

    def after_market_closes(self):
        # Reset daily loss count for next trading day
        self.vars.daily_loss_count = 0


if __name__ == "__main__":
    # Run backtest
    result = Strategy.run_backtest(
        PolygonDataBacktesting,
        backtesting_start,
        backtesting_end,
        parameters=Strategy.parameters,
        quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
        polygon_api_key=polygon_api_key,
        show_plot=False,
        show_tearsheet=False
    )
    
    # Process and analyze trades after backtest completes
    # First, find the latest trades CSV file
    try:
        file_path = get_latest_trade_report("csv")
        print(f"Found trades file: {file_path}")    
        process_backtest_executions(Strategy, file_path)
        
    except FileNotFoundError as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"Error finding or processing latest trades file: {str(e)}") 