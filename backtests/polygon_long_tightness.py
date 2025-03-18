import pandas as pd
from datetime import datetime
import os
import subprocess
import sys

# Import the Strategy base class
from lumibot.strategies.strategy import Strategy
# Import Order, Asset, TradingFee from entities for creating orders and assets
from lumibot.entities import Order, Asset, Data
# Import YahooDataBacktesting for backtesting (since we are using daily stock data)
from lumibot.backtesting import PolygonDataBacktesting, BacktestingBroker
# Import backtesting flag
from lumibot.credentials import IS_BACKTESTING

ticker = "QQQ"

class LongTightness(Strategy):
    # Define strategy parameters that can be adjusted by the user
    parameters = {
        "symbols": [ticker],
        "tight_threshold": 0.00015,           # maximum allowed difference between close and open to consider the candle as tight
        "stop_loss": 0.8,                # stop loss in dollars per share
        "risk_reward": 2,                  # risk reward multiplier, meaning the profit target is risk*4
        "side": "buy",
        "risk_per_trade": 0.005,
        "init_cash": 30000
    }

    def initialize(self):
        self.sleeptime = "1M"
        # Initialize persistent variables for risk management and trade logging
        if not hasattr(self.vars, 'daily_loss_count'):
            self.vars.daily_loss_count = 0  # track consecutive losses in the day
        # Initialize persistent variable for storing filled trade details
        if not hasattr(self.vars, 'trade_log'):
            self.vars.trade_log = []
        
        self.minutes_before_closing = 0.1 # close positions 5 minutes before market close, see below def before_market_closes()
            
    def on_trading_iteration(self):
        # Check if max daily losses reached (2 consecutive losses) to prevent further trading
        if self.vars.daily_loss_count >= 2:
            return

        symbols = self.parameters.get("symbols", [])
        tight_threshold = self.parameters.get("tight_threshold")
        stop_loss_amount = self.parameters.get("stop_loss")
        risk_reward = self.parameters.get("risk_reward")
        side = self.parameters.get("side")
        risk_per_trade = self.parameters.get("risk_per_trade")
        # Calculate risk size: 0.5% of available cash
        cash = self.parameters.get("init_cash")
        risk_size = cash * risk_per_trade

        # Check current open positions (exclude USD cash position)
        open_positions = [p for p in self.get_positions() if not (p.asset.symbol == "USD" and p.asset.asset_type == Asset.AssetType.FOREX)]
        if len(open_positions) >= 2:
            return

        # Loop through each symbol to check if the entry conditions are met
        for symbol in symbols:
            # Skip if there is already a position in this asset
            if self.get_position(symbol) is not None:
                continue

            # Obtain 30-minute historical prices instead of daily
            thirty_minute_bars = self.get_historical_prices(symbol, length=1, timestep="30minute")
            if thirty_minute_bars is None or thirty_minute_bars.df.empty:
                continue

            # Extract the latest candle from the DataFrame
            df_30min = thirty_minute_bars.df
            latest_candle = df_30min.iloc[-1]
            open_price = latest_candle['open']
            close_price = latest_candle['close']
            high_price = latest_candle['high']
            low_price = latest_candle['low']

            # T-shaped - entry indicator
            is_t_shaped = (
                (abs(open_price - close_price) / open_price) < tight_threshold and
                low_price < open_price and
                abs(low_price - open_price) / (abs(high_price - open_price) if abs(high_price - open_price) != 0 else 1) > 2.5
            )

            if is_t_shaped: # then execute the trade
                # Get the last price to use as entry price
                minute_bars = self.get_historical_prices(symbol, length=1, timestep="minute")
                entry_price = self.get_last_price(symbol)
                if entry_price is None:
                    continue

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
                    limit_price=None,  # No limit price for entry
                    stop_price=None,  # No stop price for entry - will be a market order
                    stop_loss_price=stop_loss_price,  # Exit stop loss price
                    take_profit_price=take_profit_price,  # Exit take profit price
                    custom_params={"margin": True}
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
            "take_profit_price": getattr(order, 'take_profit_price', None) if order.side == Order.OrderSide.SELL else None,  # Assuming limit_price in SELL can be take profit
            "custom_params": order.custom_params,
            "order_type": getattr(order, 'type', None),
            "date_created": getattr(order, 'date_created', None),
        }

        self.vars.trade_log.append(trade_info)

    def before_market_closes(self): 

        # close positions 5 minutes before market close

        # checks if there are any positions to close
        positions = self.get_positions()
        if len(positions) == 0:
            return
        
        # close all positions and cancel all open orders
        self.sell_all()

    def after_market_closes(self):
        # Reset daily loss count for next trading day
        self.vars.daily_loss_count = 0


def process_trades():
    """
    Call the process_trades.py script to analyze and generate reports on trades
    """
    try:
        # Determine the path to the process_trades.py script
        script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                 "scripts", "process_trades.py")
        
        # Check if the script exists
        if not os.path.exists(script_path):
            print(f"ERROR: Could not find process_trades.py at {script_path}")
            return False
        
        print("\nGenerating trade reports...")
        
        # Pass stop_loss, side, and risk_reward parameters to the script
        stop_loss = LongTightness.parameters.get("stop_loss")
        side = LongTightness.parameters.get("side")
        risk_reward = LongTightness.parameters.get("risk_reward")
        risk_per_trade = LongTightness.parameters.get("risk_per_trade", 0.005)  # Default to 0.005 if not found
        
        # Execute the script as a subprocess with parameters
        result = subprocess.run([
            sys.executable, 
            script_path, 
            "--stop_loss", str(stop_loss), 
            "--side", side,
            "--risk_reward", str(risk_reward),
            "--risk_per_trade", str(risk_per_trade)
        ], capture_output=True, text=True)
        
        # Check the result
        if result.returncode == 0:
            print(result.stdout)
            print("Trade reports generated successfully")
            return True
        else:
            print(f"ERROR running process_trades.py: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"ERROR: Failed to run process_trades.py: {str(e)}")
        return False


if __name__ == "__main__":
    
    backtesting_start = datetime.strptime(os.getenv("BACKTESTING_START"), "%Y-%m-%d")
    backtesting_end = datetime.strptime(os.getenv("BACKTESTING_END"), "%Y-%m-%d")
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    
    # Run backtest
    result = LongTightness.run_backtest(
        PolygonDataBacktesting,
        backtesting_start,
        backtesting_end,
        parameters=LongTightness.parameters,
        quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
        polygon_api_key=polygon_api_key,
    )
    
    # Process and analyze trades after backtest completes
    process_trades()