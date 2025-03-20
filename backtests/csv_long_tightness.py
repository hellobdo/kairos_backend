import pandas as pd
from datetime import datetime
import os
import subprocess
import sys
from lumibot.strategies.strategy import Strategy
from lumibot.entities import Order, Asset, Data
from lumibot.backtesting import PandasDataBacktesting, PolygonDataBacktesting
from lumibot.credentials import IS_BACKTESTING

# Import trade processing helper
from helpers import process_trades_from_strategy

# Import t-shaped indicator by loading the module directly
import importlib.util
indicator_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'indicators', 't-shaped.py')
spec = importlib.util.spec_from_file_location("t_shaped", indicator_path)
t_shaped = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t_shaped)
calculate_indicator = t_shaped.calculate_indicator

# Prepare for data loading
ticker = "QQQ"

# Load the CSV file using the ticker variable
df = pd.read_csv(f'data/csv/{ticker}.csv')
# Convert datetime column to datetime type
df['datetime'] = pd.to_datetime(df['datetime'])
# Set datetime as the index
df.set_index('datetime', inplace=True)

asset = Asset(ticker, asset_type=Asset.AssetType.STOCK)

pandas_data = {
    asset: Data(asset, df, timestep="minute"),
}

backtesting_start = datetime.strptime(os.getenv("BACKTESTING_START"), "%Y-%m-%d")
backtesting_end = datetime.strptime(os.getenv("BACKTESTING_END"), "%Y-%m-%d")

class LongTightness(Strategy):
    # Define strategy parameters that can be adjusted by the user
    parameters = {
        "symbols": [ticker],
        "stop_loss": 0.8,                # stop loss in dollars per share
        "risk_reward": 2,                  # risk reward multiplier, meaning the profit target is risk*4
        "side": "buy",
        "risk_per_trade": 0.005,
        "max_loss_positions": 2,
        "bar_signals_length": "30minute",
        "backtesting_start": backtesting_start.strftime("%Y-%m-%d"),
        "backtesting_end": backtesting_end.strftime("%Y-%m-%d"),
        "strategy_name": __qualname__, #gets the name of the class
        "sleeptime": "30M"
    }

    def initialize(self):
        self.sleeptime = self.parameters.get("sleeptime")
        # Initialize persistent variables for risk management and trade logging
        if not hasattr(self.vars, 'daily_loss_count'):
            self.vars.daily_loss_count = 0  # track consecutive losses in the day
        # Initialize persistent variable for storing filled trade details
        if not hasattr(self.vars, 'trade_log'):
            self.vars.trade_log = []
        
        self.minutes_before_closing = 0.1 # close positions before market close, see below def before_market_closes()
            
    def on_trading_iteration(self):
        symbols = self.parameters.get("symbols", [])
        stop_loss_amount = self.parameters.get("stop_loss")
        risk_reward = self.parameters.get("risk_reward")
        side = self.parameters.get("side")
        risk_per_trade = self.parameters.get("risk_per_trade")
        max_loss_positions = self.parameters.get("max_loss_positions")
        bar_signals_length = self.parameters.get("bar_signals_length")

        # Check if max daily losses reached (2 consecutive losses) to prevent further trading
        if self.vars.daily_loss_count >= max_loss_positions:
            return

        # Check current open positions (exclude USD cash position)
        open_positions = [p for p in self.get_positions() if not (p.asset.symbol == "USD" and p.asset.asset_type == Asset.AssetType.FOREX)]
        if len(open_positions) >= 2:
            return
        
        # Calculate risk size: 0.5% of available cash
        risk_size = 30000 * risk_per_trade

        # Loop through each symbol to check if the entry conditions are met
        for symbol in symbols:
            # Skip if there is already a position in this asset
            if self.get_position(symbol) is not None:
                continue

            # Obtain historical prices
            bars = self.get_historical_prices(symbol, length=1, timestep="minute")
            if bars is None or bars.df.empty:
                continue

            # Calculate T-shaped indicator
            df = bars.df.copy()
            df = calculate_indicator(df)
            
            # Check if the latest candle is t-shaped
            latest_candle = df.iloc[-1]
            if latest_candle['is_t_shaped']:
                # Get the last price to use as entry price
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
                    type="bracket",
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
        if len(positions) == 0:
            return
        else:
            self.sell_all()

    def after_market_closes(self):
        # Reset daily loss count for next trading day
        self.vars.daily_loss_count = 0


if __name__ == "__main__":
    # Run backtest
    result = LongTightness.run_backtest(
        datasource_class=PandasDataBacktesting,
        pandas_data=pandas_data,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        parameters=LongTightness.parameters,
        quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
        show_plot=False,
        show_tearsheet=False
    )
    
    # Process and analyze trades after backtest completes
    process_trades_from_strategy(LongTightness) 