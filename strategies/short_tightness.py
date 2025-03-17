import pandas as pd
from datetime import datetime
import os

# Import the Strategy base class
from lumibot.strategies.strategy import Strategy
# Import Order, Asset, TradingFee from entities for creating orders and assets
from lumibot.entities import Order, Asset, Data
# Import YahooDataBacktesting for backtesting (since we are using daily stock data)
from lumibot.backtesting import PandasDataBacktesting
# Import backtesting flag
from lumibot.credentials import IS_BACKTESTING

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

# Convert df.index.max() to naive datetime if it has timezone info
max_date = df.index.max()
if max_date.tzinfo is not None:
    # Convert to naive datetime by replacing tzinfo with None
    # This removes timezone information without changing the time
    max_date = max_date.replace(tzinfo=None)

# Use naive datetimes for both start and end
# Adjusted for your 2-hour ahead data (9:30 exchange time = 11:30 your data)
backtesting_start = datetime(2025, 1, 1, 11, 30)  # Exchange time 9:30
backtesting_end = max_date


class ShortTightness(Strategy):
    # Define strategy parameters that can be adjusted by the user
    parameters = {
        "symbols": [ticker],
        "tight_threshold": 0.00015,           # maximum allowed difference between close and open to consider the candle as tight
        "stop_loss": 1,                # stop loss in dollars per share
        "risk_reward": 2                  # risk reward multiplier, meaning the profit target is risk*4
    }

    def initialize(self):
        # Initialize persistent variables for risk management and trade logging
        if not hasattr(self.vars, 'daily_loss_count'):
            self.vars.daily_loss_count = 0  # track consecutive losses in the day

        self.minutes_before_closing = 5 # close positions 5 minutes before market close, see below def before_market_closes()
            
    def on_trading_iteration(self):
        # Check if max daily losses reached (2 consecutive losses) to prevent further trading
        if self.vars.daily_loss_count >= 2:
            return

        symbols = self.parameters.get("symbols", [])
        tight_threshold = self.parameters.get("tight_threshold")
        stop_loss_amount = self.parameters.get("stop_loss")
        risk_reward = self.parameters.get("risk_reward")

        # Calculate risk size: 0.5% of available cash
        cash = self.get_cash()
        risk_size = cash * 0.005

        # Check how many non-USD positions are currently open
        open_positions = [p for p in self.get_positions() if not (p.asset.symbol == "USD" and p.asset.asset_type == Asset.AssetType.FOREX)]
        if len(open_positions) >= 2:
            return

        # Loop through each symbol to check if the entry conditions are met
        for symbol in symbols:
            # Skip if there is already a position in this asset
            if self.get_position(symbol) is not None:
                continue

            # Obtain 30-minute historical prices instead of daily
            bars = self.get_historical_prices(symbol, length=1, timestep="minute")
            if bars is None or bars.df.empty:
                continue

            # Extract the latest candle from the DataFrame
            df = bars.df
            latest_candle = df.iloc[-1]
            open_price = latest_candle['open']
            close_price = latest_candle['close']
            high_price = latest_candle['high']
            low_price = latest_candle['low']

            # inverted-T-shaped - entry indicator
            is_inverted_t = (
                (abs(open_price - close_price) / open_price) < tight_threshold and
                low_price < open_price and
                abs(high_price - open_price) / abs(low_price - open_price) > 2.5
            )

            if is_inverted_t: # then execute the trade
                # Get the last price to use as entry price
                entry_price = self.get_last_price(symbol)
                if entry_price is None:
                    continue

                # Determine trade quantity based on risk size divided by stop loss per share
                computed_quantity = int(risk_size // stop_loss_amount)
                trade_quantity = computed_quantity

                # Calculate stop loss and take profit levels
                stop_loss_price = entry_price + stop_loss_amount # stop loss price above entry price
                take_profit_price = entry_price - (stop_loss_amount * risk_reward) # take profit price below entry price

                # Create a market order with attached stop loss and take profit levels
                # Trading on margin by passing custom parameter 'margin': True
                order = self.create_order(
                    symbol,
                    trade_quantity,
                    Order.OrderSide.SELL,
                    stop_price=stop_loss_price,          # attached stop loss
                    take_profit_price=take_profit_price,   # attached take profit target
                    custom_params={"margin": True}
                )
                # Submit the order
                self.submit_order(order)

    def before_market_closes(self): # close positions 5 minutes before market close
        positions = self.get_positions()
        if len(positions) == 0:
            return
    
        for position in positions:
            symbol = position.asset.symbol
            quantity = position.quantity
            side = "sell" if quantity > 0 else "buy"
            quantity = abs(quantity)

        order = self.create_order(symbol, quantity, side)
        self.submit_order(order)

    def after_market_closes(self):
        # Reset daily loss count for next trading day
        self.vars.daily_loss_count = 0

if __name__ == "__main__":
    # Run backtest
    result = ShortTightness.run_backtest(
        datasource_class=PandasDataBacktesting,
        pandas_data=pandas_data,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        parameters=ShortTightness.parameters,
        quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
    )