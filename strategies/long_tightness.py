import pandas as pd
from datetime import datetime
import os
import pytz

# Import the Strategy base class
from lumibot.strategies.strategy import Strategy
# Import Order, Asset, TradingFee from entities for creating orders and assets
from lumibot.entities import Order, Asset, Data
# Import YahooDataBacktesting for backtesting (since we are using daily stock data)
from lumibot.backtesting import PandasDataBacktesting
# Import backtesting flag
from lumibot.credentials import IS_BACKTESTING

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

# Get timezone from the dataframe index (if it has one)
sample_date = df.index.max()
timezone = sample_date.tzinfo

# Define specific date range for backtesting with proper timezone
# Format: year, month, day, hour, minute
if timezone:
    # If data has timezone, use it for start date
    eastern = pytz.timezone('America/New_York')
    backtesting_start = eastern.localize(datetime(2025, 1, 1, 9, 30))
else:
    # If data has no timezone, use naive datetime
    backtesting_start = datetime(2025, 1, 1, 9, 30)

# Use the max date from the dataframe for end date
backtesting_end = df.index.max()

# Print date range for confirmation
print(f"Backtesting from {backtesting_start} to {backtesting_end}")
print(f"Start date timezone: {backtesting_start.tzinfo}")
print(f"End date timezone: {backtesting_end.tzinfo}")

# Uncomment to use full data range instead
# backtesting_start = df.index.min()
# backtesting_end = df.index.max()

class TightnessStrategy(Strategy):
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

            # T-shaped with minimum length
            is_t_shaped = (
                (abs(open_price - close_price) / open_price) < tight_threshold and
                low_price < open_price and
                abs(low_price - open_price) / abs(high_price - open_price) > 2.5
            )

            if is_t_shaped:
                # Get the last price to use as entry price
                entry_price = self.get_last_price(symbol)
                if entry_price is None:
                    continue

                # Determine trade quantity based on risk size divided by stop loss per share
                computed_quantity = int(risk_size // stop_loss_amount)
                trade_quantity = computed_quantity

                # Calculate stop loss and take profit levels
                stop_loss_price = entry_price - stop_loss_amount
                take_profit_price = entry_price + (stop_loss_amount * risk_reward)

                # Create a market order with attached stop loss and take profit levels
                # Trading on margin by passing custom parameter 'margin': True
                order = self.create_order(
                    symbol,
                    trade_quantity,
                    Order.OrderSide.BUY,
                    stop_price=stop_loss_price,          # attached stop loss
                    take_profit_price=take_profit_price,   # attached take profit target
                    custom_params={"margin": True}
                )
                # Submit the order
                self.submit_order(order)

    def after_market_closes(self):
        # Reset daily loss count for next trading day
        self.vars.daily_loss_count = 0

if __name__ == "__main__":
    # Run backtest
    result = TightnessStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        pandas_data=pandas_data,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        parameters=TightnessStrategy.parameters,
        quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
    )