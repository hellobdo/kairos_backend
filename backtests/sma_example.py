from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader
from lumibot.entities import Order, Asset, Data, TradingFee
from lumibot.credentials import IS_BACKTESTING
import pandas as pd
from lumibot.backtesting import PandasDataBacktesting
from datetime import datetime
import os

# Prepare for data loading
ticker = "QQQ"

# Load the CSV file using the ticker variable
df = pd.read_csv(f'backtests/data/csv/{ticker}.csv')
# Convert datetime column to datetime type
df['datetime'] = pd.to_datetime(df['datetime'])
# Set datetime as the index
df.set_index('datetime', inplace=True)

asset = Asset(ticker, asset_type=Asset.AssetType.STOCK)

# Prepare the pandas_data dict for backtesting using minute resolution data
pandas_data = {
    asset: Data(asset, df, timestep="minute")
}

# Determine the maximum date from the CSV data
max_date = df.index.max()
if max_date.tzinfo is not None:
    # Remove timezone info if present
    max_date = max_date.replace(tzinfo=None)

# Set backtesting start and end dates (using naive datetimes)
backtesting_start = datetime(2025, 1, 1, 11, 30)  # Example start time (exchange time 9:30 adjusted to data time 11:30)
backtesting_end = max_date


class LongTightness(Strategy):
    """
    This strategy was refined based on the user prompt: 'please implement a check on the SMA12. If the latest daily price is above the SMA12 daily, then this strategy can be executed and attach a stop order and a take profit order'.
    
    Additionally, the new update asks: 'can I access the trades data? entry price, exit price, stop price, quantity, entry and exit time stamps?'
    
    The strategy uses minute-level data for entry optimization along with a daily SMA filter (SMA12) to confirm the trend before checking for a T-shaped candle on minute data. It logs key trade information when orders are filled.

    Note: This implementation now submits a market order for entry and immediately attaches a stop loss order and a take profit order as child orders. It also stores filled trade details for later access.
    """
    # Define strategy parameters that can be adjusted by the user
    parameters = {
        "symbols": [ticker],
        "tight_threshold": 0.00015,           # Maximum allowed ratio difference between open and close to consider the candle as tight
        "stop_loss": 0.8,                      # Stop loss in dollars per share
        "risk_reward": 2                       # Profit target multiplier, meaning the profit target is risk_reward * stop_loss
    }

    def initialize(self):
        # Set the sleep time between iterations
        self.sleeptime = "30M"
        # Initialize persistent variable for tracking daily losses
        if not hasattr(self.vars, 'daily_loss_count'):
            self.vars.daily_loss_count = 0
        # Initialize persistent variable for storing filled trade details
        if not hasattr(self.vars, 'trade_log'):
            self.vars.trade_log = []
        # Set minutes before market closes to exit positions
        self.minutes_before_closing = 5
        self.log_message('LongTightness strategy initialized.', color='blue')

    def on_trading_iteration(self):
        # Check if max daily losses reached (2 consecutive losses) to prevent further trading
        if self.vars.daily_loss_count >= 2:
            self.log_message('Daily loss limit reached. Skipping trading for this iteration.', color='red')
            return

        symbols = self.parameters.get("symbols", [])
        tight_threshold = self.parameters.get("tight_threshold")
        stop_loss_amount = self.parameters.get("stop_loss")
        risk_reward = self.parameters.get("risk_reward")

        # Calculate risk size as 0.5% of available cash
        cash = self.get_cash()
        risk_size = cash * 0.005

        # Check current open positions (exclude USD cash position)
        open_positions = [p for p in self.get_positions() if not (p.asset.symbol == "USD" and p.asset.asset_type == Asset.AssetType.FOREX)]
        if len(open_positions) >= 2:
            self.log_message('Max open positions reached. No new trades will be initiated.', color='yellow')
            return

        # Loop through each symbol to evaluate entry conditions
        for symbol in symbols:
            # Skip if already holding a position in the symbol
            if self.get_position(symbol) is not None:
                self.log_message(f'Already holding a position in {symbol}.', color='yellow')
                continue

            # ------------------------------
            # SMA12 Daily Trend Check Block
            # ------------------------------
            # Retrieve at least 12 days of historical daily data to compute the SMA12
            daily_bars = self.get_historical_prices(symbol, length=12, timestep='day')
            if daily_bars is None or daily_bars.df.empty or len(daily_bars.df) < 12:
                self.log_message(f'Not enough daily data for {symbol} to compute SMA12. Skipping symbol.', color='yellow')
                continue
            df_daily = daily_bars.df.copy()
            df_daily['SMA12'] = df_daily['close'].rolling(window=12).mean()
            latest_daily_close = df_daily.iloc[-1]['close']
            latest_daily_sma = df_daily.iloc[-1]['SMA12']
            self.log_message(f'SMA12 Check for {symbol} -> Close: {latest_daily_close:.2f}, SMA12: {latest_daily_sma:.2f}', color='blue')

            # Proceed only if the latest daily close is above its SMA12
            if latest_daily_close <= latest_daily_sma:
                self.log_message(f'{symbol}: Daily close ({latest_daily_close:.2f}) is not above SMA12 ({latest_daily_sma:.2f}). Skipping trade.', color='red')
                continue

            # ---------------------------------------
            # T-shaped Candle Entry Check (using minute-level data)
            # ---------------------------------------
            # Obtain recent minute-level historical prices; here we get the latest candle
            bars = self.get_historical_prices(symbol, length=1, timestep="minute")
            if bars is None or bars.df.empty:
                self.log_message(f'No recent minute-level data for {symbol}.', color='yellow')
                continue
            df_minute = bars.df
            latest_candle = df_minute.iloc[-1]
            open_price = latest_candle['open']
            close_price = latest_candle['close']
            high_price = latest_candle['high']
            low_price = latest_candle['low']

            # Calculate candle ratios for logging
            body_ratio = abs(open_price - close_price) / open_price if open_price != 0 else 0
            lower_shadow_ratio = (abs(low_price - open_price) / abs(high_price - open_price)) if (high_price - open_price) != 0 else 0
            self.log_message(f'{symbol} minute candle -> body_ratio: {body_ratio:.5f}, lower_shadow_ratio: {lower_shadow_ratio:.2f}', color='blue')

            # Identify T-shaped candle condition: tight body and a long lower shadow
            is_t_shaped = (
                body_ratio < tight_threshold and
                low_price < open_price and
                lower_shadow_ratio > 2.5
            )

            if is_t_shaped:
                # Get the current market price for entry
                entry_price = self.get_last_price(symbol)
                if entry_price is None:
                    self.log_message(f'Unable to retrieve current price for {symbol}.', color='yellow')
                    continue

                # Determine trade quantity based on risk_size and stop loss per share
                computed_quantity = int(risk_size // stop_loss_amount)
                trade_quantity = computed_quantity

                # Compute stop loss price based on the entry price
                stop_loss_price = entry_price - stop_loss_amount
                
                # Compute take profit price based on risk reward (entry_price + risk_reward * stop_loss_amount)
                take_profit_price = entry_price + (risk_reward * stop_loss_amount)

                # ---------------------------------------------
                # Modified Order Submission Block:
                # Submit a market order for entry, then attach a separate stop loss order and a take profit order.
                # ---------------------------------------------

                # Create a market order for entry (pure market order without trigger parameters)
                entry_order = self.create_order(
                    symbol,
                    trade_quantity,
                    Order.OrderSide.BUY,
                    custom_params={"margin": True}
                )
                self.submit_order(entry_order)
                self.log_message(f'Placed market entry order for {trade_quantity} shares of {symbol} at {entry_price:.2f}.', color='green')

                # Create a stop loss order attached to the entry order
                stop_order = self.create_order(
                    symbol,
                    trade_quantity,
                    Order.OrderSide.SELL,
                    stop_price=stop_loss_price,  # Triggers stop loss order
                    custom_params={"parent_order": entry_order.identifier}
                )
                self.submit_order(stop_order)
                self.log_message(f'Attached stop loss order for {symbol} at {stop_loss_price:.2f}.', color='green')

                # Create a take profit order attached to the entry order
                take_profit_order = self.create_order(
                    symbol,
                    trade_quantity,
                    Order.OrderSide.SELL,
                    limit_price=take_profit_price,  # Set limit price for taking profit
                    custom_params={"parent_order": entry_order.identifier}
                )
                self.submit_order(take_profit_order)
                self.log_message(f'Attached take profit order for {symbol} at {take_profit_price:.2f}.', color='green')
            else:
                self.log_message(f'{symbol} did not meet T-shaped candle criteria.', color='red')

    def on_filled_order(self, position, order, price, quantity, multiplier):
        # This lifecycle method logs trade details when an order is completely filled.
        trade_info = {
            "order_id": order.identifier,
            "symbol": order.asset if isinstance(order.asset, str) else order.asset.symbol,
            "filled_price": price,
            "quantity": quantity,
            "side": order.side,
            "timestamp": self.get_datetime(),  # current time stamped at fill
            "details": order.custom_params  # any additional details like parent_order info
        }
        # Append the trade details to a persistent variable so that trade data remains accessible
        self.vars.trade_log.append(trade_info)
        self.log_message(f'Trade filled: {trade_info}', color='green')

    def before_market_closes(self):
        # Close all open positions 5 minutes before market close
        positions = self.get_positions()
        if not positions:
            return
        for position in positions:
            # Skip USD (cash) position
            if position.asset.symbol == "USD" and position.asset.asset_type == Asset.AssetType.FOREX:
                continue
            symbol = position.asset.symbol
            quantity = abs(position.quantity)
            # Determine the correct side to exit the position
            side = Order.OrderSide.SELL if position.quantity > 0 else Order.OrderSide.BUY
            # Since no stop_price or other parameters are given here, the sell orders are processed as market orders
            order = self.create_order(symbol, quantity, side)
            self.submit_order(order)
            self.log_message(f'Exiting position for {symbol} before market close.', color='blue')

    def after_market_closes(self):
        # Reset daily loss count for the next trading day
        self.vars.daily_loss_count = 0


if __name__ == "__main__":
    # Run backtest
    result = LongTightness.run_backtest(
        datasource_class=PandasDataBacktesting,
        pandas_data=pandas_data,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        parameters=LongTightness.parameters,
        quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX)
    )

    # Uncomment the following lines for live trading
    # trader = Trader()
    # strategy = LongTightness(quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX))
    # trader.add_strategy(strategy)
    # trader.run_all()