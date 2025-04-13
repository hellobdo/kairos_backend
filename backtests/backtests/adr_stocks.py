import pandas as pd
from datetime import datetime, timedelta
import os
import sys
from backtests.utils.backtest_functions import BaseStrategy
from lumibot.components.vix_helper import VixHelper
from lumibot.entities import Asset
from utils.db_utils import DatabaseManager
from indicators.adv import calculate_indicator as calc_adv
from indicators.adr import calculate_indicator as calc_adr
from indicators.sma import calculate_indicator as calc_sma
import pandas_market_calendars as mcal

db = DatabaseManager()

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Define backtest dates
backtesting_start = datetime.strptime(os.getenv("BACKTESTING_START"), "%Y-%m-%d")
backtesting_end = datetime.strptime(os.getenv("BACKTESTING_END"), "%Y-%m-%d")

class Strategy(BaseStrategy):    
    # Define strategy parameters that can be adjusted by the user
    parameters = {
        "symbols": [],  # Will be populated in initialize
        "side": "buy",
        "backtesting_start": backtesting_start.strftime("%Y-%m-%d"),
        "backtesting_end": backtesting_end.strftime("%Y-%m-%d"),
        "margin": False,
        "risk_per_trade": None,
        "data_source": "polygon",
        "benchmark": "QQQ",
        "volume_threshold": 30000000,    # 30M
        "adr_threshold": 4.0,            # 4% average daily range
        "price_threshold": 40,           # Price > 40
        "vix_threshold": 20,             # VIX < 20
        "position_pct": 0.10,             # 10% allocation per ticker
    }

    def initialize(self):
        # Set bot to run every day (can be adjusted) 
        self.sleeptime = "1D"
        # Initialize the VixHelper to work with VIX-related checks
        self.vix_helper = VixHelper(self)
        # Get the symbols and update parameters
        self.parameters["symbols"] = self.get_stocks_df()
        self.margin = self.parameters.get("margin")
        self.side = self.parameters.get("side")
        self.risk_per_trade = self.parameters.get("risk_per_trade")

        self.length = (backtesting_end - backtesting_start).days
        
        # Load benchmark data once and store it
        print("Loading benchmark data once for the entire backtest...")
        self.benchmark_df = self.get_benchmark()
        # Convert the datetime column to a standard string format for easier matching
        self.benchmark_df['datetime_str'] = pd.to_datetime(self.benchmark_df['datetime']).dt.strftime('%Y-%m-%d')
        print(f"Loaded benchmark data with {len(self.benchmark_df)} rows")
        
        # Initialize NYSE calendar
        self.nyse = mcal.get_calendar('NYSE')

    def get_previous_business_day(self, date):
        """Get the previous business day based on NYSE calendar"""
        # Convert date to pandas Timestamp and handle timezone
        if not isinstance(date, pd.Timestamp):
            date = pd.Timestamp(date)
        
        # Make date timezone-naive if it has timezone info
        if date.tzinfo is not None:
            # Convert to timezone-naive by removing the timezone
            date = pd.Timestamp(date.year, date.month, date.day, 
                              date.hour, date.minute, date.second)
        
        # Get trading days before the current date
        schedule = self.nyse.schedule(start_date=date - pd.Timedelta(days=10), end_date=date)
        trading_days = schedule.index
        
        # If the current date is a trading day, get the previous one
        if date in trading_days:
            idx = trading_days.get_loc(date)
            if idx > 0:
                return trading_days[idx - 1].date()
        
        # If the current date is not a trading day, get the most recent one
        previous_days = trading_days[trading_days < date]
        if len(previous_days) > 0:
            return previous_days[-1].date()
        
        # Fallback: just return the previous calendar day
        return (date - pd.Timedelta(days=1)).date()

    def get_stocks_df(self):
        """Returns a list of stock symbols that meet the volume, ADR and price thresholds."""
        # Get stock data from database
        stocks_df = db.get_ohlcv_data("stocks")
        min_vol = self.parameters.get("volume_threshold")
        min_adr = self.parameters.get("adr_threshold")
        min_price = self.parameters.get("price_threshold")

        stocks_df_with_adv = calc_adv(stocks_df, 30) # calculates the average daily volume 30 days
        
        # Get tickers with sufficient volume
        liquid_tickers = stocks_df_with_adv[stocks_df_with_adv["adv"] >= min_vol]["ticker"].unique()
        
        # Keep only data for liquid tickers
        liquid_stocks = stocks_df[stocks_df["ticker"].isin(liquid_tickers)]
        
        # Calculate ADR for the filtered tickers
        liquid_stocks_with_adr = calc_adr(liquid_stocks, 20) # calculates the average daily range 20 days
        
        # Get tickers with sufficient ADR
        moving_tickers = liquid_stocks_with_adr[liquid_stocks_with_adr["adr"] >= min_adr]["ticker"].unique()
        
        # Keep only data for moving tickers
        moving_stocks = stocks_df[stocks_df["ticker"].isin(moving_tickers)]
        
        # Filter by price, only keeping tickers that meet price threshold
        priced_tickers = moving_stocks[moving_stocks["close"] >= min_price]["ticker"].unique()
        
        # Keep all rows but only for tickers that meet all conditions
        filtered_stocks_df = stocks_df[stocks_df["ticker"].isin(priced_tickers)]

        symbols = filtered_stocks_df["ticker"].unique().tolist()

        print(f"Number of symbols: {len(symbols)}")
        return symbols

    def get_benchmark(self):
        """Fetch benchmark from the indexes table in the database."""
        # Get stock data from database
        benchmark = self.parameters.get("benchmark")
        print(f"Fetching benchmark data for {benchmark}...")
        
        # Get the data
        indexes_df = db.get_ohlcv_data("indexes", benchmark)
        print(f"Retrieved {len(indexes_df)} rows of benchmark data")

        # Calculate SMA and above_sma indicators
        indexes_df_with_sma = calc_sma(indexes_df, 50) # calculates the simple moving average 50 days
        indexes_df_with_sma["above_sma"] = indexes_df_with_sma["close"] > indexes_df_with_sma["sma"]

        return indexes_df_with_sma
            
    def on_trading_iteration(self):
        # get params for iteration
        current_time = self.get_datetime()
        print(current_time)
        # Get previous business day instead of simply subtracting one day
        
        open_positions = self.get_positions()

        # filter interactions if vix is high
        vix_value = self.vix_helper.get_vix_1d_value(current_dt = current_time)
        print(f"VIX Price: {vix_value}")
        if vix_value >= 20:
            print(f"VIX condition not met: VIX = {vix_value} (>= {self.parameters.get('vix_threshold')})")
            if len(open_positions) > 0:
                self.sell_all()
            return  # Exit this iteration as market volatility is high
        else:
            print(f"VIX condition met: VIX = {vix_value} (< {self.parameters.get('vix_threshold')})")

        # filter interactions if benchmark is below sma
        # Convert previous business day to string format for matching
        previous_day = self.get_previous_business_day(current_time)
        previous_day_str = previous_day.strftime('%Y-%m-%d')
        
        # Use the pre-loaded benchmark data with string comparison
        matching_rows = self.benchmark_df[self.benchmark_df['datetime_str'] == previous_day_str]
        
        if len(matching_rows) == 0:
            print(f"No benchmark data found for date {previous_day_str}")
            return
            
        # Get the exact matching row
        row = matching_rows.iloc[0]
        above_sma = row["above_sma"]
        sma_50 = row["sma"]
        close_50 = row["close"]

        if above_sma is None:
            print("Benchmark SMA is None, skipping.")
            return
        if not above_sma:
            print(f"Benchmark is below SMA, benchmark close is {close_50} and sma is {sma_50}, skipping, and closing all positions")
            if len(open_positions) > 0:
                self.sell_all()
            return
        else:
            print(f"Benchmark is above SMA, benchmark close is {close_50} and sma is {sma_50}, continuing")

        # Get parameters for filtering
        min_vol = self.parameters.get("volume_threshold")
        min_adr = self.parameters.get("adr_threshold")
        min_price = self.parameters.get("price_threshold")
        symbols = self.parameters.get("symbols")

        # Get cash and allocation per ticker for entry positions
        cash = self.cash
        print(f"Available cash: ${cash}")    
        allocation_per_ticker = cash * self.parameters.get("position_pct")
        print(f"Allocation per ticker: ${allocation_per_ticker}")
        
        # Track qualified tickers for trading
        qualified_tickers = []

        # Process each ticker in our universe
        for symbol in symbols:
            print(f"Analyzing {symbol}")
            print(f"Current date: {current_time}")
            
            # Skip if we already have a position in this ticker
            if any(position.symbol == symbol for position in open_positions):
                print(f"Already have position in {symbol}, skipping")
                continue
            
            # Get data for this ticker up to current time
            bars = self.get_historical_prices(symbol, length=self.length, timestep="1 day", include_after_hours=False)
            if bars is None or bars.df.empty:
                print(f"No data found for {symbol}, skipping")
                continue
            
            ticker_df = bars.df
            
            if len(ticker_df) < 30:
                print(f"Not enough data for {symbol}, need at least 30 days")
                continue
                
            volume_data = calc_adv(ticker_df, 30)
            latest_volume = volume_data.iloc[-1]
            
            # Check volume threshold
            if 'adv' not in latest_volume or latest_volume['adv'] < min_vol:
                print(f"{symbol} failed volume test: {latest_volume.get('adv', 0)} < {min_vol}")
                continue
                
            # Calculate ADR (Average Daily Range)
            adr_data = calc_adr(ticker_df, 20)
            latest_adr = adr_data.iloc[-1]
            
            # Check ADR threshold
            if 'adr' not in latest_adr or latest_adr['adr'] < min_adr:
                print(f"{symbol} failed ADR test: {latest_adr.get('adr', 0)} < {min_adr}")
                continue
                
            # Check price threshold
            latest_price = ticker_df.iloc[-1]['close']
            if latest_price < min_price:
                print(f"{symbol} failed price test: {latest_price} < {min_price}")
                continue
                
            print(f"{symbol} passed all tests: ADV={latest_volume.get('adv', 0)}, ADR={latest_adr.get('adr', 0)}, Price={latest_price}")
            qualified_tickers.append((symbol, latest_price))
            
        # Log the qualified tickers
        print(f"Qualified tickers: {[t[0] for t in qualified_tickers]}")
        
        # Place buy orders for qualified tickers
        for symbol, price in qualified_tickers:
            shares = int(allocation_per_ticker / price)
            if shares <= 0:
                print(f"Not enough allocation to buy {symbol} at ${price}")
                continue
                
            print(f"Buying {shares} shares of {symbol} at ${price}")
            self._create_and_submit_entry_order(symbol, shares)

    def on_filled_order(self, position, order, price, quantity, multiplier):
        """Call the base class implementation to log trade information"""
        self._on_filled_order(position, order, price, quantity, multiplier)

    def after_market_closes(self):
        self._save_trades_at_end()

if __name__ == "__main__":
    # For Polygon data source, we don't need to pass a DataFrame
    result = Strategy.run_strategy()
    Strategy.rename_custom_logs()