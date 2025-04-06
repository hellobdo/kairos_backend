from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset
import pandas as pd
from datetime import datetime
from pathlib import Path
from lumibot.entities import Order, Asset, Data
from lumibot.backtesting import PolygonDataBacktesting, PandasDataBacktesting
import os
from backtests.utils.backtest_data_to_db import get_latest_settings_file
# Import indicators loader
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from indicators import load_indicators


class BaseStrategy(Strategy):
    """
    Base Strategy class containing common helper methods for trading strategies.
    
    This class provides reusable functionality for:
    - Position limit checking
    - Time condition verification
    - Indicator application
    - Quantity calculation
    - Stop loss determination
    - Price level calculation
    
    Extend this class to create specific trading strategies without duplicating code.
    """
    
    def _load_indicators(self, indicators, load_function):
        """
        Load indicator calculation functions based on indicator names
        
        Args:
            indicators (list): List of indicator names to load
            load_function (function): Function to load each indicator
            
        Returns:
            dict: Dictionary mapping indicator names to their calculation functions
        """
        calculate_indicators = {}
        for indicator in indicators:
            try:
                calculate_indicators[indicator] = load_function(indicator)
            except Exception as e:
                print(f"Error loading indicator '{indicator}': {str(e)}")
                # Continue with other indicators
        return calculate_indicators
    
    def _check_position_limits(self):
        """Check if we've reached position or loss limits"""
        max_loss_positions = self.parameters.get("max_loss_positions")
        open_positions = [p for p in self.get_positions() if not (p.asset.symbol == "USD" and p.asset.asset_type == Asset.AssetType.FOREX)]
        return len(open_positions) >= max_loss_positions or self.vars.daily_loss_count >= max_loss_positions
    
    def _check_time_conditions(self, time):
        """Check if current time meets our trading conditions (0 or 30 minutes past the hour)"""
        return time.minute == 0 or time.minute == 30
    
    def _apply_indicators(self, df, calculate_indicators):
        """Apply all indicators sequentially, return True if all signals are valid, False otherwise"""
        for calc_func in calculate_indicators.values():
            # Apply indicator and check if signal is false
            df = calc_func(df)
            if not df['is_indicator'].iloc[-1]:
                return False, df
        # All indicators returned True
        return True, df
    
    def _calculate_qty_based_on_risk_per_trade(self, stop_loss_amount, risk_per_trade):
        """Calculate quantity based on risk per trade and stop loss amount"""

        risk_size = 30000 * risk_per_trade
        return int(risk_size // stop_loss_amount)
    
    def _determine_stop_loss(self, price, stop_loss_rules):
        """Determine stop loss amount based on price and rules"""

        for rule in stop_loss_rules:
            if "price_below" in rule and price < rule["price_below"]:
                return rule["amount"]
            elif "price_above" in rule and price >= rule["price_above"]:
                return rule["amount"]
        return None  # No matching rule found
    
    def _calculate_price_levels(self, entry_price, stop_loss_amount, side, risk_reward):
        """Calculate stop loss and take profit levels based on entry price and trade side"""
        
        if side == 'buy':
            stop_loss_price = entry_price - stop_loss_amount
            take_profit_price = entry_price + (stop_loss_amount * risk_reward)
        elif side == 'sell':
            stop_loss_price = entry_price + stop_loss_amount
            take_profit_price = entry_price - (stop_loss_amount * risk_reward)
            
        return stop_loss_price, take_profit_price 

    def _save_trades_at_end(self):
        """Save trades to CSV when reaching the end of backtest"""
        current_time = self.get_datetime()
        backtesting_end = datetime.strptime(self.parameters.get("backtesting_end"), "%Y-%m-%d")
        
        next_day = current_time + pd.Timedelta(days=1)
        if next_day.date() == backtesting_end.date():
            if hasattr(self.vars, 'trade_log') and self.vars.trade_log:
                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
                df = pd.DataFrame(self.vars.trade_log)
                
                # Ensure logs directory exists
                logs_dir = Path("logs")
                logs_dir.mkdir(exist_ok=True)
                
                # Save file in logs directory with same identifier
                filename = logs_dir / f"{self.name}_{timestamp}_{'id'}_custom_trades.csv"
                df.to_csv(filename, index=False)
                print(f"Custom trades saved to {filename}")
            else:
                print("No trade log to save.")

    def _out_before_end_of_day(self):
        """Cancel all open orders and sell all positions"""
        self.cancel_open_orders()
        positions = self.get_positions()
        if len(positions) > 0:
            self.sell_all()

    def _on_filled_order(self, position, order, price, quantity, multiplier):
        """
        Process filled orders and log trade information.
        
        This method extracts information from the order, logs it to the trade_log,
        and performs any necessary post-fill actions.
        
        Args:
            position: The position that was filled
            order: The order that was filled
            price: The fill price
            quantity: The fill quantity
            multiplier: The multiplier applied to the order
        """
        # Initialize trade_log if it doesn't exist
        if not hasattr(self.vars, 'trade_log'):
            self.vars.trade_log = []
            
        # Extract stop loss and take profit from order parameters
        stop_loss = None
        take_profit = None
        if order.custom_params:
            stop_loss = order.custom_params.get('stop_loss_price')
            take_profit = order.custom_params.get('take_profit_price')

        # Create trade info dictionary
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

        # Append to trade log
        self.vars.trade_log.append(trade_info)

    @classmethod
    def run_strategy(cls):
        """
        Run the backtest for this strategy class.
        
        This method automatically uses the parameters defined in the strategy class
        to run the backtest without requiring additional arguments.
        
        Returns:
            The backtest result
        """
        # Get parameters from the class
        parameters = cls.parameters
        data_source = parameters.get("data_source", "polygon")
        symbols = parameters.get("symbols", [])
        backtesting_start = parameters.get("backtesting_start")
        backtesting_end = parameters.get("backtesting_end")
        
        # Validate required parameters
        if not backtesting_start or not backtesting_end:
            raise ValueError("Missing required parameters: backtesting_start and backtesting_end must be set in parameters")
        
        if not symbols:
            raise ValueError("No symbols defined in parameters")
        
        # Convert string dates to datetime objects if needed
        if isinstance(backtesting_start, str):
            backtesting_start = datetime.strptime(backtesting_start, "%Y-%m-%d")
        
        if isinstance(backtesting_end, str):
            backtesting_end = datetime.strptime(backtesting_end, "%Y-%m-%d")
            
        # Run appropriate backtest based on data source
        if data_source == "polygon":
            polygon_api_key = os.getenv("POLYGON_API_KEY")
            if not polygon_api_key:
                raise ValueError("POLYGON_API_KEY environment variable not set")
                
            return cls.run_backtest(
                PolygonDataBacktesting,
                backtesting_start,
                backtesting_end,
                parameters=parameters,
                quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
                polygon_api_key=polygon_api_key,
                show_plot=False,
                show_tearsheet=False
            ) 
        
        elif data_source == "csv":
            # Ensure we have at least one symbol
            symbol = symbols[0]
            csv_path = f'data/csv/{symbol}.csv'
            
            # Check if CSV file exists
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"CSV file not found: {csv_path}")
                
            df = pd.read_csv(csv_path)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            
            asset = Asset(symbol, asset_type=Asset.AssetType.STOCK)
            pandas_data = {
                asset: Data(asset, df, timestep="minute"),
            }

            return cls.run_backtest(
                PandasDataBacktesting,
                backtesting_start,
                backtesting_end,
                parameters=parameters,
                pandas_data=pandas_data
            )
        
        else:
            raise ValueError(f"Unsupported data source: {data_source}")

    def _load_parameters(self):
        self.sleeptime = self.parameters.get("sleeptime")

        if not hasattr(self.vars, 'daily_loss_count'):
            self.vars.daily_loss_count = 0
        
        # Load indicator calculation functions
        self.indicators = self.parameters.get("indicators")
        
        self.minutes_before_closing = 0.1 # close positions before market close, see below def before_market_closes()
        
        # Load additional common parameters from on_trading_iteration
        self.symbols = self.parameters.get("symbols", [])
        self.bar_signals_length = self.parameters.get("bar_signals_length")
        self.side = self.parameters.get("side")
        self.risk_reward = self.parameters.get("risk_reward")
        self.risk_per_trade = self.parameters.get("risk_per_trade")
        self.stop_loss_rules = self.parameters.get("stop_loss_rules")
        
    def _handle_trading_iteration(self, calculate_indicators):
        """
        Handles the common logic for trading iterations.
        
        Args:
            calculate_indicators (dict): Dictionary of indicator calculation functions
            
        Returns:
            bool: True if trading was processed, False if early return conditions were met
        """
        current_time = self.get_datetime()

        # Check if max daily losses reached or position limit reached
        if self._check_position_limits():
            return False

        # Check if we're at the right time to trade
        if not self._check_time_conditions(current_time):
            return False

        # Loop through each symbol to check if the entry conditions are met
        for symbol in self.symbols:
            # Skip if there is already a position in this asset
            if self.get_position(symbol) is not None:
                continue

            bars = self.get_historical_prices(symbol, length=1, timestep=self.bar_signals_length)
            if bars is None or bars.df.empty:
                continue

            # Apply indicators and check if all signals are valid
            signal_valid, df = self._apply_indicators(bars.df.copy(), calculate_indicators)
            if not signal_valid:
                continue
            
            # Process valid signal
            entry_price = self.get_last_price(symbol)
            if entry_price is None:
                continue

            # Determine stop loss amount
            price = df['close'].iloc[-1]
            stop_loss_amount = self._determine_stop_loss(price, self.stop_loss_rules)
            if stop_loss_amount is None:
                continue  # No matching rule found

            stop_loss_price, take_profit_price = self._calculate_price_levels(entry_price, stop_loss_amount, self.side, self.risk_reward)
            quantity = self._calculate_qty_based_on_risk_per_trade(stop_loss_amount, self.risk_per_trade)
            
            # Create and submit an order
            self._create_and_submit_entry_order(symbol, quantity, stop_loss_price, take_profit_price)
            
        return True
        
    def _create_and_submit_entry_order(self, symbol, quantity, stop_loss_price, take_profit_price):
        """
        Creates and submits a bracket order with stop loss and take profit.
        
        Args:
            symbol (str): Trading symbol
            quantity (float): Order quantity
            stop_loss_price (float): Stop loss price level
            take_profit_price (float): Take profit price level
        """
        # Create a market order with attached stop loss and take profit orders
        # Trading on margin by passing custom parameter 'margin': True
        entry_order = self.create_order(
            symbol,
            quantity,
            side=self.side,
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
        
    def initialize_strategy(self):
        """
        Initialize strategy with common parameters and indicator functions.
        """
        # Load all common parameters first
        self._load_parameters()
        
        # Load indicator calculation functions and store as instance variable
        self.calculate_indicators = self._load_indicators(self.indicators, load_indicators)
        
    @classmethod
    def rename_custom_logs(cls):
        """
        Rename custom log files with the proper identifier from settings file.
        This should be called after running the strategy.
        """
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