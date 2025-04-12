import os
import pandas as pd
import sys

from datetime import datetime
from pathlib import Path
from lumibot.entities import Order, Asset, Data
from lumibot.backtesting import PolygonDataBacktesting, PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset

from backtests.utils.backtest_data_to_db import get_latest_settings_file
# Import indicators loader

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
    
    def _apply_indicators(self, df, calculate_indicators):
        """Apply all indicators sequentially, return True if all signals are valid, False otherwise"""
        for calc_func in calculate_indicators.values():
            # Apply indicator and check if signal is false
            df = calc_func(df)
            if not df['is_indicator'].iloc[-1]:
                return False, df
        # All indicators returned True
        return True, df
    
    def _calculate_qty(self, stop_loss_amount, entry_price):
        """Calculate quantity based on risk per trade and stop loss amount"""

        if self.risk_per_trade is None:
            if self.cash < entry_price:
                return False
            else:
                qty = int(self.cash // entry_price)
        else:
            qty = int(self.cash * self.risk_per_trade // stop_loss_amount)
        
        return qty
    
    def _determine_stop_loss(self, price, stop_loss_rules):
        """Determine stop loss amount based on price and rules"""

        if stop_loss_rules is None:
            return None

        try:
            for rule in stop_loss_rules:
                if "price_below" in rule and price < rule["price_below"]:
                    return rule["amount"]
                elif "price_above" in rule and price >= rule["price_above"]:
                    return rule["amount"]
        except Exception as e:
            print(f"Error determining stop loss: {e}")
            return None
    
    def _calculate_stop_loss_price(self, entry_price, stop_loss_amount, side):
        """Calculate stop loss price based on entry price and trade side"""
        if side == 'buy':
            return entry_price - stop_loss_amount
        elif side == 'sell':
            return entry_price + stop_loss_amount
    
    def _calculate_take_profit_price(self, entry_price, stop_loss_amount, side, risk_reward):
        """Calculate take profit price based on entry price and trade side"""
        
        if side == 'buy':
            take_profit_price = entry_price + (stop_loss_amount * risk_reward)
        elif side == 'sell':
            take_profit_price = entry_price - (stop_loss_amount * risk_reward)
            
        return take_profit_price 

    def _save_trades_at_end(self):
        """Save trades to CSV when reaching the end of backtest"""
        current_time = self.get_datetime()
        backtesting_end = datetime.strptime(self.parameters.get("backtesting_end"), "%Y-%m-%d")
        
        next_day = current_time + pd.Timedelta(days=1)

        if next_day.date() == backtesting_end.date() or current_time.date() == backtesting_end.date():
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
            "risk_per_trade": self.risk_per_trade,
        }

        # Append to trade log
        self.vars.trade_log.append(trade_info)

    def _load_parameters(self):
        parameters = self.parameters
        
        self.minutes_before_closing = 0.1 
        
        # Load additional common parameters from on_trading_iteration
        self.symbols = parameters.get("symbols", [])
        self.risk_reward = parameters.get("risk_reward")
        self.side = parameters.get("side")
        self.risk_per_trade = parameters.get("risk_per_trade")
        self.bar_signals_length = parameters.get("bar_signals_length")
        self.sleeptime = parameters.get("sleeptime")
        self.indicators = parameters.get("indicators")
        self.stop_loss_rules = parameters.get("stop_loss_rules")
        self.margin = parameters.get("margin")
        
    def _handle_trading_iteration(self):
        """
        Handles the common logic for trading iterations.
        
        Returns:
            bool: True if trading was processed, False if early return conditions were met
        """

        # Loop through each symbol to check if the entry conditions are met
        for symbol in self.symbols:

            # Apply indicators and check if all signals are valid
            if self.indicators is not None:
                bars = self.get_historical_prices(symbol, length=1, timestep=self.bar_signals_length)
                if bars is None or bars.df.empty:
                    continue

                signal_valid = self._apply_indicators(bars.df.copy(), self.calculate_indicators)
                if not signal_valid:
                    continue
            
            # Process valid signal
            entry_price = self.get_last_price(symbol)
            if entry_price is None:
                continue

            # Determine stop loss amount
            stop_loss_amount = self._determine_stop_loss(entry_price, self.stop_loss_rules)

            # Calculate quantity
            quantity = self._calculate_qty(stop_loss_amount, entry_price)

            if quantity:
                # Prepare order parameters
                stop_loss_price, take_profit_price = self._prepare_order_parameters(stop_loss_amount, entry_price)

                # Create and submit an order
                self._create_and_submit_entry_order(symbol, quantity, stop_loss_price, take_profit_price)
            
        return True
    
    def _prepare_order_parameters(self, stop_loss_amount, entry_price):
        """
        Prepare order parameters based on stop loss rules and risk reward.
        
        Args:
            stop_loss_amount (float): Stop loss amount
            entry_price (float): Entry price
            
        Returns:
            tuple: stop_loss_price, take_profit_price, order_type
        """
        stop_loss_price = None
        take_profit_price = None

        if stop_loss_amount is not None:
            stop_loss_price = self._calculate_stop_loss_price(entry_price, stop_loss_amount, self.side)
            
            if self.risk_reward is not None:
                take_profit_price = self._calculate_take_profit_price(entry_price, stop_loss_amount, self.side, self.risk_reward)

        return stop_loss_price, take_profit_price
        
    def _create_and_submit_entry_order(self, symbol, quantity, stop_loss_price=None, take_profit_price=None, type="market"):
        """
        Creates and submits a bracket order with stop loss and take profit.
        
        Args:
            symbol (str): Trading symbol
            quantity (float): Order quantity
            stop_loss_price (float, optional): Stop loss price level
            take_profit_price (float, optional): Take profit price level
            type (str, optional): Order type, defaults to "bracket"
        """
        # Create a market order with attached stop loss and take profit orders
        # Trading on margin by passing custom parameter 'margin': True
        custom_params = {"margin": False}
        if self.margin:
            custom_params["margin"] = True
        
        if stop_loss_price:
            custom_params["stop_loss_price"] = stop_loss_price
            
        if take_profit_price:
            custom_params["take_profit_price"] = take_profit_price

        if stop_loss_price is not None or take_profit_price is not None:
            type = "bracket"
            
        entry_order = self.create_order(
            symbol,
            quantity,
            side=self.side,
            type= type,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            custom_params=custom_params,
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
        if self.indicators is not None:
            self.calculate_indicators = self._load_indicators(self.indicators, load_indicators)

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
        symbols = parameters.get("symbols", [])
        backtesting_start = parameters.get("backtesting_start")
        backtesting_end = parameters.get("backtesting_end")
        polygon_api_key = os.getenv("POLYGON_API_KEY")
        
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
        if not polygon_api_key:
            raise ValueError("POLYGON_API_KEY environment variable not set")
            
        return cls.run_backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            parameters=parameters,
            quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
            polygon_api_key=polygon_api_key,
        ) 
        
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
                
                # Get list of files matching the pattern
                matching_files = list(logs_dir.glob(pattern))
                
                if matching_files:
                    for file in matching_files:
                        print(f"Found file to rename: {file}")
                        new_name = str(file).replace("_id_", f"_{identifier}_")
                        os.rename(file, new_name)
                        print(f"Renamed to: {new_name}")
                else:
                    print(f"No files found matching pattern: {pattern}")
        else:
            identifier = "id"
            print(f"Identifier not replaced")