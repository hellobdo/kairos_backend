import os
import pandas as pd
import sys

from datetime import datetime
from pathlib import Path
from lumibot.entities import Order, Asset, Data
from lumibot.backtesting import PolygonDataBacktesting, PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset
from lumibot.data_sources.pandas_data import PandasData


from backtests.utils.backtest_data_to_db import get_latest_settings_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class BaseStrategy(Strategy):
    
    """
    Base Strategy class containing common helper methods for trading strategies.
    """

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
            type=type,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            custom_params=custom_params,
            time_in_force="day"
        )
        self.submit_order(entry_order)

    @classmethod
    def run_strategy(cls):
        """
        Run the backtest for this strategy class.
        
        This method automatically uses the parameters defined in the strategy class
        to run the backtest without requiring additional arguments.
        
        Args:
            df (pandas.DataFrame, optional): DataFrame to use for CSV data source
            
        Returns:
            The backtest result
        """
        # Get parameters from the class
        parameters = cls.parameters
        backtesting_start = parameters.get("backtesting_start")
        backtesting_end = parameters.get("backtesting_end")
        data_source = parameters.get("data_source")

        if not data_source:
            raise ValueError("Missing required parameters: data_source must be set in parameters")
        
        # Validate required parameters
        if not backtesting_start or not backtesting_end:
            raise ValueError("Missing required parameters: backtesting_start and backtesting_end must be set in parameters")
        
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
                benchmark_asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
                quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
                polygon_api_key=polygon_api_key,
            ) 
        
        else:
            raise ValueError("Invalid data source")

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