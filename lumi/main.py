from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader
from lumibot.entities import Asset, TradingFee, Order
from sqlalchemy import create_engine
import pandas as pd
import os
import importlib.util
import termcolor
import requests
from datetime import datetime

from lumibot.credentials import IS_BACKTESTING, DB_CONNECTION_STR

from components.configs_helper import ConfigsHelper

# Set the backtesting config file to use
BACKTESTING_CONFIG = "tqqq_spy"
BACKTESTING_START = datetime(2014, 1, 1)
BACKTESTING_END = datetime(2014, 1, 15)

"""
Strategy Description

This strategy is an example of a stock trading strategy. It splits the cash available equally between the stocks in the symbols list
right at the start of the strategy and holds the stocks indefinitely. This is a simple strategy that is used to demonstrate
how to create a stock trading strategy in Lumibot.


"""

###################
# Configuration
###################

class StockExampleAlgo(Strategy):
    # =====Overloading lifecycle methods=============

    parameters = {
        # Example of parameters that can be set in the strategy, see teh configurations folder for working examples
        # "symbols": ["AAPL"], # The stock symbols we are using
    }

    def initialize(self):
        # Setting the sleep time (in days) - this is the time between each trading iteration
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        # Get the parameters
        symbols = self.parameters.get("symbols")

        # Check if it's the first iteration
        if self.first_iteration:
            # Get the cash available
            cash = self.get_cash()

            # Calculate the cash per asset (only use 98% of cash)
            cash_per_asset = (cash * 0.98) / len(symbols)

            # Loop through the symbols
            for symbol in symbols:
                # Get the price of the asset
                price = self.get_last_price(symbol)

                # Calculate the number of shares to buy
                shares_to_buy = int(cash_per_asset / price)

                # Create the order
                order = self.create_order(symbol, shares_to_buy, Order.OrderSide.BUY)

                # Submit the order
                self.submit_order(order)


if __name__ == "__main__":
    ####
    # Backtesting
    ####

    from lumibot.backtesting import YahooDataBacktesting

    # 0.1% fee
    trading_fee = TradingFee(percent_fee=0.001)

    # Create the configs helper
    configs_helper = ConfigsHelper()

    # Load the backtesting config
    params = configs_helper.load_config(BACKTESTING_CONFIG)

    # Set the parameters for the strategy
    StockExampleAlgo.parameters = params

    # Backtesting
    result = StockExampleAlgo.backtest(
        YahooDataBacktesting,
        BACKTESTING_START,
        BACKTESTING_END,
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
    )