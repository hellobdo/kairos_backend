from typing import Optional
from ..reports_and_metrics.trade_metrics import (
    calculate_risk_size,
    calculate_capital_required,
    calculate_risk_percentage,
    calculate_trade_duration,
    calculate_risk_reward_ratio,
    determine_winning_trade,
    calculate_trade_return
)

class Trade:
    def __init__(self, 
                 symbol: str,
                 direction: str,
                 entry_date: str,
                 entry_timestamp: str,
                 entry_price: float,
                 stop_price: float,
                 quantity: int,
                 account_size: float,
                 variant: str):
        self.symbol = symbol
        self.direction = direction
        self.entry_date = entry_date
        self.entry_timestamp = entry_timestamp
        self.entry_price = entry_price
        self.stop_price = stop_price
        self.quantity = quantity
        self.variant = variant
        self.account_size = account_size
        
        # Calculate risk metrics using trade_metrics functions
        self.risk_size = calculate_risk_size(entry_price, stop_price, quantity)
        self.capital_required = calculate_capital_required(entry_price, quantity)
        self.risk_per_trade = calculate_risk_percentage(entry_price, stop_price, quantity, account_size)
        
        # Fields to be populated at exit
        self.exit_price: Optional[float] = None
        self.exit_date: Optional[str] = None
        self.exit_timestamp: Optional[str] = None
        self.trade_duration: Optional[float] = None
        self.winning_trade: Optional[int] = None
        self.risk_reward: Optional[float] = None
        self.perc_return: Optional[float] = None
        self.exit_reason: Optional[str] = None
    
    def set_exit(self, exit_price: float, exit_date: str, exit_timestamp: str, exit_reason: str = ""):
        """Set exit information and calculate trade metrics"""
        self.exit_price = exit_price
        self.exit_date = exit_date
        self.exit_timestamp = exit_timestamp
        self.exit_reason = exit_reason
        
        # Calculate all trade metrics at once
        self.trade_duration = calculate_trade_duration(self.entry_timestamp, exit_timestamp)
        self.risk_reward = calculate_risk_reward_ratio(self.entry_price, exit_price, self.stop_price, self.direction)
        self.winning_trade = determine_winning_trade(self.entry_price, exit_price, self.stop_price, self.direction)
        self.perc_return = calculate_trade_return(self.risk_per_trade, self.risk_reward) 