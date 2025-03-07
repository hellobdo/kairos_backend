"""
Trade metrics module for trading strategies.
This module contains functions for calculating position sizes, risk amounts, trade durations, and other trade metrics.
"""
from typing import Dict, Any, Tuple, Optional, Union, NamedTuple
from datetime import datetime

# Constants
DEFAULT_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
DIRECTION_BULLISH = 'Bullish'
DIRECTION_BEARISH = 'Bearish'
MIN_PRICE = 0.01  # Minimum price to prevent negative or zero prices


class TradeMetrics(NamedTuple):
    """Container for trade metrics results"""
    risk_reward: float
    winning_trade: int
    risk_size: Optional[float] = None
    risk_percentage: Optional[float] = None
    capital_required: Optional[float] = None
    trade_duration: Optional[float] = None
    trade_return: Optional[float] = None


# Position sizing and capital functions
def calculate_capital_required(entry_price: float, quantity: int) -> float:
    """
    Calculate the capital required to enter a trade.
    
    Args:
        entry_price: The entry price of the trade
        quantity: Number of shares/contracts
        
    Returns:
        The capital required in dollars
    """
    return entry_price * quantity


def calculate_position_size(entry_price: float, stop_price: float, max_risk_amount: float) -> int:
    """
    Calculate position size based on risk size and price difference.
    
    Args:
        entry_price: The entry price of the trade
        stop_price: The stop loss price 
        max_risk_amount: The maximum dollar amount to risk
        
    Returns:
        The number of shares to buy/sell
    """
    price_difference = abs(entry_price - stop_price)
    if price_difference == 0:
        return 1  # Avoid division by zero
    
    quantity = int(max_risk_amount / price_difference)
    return max(1, quantity)  # Ensure at least 1 share


def calculate_stop_price(entry_price: float, direction: str, delta_amount: float) -> float:
    """
    Calculate stop loss price based on a delta amount from entry price.
    
    Args:
        entry_price: The entry price of the trade
        direction: 'Bullish' or 'Bearish'
        delta_amount: The fixed dollar amount delta from entry price
        
    Returns:
        The stop loss price
    """
    if direction == DIRECTION_BULLISH:
        return max(MIN_PRICE, entry_price - delta_amount)  # Prevent negative/zero prices
    else:
        return entry_price + delta_amount


# Risk calculation functions
def calculate_risk_size(entry_price: float, stop_price: float, quantity: int) -> float:
    """
    Calculate the dollar amount at risk for the trade.
    
    Args:
        entry_price: The entry price of the trade
        stop_price: The stop loss price
        quantity: Number of shares/contracts
        
    Returns:
        The dollar amount at risk
    """
    return abs(entry_price - stop_price) * quantity


def calculate_risk_percentage(entry_price: float, stop_price: float, quantity: int, account_size: float) -> float:
    """
    Calculate the percentage of account risked on a trade.
    
    Args:
        entry_price: The entry price of the trade
        stop_price: The stop loss price
        quantity: Number of shares/contracts
        account_size: The total account size in dollars
        
    Returns:
        The percentage of account at risk (0.5 means 0.5%)
    """
    if account_size <= 0:
        return 0.0  # Avoid division by zero
        
    risk_size = calculate_risk_size(entry_price, stop_price, quantity)
    return (risk_size / account_size) * 100


# Trade performance metrics functions
def calculate_risk_reward_ratio(entry_price: float, exit_price: float, stop_price: float, direction: str) -> float:
    """
    Calculate the achieved risk/reward ratio.
    
    Args:
        entry_price: The entry price of the trade
        exit_price: The exit price of the trade
        stop_price: The stop loss price
        direction: 'Bullish' or 'Bearish'
        
    Returns:
        The achieved risk/reward ratio
    """
    # Avoid division by zero
    price_difference = abs(entry_price - stop_price)
    if price_difference == 0:
        return 0.0
        
    if direction == DIRECTION_BULLISH:
        return (exit_price - entry_price) / price_difference
    else:
        return (entry_price - exit_price) / price_difference


def determine_winning_trade(entry_price: float, exit_price: float, stop_price: float, direction: str) -> int:
    """
    Determine if a trade is winning or losing based on price movement and direction.
    This is a convenience function that calls calculate_risk_reward_ratio.
    
    Args:
        entry_price: The entry price of the trade
        exit_price: The exit price of the trade
        stop_price: The stop loss price
        direction: 'Bullish' or 'Bearish'
        
    Returns:
        1 if winning trade, 0 if losing trade
    """
    risk_reward = calculate_risk_reward_ratio(entry_price, exit_price, stop_price, direction)
    return 1 if risk_reward > 0 else 0


def calculate_trade_return(risk_per_trade: float, risk_reward: float) -> float:
    """
    Calculate the percentage return for a trade.
    
    Args:
        risk_per_trade: The percentage risked on the trade
        risk_reward: The achieved risk/reward ratio
        
    Returns:
        The percentage return
    """
    return risk_per_trade * risk_reward


def calculate_trade_duration(entry_timestamp: str, exit_timestamp: str, timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT) -> float:
    """
    Calculate the duration of a trade in hours.
    
    Args:
        entry_timestamp: The timestamp when the trade was entered
        exit_timestamp: The timestamp when the trade was exited
        timestamp_format: The format of the timestamp strings
        
    Returns:
        The duration of the trade in hours
    """
    entry_dt = datetime.strptime(entry_timestamp, timestamp_format)
    exit_dt = datetime.strptime(exit_timestamp, timestamp_format)
    duration_seconds = (exit_dt - entry_dt).total_seconds()
    return duration_seconds / 3600  # Convert seconds to hours


# Composite functions for calculating multiple metrics at once
def calculate_trade_metrics(entry_price: float, exit_price: float, stop_price: float, direction: str) -> Tuple[float, int]:
    """
    Calculate risk/reward ratio and determine if the trade is winning in one operation.
    
    Args:
        entry_price: The entry price of the trade
        exit_price: The exit price of the trade
        stop_price: The stop loss price
        direction: 'Bullish' or 'Bearish'
        
    Returns:
        Tuple of (risk_reward_ratio, winning_trade_flag)
    """
    risk_reward = calculate_risk_reward_ratio(entry_price, exit_price, stop_price, direction)
    winning_trade = 1 if risk_reward > 0 else 0
    return risk_reward, winning_trade


def calculate_all_trade_metrics(
    entry_price: float, 
    exit_price: float, 
    stop_price: float, 
    direction: str,
    quantity: int,
    account_size: float,
    entry_timestamp: Optional[str] = None,
    exit_timestamp: Optional[str] = None,
    timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT
) -> TradeMetrics:
    """
    Calculate all trade metrics in a single function call.
    
    Args:
        entry_price: The entry price of the trade
        exit_price: The exit price of the trade
        stop_price: The stop loss price
        direction: 'Bullish' or 'Bearish' 
        quantity: Number of shares/contracts
        account_size: The total account size in dollars
        entry_timestamp: Optional timestamp when the trade was entered
        exit_timestamp: Optional timestamp when the trade was exited
        timestamp_format: The format of the timestamp strings
        
    Returns:
        TradeMetrics object with calculated metrics
    """
    # Calculate base metrics
    risk_reward = calculate_risk_reward_ratio(entry_price, exit_price, stop_price, direction)
    winning_trade = 1 if risk_reward > 0 else 0
    
    # Calculate additional metrics
    risk_size = calculate_risk_size(entry_price, stop_price, quantity)
    risk_percentage = calculate_risk_percentage(entry_price, stop_price, quantity, account_size)
    capital_required = calculate_capital_required(entry_price, quantity)
    
    # Calculate trade return
    trade_return = calculate_trade_return(risk_percentage, risk_reward)
    
    # Calculate duration if timestamps are provided
    trade_duration = None
    if entry_timestamp and exit_timestamp:
        trade_duration = calculate_trade_duration(entry_timestamp, exit_timestamp, timestamp_format)
    
    return TradeMetrics(
        risk_reward=risk_reward,
        winning_trade=winning_trade,
        risk_size=risk_size,
        risk_percentage=risk_percentage,
        capital_required=capital_required,
        trade_duration=trade_duration,
        trade_return=trade_return
    ) 