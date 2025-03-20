"""
Helper modules for backtests.
"""

from .process_trades import process_trades_from_strategy
from .stop_loss_module import get_stop_loss_by_price

__all__ = ['process_trades_from_strategy', 'get_stop_loss_by_price'] 