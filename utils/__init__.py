"""
Utility functions for backtest processing and analysis.
This package contains utility functions used across the backtesting workflow.
"""

from utils.get_latest_trade_report import get_latest_trade_report
from utils.db_utils import DatabaseManager
from utils.html_generator import generate_html_report

__all__ = ['get_latest_trade_report', 'DatabaseManager', 'generate_html_report']