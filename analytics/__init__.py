"""
Analytics package for trade processing and reporting.
"""

# Import main functions to expose at package level
from .trade_results import run_report, generate_comparison_data
from ..api.yf import download_data

# Define what gets imported with "from analytics import *"
__all__ = [
    'run_report',
    'generate_comparison_data',
    'download_data'
]
