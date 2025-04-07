"""
Indicators Package

This package provides technical indicators for use in trading strategies.
"""

import os
import importlib.util
from pathlib import Path

def load_indicators(indicator_name):
    """
    Load an indicator's calculate_indicator function directly by name.
    
    Args:
        indicator_name: Name of the indicator file (can include .py extension)
        
    Returns:
        calculate_indicator function from the indicator module
    """
    # Handle both with and without .py extension
    if indicator_name.endswith('.py'):
        indicator_name = indicator_name[:-3]
        
    # First try in the main indicators directory
    indicator_file = Path(__file__).parent / f"{indicator_name}.py"
    
    # If not found, try in the entry subdirectory
    if not indicator_file.exists():
        indicator_file = Path(__file__).parent / "entry" / f"{indicator_name}.py"
    
    if not indicator_file.exists():
        raise ValueError(f"Indicator {indicator_name} not found in indicators/ or indicators/entry/")
    
    # Import the indicator module
    spec = importlib.util.spec_from_file_location(indicator_name, indicator_file)
    indicator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(indicator)
    
    if not hasattr(indicator, 'calculate_indicator'):
        raise ValueError(f"Indicator {indicator_name} must have a calculate_indicator function")
    
    # Return the calculate_indicator function directly
    return indicator.calculate_indicator

# Export only the load_indicators function
__all__ = ['load_indicators'] 