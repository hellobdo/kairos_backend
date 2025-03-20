#!/usr/bin/env python3
"""
Price Helper

This module provides helper functions for price-based calculations,
specifically for determining stop loss amounts based on price ranges.
"""

def get_stop_loss_by_price(price: float) -> float:
    """
    Returns an appropriate stop loss amount based on the price range.
    
    Args:
        price: Current price to evaluate
        
    Returns:
        float: Stop loss amount in dollars
    """
    if price < 150:
        return 0.30
    else:
        return 1.00 