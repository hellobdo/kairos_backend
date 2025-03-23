#!/usr/bin/env python3
"""
Utility to find the most recent trade report file.
in a specified directory.
"""
from pathlib import Path
import os
from datetime import datetime

def get_latest_trade_report(type: str) -> Path:
    """
    Find the most recent trade trade report file in the logs directory.
        
    Returns:
        Path to the latest trade report file
        
    Raises:
        ValueError: If the type is not 'html' or 'csv'
        FileNotFoundError: If no trade report files are found or if logs directory doesn't exist
    """

    # Check if the type is valid
    if type not in ["html", "csv"]:
        raise ValueError("Invalid report type. Must be 'html' or 'csv'.")

    # Get the logs directory
    logs_dir = Path("logs")
    
    # Check if logs directory exists
    if not logs_dir.exists():
        raise FileNotFoundError(f"Logs directory not found: {logs_dir}")

    # Get the report files
    if type == "html":
        file = list(logs_dir.glob("*trade_report*.html"))
    elif type == "csv":
        file = list(logs_dir.glob("*trades*.csv"))
    
    if not file:
        raise FileNotFoundError(f"No {type} trade report files found in {logs_dir}")
    
    # Get the latest file based on creation time
    latest_file = max(file, key=os.path.getctime)
    
    print(f"Latest {type} trade report found: {latest_file}")
    print(f"Created at: {datetime.fromtimestamp(os.path.getctime(latest_file))}")
    
    return latest_file 