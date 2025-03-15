import pandas as pd
import sqlite3
import logging
from datetime import datetime
import numpy as np
from .helpers.get_stoploss_config import get_stoploss_config
from .helpers.get_risk_config import get_risk_config
from .helpers.get_exits_config import get_exits_config
from .helpers.get_swing_config import get_swing_config
from .helpers.get_entry_config import get_entry_config
import json
import os
from typing import Dict, Any, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def generate_entry_signals(df: pd.DataFrame, entry_config: Dict[str, Any]) -> Tuple[pd.Series, str]:
    """
    Generate entry signals based on the entry configuration.
    
    Args:
        df: DataFrame containing historical data
        entry_config: Entry configuration dictionary with field, signal, and direction
        
    Returns:
        Tuple containing:
            - Series of boolean values indicating entry points
            - Direction string ("long" or "short")
    """
    field = entry_config['field']
    signal_value = entry_config['signal']
    direction = entry_config.get('direction', 'long')  # Default to 'long' if not specified
    
    logger.info(f"Generating {direction} entry signals for {field}={signal_value}")
    
    # Check if the field exists in the dataframe
    if field not in df.columns:
        logger.error(f"Field '{field}' not found in dataframe. Available columns: {df.columns.tolist()}")
        return pd.Series(False, index=df.index), direction
    
    # Create entry signals where the field value matches the signal value
    try:
        # Generate entries based on field and signal from entry_config
        # This allows different types of signals to be used in the future
        
        # For now, we use direct equality comparison as the default approach
        # But this structure allows adding more complex signal types in the future
        entries = pd.Series(False, index=df.index)
        
        # Check if the field contains the signal value
        field_values = df[field]
        
        # Simple direct comparison - this is the basic implementation
        # More complex signal types can be added here in the future
        entries = field_values == signal_value
        
        entry_count = entries.sum()
        logger.info(f"Generated {entry_count} {direction} entry signals based on {field}={signal_value}")
        
        # Add additional logging for debugging
        if entry_count == 0:
            logger.warning(f"No entries found. Unique values in '{field}' column: {field_values.unique()}")
        
        return entries, direction
    
    except Exception as e:
        logger.error(f"Error generating entry signals: {e}")
        return pd.Series(False, index=df.index), direction

def create_backtest_run(entry_config_id, stoploss_config_id, risk_config_id, exit_config_id, swing_config_id, exits_swings_config_id=None):
    """
    Create a new backtest run record and return its ID.
    
    Args:
        entry_config_id (int): ID for entry configuration
        stoploss_config_id (int): ID for stop loss configuration
        risk_config_id (int): ID for risk configuration
        exit_config_id (int): ID for exit configuration
        swing_config_id (int): ID for swing configuration
        exits_swings_config_id (int, optional): ID for exits swings configuration
        
    Returns:
        int: ID of the created backtest run
    """
    conn = sqlite3.connect('data/algos.db')
    cursor = conn.cursor()
    
    # Prepare the SQL query with all fields
    query = """
        INSERT INTO backtest_runs (
            execution_date,
            entry_config_id,
            stoploss_config_id,
            risk_config_id,
            exit_config_id,
            swing_config_id,
            exits_swings_config_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    # Prepare parameters for the query
    params = [
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        entry_config_id,
        stoploss_config_id,
        risk_config_id,
        exit_config_id,
        swing_config_id,
        exits_swings_config_id  # This can be None, which will be stored as NULL in SQLite
    ]
    
    cursor.execute(query, params)
    
    run_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    logger.info(f"Created backtest run record with ID {run_id}")
    return run_id

def load_data_from_db(symbol, risk_config, date_range=None):
    """
    Load historical data from SQLite database.
    
    Args:
        symbol (str): The trading symbol to load data for
        risk_config (dict): Risk configuration containing outside_regular_hours_allowed setting
        date_range (dict, optional): Dictionary with 'start' and 'end' date strings
        
    Returns:
        pd.DataFrame: Price data with datetime index
    """
    conn = sqlite3.connect('data/algos.db')
    
    # Check if trading outside regular hours is allowed
    outside_regular_hours_allowed = risk_config.get('outside_regular_hours_allowed', 0)
    
    # Base query
    query = """
    SELECT 
        date_and_time,
        open,
        high,
        low,
        close,
        volume,
        tightness,
        market_session
    FROM historical_data_30mins
    WHERE symbol = ? 
    """
    
    # Add filter for regular market hours if outside hours not allowed
    if not outside_regular_hours_allowed:
        query += " AND market_session = 'regular'"
    
    # Add date range filter if provided
    params = [symbol]
    if date_range and date_range.get('start') and date_range.get('end'):
        query += " AND date_and_time BETWEEN ? AND ?"
        params.append(date_range['start'])
        params.append(date_range['end'])
        logger.info(f"Filtering data between {date_range['start']} and {date_range['end']}")
    
    query += " ORDER BY date_and_time"
    
    logger.info(f"Loading data with outside_regular_hours_allowed = {outside_regular_hours_allowed}")
    
    # Load data into DataFrame
    df = pd.read_sql_query(query, conn, params=params)
    
    # Convert date_and_time to datetime index
    df['date_and_time'] = pd.to_datetime(df['date_and_time'])
    df.set_index('date_and_time', inplace=True)
    
    # Calculate additional fields that might be needed for entry conditions
    df['diff_pct_open_close'] = abs((df['close'] - df['open']) / df['open'])
    
    # Additional log to show available fields for entry conditions
    logger.info(f"Available fields for entry conditions: {df.columns.tolist()}")
    
    conn.close()
    
    logger.info(f"Loaded {len(df)} data points for {symbol}")
    
    return df

def get_configs(entry_config_id, stoploss_config_id, risk_config_id, exit_config_id, swing_config_id, exits_swings_config_id=None):
    """
    Get all configurations needed for a backtest.
    
    Args:
        entry_config_id (int): ID for entry configuration
        stoploss_config_id (int): ID for stop loss configuration
        risk_config_id (int): ID for risk configuration
        exit_config_id (int): ID for exit configuration
        swing_config_id (int): ID for swing configuration
        exits_swings_config_id (int, optional): ID for exits swings configuration
        
    Returns:
        tuple: (entry_config, stop_config, risk_config, exit_config, swing_config, exits_swings_config)
    """
    # Get entry configuration
    logger.info(f"Getting entry configuration for ID {entry_config_id}...")
    entry_config = get_entry_config(entry_config_id)
    if not entry_config:
        logger.error("Failed to get entry configuration")
        return None, None, None, None, None, None
    
    # Get stoploss configuration
    logger.info(f"Getting stoploss configuration for ID {stoploss_config_id}...")
    stop_config = get_stoploss_config(stoploss_config_id)
    if not stop_config:
        logger.error("Failed to get stoploss configuration")
        return None, None, None, None, None, None
        
    # Get risk configuration
    logger.info(f"Getting risk configuration for ID {risk_config_id}...")
    risk_config = get_risk_config(risk_config_id)
    if not risk_config:
        logger.error("Failed to get risk configuration")
        return None, None, None, None, None, None

    # Get exit configuration
    logger.info(f"Getting exit configuration for ID {exit_config_id}...")
    exit_config = get_exits_config(exit_config_id)
    if not exit_config:
        logger.error("Failed to get exit configuration")
        return None, None, None, None, None, None
    
    # Get swing configuration
    logger.info(f"Getting swing configuration for ID {swing_config_id}...")
    swing_config = get_swing_config(swing_config_id)
    if not swing_config:
        logger.error("Failed to get swing configuration")
        return None, None, None, None, None, None
    
    # Only get exits_swings_config if swing_config allows swings (swings_allowed=1)
    exits_swings_config = None
    if swing_config['swings_allowed'] == 1 and exits_swings_config_id is not None:
        logger.info(f"Getting exits swings configuration for ID {exits_swings_config_id}...")
        exits_swings_config = get_exits_config(exits_swings_config_id)
        if not exits_swings_config:
            logger.error("Failed to get exits swings configuration")
            return None, None, None, None, None, None
    
    return entry_config, stop_config, risk_config, exit_config, swing_config, exits_swings_config

def setup_backtest(symbol, entry_config_id, stoploss_config_id, risk_config_id, exit_config_id, swing_config_id, exits_swings_config_id=None, date_range=None):
    """
    Set up a backtest by loading data and configurations and creating a run record.
    
    Args:
        symbol (str): Trading symbol
        entry_config_id (int): ID for entry configuration
        stoploss_config_id (int): ID for stop loss configuration
        risk_config_id (int): ID for risk configuration
        exit_config_id (int): ID for exit configuration
        swing_config_id (int): ID for swing configuration
        exits_swings_config_id (int, optional): ID for exits swings configuration
        date_range (dict, optional): Dictionary with 'start' and 'end' date strings
        
    Returns:
        dict: All data and configurations needed for a backtest
    """
    # Get configurations
    entry_config, stop_config, risk_config, exit_config, swing_config, exits_swings_config = get_configs(
        entry_config_id, stoploss_config_id, risk_config_id, exit_config_id, swing_config_id, exits_swings_config_id
    )
    
    if not all([entry_config, stop_config, risk_config, exit_config, swing_config]):
        logger.error("Failed to get all required configurations")
        return None
    
    # Check if exit_config type is supported
    if exit_config['type'] != 'fixed':
        logger.error("Currently only supporting fixed exits")
        return None
    
    # Create backtest run record
    logger.info("Creating backtest run record...")
    run_id = create_backtest_run(
        entry_config_id, stoploss_config_id, risk_config_id, exit_config_id, swing_config_id, exits_swings_config_id
    )
    
    if run_id is None:
        logger.error("Failed to create backtest run record")
        return None
    
    # Load data
    logger.info("Loading historical data from database...")
    df = load_data_from_db(symbol, risk_config, date_range)
    
    if df.empty:
        logger.error("No data found for the given symbol and date range")
        return None
    
    # Generate entry signals based on entry configuration
    logger.info(f"Generating entry signals using field '{entry_config['field']}' with signal '{entry_config['signal']}' (direction: {entry_config['direction']})...")
    entries, direction = generate_entry_signals(df, entry_config)
    
    # Log how many entries were found
    entry_count = entries.sum()
    logger.info(f"Found {entry_count} entry points out of {len(df)} data points")
    
    if entry_count == 0:
        logger.warning("No entry points found with the current configuration. Backtest will not produce trades.")
    
    # Return all data needed for the backtest
    return {
        'run_id': run_id,
        'df': df,
        'entries': entries,
        'entry_config': entry_config,
        'direction': direction,  # Add direction to the returned data
        'stop_config': stop_config,
        'risk_config': risk_config,
        'exit_config': exit_config,
        'swing_config': swing_config,
        'exits_swings_config': exits_swings_config,
        'symbol': symbol
    } 