# Kairos Trading System

A modular, extensible backtesting and trading system built designed for developing and testing systematic trading strategies.

## Configuration Getters

The system uses several getter functions to retrieve configuration from the database:

### get_entry_config

**Purpose:** Retrieves entry signal configuration from the database.

**Parameters:**
- `config_id`: ID of the entry configuration to retrieve
- `db_path`: Path to the database file (optional)

**Returns:** Dictionary containing:
- `id`: Entry configuration ID
- `field`: Field to use for entry signal (e.g., 'close', 'high', 'low')
- `signal`: Signal type (e.g., 'crossover', 'crossunder')
- `direction`: Trade direction ('long' or 'short')
- `type`: Always 'entry'

**Used by:** Backtest system to determine entry points for trades.

### get_exits_config

**Purpose:** Retrieves exit strategy configuration from the database.

**Parameters:**
- `config_id`: ID of the exit configuration to retrieve

**Returns:**
- For fixed exits: Dictionary with `id`, `type`, `name`, `description`, `size_exit`, and `risk_reward`
- For variable exits: Dictionary with `id`, `type`, `name`, `description`, and `ranges` (list of dictionaries with `size_exit` and `risk_reward`)

**Used by:** Backtest system to determine exit points and position sizing.

### get_risk_config

**Purpose:** Retrieves risk management configuration from the database.

**Parameters:**
- `risk_id`: ID of the risk configuration to retrieve

**Returns:** Dictionary containing:
- `id`: Risk configuration ID
- `risk_per_trade`: Percentage of account to risk per trade (as decimal)
- `max_daily_risk`: Maximum percentage of account to risk per day (as decimal)
- `outside_regular_hours_allowed`: Boolean flag (1 or 0) indicating whether trading outside regular hours is allowed

**Used by:** Risk management module to enforce position sizing and risk limits.

### get_stoploss_config

**Purpose:** Retrieves stop loss configuration from the database.

**Parameters:**
- `stoploss_id`: ID of the stop loss configuration to retrieve

**Returns:**
- For fixed absolute stops: Dictionary with `stop_type` ('abs'), `stop_value`, `name`, and `description`
- For fixed percentage stops: Dictionary with `stop_type` ('perc'), `stop_value`, `name`, and `description`
- For variable stops: Dictionary with `stop_type` ('custom'), `stop_func` (function that calculates stop based on price), `name`, and `description`

**Used by:** Backtest system to determine stop loss levels for risk management.

### get_swing_config

**Purpose:** Retrieves swing trading configuration from the database.

**Parameters:**
- `swing_config_id`: ID of the swing configuration to retrieve

**Returns:** Dictionary containing:
- `id`: Swing configuration ID
- `swings_allowed`: Boolean flag (1 or 0) indicating whether swing trading is allowed
- `description`: Text description of the configuration

**Used by:** Backtest system to determine if swing trading is permitted for a strategy.

## Backtest Loader Module

The backtest loader module (`backtest_loader.py`) provides functions for loading and processing backtest configurations and data:

### load_config

**Purpose:** Loads configuration from a JSON file.

**Parameters:**
- `config_file`: Path to the JSON configuration file

**Returns:** Dictionary containing the parsed JSON configuration or None if loading fails

**Used by:** Backtest system to load configuration settings.

### load_backtest_config

**Purpose:** Parses the backtest section from a configuration file.

**Parameters:**
- `config_file`: Path to the JSON configuration file

**Returns:** Tuple containing:
- `symbol`: Trading symbol
- `entry_config_id`: ID for entry configuration
- `stoploss_config_id`: ID for stop loss configuration
- `risk_config_id`: ID for risk configuration
- `exit_config_id`: ID for exit configuration
- `swing_config_id`: ID for swing configuration
- `exits_swings_config_id`: ID for exits swings configuration
- `date_range`: Dictionary with start and end dates
- `backtest_config`: The entire backtest configuration section

**Used by:** Backtest runner to extract parameters from the configuration file.

### generate_entry_signals

**Purpose:** Creates entry signals based on price data and entry configuration.

**Parameters:**
- `df`: DataFrame containing historical price data
- `entry_config`: Entry configuration dictionary

**Returns:** Tuple containing:
- `entries`: Series of boolean values indicating entry points
- `direction`: Trading direction ("long" or "short")

**Used by:** Backtest system to determine when to enter trades.

### create_backtest_run

**Purpose:** Creates a record of the backtest run in the database.

**Parameters:**
- `entry_config_id`: ID for entry configuration
- `stoploss_config_id`: ID for stop loss configuration
- `risk_config_id`: ID for risk configuration
- `exit_config_id`: ID for exit configuration
- `swing_config_id`: ID for swing configuration
- `exits_swings_config_id`: ID for exits swings configuration

**Returns:** ID of the created backtest run

**Used by:** Backtest system to track and reference backtest runs.

### load_data_from_db

**Purpose:** Loads historical price data from the database and optionally generates entry signals.

**Parameters:**
- `symbol`: Trading symbol
- `risk_config`: Risk configuration dictionary
- `entry_config`: Entry configuration dictionary (optional)
- `date_range`: Dictionary with start and end dates (optional)

**Returns:** Tuple containing:
- `df`: DataFrame with price data
- `entries`: Series of boolean entry points (None if entry_config not provided)
- `direction`: Trading direction (None if entry_config not provided)

**Used by:** Backtest system to load historical data for analysis.

### get_configs

**Purpose:** Retrieves all configuration objects needed for a backtest.

**Parameters:**
- `entry_config_id`: ID for entry configuration
- `stoploss_config_id`: ID for stop loss configuration
- `risk_config_id`: ID for risk configuration
- `exit_config_id`: ID for exit configuration
- `swing_config_id`: ID for swing configuration
- `exits_swings_config_id`: ID for exits swings configuration (optional)

**Returns:** Tuple containing configuration dictionaries:
- `entry_config`: Entry signal configuration
- `stop_config`: Stop loss configuration
- `risk_config`: Risk management configuration
- `exit_config`: Exit strategy configuration
- `swing_config`: Swing trading configuration
- `exits_swings_config`: Exit configuration for swing trades

**Used by:** Backtest system to prepare all configuration objects.

### setup_backtest

**Purpose:** Orchestrates the entire backtest setup process.

**Parameters:**
- `symbol`: Trading symbol
- `entry_config_id`: ID for entry configuration
- `stoploss_config_id`: ID for stop loss configuration
- `risk_config_id`: ID for risk configuration
- `exit_config_id`: ID for exit configuration
- `swing_config_id`: ID for swing configuration
- `exits_swings_config_id`: ID for exits swings configuration (optional)
- `date_range`: Dictionary with start and end dates (optional)

**Returns:** Dictionary containing:
- `run_id`: ID of the created backtest run
- `df`: DataFrame with price data
- `entries`: Series of boolean entry points
- `entry_config`: Entry configuration dictionary
- `direction`: Trading direction
- `stop_config`: Stop loss configuration dictionary
- `risk_config`: Risk management configuration dictionary
- `exit_config`: Exit strategy configuration dictionary
- `swing_config`: Swing trading configuration dictionary
- `exits_swings_config`: Exit configuration for swing trades
- `symbol`: Trading symbol

**Used by:** Backtest runner to initialize and configure a backtest run.

## Backtest Runner Module

The backtest runner module (`backtest_runner.py`) simulates trading strategies using historical data and predefined signals:

### calculate_risk_reward

**Purpose:** Calculates risk/reward ratio differently for long vs short trades.

**Parameters:**
- `entry_price`: Entry price of the trade
- `exit_price`: Exit price of the trade
- `stop_price`: Stop loss price of the trade
- `is_long`: Boolean indicating if this is a long trade

**Returns:** A floating-point risk/reward ratio

**Used by:** Trade processing logic to calculate trade performance metrics.

### format_trades

**Purpose:** Formats raw trade records into a structured DataFrame with performance metrics.

**Parameters:**
- `trade_list`: List of dictionaries containing trade data
- `df`: DataFrame with price data
- `stop_config`, `risk_config`, `exit_config`, `swing_config`: Configuration dictionaries

**Returns:** DataFrame with formatted trade details and metrics

**Used by:** Analysis functions to prepare trade data for logging and reporting.

### log_trades_to_db

**Purpose:** Stores trade records in the database.

**Parameters:**
- `trades_df`: DataFrame containing trade records
- `run_id`: ID of the backtest run
- `symbol`: Trading symbol

**Returns:** None (stores records in the database)

**Used by:** Backtest runner to persist trade history for future analysis.

### run_backtest

**Purpose:** Executes the entire backtesting process for a strategy.

**Parameters:**
- `config_file`: Path to the JSON configuration file

**Returns:** 
- `True` if the backtest completed successfully
- `False` if an error occurred

**Process:**
1. Loads configuration and setup data
2. Calculates stop prices for each entry point
3. Processes price data sequentially, checking for entries and exits
4. Applies risk management rules for position sizing
5. Handles stop loss, take profit, and time-based exits
6. Logs trades to the database and prints performance metrics

**Used by:** Command line interface or other scripts to trigger backtests.

### main

**Purpose:** Provides a simple entry point to run backtests with default configuration.

**Parameters:** None

**Returns:** 
- Exit code `0` if successful
- Exit code `1` if failed

**Used by:** Command line execution to run the backtest with default settings.


