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


