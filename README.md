# Kairos Trading System

A modular, extensible backtesting and trading system built on VectorBT designed for developing and testing systematic trading strategies.


# Backtesting Framework

This directory contains a modular backtesting framework built on top of VectorBT for trading strategy development and evaluation.

## Structure

The backtesting framework is organized into the following components:

- **backtest_runner.py**: Main entry point for running backtests with different strategies and configurations
- **backtest_entry.py**: Handles database connections, configuration loading, and backtest run creation
- **trade_logger.py**: Formats and logs trade data to the database
- **longs_backtest.py**: Example strategy implementation for long-only trades based on tightness

## How to Use

### Running a Backtest

You can run a backtest using the command-line interface:

```bash
python -m backtests.backtest_runner longs_backtest --symbol QQQ --stoploss 3 --risk 1 --exit 1 --swing 1
```

Or you can import and use the modules directly in your code:

```python
from backtests.backtest_runner import run_backtest

run_backtest('longs_backtest', symbol='QQQ', stoploss_id=3, risk_id=1, exit_id=1, swing_id=1)
```

### Creating a New Strategy

To create a new strategy:

1. Create a new Python file in the `backtests` directory (e.g., `my_strategy.py`)
2. Import the necessary modules:
   ```python
   import vectorbt as vbt
   import pandas as pd
   import logging
   import numpy as np
   from datetime import datetime
   
   from backtests.backtest_entry import create_backtest_run, load_data_from_db, get_stoploss_config, get_risk_config, get_exits_config, get_swing_config
   from backtests.trade_logger import log_trades, format_trades, print_performance_metrics
   ```
3. Define your strategy configuration:
   ```python
   # Strategy configuration
   SYMBOL = 'SPY'
   STOPLOSS_CONFIG_ID = 1
   RISK_CONFIG_ID = 1
   EXIT_CONFIG_ID = 1
   SWING_CONFIG_ID = 1
   EXITS_SWINGS_CONFIG_ID = None
   ```
4. Implement a `run_strategy()` function that:
   - Loads configurations
   - Creates a backtest run
   - Loads data
   - Generates entry and exit signals
   - Runs the backtest
   - Formats and logs trades

## Configuration System

The framework uses a database-driven configuration system:

- **Stop Loss Configurations**: Define how stop losses are calculated (percentage, absolute, or custom)
- **Risk Configurations**: Define risk parameters like risk per trade and whether trading outside regular hours is allowed
- **Exit Configurations**: Define exit strategies like fixed risk/reward ratios
- **Swing Configurations**: Define whether trades can be held overnight

## End-of-Day Exit Logic

For strategies that don't allow overnight positions (swings), the framework includes logic to close positions at the end of the trading day:

1. Identifies the last candle of each trading day
2. Creates exit signals for those candles
3. Configures the portfolio to prioritize exits over entries
4. Disables entries on the last candle of the day

## Trade Logging

The framework logs detailed trade information to the database, including:

- Entry and exit timestamps and prices
- Position size and risk
- Risk/reward ratio and percentage return
- Exit type (Stop Loss, Take Profit, End of Day)
- Trade duration and capital required

## Performance Metrics

The framework calculates and displays various performance metrics:

- Total return and number of trades
- Win rate overall and by exit type
- Average PnL and risk/reward ratio by exit type 