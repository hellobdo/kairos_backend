# Kairos Trading System

A modular, extensible backtesting and trading system built on VectorBT designed for developing and testing systematic trading strategies.

### File Structure
```
kairos/
├── backtest_config/               # Configuration files
│   ├── strategies/               # Individual strategy configs
│   │   └── tight_candle_qqq.json
│   └── portfolios/               # Portfolio allocation configs
│       └── single_qqq.json
│
├── infrastructure/               # Core system infrastructure
│   ├── core/
│   │   └── vectorbt_strategy.py  # Base strategy class
│   ├── indicators/
│   │   ├── tight_candle.py      # Signal generators
│   │   └── visualize_tight_candles.py
│   ├── risk_management/
│   │   └── risk_manager.py      # Risk control system
│   ├── strategy_management/
│   │   ├── strategy_creation.py  # Database operations
│   │   ├── strategy_loader.py    # Config file handling
│   │   └── strategy_manager.py   # Strategy orchestration
│   └── trade_metrics/
│       └── trade_metrics.py      # Trade calculations
│
├── strategies/                   # Strategy implementations
│   └── tight_candle_strategy.py
│
├── kairos.db                    # SQLite database
└── main.py                      # Entry point
```

### Component Relationships
```
                           ┌─────────────────┐
                           │  StrategyLoader │
                           │                 │
                           │ Loads config    │
                           │ files & creates │
                           │ strategies      │
                           └────────┬────────┘
                                    │ creates
                                    ▼
                           ┌─────────────────┐
                           │StrategyManager  │
                           │                 │
                           │Manages portfolio│
                           │and runs backtest│
                           └────────┬────────┘
                                    │ uses
                                    ▼
                         ┌───────────────────┐
                         │ StrategyCreation  │
                         │                   │
                         │ Handles database  │
                         │ operations        │
                         └─────────┬─────────┘
                                   │ manages
                                   ▼
┌───────────────┬──────────────┬────────────────┬────────────────┐
│               │              │                │                │
▼               ▼              ▼                ▼                ▼
┌─────────────┐ ┌────────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────┐
│ Strategy A  │ │ Strategy B │ │  Strategy C  │ │  Future      │ │   ...   │
│             │ │            │ │              │ │  Strategies  │ │         │
└──────┬──────┘ └─────┬──────┘ └───────┬──────┘ └───────┬──────┘ └────┬────┘
       │              │                │                │             │
       │              │                │                │             │
       └──────────────┴────────┬───────┴────────────────┴─────────────┘
                               │ inherit from
                               ▼
                      ┌──────────────────┐
                      │ VectorBTStrategy │
                      │                  │     ┌───────────────┐
                      │ Abstract base    │◄────┤  RiskManager  │
                      │ class for all    │     │               │
                      │ strategies       │     │ Risk controls │
                      └────────┬─────────┘     └───────────────┘
                               │
                               │ uses
                               ▼
                  ┌─────────────────────────────┐
                  │         Indicators          │◄─────┐
                  │                             │      │
                  │ Reusable signal generators  │      │
                  └─────────────────────────────┘      │
                               │                       │
                               │ processed by          │ visualized by
                               ▼                       │
                  ┌─────────────────────────────┐     │
                  │       TradeMetrics          │     │
                  │                             │     │
                  │ Calculate trade statistics  │     │
                  └─────────────────────────────┘     │
                                                      │
                                            ┌─────────────────┐
                                            │ Visualization   │
                                            │                 │
                                            │ Plot indicators │
                                            └─────────────────┘
```

### Database Schema
```sql
algo_strategies
├── strategy_id (PK)
├── strategy_name
├── strategy_version
├── variant
├── indicator_name
├── indicator_parameters (JSON)
├── risk_reward
├── stop_loss_type
├── stop_loss_value
├── tightness_threshold
├── wick_ratio_threshold
└── creation_date

backtest_runs
├── run_id (PK)
└── execution_date

backtest_strategy_allocations
├── run_id (FK)
├── strategy_id (FK)
└── allocation_percentage

algo_trades
├── trade_id (PK)
├── strategy_id (FK)
├── run_id (FK)
├── account_id
├── symbol
├── direction
├── entry_date
├── entry_timestamp
├── exit_date
├── exit_timestamp
├── entry_price
├── exit_price
├── stop_price
├── quantity
├── instrument_type
├── capital_required
├── trade_duration
├── winning_trade
├── risk_reward
├── risk_per_trade
├── perc_return
└── risk_size
```

### Workflow
```
1. Configuration Loading
   JSON Files -> StrategyLoader -> StrategyConfig objects

2. Strategy Creation
   StrategyLoader
   └─> StrategyCreation.create_strategy()
       └─> Saves to algo_strategies table
           └─> Returns strategy_id

3. Backtest Execution
   StrategyManager.run_all()
   ├─> Creates backtest_run -> run_id
   ├─> Saves allocations to backtest_strategy_allocations
   └─> For each strategy:
       └─> VectorBTStrategy
           ├─> Generates signals using Indicators
           ├─> Runs backtest using VectorBT
           └─> Processes trades using TradeMetrics
               └─> Saves to algo_trades with strategy_id and run_id
```

## Core Components

### 1. VectorBTStrategy 
**Location:** `infrastructure/core/vectorbt_strategy.py`

The abstract base class that all strategies inherit from. It provides:
- Market data loading from SQLite database
- Position sizing based on risk parameters
- Integration with VectorBT for efficient backtesting
- Performance metrics calculation

This is the fundamental building block of all strategies. It defines the interface that all strategy implementations must follow, ensuring consistency and reducing code duplication.

### 2. StrategyManager
**Location:** `infrastructure/strategy_management/strategy_manager.py`

Coordinates multiple trading strategies, handling:
- Capital allocation across strategies
- Strategy lifecycle management
- Consolidated performance reporting
- Centralized execution of all strategies

Acts as a portfolio manager, distributing capital and running multiple strategies with different parameters simultaneously.

### 3. RiskManager
**Location:** `infrastructure/risk_management/risk_manager.py`

Handles all risk-related calculations and constraints:
- Risk per trade calculation
- Daily risk limits enforcement
- Position sizing based on risk parameters
- Trade validation based on risk rules

Ensures risk is consistently managed across all strategies.

### 4. Indicators
**Location:** `infrastructure/indicators/`

Reusable technical analysis components:
- `tight_candle.py`: Implementation of the tight candle pattern detector
- `visualize_tight_candles.py`: Visualization tools for tight candle analysis

Indicators are independent of strategies and can be reused across multiple strategy implementations.

### 5. Strategy Implementations
**Location:** `strategies/`

Individual strategy implementations:
- `tight_candle_strategy.py`: Strategy based on the tight candle pattern

Each strategy inherits from VectorBTStrategy and implements its specific trading logic.

### 6. Trade Metrics
**Location:** `infrastructure/trade_metrics/`

Tools for analyzing trade performance:
- `trade_metrics.py`: Calculates and tracks trade-level statistics

## Data Flow

1. The system loads market data from a SQLite database (`kairos.db`)
2. Indicators process this data to generate signals
3. Strategy implementations use these signals to generate entry/exit points
4. The VectorBTStrategy base class handles the backtest execution
5. StrategyManager coordinates multiple strategies and allocates capital
6. RiskManager enforces risk constraints throughout the process
7. Results are calculated and reported at both strategy and portfolio levels

## Getting Started

### Prerequisites
- Python 3.8+
- Required packages in `requirements.txt`

### Basic Usage

Run a backtest with a single strategy:
```python
python strategies/tight_candle_strategy.py
```

Run a backtest with multiple strategies:
```python
python main.py
```

## Extending the System

### Adding a New Indicator
1. Create a new file in `infrastructure/indicators/`
2. Implement your indicator class with relevant methods

### Adding a New Strategy
1. Create a new file in `strategies/`
2. Create a class that inherits from `VectorBTStrategy`
3. Implement the `generate_signals` method
4. Add your strategy to `StrategyManager` in `main.py`

### Customizing Risk Parameters
Modify the `RiskManager` initialization parameters to adjust risk profiles:
```python
risk_manager = RiskManager(
    db_path=db_path, 
    risk_per_trade=0.5,  # 0.5% risk per trade
    max_daily_risk=-2.0  # -2% max daily risk
)
```

## System Design Principles

1. **Separation of Concerns**: Each component has a specific responsibility
2. **Modularity**: Components can be developed and tested independently
3. **Extensibility**: New strategies and indicators can be easily added
4. **Reusability**: Core components are designed for reuse across strategies
5. **Testability**: Components are structured to facilitate testing