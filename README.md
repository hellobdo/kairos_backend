# kairos

This repo of the Kairos project. The Kairos project is an attempt to create not one trading strategy, but rather a system that creates, tests, and deploys trading systems with the aim of generating performance returns for an accepting price.

## Infrastructure Architecture

The `infrastructure` folder contains the core components that power the trading system. It's designed to be modular, reusable, and easy to extend. Here's how it's organized:

### Directory Structure
```
infrastructure/
├── core/
│   ├── base_strategy.py     # Abstract base class for all trading strategies
│   └── trade.py            # Trade class for managing individual trades
├── data/
│   └── database_manager.py # Handles all database operations
└── reports_and_metrics/
    └── trade_metrics.py    # Collection of functions for trade calculations
```

### Core Components

#### BaseStrategy (base_strategy.py)
The foundation for all trading strategies. It provides:
- Common initialization for strategy parameters
- Data loading from database
- Backtest execution framework
- Trade management (entries/exits)

Abstract methods that must be implemented by each strategy:
- `get_strategy_name()`: Returns the strategy identifier
- `get_strategy_version()`: Returns the strategy version
- `calculate_entry_signals()`: Strategy-specific entry logic
- `calculate_exit_signals()`: Strategy-specific exit logic
- `_handle_entry()`: Entry execution logic
- `_handle_exit()`: Exit execution logic

#### Trade (trade.py)
Represents a single trade with all its properties and metrics:
- Entry/exit information (price, date, timestamp)
- Risk metrics (size, capital required, risk percentage)
- Performance metrics (duration, risk/reward, return)
- Trade status tracking

### Data Management

#### DatabaseManager (database_manager.py)
Centralizes all database operations:
- Table verification
- Trade persistence
- Data retrieval
- Error handling

### Reports and Metrics

#### TradeMetrics (trade_metrics.py)
Collection of pure functions for trade calculations:
- Risk size calculation
- Position sizing
- Stop price determination
- Performance metrics
- Return calculations

### Creating a New Strategy

To create a new strategy:

1. Create a new file in the `strategies` folder
2. Inherit from `BaseStrategy`
3. Implement required abstract methods
4. Define strategy-specific parameters and logic

Example:
```python
from infrastructure.core.base_strategy import BaseStrategy

class MyNewStrategy(BaseStrategy):
    def __init__(self, db_path: str, symbols: list, ...):
        super().__init__(db_path, symbols, ...)
        # Add strategy-specific parameters
        
    def get_strategy_name(self) -> str:
        return "my_strategy"
        
    def calculate_entry_signals(self, df, idx, row) -> bool:
        # Implement entry logic
        return True  # or False
```

### Benefits of This Architecture

1. **Separation of Concerns**: Each component has a single responsibility
2. **Code Reusability**: Common functionality is shared across strategies
3. **Maintainability**: Changes to core functionality can be made in one place
4. **Testability**: Components can be tested independently
5. **Extensibility**: Easy to add new strategies or modify existing ones
6. **Standardization**: Consistent approach to strategy development
7. **Risk Management**: Centralized trade and risk calculations

### Usage Example

```python
from infrastructure.data.database_manager import DatabaseManager
from strategies.my_strategy import MyStrategy

# Initialize components
db_manager = DatabaseManager('data.db')
strategy = MyStrategy(
    db_path='data.db',
    symbols=['AAPL', 'MSFT'],
    start_date='2024-01-01',
    end_date='2024-03-15'
)

# Run backtest
strategy.run()

# Save results
db_manager.save_trades(
    trades=strategy.trades,
    strategy_name=strategy.strategy_name,
    strategy_version=strategy.strategy_version,
    variant=strategy.variant,
    execution_date=strategy.execution_date
)
```