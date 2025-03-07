from typing import Dict, List, Type
import os
import importlib
import inspect
from datetime import datetime
from .base_strategy import BaseStrategy
from ..data.database_manager import DatabaseManager
from ..capital_management.risk_manager import RiskManager

class StrategyManager:
    def __init__(self, 
                 db_path: str,
                 initial_capital: float = 100000.0):
        """
        Initialize the strategy manager
        
        Args:
            db_path: Path to the database
            initial_capital: Initial capital to be allocated across strategies
        """
        self.db_path = db_path
        self.initial_capital = initial_capital
        
        # Initialize managers
        self.db_manager = DatabaseManager(db_path)
        self.risk_manager = RiskManager(db_path, initial_capital)
        
        # Strategy registry
        self.strategies: Dict[str, Dict] = {}
        
    def register_strategy(self, 
                         strategy_class: Type[BaseStrategy],
                         risk_allocation_pct: float,
                         symbols: List[str],
                         start_date: str,
                         end_date: str,
                         strategy_params: Dict = None,
                         variants: List[Dict] = None):
        """
        Register a strategy with the manager
        
        Args:
            strategy_class: The strategy class to register
            risk_allocation_pct: Percentage of total risk to allocate to this strategy
            symbols: List of symbols to trade
            start_date: Start date for backtesting
            end_date: End date for backtesting
            strategy_params: Additional strategy parameters
            variants: List of variant configurations to run
        """
        strategy_name = strategy_class.get_strategy_name()
        
        if strategy_name in self.strategies:
            raise ValueError(f"Strategy {strategy_name} already registered")
            
        if not 0 < risk_allocation_pct <= 100:
            raise ValueError("Risk allocation must be between 0 and 100 percent")
            
        # Calculate capital and risk allocation for this strategy
        strategy_capital = self.initial_capital * (risk_allocation_pct / 100)
        strategy_risk_pct = self.risk_manager.max_daily_risk_pct * (risk_allocation_pct / 100)
        
        # Store strategy configuration
        self.strategies[strategy_name] = {
            'class': strategy_class,
            'risk_allocation_pct': risk_allocation_pct,
            'symbols': symbols,
            'start_date': start_date,
            'end_date': end_date,
            'params': strategy_params or {},
            'variants': variants or [{'name': 'default'}],
            'capital': strategy_capital,
            'risk_pct': strategy_risk_pct
        }
        
    def run_strategies(self):
        """Run all registered strategies with their variants"""
        execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for strategy_name, config in self.strategies.items():
            print(f"\nRunning strategy: {strategy_name}")
            
            for variant in config['variants']:
                variant_name = variant.get('name', 'default')
                print(f"\nRunning variant: {variant_name}")
                
                # Create strategy instance with allocated capital and risk
                strategy_instance = config['class'](
                    db_path=self.db_path,
                    symbols=config['symbols'],
                    start_date=config['start_date'],
                    end_date=config['end_date'],
                    initial_capital=config['capital'],
                    risk_per_trade_pct=config['risk_pct'],
                    variant=variant_name,
                    **config['params'],
                    **variant.get('params', {})
                )
                
                # Run strategy
                strategy_instance.run()
                
                # Save trades to database
                self.db_manager.save_trades(
                    trades=strategy_instance.trades,
                    strategy_name=strategy_name,
                    strategy_version=strategy_instance.strategy_version,
                    variant=variant_name,
                    execution_date=execution_date
                )
                
                print(f"Completed {variant_name} variant")
            
            print(f"Completed {strategy_name} strategy")
            
    def get_strategy_allocations(self) -> Dict[str, Dict]:
        """Get current strategy allocations and configurations"""
        return {
            name: {
                'risk_allocation_pct': config['risk_allocation_pct'],
                'allocated_capital': config['capital'],
                'max_daily_risk_pct': config['risk_pct'],
                'symbols': config['symbols'],
                'variants': [v.get('name', 'default') for v in config['variants']]
            }
            for name, config in self.strategies.items()
        }

    @staticmethod
    def discover_strategies() -> Dict[str, Type[BaseStrategy]]:
        """Discover all available strategy classes in the strategies directory"""
        strategies = {}
        strategies_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'strategies')
        
        # List all Python files in strategies directory
        for file in os.listdir(strategies_dir):
            if file.endswith('.py') and not file.startswith('__'):
                module_name = file[:-3]  # Remove .py extension
                
                try:
                    # Import the module
                    module = importlib.import_module(f'strategies.{module_name}')
                    
                    # Find all classes in the module that inherit from BaseStrategy
                    for name, obj in inspect.getmembers(module):
                        if (inspect.isclass(obj) and 
                            issubclass(obj, BaseStrategy) and 
                            obj != BaseStrategy):
                            strategies[obj.get_strategy_name()] = obj
                            
                except Exception as e:
                    print(f"Error loading strategy from {file}: {e}")
        
        return strategies

    @classmethod
    def run_interactive(cls, db_path: str = 'kairos.db', initial_capital: float = 100000.0):
        """Run strategies interactively, letting user choose which ones to run"""
        # Initialize manager
        manager = cls(db_path=db_path, initial_capital=initial_capital)
        
        # Discover available strategies
        available_strategies = cls.discover_strategies()
        
        if not available_strategies:
            print("No strategies found in the strategies directory.")
            return
        
        print("\nAvailable Strategies:")
        print("--------------------")
        for name in available_strategies.keys():
            print(f"- {name}")
        
        # Common parameters for all strategies
        symbols = ['AAPL', 'MSFT', 'NVDA', 'AMD', 'META', 'GOOGL', 'AMZN', 'NFLX', 'TSLA']
        start_date = '2024-01-01'
        end_date = '2024-03-15'
        
        # Let user select strategies to run
        total_allocation = 0
        for name, strategy_class in available_strategies.items():
            while True:
                response = input(f"\nRun {name} strategy? (y/n): ").lower()
                if response not in ['y', 'n']:
                    print("Please enter 'y' for yes or 'n' for no.")
                    continue
                
                if response == 'y':
                    # Get risk allocation if not the only strategy
                    if len(available_strategies) > 1:
                        while True:
                            try:
                                allocation = float(input(f"Risk allocation for {name} (remaining: {100-total_allocation}%): "))
                                if allocation <= 0 or allocation > (100-total_allocation):
                                    print(f"Please enter a value between 0 and {100-total_allocation}")
                                    continue
                                break
                            except ValueError:
                                print("Please enter a valid number")
                    else:
                        allocation = 100
                    
                    # Strategy-specific parameters
                    if name == "tight_candle":
                        strategy_params = {
                            'tightness_threshold': 'Ultra Tight',
                            'stop_loss_amount': 0.5
                        }
                        variants = [
                            {'name': f'RR_{rr}.0', 'params': {'target_risk_reward': float(rr)}}
                            for rr in range(2, 6)
                        ]
                    else:
                        strategy_params = {}
                        variants = [{'name': 'default'}]
                    
                    # Register strategy
                    manager.register_strategy(
                        strategy_class=strategy_class,
                        risk_allocation_pct=allocation,
                        symbols=symbols,
                        start_date=start_date,
                        end_date=end_date,
                        strategy_params=strategy_params,
                        variants=variants
                    )
                    
                    total_allocation += allocation
                break
        
        if not manager.strategies:
            print("\nNo strategies selected. Exiting...")
            return
        
        # Print strategy allocations
        print("\nStrategy Allocations:")
        print("--------------------")
        for strategy, config in manager.get_strategy_allocations().items():
            print(f"\nStrategy: {strategy}")
            print(f"Risk Allocation: {config['risk_allocation_pct']}%")
            print(f"Allocated Capital: ${config['allocated_capital']:,.2f}")
            print(f"Max Daily Risk: {config['max_daily_risk_pct']}%")
            print(f"Symbols: {', '.join(config['symbols'])}")
            print(f"Variants: {', '.join(config['variants'])}")
        
        # Run all strategies
        print("\nStarting strategy execution...")
        manager.run_strategies()

    @classmethod
    def run_tight_candle_backtest(cls, db_path: str = 'kairos.db', initial_capital: float = 100000.0):
        """Run a backtest of the TightCandleStrategy with default configuration"""
        from strategies.tight_candle_strategy import TightCandleStrategy
        
        # Initialize manager
        manager = cls(db_path=db_path, initial_capital=initial_capital)
        
        # Strategy parameters
        symbols = ['AAPL', 'MSFT', 'NVDA', 'AMD', 'META', 'GOOGL', 'AMZN', 'NFLX', 'TSLA']
        start_date = '2024-01-01'
        end_date = '2024-03-15'
        
        # Strategy-specific parameters
        strategy_params = {
            'tightness_threshold': 'Ultra Tight',
            'stop_loss_amount': 0.5
        }
        
        # Define variants
        variants = [
            {
                'name': 'RR_2.0',
                'params': {'target_risk_reward': 2.0}
            },
            {
                'name': 'RR_3.0',
                'params': {'target_risk_reward': 3.0}
            },
            {
                'name': 'RR_4.0',
                'params': {'target_risk_reward': 4.0}
            },
            {
                'name': 'RR_5.0',
                'params': {'target_risk_reward': 5.0}
            }
        ]
        
        # Register TightCandleStrategy
        manager.register_strategy(
            strategy_class=TightCandleStrategy,
            risk_allocation_pct=100.0,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            strategy_params=strategy_params,
            variants=variants
        )
        
        # Print strategy allocations
        print("\nStrategy Allocations:")
        print("--------------------")
        for strategy, config in manager.get_strategy_allocations().items():
            print(f"\nStrategy: {strategy}")
            print(f"Risk Allocation: {config['risk_allocation_pct']}%")
            print(f"Allocated Capital: ${config['allocated_capital']:,.2f}")
            print(f"Max Daily Risk: {config['max_daily_risk_pct']}%")
            print(f"Symbols: {', '.join(config['symbols'])}")
            print(f"Variants: {', '.join(config['variants'])}")
        
        # Run all strategies
        print("\nStarting strategy execution...")
        manager.run_strategies() 