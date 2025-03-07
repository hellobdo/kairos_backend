import vectorbt as vbt
import pandas as pd
from typing import Dict, List, Type, Optional
from infrastructure.core.vectorbt_strategy import VectorBTStrategy
import sqlite3

class StrategyManager:
    def __init__(self,
                 db_path: str,
                 total_capital: float = 100000):
        """
        Manages multiple trading strategies with capital allocation.
        
        Args:
            db_path: Path to SQLite database
            total_capital: Total capital available
        """
        self.db_path = db_path
        self.total_capital = total_capital
        self.strategies: Dict[str, VectorBTStrategy] = {}
        self.capital_allocations: Dict[str, float] = {}
    
    def add_strategy(self,
                    name: str,
                    strategy_class: Type[VectorBTStrategy],
                    symbols: List[str],
                    capital_allocation: float,  # Percentage of total capital (0-1)
                    **strategy_params) -> None:
        """
        Add a strategy to the manager.
        
        Args:
            name: Unique name for the strategy
            strategy_class: Strategy class (must inherit from VectorBTStrategy)
            symbols: List of symbols for the strategy
            capital_allocation: Percentage of total capital to allocate (0-1)
            **strategy_params: Additional parameters for the strategy
        """
        if name in self.strategies:
            raise ValueError(f"Strategy '{name}' already exists")
        
        if not issubclass(strategy_class, VectorBTStrategy):
            raise ValueError("Strategy class must inherit from VectorBTStrategy")
        
        if not (0 <= capital_allocation <= 1):
            raise ValueError("Capital allocation must be between 0 and 1")
        
        # Calculate allocated capital
        allocated_capital = self.total_capital * capital_allocation
        
        # Create strategy instance
        strategy = strategy_class(
            db_path=self.db_path,
            symbols=symbols,
            initial_capital=allocated_capital,
            **strategy_params
        )
        
        self.strategies[name] = strategy
        self.capital_allocations[name] = capital_allocation
    
    def remove_strategy(self, name: str) -> None:
        """Remove a strategy from the manager."""
        if name not in self.strategies:
            raise ValueError(f"Strategy '{name}' not found")
        
        del self.strategies[name]
        del self.capital_allocations[name]
    
    def get_strategy_info(self) -> pd.DataFrame:
        """Get information about all strategies."""
        info = []
        for name, strategy in self.strategies.items():
            info.append({
                'name': name,
                'symbols': ', '.join(strategy.symbols),
                'capital_allocation_pct': self.capital_allocations[name] * 100,
                'allocated_capital': self.total_capital * self.capital_allocations[name]
            })
        
        return pd.DataFrame(info)
    
    def run_all(self,
                start_date: Optional[str] = None,
                end_date: Optional[str] = None) -> Dict[str, Dict[str, vbt.Portfolio]]:
        """
        Run all strategies.
        
        Args:
            start_date: Optional start date for backtest
            end_date: Optional end date for backtest
            
        Returns:
            Dictionary of strategy name -> results dictionary
        """
        results = {}
        combined_stats = []
        
        print("\nRunning all strategies...")
        print("=" * 50)
        
        for name, strategy in self.strategies.items():
            print(f"\nStrategy: {name}")
            print("-" * 50)
            
            # Update date range if provided
            if start_date:
                strategy.start_date = start_date
            if end_date:
                strategy.end_date = end_date
            
            # Run strategy
            strategy_results = strategy.run_all()
            results[name] = strategy_results
            
            # Calculate combined statistics
            total_return = sum(pf.total_return for pf in strategy_results.values())
            avg_sharpe = sum(pf.sharpe_ratio for pf in strategy_results.values()) / len(strategy_results)
            max_dd = min(pf.max_drawdown for pf in strategy_results.values())
            total_trades = sum(len(pf.trades) for pf in strategy_results.values())
            
            # Get capital allocation
            allocated_capital = self.total_capital * self.capital_allocations[name]
            
            combined_stats.append({
                'strategy': name,
                'total_return': total_return,
                'avg_sharpe': avg_sharpe,
                'max_drawdown': max_dd,
                'total_trades': total_trades,
                'allocated_capital': allocated_capital
            })
        
        # Print overall summary
        print("\nOverall Performance Summary:")
        print("=" * 50)
        stats_df = pd.DataFrame(combined_stats)
        print(stats_df.to_string(float_format=lambda x: f"{x:.2%}" if abs(x) < 1 else f"{x:.2f}"))
        
        # Calculate portfolio-level statistics
        portfolio_return = sum(s['total_return'] * self.capital_allocations[s['strategy']] 
                             for s in combined_stats)
        portfolio_sharpe = sum(s['avg_sharpe'] * self.capital_allocations[s['strategy']] 
                             for s in combined_stats)
        
        print("\nPortfolio Statistics:")
        print(f"Total Return: {portfolio_return:.2%}")
        print(f"Weighted Sharpe Ratio: {portfolio_sharpe:.2f}")
        print(f"Total Allocated Capital: ${self.total_capital:,.2f}")
        
        return results 