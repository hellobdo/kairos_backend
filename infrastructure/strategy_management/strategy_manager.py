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
            
            # Get allocated capital
            allocated_capital = self.total_capital * self.capital_allocations[name]
            
            # Calculate new combined statistics
            all_trades = []
            for symbol, pf in strategy_results.items():
                all_trades.extend(pf.trades.records_readable)
            
            # Convert to DataFrame for easier analysis
            if all_trades:
                all_trades_df = pd.DataFrame(all_trades)
                total_trades = len(all_trades_df)
                winning_trades = len(all_trades_df[all_trades_df['return'] > 0])
                accuracy = winning_trades / total_trades if total_trades > 0 else 0
                
                # Return metrics
                avg_win = all_trades_df[all_trades_df['return'] > 0]['return'].mean() if winning_trades > 0 else 0
                avg_loss = all_trades_df[all_trades_df['return'] < 0]['return'].mean() if (total_trades - winning_trades) > 0 else 0
                avg_return = all_trades_df['return'].mean() if total_trades > 0 else 0
                total_return = all_trades_df['return'].sum() if total_trades > 0 else 0
                
                # Get average risk per trade from risk manager
                avg_risk_per_trade = strategy.risk_manager.risk_per_trade
                
                # Calculate average trades per period
                timeframes = {}
                for symbol, pf in strategy_results.items():
                    data = strategy.data[symbol]
                    if len(data) > 1:
                        timeframe = pd.Timedelta(data.index[1] - data.index[0])
                        if timeframe.days >= 1:
                            period_type = 'day'
                        elif timeframe.seconds // 3600 >= 1:
                            period_type = 'hour'
                        elif timeframe.seconds // 60 >= 1:
                            period_type = 'minute'
                        else:
                            period_type = 'second'
                            
                        if period_type not in timeframes:
                            timeframes[period_type] = {'count': len(data), 'trades': len(pf.trades)}
                        else:
                            timeframes[period_type]['count'] += len(data)
                            timeframes[period_type]['trades'] += len(pf.trades)
                
                # Calculate average trades per most common period type
                if timeframes:
                    most_common_period = max(timeframes.items(), key=lambda x: x[1]['count'])[0]
                    avg_trades_per_period = timeframes[most_common_period]['trades'] / timeframes[most_common_period]['count']
                else:
                    most_common_period = 'unknown'
                    avg_trades_per_period = 0
            else:
                total_trades = 0
                accuracy = 0
                avg_win = 0
                avg_loss = 0
                avg_return = 0
                total_return = 0
                avg_risk_per_trade = strategy.risk_manager.risk_per_trade
                most_common_period = 'unknown'
                avg_trades_per_period = 0
            
            # Store combined stats
            combined_stats.append({
                'strategy': name,
                'total_trades': total_trades,
                'allocated_capital': allocated_capital,
                'accuracy': accuracy,
                'avg_risk_per_trade': avg_risk_per_trade,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'avg_return': avg_return,
                'total_return': total_return,
                'period_type': most_common_period,
                'avg_trades_per_period': avg_trades_per_period
            })
        
        # Print overall summary
        print("\nOverall Performance Summary:")
        print("=" * 50)
        stats_df = pd.DataFrame(combined_stats)
        
        # Format for display
        for col in ['accuracy', 'avg_win', 'avg_loss', 'avg_return', 'total_return']:
            if col in stats_df.columns:
                stats_df[col] = stats_df[col].map(lambda x: f"{x:.2%}" if not pd.isna(x) else 'N/A')
                
        stats_df['avg_risk_per_trade'] = stats_df['avg_risk_per_trade'].map(lambda x: f"{x:.2%}")
        stats_df['allocated_capital'] = stats_df['allocated_capital'].map(lambda x: f"${x:,.2f}")
        stats_df['avg_trades_per_period'] = stats_df['avg_trades_per_period'].map(lambda x: f"{x:.2f}")
        
        print(stats_df.to_string())
        
        # Calculate portfolio-level statistics
        portfolio_total_trades = sum(s['total_trades'] for s in combined_stats)
        portfolio_accuracy = sum(s['accuracy'] * s['total_trades'] for s in combined_stats if isinstance(s['accuracy'], (int, float))) / portfolio_total_trades if portfolio_total_trades > 0 else 0
        weighted_total_return = sum(s['total_return'] * self.capital_allocations[s['strategy']] for s in combined_stats if isinstance(s['total_return'], (int, float)))
        
        print("\nPortfolio Statistics:")
        print(f"Total Trades: {portfolio_total_trades}")
        print(f"Portfolio Accuracy: {portfolio_accuracy:.2%}")
        print(f"Weighted Total Return: {weighted_total_return:.2%}")
        print(f"Total Allocated Capital: ${self.total_capital:,.2f}")
        
        return results 