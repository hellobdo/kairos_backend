from infrastructure.strategy_management.strategy_manager import StrategyManager
from strategies.tight_candle_strategy import TightCandleStrategy

def main():
    """Run the strategy manager with multiple strategies."""
    db_path = "/Users/brunodeoliveira/Library/Mobile Documents/com~apple~CloudDocs/repos/kairos/kairos.db"
    
    # Initialize strategy manager
    manager = StrategyManager(
        db_path=db_path,
        total_capital=100000
    )
    
    # Add TightCandle strategy with different capital allocations
    manager.add_strategy(
        name="TightCandle_Tech",
        strategy_class=TightCandleStrategy,
        symbols=['NVDA', 'META', 'TSLA'],
        capital_allocation=0.4,  # 40% of total capital ($40,000)
        tightness_threshold=0.1,
        target_risk_reward=2.0
    )
    
    manager.add_strategy(
        name="TightCandle_Growth",
        strategy_class=TightCandleStrategy,
        symbols=['HOOD', 'PLTR'],
        capital_allocation=0.3,  # 30% of total capital ($30,000)
        tightness_threshold=0.15,
        target_risk_reward=1.5
    )
    
    manager.add_strategy(
        name="TightCandle_Index",
        strategy_class=TightCandleStrategy,
        symbols=['QQQ'],
        capital_allocation=0.3,  # 30% of total capital ($30,000)
        tightness_threshold=0.08,
        target_risk_reward=2.5
    )
    
    # Print strategy information
    print("\nStrategy Allocations:")
    print("=" * 50)
    print(manager.get_strategy_info().to_string())
    
    # Run all strategies
    results = manager.run_all()

if __name__ == "__main__":
    main() 