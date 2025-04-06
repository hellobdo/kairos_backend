import sys
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import json
from sklearn.model_selection import ParameterGrid
from backtests import backtest_runner
from backtests.backtests.dt_tshaped import Strategy

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class StrategyOptimizer:
    """
    Class for optimizing trading strategy parameters using a scikit-learn style approach.
    """
    
    def __init__(self, strategy_class, param_grid):
        """
        Initialize the optimizer with the strategy class and parameters to optimize.
        
        Args:
            strategy_class: The strategy class to optimize
            param_grid (dict): Dictionary mapping parameter names to lists of values to try
        """
        self.strategy_class = strategy_class
        
        # Store the parameters to optimize
        self.param_grid = param_grid
        
        # Create results directory
        self.results_dir = Path("optimization_results")
        self.results_dir.mkdir(exist_ok=True)
        
        # Add timestamp for this optimization run
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def fit(self):
        """
        Run the parameter search for the provided strategy.
        
        Returns:
            None: The function now just saves CSV files without returning results
        """
        # Create a grid of all parameter combinations
        param_combinations = list(ParameterGrid(self.param_grid))
        
        print(f"Testing {len(param_combinations)} parameter combinations")
        print(f"Using default values from Strategy.parameters for unspecified parameters")
        print(f"CSV files will be saved to {self.results_dir}")
        
        for i, params in enumerate(param_combinations):
            print(f"\nEvaluating combination {i+1}/{len(param_combinations)}")
            
            # Start with the strategy's default parameters
            full_params = self.strategy_class.parameters.copy()
            
            # Override with the current combination
            full_params.update(params)
            
            # Show what we're testing
            print(f"Testing parameters: {', '.join([f'{k}={v}' for k, v in params.items()])}")
            
            # Evaluate this parameter combination
            self._evaluate_params(full_params, i)
        
        print(f"\nOptimization complete. All CSV files saved to {self.results_dir}")
        return None

    def _evaluate_params(self, params, run_index):
        """
        Evaluate a single set of parameters.
        
        Args:
            params (dict): Parameters to evaluate
            run_index (int): Index of this run for file naming
        """
        # Override the Strategy's parameters
        self.strategy_class.parameters = params
        
        try:
            # Run the backtest
            self.strategy_class.run_strategy()
            
            # Process the results using backtest_runner
            executions_df, trades_df, reports = backtest_runner.run_backtest(backtest=False)
            
            if executions_df is None or trades_df is None or reports is None:
                print(f"Failed to process results for parameter combination {run_index}")
                return
            
            # Save each result with parameter information
            self._saved_optimisation_results(executions_df, params, run_index)
            self._saved_optimisation_results(trades_df, params, run_index)
            self._saved_optimisation_results(reports, params, run_index)
            
            print(f"Completed evaluation for parameter combination {run_index}")
            
        except Exception as e:
            print(f"Error evaluating parameters for combination {run_index}: {str(e)}")
            import traceback
            traceback.print_exc()

    def _saved_optimisation_results(self, df, params, run_index):
        """
        Save the optimisation results to a CSV file.
        
        Args:
            df: Either a single DataFrame or a dictionary of DataFrames (like reports)
            params (dict): Parameters used in this run
            run_index (int): Index of this run
        """
        # Create parameter dictionary for optimization params only and run_id
        params_for_df = {k: v for k, v in params.items() if k in self.param_grid}
        run_id = f"run_{run_index + 1:03d}"
        
        # Create parameter string for filenames
        param_str = "_".join([f"{k}_{v}" for k, v in params_for_df.items()])
        
        if isinstance(df, dict):
            # If it's a dictionary, process each item
            for key, item in df.items():
                # Convert to DataFrame if possible
                if hasattr(item, 'metrics'):
                    item_df = pd.DataFrame([item.metrics])
                elif isinstance(item, pd.DataFrame):
                    item_df = item
                else:
                    print(f"Warning: Unsupported type for item '{key}': {type(item)}")
                    continue
                
                # Add params
                item_df['params'] = str(params_for_df)
                item_df['run_id'] = run_id
                
                # Save
                filepath = self.results_dir / f"{run_id}_{param_str}_{key}.csv"
                item_df.to_csv(filepath, index=False)
                print(f"Saved {key} to {filepath}")
        
        elif isinstance(df, pd.DataFrame):
            # If it's already a DataFrame, just add params and save
            df['params'] = str(params_for_df)
            df['run_id'] = run_id
            
            # Determine if it's a trades or executions DataFrame based on first column
            df_type = 'unknown'
            first_col = df.columns[0]  # Get the first column name
            if first_col == 'trade_id':
                df_type = 'trades'
            else:
                df_type = 'executions'
            
            # Create filepath with appropriate suffix
            filepath = self.results_dir / f"{run_id}_{param_str}_{df_type}.csv"
            df.to_csv(filepath, index=False)
            print(f"Saved {df_type} to {filepath}")
        
        else:
            print(f"Warning: Unsupported type for results: {type(df)}")

def main():
    """Run the grid search with specified parameters"""

    # Define parameters to optimize
    param_grid = {
        'risk_reward': [1.5],
    }
    
    # Create and run optimizer
    optimizer = StrategyOptimizer(Strategy, param_grid)
    optimizer.fit()

if __name__ == "__main__":
    main()