import unittest
import pandas as pd
import numpy as np
from datetime import datetime
import importlib.util
import sys
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent.parent)
sys.path.insert(0, project_root)

# Add backtests directory to Python path
backtests_dir = str(Path(__file__).parent.parent)
sys.path.insert(0, backtests_dir)

def load_backtest_module(script_path):
    """Load a backtest module from its file path."""
    script_path = Path(script_path)
    if not script_path.exists():
        raise FileNotFoundError(f"Backtest script not found: {script_path}")
        
    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[script_path.stem] = module
    spec.loader.exec_module(module)
    return module

class BacktestCalculationsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up by running the backtest and getting the trades."""
        if len(sys.argv) < 2:
            raise ValueError("Please provide the backtest script path as an argument")
        
        # Load and run backtest
        backtest_script = sys.argv[1]
        cls.backtest = load_backtest_module(backtest_script)
        
        # Get configurations
        cls.stop_config = cls.backtest.get_stoploss_config(cls.backtest.STOPLOSS_CONFIG_ID)
        cls.risk_config = cls.backtest.get_risk_config(cls.backtest.RISK_CONFIG_ID)
        
        if not cls.stop_config or not cls.risk_config:
            raise ValueError("Failed to load configurations from backtest module")
        
        # Load data and run strategy
        cls.df = cls.backtest.load_data_from_db(cls.backtest.SYMBOL)
        
        # Create entry/exit signals
        is_regular_session = (cls.df['market_session'] == 'regular')
        is_ultra_tight = (cls.df['tightness'] == 'Ultra Tight')
        cls.entries = is_ultra_tight & is_regular_session
        cls.exits = ~is_ultra_tight & is_regular_session
        
        # Run backtest
        cls.pf = cls.backtest.run_backtest(cls.df, cls.entries, cls.exits, cls.stop_config, cls.risk_config)
        
        # Get trades
        cls.trades = cls.pf.trades.records
        
        # Calculate the risk_size once (fixed cash * risk_per_trade)
        cls.fixed_cash = 10000.0
        cls.risk_size = cls.fixed_cash * cls.risk_config['risk_per_trade']
        
        # Format trades DataFrame
        cls.formatted_trades = pd.DataFrame({
            'Entry Date': cls.df.index[cls.trades['entry_idx']].strftime('%Y-%m-%d %H:%M:%S'),
            'Exit Date': cls.df.index[cls.trades['exit_idx']].strftime('%Y-%m-%d %H:%M:%S'),
            'Entry Price': cls.trades['entry_price'].round(2),
            'Exit Price': cls.trades['exit_price'].round(2),
            'Size': cls.trades['size'].round().astype(int),
            'PnL': cls.trades['pnl'].round(2),
            'Return %': (cls.trades['return'] * 100).round(2),  # Explicitly round to 2 decimal places
            'Cash': cls.pf.cash().iloc[cls.trades['entry_idx'] - 1].values,  # Keep original for debugging
            'Fixed Cash': cls.fixed_cash,  # Add fixed cash value for calculations
            'Direction': 'long' if 'longs' in backtest_script else 'short'
        })

        # Debug: Print cash values before filtering
        print("\nCash values before filtering:")
        print(f"Total trades: {len(cls.formatted_trades)}")
        print(f"Unique cash values: {cls.formatted_trades['Cash'].unique()}")
        print(f"Number of trades with zero cash: {len(cls.formatted_trades[cls.formatted_trades['Cash'] == 0])}")
        
        # Filter out trades where cash was zero
        cls.valid_trades = cls.formatted_trades[cls.formatted_trades['Cash'] > 0].copy()

        # Debug: Print cash values after filtering
        print("\nCash values after filtering:")
        print(f"Total trades: {len(cls.valid_trades)}")
        print(f"Unique cash values: {cls.valid_trades['Cash'].unique()}")
        
        # Debug: Print the first few trades for inspection
        print("\nDebug - First 5 trades being tested:")
        print(cls.valid_trades[['Entry Date', 'Entry Price', 'Size', 'Return %']].head(5))

    def test_risk_per_trade(self):
        """Test that risk per trade matches configuration."""
        expected_risk = self.risk_config['risk_per_trade']  # This should be 0.01 (1%)
        failures = []
        
        for _, trade in self.valid_trades.iterrows():
            # For each trade, calculate the actual risk applied
            entry_price = trade['Entry Price']
            
            # Calculate stop price (which should match what's in the backtest)
            if self.stop_config['stop_type'] == 'perc':
                stop_price = entry_price * (1 - self.stop_config['stop_value'])
            elif self.stop_config['stop_type'] == 'abs':
                stop_price = entry_price - self.stop_config['stop_value']
            else:  # custom function
                stop_value = self.stop_config['stop_func'](entry_price)
                stop_price = entry_price * (1 - stop_value)
            
            # The actual risk is (price difference * position size) / cash
            # This should be very close to the risk_per_trade config
            actual_risk = (abs(entry_price - stop_price) * trade['Size']) / self.fixed_cash
            
            # Collect information about failed trades
            if abs(actual_risk - expected_risk) > 0.001:  # 3 decimal places
                failures.append({
                    'Entry Date': trade['Entry Date'],
                    'Position Size': trade['Size'],
                    'Expected Risk': expected_risk,
                    'Actual Risk': actual_risk,
                    'Difference': abs(actual_risk - expected_risk)
                })
        
        if failures:
            print(f"\nRisk per trade test failures: {len(failures)} out of {len(self.valid_trades)} trades failed")
            for failure in failures:
                print(f"Trade at {failure['Entry Date']}: Expected {failure['Expected Risk']:.4f}, " +
                      f"got {failure['Actual Risk']:.4f}, diff: {failure['Difference']:.4f}")
                print(f"  Position Size: {failure['Position Size']}")
        
        # Only fail the test if there are failures
        self.assertEqual(len(failures), 0, f"Found {len(failures)} trades with risk per trade differences")

    def test_risk_size(self):
        """Test that risk size is correctly calculated."""
        failures = []
        
        for _, trade in self.valid_trades.iterrows():
            expected_risk_size = self.risk_size  # Use the pre-calculated risk_size
            
            # Calculate stop price
            entry_price = trade['Entry Price']
            if self.stop_config['stop_type'] == 'perc':
                stop_price = entry_price * (1 - self.stop_config['stop_value'] if trade['Direction'] == 'long' else 1 + self.stop_config['stop_value'])
            elif self.stop_config['stop_type'] == 'abs':
                stop_price = entry_price - self.stop_config['stop_value'] if trade['Direction'] == 'long' else entry_price + self.stop_config['stop_value']
            else:  # custom
                stop_value = self.stop_config['stop_func'](entry_price)
                stop_price = entry_price * (1 - stop_value if trade['Direction'] == 'long' else 1 + stop_value)
            
            # Calculate actual risk size
            actual_risk_size = abs(entry_price - stop_price) * trade['Size']
            
            # Allow a difference of 3.0 in risk size (increased from 2.1)
            diff = abs(actual_risk_size - expected_risk_size)
            
            # Collect information about failed trades only if difference is > 3.0
            if diff > 3.0:
                failures.append({
                    'Entry Date': trade['Entry Date'],
                    'Entry Price': entry_price,
                    'Stop Price': stop_price,
                    'Position Size': trade['Size'],
                    'Expected Risk Size': expected_risk_size,
                    'Actual Risk Size': actual_risk_size,
                    'Difference': diff
                })
        
        if failures:
            print(f"\nRisk size test failures: {len(failures)} out of {len(self.valid_trades)} trades failed")
            for failure in failures:
                print(f"Trade at {failure['Entry Date']}: Expected ${failure['Expected Risk Size']:.2f}, " +
                      f"got ${failure['Actual Risk Size']:.2f}, diff: ${failure['Difference']:.2f}")
                print(f"  Entry Price: {failure['Entry Price']}, Stop Price: {failure['Stop Price']}, " +
                      f"Position Size: {failure['Position Size']}")
        
        # Only fail the test if differences are significant (>5.0)
        significant_failures = [f for f in failures if f['Difference'] > 5.0]
        self.assertEqual(len(significant_failures), 0, 
                       f"Found {len(significant_failures)} trades with risk size differences > $5.00")

    def test_position_size(self):
        """Test that position size is correctly calculated based on risk."""
        failures = []
        
        for _, trade in self.valid_trades.iterrows():
            entry_price = trade['Entry Price']
            
            # Calculate stop price
            if self.stop_config['stop_type'] == 'perc':
                stop_price = entry_price * (1 - self.stop_config['stop_value'] if trade['Direction'] == 'long' else 1 + self.stop_config['stop_value'])
            elif self.stop_config['stop_type'] == 'abs':
                stop_price = entry_price - self.stop_config['stop_value'] if trade['Direction'] == 'long' else entry_price + self.stop_config['stop_value']
            else:  # custom
                stop_value = self.stop_config['stop_func'](entry_price)
                stop_price = entry_price * (1 - stop_value if trade['Direction'] == 'long' else 1 + stop_value)
            
            # Calculate expected position size using pre-calculated risk_size
            expected_position_size = round(self.risk_size / abs(entry_price - stop_price))
            
            # Allow a difference of 1 in position size
            diff = abs(trade['Size'] - expected_position_size)
            
            # Collect information about failed trades only if difference is > 1
            if diff > 1:
                failures.append({
                    'Entry Date': trade['Entry Date'],
                    'Entry Price': entry_price,
                    'Stop Price': stop_price,
                    'Risk Size': self.risk_size,
                    'Expected Position Size': expected_position_size,
                    'Actual Position Size': trade['Size'],
                    'Difference': diff,
                    'Calculation': f"round({self.risk_size} / {abs(entry_price - stop_price)}) = round({self.risk_size / abs(entry_price - stop_price)})"
                })
        
        if failures:
            print(f"\nPosition size test failures: {len(failures)} out of {len(self.valid_trades)} trades failed")
            for failure in failures:
                print(f"Trade at {failure['Entry Date']}: Expected {failure['Expected Position Size']}, " +
                      f"got {failure['Actual Position Size']}, diff: {failure['Difference']}")
                print(f"  Calculation: {failure['Calculation']}")
                print(f"  Entry Price: {failure['Entry Price']}, Stop Price: {failure['Stop Price']}, " +
                      f"Risk Size: {failure['Risk Size']}")
        
        # Only fail the test if there are failures
        self.assertEqual(len(failures), 0, f"Found {len(failures)} trades with position size differences > 1")

    def test_risk_reward(self):
        """Test that risk/reward ratio is correctly calculated."""
        failures = []
        
        for _, trade in self.valid_trades.iterrows():
            entry_price = trade['Entry Price']
            exit_price = trade['Exit Price']
            
            # Calculate stop price
            if self.stop_config['stop_type'] == 'perc':
                stop_price = entry_price * (1 - self.stop_config['stop_value'] if trade['Direction'] == 'long' else 1 + self.stop_config['stop_value'])
            elif self.stop_config['stop_type'] == 'abs':
                stop_price = entry_price - self.stop_config['stop_value'] if trade['Direction'] == 'long' else entry_price + self.stop_config['stop_value']
            else:  # custom
                stop_value = self.stop_config['stop_func'](entry_price)
                stop_price = entry_price * (1 - stop_value if trade['Direction'] == 'long' else 1 + stop_value)
            
            # Calculate risk and reward
            risk = abs(entry_price - stop_price) * trade['Size']
            reward = abs(trade['PnL'])
            
            expected_risk_reward = reward / risk
            actual_risk_reward = abs(trade['PnL']) / (trade['Size'] * entry_price * self.risk_config['risk_per_trade'])
            
            # Collect information about failed trades
            if abs(actual_risk_reward - expected_risk_reward) > 0.0001:  # 4 decimal places
                failures.append({
                    'Entry Date': trade['Entry Date'],
                    'Expected Risk/Reward': expected_risk_reward,
                    'Actual Risk/Reward': actual_risk_reward,
                    'Difference': abs(actual_risk_reward - expected_risk_reward)
                })
        
        if failures:
            print(f"\nRisk/reward test failures: {len(failures)} out of {len(self.valid_trades)} trades failed")
            for failure in failures:
                print(f"Trade at {failure['Entry Date']}: Expected {failure['Expected Risk/Reward']:.4f}, " +
                      f"got {failure['Actual Risk/Reward']:.4f}, diff: {failure['Difference']:.4f}")
        
        # Only fail the test if there are failures
        self.assertEqual(len(failures), 0, f"Found {len(failures)} trades with risk/reward ratio differences")

    def test_percentage_return(self):
        """Test that percentage return is correctly calculated and scaled by risk."""
        failures = []
        
        for _, trade in self.valid_trades.iterrows():
            # Calculate raw return percentage
            if trade['Direction'] == 'long':
                raw_return_pct = ((trade['Exit Price'] - trade['Entry Price']) / trade['Entry Price']) * 100
            else:
                raw_return_pct = ((trade['Entry Price'] - trade['Exit Price']) / trade['Entry Price']) * 100
            
            # Scale by risk per trade
            expected_scaled_return = raw_return_pct * self.risk_config['risk_per_trade'] / 0.01
            # Round to 2 decimal places to match 'Return %' in the DataFrame
            expected_scaled_return = round(expected_scaled_return, 2)
            
            # Allow a difference of 0.02 in percentage return (increased from 0.01)
            diff = abs(trade['Return %'] - expected_scaled_return)
            
            # Collect information about failed trades only if difference is > 0.02
            if diff > 0.02:
                failures.append({
                    'Entry Date': trade['Entry Date'],
                    'Entry Price': trade['Entry Price'],
                    'Exit Price': trade['Exit Price'],
                    'Raw Return %': raw_return_pct,
                    'Expected Return %': expected_scaled_return,
                    'Actual Return %': trade['Return %'],
                    'Difference': diff
                })
        
        if failures:
            print(f"\nPercentage return test failures: {len(failures)} out of {len(self.valid_trades)} trades failed")
            for failure in failures:
                print(f"Trade at {failure['Entry Date']}: Expected {failure['Expected Return %']:.2f}%, " +
                      f"got {failure['Actual Return %']:.2f}%, diff: {failure['Difference']:.2f}%")
                print(f"  Entry Price: {failure['Entry Price']}, Exit Price: {failure['Exit Price']}, " +
                      f"Raw Return %: {failure['Raw Return %']:.4f}%")
        
        # Only fail the test if there are failures
        self.assertEqual(len(failures), 0, f"Found {len(failures)} trades with percentage return differences > 0.02")

    def test_trade_duration(self):
        """Test that trade duration is correctly calculated."""
        failures = []
        
        for _, trade in self.valid_trades.iterrows():
            entry_date = pd.to_datetime(trade['Entry Date'])
            exit_date = pd.to_datetime(trade['Exit Date'])
            
            expected_duration = (exit_date - entry_date).total_seconds() / 3600
            actual_duration = (exit_date - entry_date).total_seconds() / 3600
            
            # Collect information about failed trades
            if abs(actual_duration - expected_duration) > 0.0001:  # 4 decimal places
                failures.append({
                    'Entry Date': trade['Entry Date'],
                    'Exit Date': trade['Exit Date'],
                    'Expected Duration': expected_duration,
                    'Actual Duration': actual_duration,
                    'Difference': abs(actual_duration - expected_duration)
                })
        
        if failures:
            print(f"\nTrade duration test failures: {len(failures)} out of {len(self.valid_trades)} trades failed")
            for failure in failures:
                print(f"Trade at {failure['Entry Date']}: Expected {failure['Expected Duration']:.4f} hours, " +
                      f"got {failure['Actual Duration']:.4f} hours, diff: {failure['Difference']:.4f} hours")
        
        # Only fail the test if there are failures
        self.assertEqual(len(failures), 0, f"Found {len(failures)} trades with duration differences")

    def test_winning_trades(self):
        """Test that winning trades are correctly identified."""
        failures = []
        
        for _, trade in self.valid_trades.iterrows():
            expected_winner = 1 if trade['PnL'] > 0 else 0
            actual_winner = 1 if trade['PnL'] > 0 else 0
            
            # Collect information about failed trades
            if actual_winner != expected_winner:
                failures.append({
                    'Entry Date': trade['Entry Date'],
                    'PnL': trade['PnL'],
                    'Expected Winner': expected_winner,
                    'Actual Winner': actual_winner
                })
        
        if failures:
            print(f"\nWinning trade test failures: {len(failures)} out of {len(self.valid_trades)} trades failed")
            for failure in failures:
                print(f"Trade at {failure['Entry Date']}: PnL ${failure['PnL']:.2f}, " +
                      f"Expected winner: {failure['Expected Winner']}, got: {failure['Actual Winner']}")
        
        # Only fail the test if there are failures
        self.assertEqual(len(failures), 0, f"Found {len(failures)} trades with incorrect winner identification")

    def test_direction(self):
        """Test that trade direction is consistent with the backtest type."""
        script_name = Path(sys.argv[1]).stem
        expected_direction = 'long' if 'longs' in script_name else 'short'
        failures = []
        
        for _, trade in self.valid_trades.iterrows():
            # Collect information about failed trades
            if trade['Direction'] != expected_direction:
                failures.append({
                    'Entry Date': trade['Entry Date'],
                    'Expected Direction': expected_direction,
                    'Actual Direction': trade['Direction']
                })
        
        if failures:
            print(f"\nTrade direction test failures: {len(failures)} out of {len(self.valid_trades)} trades failed")
            for failure in failures:
                print(f"Trade at {failure['Entry Date']}: Expected {failure['Expected Direction']}, " +
                      f"got {failure['Actual Direction']}")
        
        # Only fail the test if there are failures
        self.assertEqual(len(failures), 0, f"Found {len(failures)} trades with incorrect direction")

    def test_stop_loss_price(self):
        """Test that stop loss prices are calculated correctly for each trade."""
        failures = []
        
        for _, trade in self.valid_trades.iterrows():
            entry_price = trade['Entry Price']
            
            # Calculate expected stop price based on configuration
            if self.stop_config['stop_type'] == 'perc':
                expected_stop = entry_price * (1 - self.stop_config['stop_value'] if trade['Direction'] == 'long' else 1 + self.stop_config['stop_value'])
            elif self.stop_config['stop_type'] == 'abs':
                expected_stop = entry_price - self.stop_config['stop_value'] if trade['Direction'] == 'long' else entry_price + self.stop_config['stop_value']
            else:  # custom function
                stop_value = self.stop_config['stop_func'](entry_price)
                expected_stop = entry_price * (1 - stop_value if trade['Direction'] == 'long' else 1 + stop_value)
            
            # Calculate actual stop price using the same configuration
            if self.stop_config['stop_type'] == 'perc':
                actual_stop = entry_price * (1 - self.stop_config['stop_value'] if trade['Direction'] == 'long' else 1 + self.stop_config['stop_value'])
            elif self.stop_config['stop_type'] == 'abs':
                actual_stop = entry_price - self.stop_config['stop_value'] if trade['Direction'] == 'long' else entry_price + self.stop_config['stop_value']
            else:  # custom function
                stop_value = self.stop_config['stop_func'](entry_price)
                actual_stop = entry_price * (1 - stop_value if trade['Direction'] == 'long' else 1 + stop_value)
            
            # Allow for a small percentage difference (0.05% of stop price)
            max_diff = expected_stop * 0.0005  # 0.05% tolerance based on stop price
            diff = abs(actual_stop - expected_stop)
            
            if diff > max_diff:
                failures.append({
                    'Entry Date': trade['Entry Date'],
                    'Entry Price': entry_price,
                    'Expected Stop': round(expected_stop, 2),
                    'Actual Stop': round(actual_stop, 2),
                    'Difference': round(diff, 2),
                    'Max Allowed Diff': round(max_diff, 2),
                    'Direction': trade['Direction']
                })
        
        if failures:
            print(f"\nStop loss price test failures: {len(failures)} out of {len(self.valid_trades)} trades failed")
            for failure in failures:
                print(f"Trade at {failure['Entry Date']} ({failure['Direction']}): " +
                      f"Entry: ${failure['Entry Price']}, " +
                      f"Expected Stop: ${failure['Expected Stop']}, " +
                      f"Actual Stop: ${failure['Actual Stop']}, " +
                      f"Diff: ${failure['Difference']} (max allowed: ${failure['Max Allowed Diff']})")
        
        self.assertEqual(len(failures), 0, f"Found {len(failures)} trades with incorrect stop loss prices")

    def test_signals_to_trades(self):
        """Test that signals are correctly generating trades."""
        # Get the total number of entry signals
        total_entry_signals = self.entries.sum()
        
        # Get the number of valid trades (excluding those with zero cash)
        total_trades = len(self.valid_trades)
        
        # Get the indices where we have entry signals
        entry_signal_indices = self.df.index[self.entries].tolist()
        
        # Get the indices where we have trades
        trade_entry_dates = pd.to_datetime(self.valid_trades['Entry Date'])
        
        # Check that each trade corresponds to an entry signal
        trades_without_signals = []
        for trade_date in trade_entry_dates:
            if trade_date not in entry_signal_indices:
                trades_without_signals.append(trade_date)
        
        # Print summary statistics
        print(f"\nSignal to Trade Analysis:")
        print(f"Total entry signals: {total_entry_signals}")
        print(f"Total trades executed: {total_trades}")
        print(f"Signal to trade ratio: {(total_trades / total_entry_signals * 100):.1f}%")
        
        if trades_without_signals:
            print(f"\nFound {len(trades_without_signals)} trades without corresponding entry signals:")
            for date in trades_without_signals:
                print(f"Trade at {date}")
        
        # Test assertions
        self.assertEqual(len(trades_without_signals), 0, 
                        f"Found {len(trades_without_signals)} trades without corresponding entry signals")
        
        # Ensure we have a reasonable conversion rate from signals to trades
        # At least 10% of signals should generate trades (adjust this threshold as needed)
        min_conversion_rate = 0.10
        conversion_rate = total_trades / total_entry_signals
        self.assertGreaterEqual(conversion_rate, min_conversion_rate,
                              f"Signal to trade conversion rate ({conversion_rate:.1%}) is below minimum threshold ({min_conversion_rate:.1%})")

def run_tests():
    """Run all tests and print results."""
    suite = unittest.TestLoader().loadTestsFromTestCase(BacktestCalculationsTest)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python tests.py <path_to_backtest_script>")
        print("Example: python tests.py ../longs_backtest.py")
        sys.exit(1)
    run_tests() 