import unittest
import os
import pandas as pd
import numpy as np
import json
import sys
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the modules to test
from backtests.backtest_runner import (
    calculate_risk_reward,
    format_trades,
    log_trades_to_db,
    run_backtest,
    main
)

class TestBacktestRunner(unittest.TestCase):
    """Test cases for the backtest_runner.py module."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Sample trade data for testing
        self.sample_trades = [
            {
                'entry_timestamp': pd.Timestamp('2023-01-02 10:00:00'),
                'exit_timestamp': pd.Timestamp('2023-01-02 14:30:00'),
                'entry_price': 100.0,
                'exit_price': 105.0,
                'stop_price': 98.0,
                'position_size': 50,
                'risk_size': 100.0,
                'risk_per_trade': 1.0,  # 1%
                'risk_reward': 2.5,
                'perc_return': 2.5,
                'winning_trade': 1,  # 1 for winning, 0 for losing
                'trade_duration': 4.5,  # in hours
                'capital_required': 5000.0,
                'direction': 'long',
                'exit_type': 'Take Profit'
            },
            {
                'entry_timestamp': pd.Timestamp('2023-01-03 10:00:00'),
                'exit_timestamp': pd.Timestamp('2023-01-03 15:30:00'),
                'entry_price': 105.0,
                'exit_price': 102.0,
                'stop_price': 106.0,
                'position_size': 100,
                'risk_size': 100.0,
                'risk_per_trade': 1.0,  # 1%
                'risk_reward': -3.0,
                'perc_return': -3.0,
                'winning_trade': 0,  # 0 for losing
                'trade_duration': 5.5,  # in hours
                'capital_required': 10500.0,
                'direction': 'short',
                'exit_type': 'Stop Loss'
            }
        ]
        
        # Sample price DataFrame
        self.sample_df = pd.DataFrame({
            'open': [100, 102, 104, 105, 103],
            'high': [103, 104, 106, 107, 105],
            'low': [99, 101, 103, 104, 101],
            'close': [102, 103, 105, 106, 102],
            'volume': [1000, 1100, 1200, 1300, 1400],
            'tightness': ['Ultra Tight', 'Tight', 'Normal', 'Tight', 'Ultra Tight'],
            'market_session': ['regular', 'regular', 'after', 'regular', 'regular']
        }, index=pd.date_range(start='2023-01-01', periods=5, freq='D'))
        
        # Sample configurations
        self.sample_stop_config = {'stop_type': 'perc', 'stop_value': 0.02}
        self.sample_risk_config = {'risk_per_trade': 1.0, 'max_daily_risk': 5.0}
        self.sample_exit_config = {'type': 'fixed', 'risk_reward': 2.0}
        self.sample_swing_config = {'swings_allowed': 0}
        
        # Sample backtest config
        self.sample_backtest_config = {
            "database": {
                "path": "data/algos.db"
            },
            "backtest": {
                "symbol": "QQQ",
                "entry_config_id": 1,
                "stoploss_config_id": 3,
                "risk_config_id": 1,
                "exit_config_id": 1,
                "swing_config_id": 1,
                "exits_swings_config_id": None,
                "init_cash": 10000,
                "date_range": {
                    "start": "2023-01-01",
                    "end": "2023-05-15"
                }
            }
        }
    
    def test_calculate_risk_reward_long(self):
        """Test calculating risk/reward ratio for long trades."""
        entry_price = 100.0
        exit_price = 105.0
        stop_price = 98.0
        is_long = True
        
        # Risk: entry - stop = 2.0, Reward: exit - entry = 5.0, R:R = 5.0/2.0 = 2.5
        expected_rr = 2.5
        
        rr = calculate_risk_reward(entry_price, exit_price, stop_price, is_long)
        self.assertEqual(rr, expected_rr)
    
    def test_calculate_risk_reward_short(self):
        """Test calculating risk/reward ratio for short trades."""
        entry_price = 100.0
        exit_price = 95.0
        stop_price = 102.0
        is_long = False
        
        # Risk: stop - entry = 2.0, Reward: entry - exit = 5.0, R:R = 5.0/2.0 = 2.5
        expected_rr = 2.5
        
        rr = calculate_risk_reward(entry_price, exit_price, stop_price, is_long)
        self.assertEqual(rr, expected_rr)
    
    def test_format_trades(self):
        """Test that trades are formatted correctly."""
        # Create a copy of sample_trades without winning_trade and trade_duration
        # as these are added by format_trades
        base_trades = []
        for trade in self.sample_trades:
            base_trade = trade.copy()
            if 'winning_trade' in base_trade:
                del base_trade['winning_trade']
            if 'trade_duration' in base_trade:
                del base_trade['trade_duration']
            base_trades.append(base_trade)
        
        # Convert to pandas DataFrame
        df = pd.DataFrame({
            'close': [100, 102, 98, 105],
            'high': [101, 103, 99, 106],
            'low': [99, 101, 97, 104]
        }, index=pd.date_range(start='2023-01-01', periods=4, freq='H'))
        
        # Call format_trades
        result = format_trades(
            base_trades, df, 
            self.sample_stop_config,
            self.sample_risk_config,
            self.sample_exit_config,
            self.sample_swing_config
        )
        
        # Verify result has correct shape
        self.assertEqual(len(result), len(self.sample_trades))
        
        # Verify correct columns are present
        self.assertIn('winning_trade', result.columns)
        self.assertIn('trade_duration', result.columns)
        
        # Check if winning_trade is correct (positive risk_reward = winning trade)
        for i, trade in result.iterrows():
            self.assertEqual(trade['winning_trade'], 1 if trade['risk_reward'] > 0 else 0)
    
    @patch('sqlite3.connect')
    def test_log_trades_to_db(self, mock_connect):
        """Test logging trades to the database."""
        # Set up the mock connection and cursor
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (0,)  # No existing trades
        
        # Convert sample trades to DataFrame
        trades_df = pd.DataFrame(self.sample_trades)
        
        # Call the function
        log_trades_to_db(trades_df, 42, 'QQQ')
        
        # Assert SELECT was called
        select_calls = [call for call in mock_cursor.execute.call_args_list 
                        if isinstance(call[0][0], str) and "SELECT" in call[0][0]]
        self.assertTrue(len(select_calls) > 0, "Should check for existing trades with SELECT")
        
        # Since fetchone returned 0, DELETE should not be called
        
        # Assert INSERT was called for each trade
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                        if isinstance(call[0][0], str) and "INSERT INTO" in call[0][0]]
        self.assertEqual(len(insert_calls), len(trades_df), 
                        f"Should execute INSERT {len(trades_df)} times, once for each trade")
        
        # Test with existing trades scenario
        mock_cursor.reset_mock()
        mock_cursor.fetchone.return_value = (2,)  # 2 existing trades
        
        # Call the function again
        log_trades_to_db(trades_df, 42, 'QQQ')
        
        # Assert DELETE was called
        delete_calls = [call for call in mock_cursor.execute.call_args_list 
                       if isinstance(call[0][0], str) and "DELETE" in call[0][0]]
        self.assertTrue(len(delete_calls) > 0, "Should delete existing trades with DELETE")
        
        # Assert that commit and close were called
        mock_connect.return_value.commit.assert_called()
        mock_connect.return_value.close.assert_called()
    
    @patch('backtests.backtest_runner.log_trades_to_db')
    @patch('backtests.backtest_runner.format_trades')
    @patch('backtests.backtest_runner.setup_backtest')
    @patch('backtests.backtest_runner.load_backtest_config')
    def test_run_backtest(self, mock_load_config, mock_setup, mock_format, mock_log):
        """Test running a backtest."""
        # Set up the mocks
        mock_load_config.return_value = (
            'QQQ', 1, 3, 1, 1, 1, None, 
            {"start": "2023-01-01", "end": "2023-05-15"},
            self.sample_backtest_config['backtest']
        )
        
        # Mock data returned from setup_backtest
        mock_setup.return_value = {
            'run_id': 42,
            'df': self.sample_df,
            'entries': pd.Series([True, False, False, True, False], index=self.sample_df.index),
            'entry_config': {'field': 'tightness', 'signal': 'Ultra Tight', 'direction': 'long'},
            'direction': 'long',
            'stop_config': self.sample_stop_config,
            'risk_config': self.sample_risk_config,
            'exit_config': self.sample_exit_config,
            'swing_config': self.sample_swing_config,
            'exits_swings_config': None,
            'symbol': 'QQQ'
        }
        
        # Mock format_trades to return a DataFrame
        trades_df = pd.DataFrame(self.sample_trades)
        mock_format.return_value = trades_df
        
        # Call the function
        result = run_backtest('dummy_config.json')
        
        # Assert that the function returned True (success)
        self.assertTrue(result)
        
        # Assert that all required functions were called
        mock_load_config.assert_called_once()
        mock_setup.assert_called_once()
        mock_format.assert_called_once()
        mock_log.assert_called_once()
    
    @patch('backtests.backtest_runner.run_backtest')
    def test_main_success(self, mock_run_backtest):
        """Test the main function with a successful backtest."""
        # Mock run_backtest to return True (success)
        mock_run_backtest.return_value = True
        
        # Call the main function
        exit_code = main()
        
        # Assert that the function returned exit code 0 (success)
        self.assertEqual(exit_code, 0)
        
        # Assert that run_backtest was called once
        mock_run_backtest.assert_called_once()
    
    @patch('backtests.backtest_runner.run_backtest')
    def test_main_failure(self, mock_run_backtest):
        """Test the main function with a failed backtest."""
        # Mock run_backtest to return False (failure)
        mock_run_backtest.return_value = False
        
        # Call the main function
        exit_code = main()
        
        # Assert that the function returned exit code 1 (failure)
        self.assertEqual(exit_code, 1)
        
        # Assert that run_backtest was called once
        mock_run_backtest.assert_called_once()

    @patch('backtests.backtest_runner.log_trades_to_db')
    @patch('backtests.backtest_runner.format_trades')
    @patch('backtests.backtest_runner.setup_backtest')
    @patch('backtests.backtest_runner.load_backtest_config')
    def test_no_swing_trading_end_of_day_exit(self, mock_load_config, mock_setup, mock_format, mock_log):
        """Test that positions are closed at the end of the day when swing trading is not allowed."""
        # Set up the mocks
        mock_load_config.return_value = (
            'QQQ', 1, 3, 1, 1, 1, None, 
            {"start": "2023-01-01", "end": "2023-01-03"},
            self.sample_backtest_config['backtest']
        )
        
        # Create a multi-day DataFrame with dates that clearly span different days
        date_index = pd.DatetimeIndex([
            # Day 1
            '2023-01-01 09:30:00', '2023-01-01 10:00:00', '2023-01-01 15:30:00', '2023-01-01 16:00:00',
            # Day 2
            '2023-01-02 09:30:00', '2023-01-02 10:00:00', '2023-01-02 15:30:00', '2023-01-02 16:00:00'
        ])
        
        multi_day_df = pd.DataFrame({
            'open': [100, 101, 102, 103, 104, 105, 106, 107],
            'high': [102, 103, 104, 105, 106, 107, 108, 109],
            'low': [99, 100, 101, 102, 103, 104, 105, 106],
            'close': [101, 102, 103, 104, 105, 106, 107, 108],
            'volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700],
            'market_session': ['regular', 'regular', 'regular', 'regular', 'regular', 'regular', 'regular', 'regular']
        }, index=date_index)
        
        # Create entries at the beginning of each day
        entries = pd.Series([
            True, False, False, False,  # First entry on day 1
            True, False, False, False   # Second entry on day 2
        ], index=date_index)
        
        # Mock data returned from setup_backtest
        mock_setup.return_value = {
            'run_id': 42,
            'df': multi_day_df,
            'entries': entries,
            'entry_config': {'field': 'tightness', 'signal': 'Ultra Tight', 'direction': 'long'},
            'direction': 'long',
            'stop_config': {'stop_type': 'perc', 'stop_value': 0.02},
            'risk_config': {'risk_per_trade': 1.0, 'max_daily_risk': 5.0},
            'exit_config': {'type': 'fixed', 'risk_reward': 2.0},
            'swing_config': {'swings_allowed': 0, 'description': 'not allowed'},  # No swing trading
            'exits_swings_config': None,
            'symbol': 'QQQ'
        }
        
        # Record the trades that would be created
        recorded_trades = []
        
        # Mock format_trades to capture the trades
        def side_effect_format(trades, *args, **kwargs):
            nonlocal recorded_trades
            recorded_trades = trades
            return pd.DataFrame(trades)
        
        mock_format.side_effect = side_effect_format
        
        # Call the function
        result = run_backtest('dummy_config.json')
        
        # Assert that the function returned True (success)
        self.assertTrue(result)
        
        # Verify trades were created
        self.assertGreater(len(recorded_trades), 0, "No trades were generated")
        
        # Extract entry and exit dates to check for multi-day trades
        trade_durations = []
        for trade in recorded_trades:
            entry_date = pd.Timestamp(trade['entry_timestamp']).date()
            exit_date = pd.Timestamp(trade['exit_timestamp']).date()
            trade_durations.append((exit_date - entry_date).days)
        
        # Verify that no trades span multiple days when swing trading is disabled
        self.assertTrue(all(duration == 0 for duration in trade_durations), 
                      f"Found trades spanning multiple days: {trade_durations}")
        
        # Verify that we have end-of-day exits
        exit_types = [trade['exit_type'] for trade in recorded_trades]
        self.assertTrue(any("End of Day" in exit_type for exit_type in exit_types),
                      f"No end-of-day exits found: {exit_types}")

    @patch('backtests.backtest_runner.log_trades_to_db')
    @patch('backtests.backtest_runner.format_trades')
    @patch('backtests.backtest_runner.setup_backtest')
    @patch('backtests.backtest_runner.load_backtest_config')
    def test_position_metrics_calculation(self, mock_load_config, mock_setup, mock_format, mock_log):
        """Test that position metrics (position_size, risk_size, risk_per_trade, etc.) are calculated correctly."""
        # Set up the mocks
        mock_load_config.return_value = (
            'QQQ', 1, 3, 1, 1, 1, None, 
            {"start": "2023-01-01", "end": "2023-01-03"},
            {'init_cash': 10000}  # Make sure we have initial cash value
        )
        
        # Test data
        test_df = pd.DataFrame({
            'open': [100, 101, 102, 103],
            'high': [102, 103, 104, 105],
            'low': [99, 100, 101, 102],
            'close': [101, 102, 103, 104],
            'volume': [1000, 1100, 1200, 1300],
            'market_session': ['regular', 'regular', 'regular', 'regular']
        }, index=pd.date_range(start='2023-01-01 09:30:00', periods=4, freq='H'))
        
        # Create entries at a specific point
        entries = pd.Series([True, False, False, False], index=test_df.index)
        
        # Define test parameters
        entry_price = 101  # First close price
        stop_price = 99    # 2% below entry
        init_cash = 10000
        risk_per_trade = 1.0  # 1% risk per trade
        
        # Calculate expected values
        expected_risk_amount = init_cash * risk_per_trade / 100  # $100
        expected_price_diff = abs(entry_price - stop_price)      # $2
        expected_position_size = round(expected_risk_amount / expected_price_diff)  # 50 shares
        expected_risk_size = expected_position_size * expected_price_diff  # $100
        expected_capital_required = expected_position_size * entry_price   # $5050
        
        # Mock data returned from setup_backtest
        mock_setup.return_value = {
            'run_id': 42,
            'df': test_df,
            'entries': entries,
            'entry_config': {'field': 'tightness', 'signal': 'Ultra Tight', 'direction': 'long'},
            'direction': 'long',
            'stop_config': {'stop_type': 'perc', 'stop_value': 0.02},
            'risk_config': {'risk_per_trade': risk_per_trade, 'max_daily_risk': 5.0},
            'exit_config': {'type': 'fixed', 'risk_reward': 2.0},
            'swing_config': {'swings_allowed': 1},
            'exits_swings_config': None,
            'symbol': 'QQQ'
        }
        
        # Record the trades that would be created
        recorded_trades = []
        
        # Mock format_trades to capture the trades
        def side_effect_format(trades, *args, **kwargs):
            nonlocal recorded_trades
            recorded_trades = trades
            return pd.DataFrame(trades)
        
        mock_format.side_effect = side_effect_format
        
        # Call the function
        result = run_backtest('dummy_config.json')
        
        # Assert that the function returned True (success)
        self.assertTrue(result)
        
        # Verify trades were created
        self.assertGreater(len(recorded_trades), 0, "No trades were generated")
        
        # Get the first trade for verification
        trade = recorded_trades[0]
        
        # Verify position metrics
        self.assertGreater(trade['position_size'], 0, 
                          f"Position size should be greater than 0, got {trade['position_size']}")
        
        self.assertEqual(trade['position_size'], expected_position_size, 
                         f"Position size incorrect: expected {expected_position_size}, got {trade['position_size']}")
        
        # Use relative tolerance for risk_size since it's calculated based on prices
        self.assertGreater(trade['risk_size'], 0,
                          f"Risk size should be greater than 0, got {trade['risk_size']}")
        
        self.assertAlmostEqual(trade['risk_size'], expected_risk_size, delta=1.0,
                              msg=f"Risk size incorrect: expected {expected_risk_size}, got {trade['risk_size']}")
        
        # risk_per_trade should be *100 (percentage)
        self.assertEqual(trade['risk_per_trade'], risk_per_trade * 100,
                         f"risk_per_trade incorrect: expected {risk_per_trade * 100}, got {trade['risk_per_trade']}")
        
        # Calculate expected perc_return (risk_reward * risk_per_trade[%])
        risk_reward = trade['risk_reward']
        expected_perc_return = risk_reward * (risk_per_trade * 100)
        self.assertAlmostEqual(trade['perc_return'], expected_perc_return, places=2,
                              msg=f"perc_return incorrect: expected {expected_perc_return}, got {trade['perc_return']}")
        
        self.assertGreater(trade['capital_required'], 0,
                          f"Capital required should be greater than 0, got {trade['capital_required']}")
        
        self.assertEqual(trade['capital_required'], expected_capital_required,
                        f"capital_required incorrect: expected {expected_capital_required}, got {trade['capital_required']}")
        
        # Verify trade data is passed correctly to log_trades_to_db
        self.assertTrue(mock_log.called, "log_trades_to_db was not called")
        df_arg = mock_log.call_args[0][0]  # Get the first argument (DataFrame)
        
        # Access the position_size, risk_size, and capital_required columns
        # to make sure they're not zero in the DataFrame sent to log_trades_to_db
        self.assertTrue('position_size' in df_arg.columns, "position_size column missing in DataFrame sent to log_trades_to_db")
        self.assertTrue('risk_size' in df_arg.columns, "risk_size column missing in DataFrame sent to log_trades_to_db")
        self.assertTrue('capital_required' in df_arg.columns, "capital_required column missing in DataFrame sent to log_trades_to_db")
        
        # These checks are against the formatted DataFrame that would be sent to the database
        self.assertGreater(df_arg['position_size'].iloc[0], 0, "position_size is zero or missing in DB data")
        self.assertGreater(df_arg['risk_size'].iloc[0], 0, "risk_size is zero or missing in DB data")
        self.assertGreater(df_arg['capital_required'].iloc[0], 0, "capital_required is zero or missing in DB data")

    @patch('backtests.backtest_runner.log_trades_to_db')
    @patch('backtests.backtest_runner.format_trades')
    @patch('backtests.backtest_runner.setup_backtest')
    @patch('backtests.backtest_runner.load_backtest_config')
    def test_init_cash_for_multiple_trades(self, mock_load_config, mock_setup, mock_format, mock_log):
        """Test that init_cash is available and used correctly for sizing all trades."""
        # Set up the mocks with explicit init_cash
        init_cash = 10000
        mock_load_config.return_value = (
            'QQQ', 1, 3, 1, 1, 1, None, 
            {"start": "2023-01-01", "end": "2023-01-05"},
            {'init_cash': init_cash}  # Explicit init_cash value
        )
        
        # Create a DataFrame with multiple days and multiple entry signals
        date_index = pd.DatetimeIndex([
            # Day 1
            '2023-01-01 09:30:00', '2023-01-01 10:00:00', '2023-01-01 16:00:00',
            # Day 2
            '2023-01-02 09:30:00', '2023-01-02 10:00:00', '2023-01-02 16:00:00',
            # Day 3
            '2023-01-03 09:30:00', '2023-01-03 10:00:00', '2023-01-03 16:00:00'
        ])
        
        multi_day_df = pd.DataFrame({
            'open': [100, 101, 103, 105, 106, 108, 110, 111, 113],
            'high': [102, 103, 105, 107, 108, 110, 112, 113, 115],
            'low':  [99, 100, 102, 104, 105, 107, 109, 110, 112],
            'close': [101, 102, 104, 106, 107, 109, 111, 112, 114],
            'volume': [1000, 1100, 1300, 1400, 1500, 1700, 1800, 1900, 2100],
            'market_session': ['regular', 'regular', 'regular', 'regular', 'regular', 'regular', 'regular', 'regular', 'regular']
        }, index=date_index)
        
        # Create entries at the beginning of each day
        entries = pd.Series([
            True, False, False,  # Entry on day 1
            True, False, False,  # Entry on day 2
            True, False, False   # Entry on day 3
        ], index=date_index)
        
        # Set risk and stop parameters
        risk_per_trade = 1.0  # 1% risk per trade
        stop_perc = 0.02     # 2% stop loss
        
        # Mock data returned from setup_backtest
        mock_setup.return_value = {
            'run_id': 42,
            'df': multi_day_df,
            'entries': entries,
            'entry_config': {'field': 'tightness', 'signal': 'Ultra Tight', 'direction': 'long'},
            'direction': 'long',
            'stop_config': {'stop_type': 'perc', 'stop_value': stop_perc},
            'risk_config': {'risk_per_trade': risk_per_trade, 'max_daily_risk': 5.0},
            'exit_config': {'type': 'fixed', 'risk_reward': 2.0},
            'swing_config': {'swings_allowed': 0},  # No swing trading
            'exits_swings_config': None,
            'symbol': 'QQQ'
        }
        
        # Record the trades that would be created
        recorded_trades = []
        
        # Mock format_trades to capture the trades
        def side_effect_format(trades, *args, **kwargs):
            nonlocal recorded_trades
            recorded_trades = trades
            return pd.DataFrame(trades)
        
        mock_format.side_effect = side_effect_format
        
        # Call the function
        result = run_backtest('dummy_config.json')
        
        # Assert that the function returned True (success)
        self.assertTrue(result)
        
        # Verify trades were created (should be 3 entry points)
        self.assertEqual(len(recorded_trades), 3, "Expected 3 trades (one for each day)")
        
        # Verify each trade has a valid position_size, risk_size, and capital_required
        for i, trade in enumerate(recorded_trades):
            # Get trade details
            entry_price = trade['entry_price']
            stop_price = trade['stop_price']
            
            # Expected calculations
            expected_risk_amount = init_cash * risk_per_trade / 100  # in dollars
            expected_price_diff = abs(entry_price - stop_price)     # dollar per share risk
            expected_position_size = round(expected_risk_amount / expected_price_diff)  # shares to trade
            expected_risk_size = expected_position_size * expected_price_diff  # actual dollar risk
            expected_capital_required = expected_position_size * entry_price   # total capital needed
            
            # Important assertions
            self.assertGreater(trade['position_size'], 0, 
                               f"Trade {i+1}: Position size should be greater than 0, got {trade['position_size']}")
                               
            self.assertGreater(trade['risk_size'], 0,
                               f"Trade {i+1}: Risk size should be greater than 0, got {trade['risk_size']}")
                               
            self.assertGreater(trade['capital_required'], 0,
                               f"Trade {i+1}: Capital required should be greater than 0, got {trade['capital_required']}")
            
            # Verify calculations are correct
            self.assertEqual(trade['position_size'], expected_position_size, 
                             f"Trade {i+1}: Position size incorrect. Expected {expected_position_size}, got {trade['position_size']}")
                             
            self.assertAlmostEqual(trade['risk_size'], expected_risk_size, delta=1.0,
                                  msg=f"Trade {i+1}: Risk size incorrect. Expected ~{expected_risk_size}, got {trade['risk_size']}")
                                  
            self.assertEqual(trade['capital_required'], expected_capital_required,
                            f"Trade {i+1}: Capital required incorrect. Expected {expected_capital_required}, got {trade['capital_required']}")
        
        # Verify log_trades_to_db received the correct data
        self.assertTrue(mock_log.called, "log_trades_to_db was not called")
        df_arg = mock_log.call_args[0][0]  # Get the first DataFrame argument
        
        # Check all trades in the DataFrame
        for i in range(len(df_arg)):
            self.assertGreater(df_arg['position_size'].iloc[i], 0, f"Trade {i+1}: position_size is zero in DataFrame to DB")
            self.assertGreater(df_arg['risk_size'].iloc[i], 0, f"Trade {i+1}: risk_size is zero in DataFrame to DB")
            self.assertGreater(df_arg['capital_required'].iloc[i], 0, f"Trade {i+1}: capital_required is zero in DataFrame to DB")

    @patch('sqlite3.connect')
    def test_position_metrics_in_database(self, mock_connect):
        """Test that position metrics (position_size, risk_size, capital_required) are correctly passed to the database."""
        # Set up mock connection and cursor
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (0,)  # No existing trades
        
        # Create sample trades with explicit position metrics
        test_trades = [
            {
                'entry_timestamp': pd.Timestamp('2023-01-02 10:00:00').strftime('%Y-%m-%d %H:%M:%S'),
                'exit_timestamp': pd.Timestamp('2023-01-02 14:30:00').strftime('%Y-%m-%d %H:%M:%S'),
                'entry_price': 100.0,
                'exit_price': 105.0,
                'stop_price': 98.0,
                'position_size': 50,  # Explicitly set
                'risk_size': 100.0,   # Explicitly set
                'risk_per_trade': 100.0,  # 1% * 100
                'risk_reward': 2.5,
                'perc_return': 250.0,
                'winning_trade': 1,  # Add winning_trade field
                'trade_duration': 4.5,  # Add trade_duration field
                'capital_required': 5000.0,  # Explicitly set
                'direction': 'long',
                'exit_type': 'Take Profit'
            }
        ]
        
        # Convert to DataFrame
        trades_df = pd.DataFrame(test_trades)
        
        # Call the function
        log_trades_to_db(trades_df, 999, 'TEST')
        
        # Get the INSERT statement calls
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if isinstance(call[0][0], str) and "INSERT INTO" in call[0][0]]
        
        # Verify an INSERT was made
        self.assertEqual(len(insert_calls), 1, "Should execute INSERT once")
        
        # Get the parameters passed to the INSERT statement
        insert_params = insert_calls[0][0][1]  # Second argument of the first call
        
        # Check position metrics were passed correctly
        # Parameters are in the order defined in the INSERT statement
        # position_size is the 8th parameter (0-indexed)
        # risk_size is the 9th parameter
        # capital_required is the 15th parameter
        self.assertEqual(insert_params[7], 50, "position_size not passed correctly to INSERT")
        self.assertEqual(insert_params[8], 100.0, "risk_size not passed correctly to INSERT")
        self.assertEqual(insert_params[14], 5000.0, "capital_required not passed correctly to INSERT")
        
        # Verify commit was called
        mock_connect.return_value.commit.assert_called_once()

    @patch('backtests.backtest_runner.log_trades_to_db')
    @patch('backtests.backtest_runner.format_trades')
    @patch('backtests.backtest_runner.setup_backtest')
    @patch('backtests.backtest_runner.load_backtest_config')
    def test_position_metrics_end_to_end(self, mock_load_config, mock_setup, mock_format, mock_log):
        """Test that position metrics are correctly calculated and passed through the entire pipeline."""
        # Set up the mocks
        mock_load_config.return_value = (
            'QQQ', 1, 3, 1, 1, 1, None, 
            {"start": "2023-01-01", "end": "2023-01-03"},
            {'init_cash': 10000}  # Explicit init_cash
        )
        
        # Create test data
        test_df = pd.DataFrame({
            'open': [100, 101, 102, 103],
            'high': [102, 103, 104, 105],
            'low': [99, 100, 101, 102],
            'close': [101, 102, 103, 104],
            'volume': [1000, 1100, 1200, 1300],
            'market_session': ['regular', 'regular', 'regular', 'regular']
        }, index=pd.date_range(start='2023-01-01 09:30:00', periods=4, freq='H'))
        
        # Create entries
        entries = pd.Series([True, False, False, False], index=test_df.index)
        
        # Mock setup_backtest
        mock_setup.return_value = {
            'run_id': 42,
            'df': test_df,
            'entries': entries,
            'entry_config': {'field': 'tightness', 'signal': 'Ultra Tight', 'direction': 'long'},
            'direction': 'long',
            'stop_config': {'stop_type': 'perc', 'stop_value': 0.02},
            'risk_config': {'risk_per_trade': 1.0, 'max_daily_risk': 5.0},
            'exit_config': {'type': 'fixed', 'risk_reward': 2.0},
            'swing_config': {'swings_allowed': 1},
            'exits_swings_config': None,
            'symbol': 'QQQ'
        }
        
        # Create a spy for format_trades to capture the trades before they're formatted
        original_format_trades = format_trades
        raw_trades = []
        
        def format_trades_spy(trades, *args, **kwargs):
            nonlocal raw_trades
            raw_trades = trades.copy()  # Store a copy of the raw trades
            return original_format_trades(trades, *args, **kwargs)
        
        mock_format.side_effect = format_trades_spy
        
        # Run the backtest
        result = run_backtest('dummy_config.json')
        
        # Verify success
        self.assertTrue(result)
        
        # Verify trades were created
        self.assertGreater(len(raw_trades), 0, "No trades were generated")
        
        # Check position metrics in the raw trades
        for i, trade in enumerate(raw_trades):
            self.assertGreater(trade['position_size'], 0, 
                              f"Trade {i+1}: position_size should be > 0, got {trade['position_size']}")
            self.assertGreater(trade['risk_size'], 0, 
                              f"Trade {i+1}: risk_size should be > 0, got {trade['risk_size']}")
            self.assertGreater(trade['capital_required'], 0, 
                              f"Trade {i+1}: capital_required should be > 0, got {trade['capital_required']}")
        
        # Verify log_trades_to_db was called with the correct data
        self.assertTrue(mock_log.called)
        
        # Get the DataFrame passed to log_trades_to_db
        df_to_db = mock_log.call_args[0][0]
        
        # Check position metrics in the DataFrame passed to the database
        for i in range(len(df_to_db)):
            self.assertGreater(df_to_db['position_size'].iloc[i], 0, 
                              f"Trade {i+1}: position_size is zero in DataFrame to DB")
            self.assertGreater(df_to_db['risk_size'].iloc[i], 0, 
                              f"Trade {i+1}: risk_size is zero in DataFrame to DB")
            self.assertGreater(df_to_db['capital_required'].iloc[i], 0, 
                              f"Trade {i+1}: capital_required is zero in DataFrame to DB")

if __name__ == '__main__':
    unittest.main() 