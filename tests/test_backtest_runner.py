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
        """Test formatting trade data into a DataFrame."""
        # Since our sample_trades now include winning_trade and trade_duration,
        # we need to create a version without these fields to test format_trades properly
        base_trades = []
        for trade in self.sample_trades:
            base_trade = trade.copy()
            if 'winning_trade' in base_trade:
                del base_trade['winning_trade']
            if 'trade_duration' in base_trade:
                del base_trade['trade_duration']
            base_trades.append(base_trade)
        
        # Call the function with the base trades
        trades_df = format_trades(
            base_trades, 
            self.sample_df, 
            self.sample_stop_config, 
            self.sample_risk_config, 
            self.sample_exit_config, 
            self.sample_swing_config
        )
        
        # Assert that the function returned a DataFrame
        self.assertIsInstance(trades_df, pd.DataFrame)
        
        # Check that all trades were included
        self.assertEqual(len(trades_df), len(base_trades))
        
        # Check that the winning_trade column was added correctly
        self.assertEqual(trades_df.iloc[0]['winning_trade'], 1)  # First trade is winning (risk_reward > 0)
        self.assertEqual(trades_df.iloc[1]['winning_trade'], 0)  # Second trade is losing (risk_reward < 0)
        
        # Check that the trade_duration column was calculated correctly
        # It should be close to 4.5 hours for the first trade
        duration_hrs = (pd.Timestamp('2023-01-02 14:30:00') - pd.Timestamp('2023-01-02 10:00:00')).total_seconds() / 3600
        self.assertAlmostEqual(trades_df.iloc[0]['trade_duration'], duration_hrs, places=1)
    
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

if __name__ == '__main__':
    unittest.main() 