import unittest
import os
import pandas as pd
import json
import sqlite3
import sys
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the modules to test
from backtests.backtest_loader import (
    load_config, 
    load_backtest_config, 
    generate_entry_signals, 
    create_backtest_run,
    load_data_from_db,
    get_configs,
    setup_backtest
)

class TestBacktestLoader(unittest.TestCase):
    """Test cases for the backtest_loader.py module."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a test directory and sample config file
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.sample_config = {
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
        
        # Sample entry config for testing
        self.sample_entry_config = {
            'id': 1,
            'field': 'tightness',
            'signal': 'Ultra Tight',
            'direction': 'long',
            'type': 'entry'
        }
        
        # Sample risk config for testing
        self.sample_risk_config = {
            'id': 1,
            'risk_per_trade': 0.01,
            'max_daily_risk': 0.05,
            'outside_regular_hours_allowed': 1
        }
        
        # Create a sample DataFrame for testing
        self.sample_df = pd.DataFrame({
            'date_and_time': pd.date_range(start='2023-01-01', periods=10, freq='30min'),
            'open': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
            'high': [102, 103, 104, 105, 106, 107, 108, 109, 110, 111],
            'low': [99, 100, 101, 102, 103, 104, 105, 106, 107, 108],
            'close': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
            'volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900],
            'tightness': ['Ultra Tight', 'Tight', 'Normal', 'Tight', 'Ultra Tight', 
                          'Normal', 'Tight', 'Ultra Tight', 'Normal', 'Tight'],
            'market_session': ['regular', 'regular', 'after', 'regular', 'regular',
                              'pre', 'regular', 'regular', 'after', 'regular']
        })
        self.sample_df.set_index('date_and_time', inplace=True)
    
    def test_load_config(self):
        """Test the load_config function."""
        # Mock the open function to return our sample config
        with patch('builtins.open', mock_open(read_data=json.dumps(self.sample_config))):
            # Call the function
            config = load_config('dummy_path.json')
            
            # Assert that the config was loaded correctly
            self.assertEqual(config, self.sample_config)
            self.assertEqual(config['backtest']['symbol'], 'QQQ')
    
    def test_load_config_error(self):
        """Test the load_config function with an error scenario."""
        # Mock the open function to raise an exception
        with patch('builtins.open', side_effect=Exception("File not found")):
            # Call the function
            config = load_config('non_existent_file.json')
            
            # Assert that the function returned None
            self.assertIsNone(config)
    
    def test_load_backtest_config(self):
        """Test the load_backtest_config function."""
        # Mock the load_config function to return our sample config
        with patch('backtests.backtest_loader.load_config', return_value=self.sample_config):
            # Call the function
            result = load_backtest_config('dummy_path.json')
            
            # Assert that the function returned the expected values
            self.assertIsNotNone(result)
            
            # Unpack the result
            symbol, entry_config_id, stoploss_config_id, risk_config_id, exit_config_id, \
            swing_config_id, exits_swings_config_id, date_range, bc = result
            
            # Check each value
            self.assertEqual(symbol, 'QQQ')
            self.assertEqual(entry_config_id, 1)
            self.assertEqual(stoploss_config_id, 3)
            self.assertEqual(risk_config_id, 1)
            self.assertEqual(exit_config_id, 1)
            self.assertEqual(swing_config_id, 1)
            self.assertIsNone(exits_swings_config_id)
            self.assertEqual(date_range, {"start": "2023-01-01", "end": "2023-05-15"})
            self.assertEqual(bc, self.sample_config['backtest'])
    
    def test_load_backtest_config_error(self):
        """Test the load_backtest_config function with an error scenario."""
        # Mock the load_config function to return None
        with patch('backtests.backtest_loader.load_config', return_value=None):
            # Call the function
            result = load_backtest_config('non_existent_file.json')
            
            # Assert that the function returned None
            self.assertIsNone(result)
    
    def test_generate_entry_signals(self):
        """Test the generate_entry_signals function."""
        # Call the function
        entries, direction = generate_entry_signals(self.sample_df, self.sample_entry_config)
        
        # Assert the function returned the expected values
        self.assertIsInstance(entries, pd.Series)
        self.assertEqual(direction, 'long')
        
        # Check that entries are correctly identified
        # In our sample data, 'Ultra Tight' appears at indices 0, 4, and 7
        expected_entries = pd.Series([True, False, False, False, True, False, False, True, False, False], 
                                    index=self.sample_df.index,
                                    name='tightness')  # Set the name to match the generated Series
        pd.testing.assert_series_equal(entries, expected_entries)
    
    def test_generate_entry_signals_missing_field(self):
        """Test the generate_entry_signals function with a missing field."""
        # Create a modified entry config with a non-existent field
        bad_entry_config = self.sample_entry_config.copy()
        bad_entry_config['field'] = 'non_existent_field'
        
        # Call the function
        entries, direction = generate_entry_signals(self.sample_df, bad_entry_config)
        
        # Assert that the function returned an empty series and the correct direction
        self.assertIsInstance(entries, pd.Series)
        self.assertFalse(entries.any())  # All entries should be False
        self.assertEqual(direction, 'long')
    
    @patch('sqlite3.connect')
    def test_create_backtest_run(self, mock_connect):
        """Test the create_backtest_run function."""
        # Set up the mock connection and cursor
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.lastrowid = 42  # Mock run ID
        
        # Call the function
        run_id = create_backtest_run(1, 3, 1, 1, 1)
        
        # Assert that the function returned the expected run ID
        self.assertEqual(run_id, 42)
        
        # Assert that the correct SQL was executed
        mock_cursor.execute.assert_called_once()
        self.assertIn("INSERT INTO backtest_runs", mock_cursor.execute.call_args[0][0])
        
        # Assert that commit was called
        mock_connect.return_value.commit.assert_called_once()
        
        # Assert that close was called
        mock_connect.return_value.close.assert_called_once()
    
    @patch('pandas.read_sql_query')
    @patch('sqlite3.connect')
    def test_load_data_from_db(self, mock_connect, mock_read_sql):
        """Test the load_data_from_db function."""
        # Mock the SQL query to return our sample DataFrame
        mock_read_sql.return_value = self.sample_df.reset_index()
        
        # Call the function
        df, entries, direction = load_data_from_db('QQQ', self.sample_risk_config)
        
        # Assert that the function returned the expected DataFrame
        self.assertIsInstance(df, pd.DataFrame)
        
        # Since we didn't provide entry_config, entries and direction should be None
        self.assertIsNone(entries)
        self.assertIsNone(direction)
        
        # Assert that read_sql_query was called
        mock_read_sql.assert_called_once()
        
        # Assert that close was called
        mock_connect.return_value.close.assert_called_once()
    
    @patch('backtests.backtest_loader.generate_entry_signals')
    @patch('pandas.read_sql_query')
    @patch('sqlite3.connect')
    def test_load_data_from_db_with_entry_config(self, mock_connect, mock_read_sql, mock_generate_signals):
        """Test the load_data_from_db function with entry_config."""
        # Mock the SQL query to return our sample DataFrame
        mock_read_sql.return_value = self.sample_df.reset_index()
        
        # Mock the generate_entry_signals function
        expected_entries = pd.Series([True, False, False, False, True, False, False, True, False, False], 
                                    index=self.sample_df.index,
                                    name='tightness')  # Match the name from generate_entry_signals
        mock_generate_signals.return_value = (expected_entries, 'long')
        
        # Call the function
        df, entries, direction = load_data_from_db('QQQ', self.sample_risk_config, self.sample_entry_config)
        
        # Assert that the function returned the expected values
        self.assertIsInstance(df, pd.DataFrame)
        pd.testing.assert_series_equal(entries, expected_entries)
        self.assertEqual(direction, 'long')
        
        # Assert that generate_entry_signals was called with correct parameters
        mock_generate_signals.assert_called_once()
        self.assertEqual(mock_generate_signals.call_args[0][1], self.sample_entry_config)
    
    @patch('backtests.backtest_loader.get_entry_config')
    @patch('backtests.backtest_loader.get_stoploss_config')
    @patch('backtests.backtest_loader.get_risk_config')
    @patch('backtests.backtest_loader.get_exits_config')
    @patch('backtests.backtest_loader.get_swing_config')
    def test_get_configs(self, mock_get_swing, mock_get_exits, mock_get_risk, 
                        mock_get_stoploss, mock_get_entry):
        """Test the get_configs function."""
        # Set up the mock return values
        mock_get_entry.return_value = {'id': 1, 'field': 'tightness', 'signal': 'Ultra Tight', 'direction': 'long'}
        mock_get_stoploss.return_value = {'id': 3, 'stop_type': 'perc', 'stop_value': 0.01}
        mock_get_risk.return_value = {'id': 1, 'risk_per_trade': 0.01}
        mock_get_exits.return_value = {'id': 1, 'type': 'fixed', 'size_exit': 1.0, 'risk_reward': 2.0}
        mock_get_swing.return_value = {'id': 1, 'swings_allowed': 0}
        
        # Call the function
        result = get_configs(1, 3, 1, 1, 1)
        
        # Assert that the function returned the expected values
        self.assertIsNotNone(result)
        entry_config, stop_config, risk_config, exit_config, swing_config, exits_swings_config = result
        
        # Check each value
        self.assertEqual(entry_config['id'], 1)
        self.assertEqual(stop_config['id'], 3)
        self.assertEqual(risk_config['id'], 1)
        self.assertEqual(exit_config['id'], 1)
        self.assertEqual(swing_config['id'], 1)
        self.assertIsNone(exits_swings_config)
        
        # Assert that each getter was called with correct parameters
        mock_get_entry.assert_called_once_with(1)
        mock_get_stoploss.assert_called_once_with(3)
        mock_get_risk.assert_called_once_with(1)
        mock_get_exits.assert_called_once_with(1)
        mock_get_swing.assert_called_once_with(1)
    
    @patch('backtests.backtest_loader.get_configs')
    @patch('backtests.backtest_loader.create_backtest_run')
    @patch('backtests.backtest_loader.load_data_from_db')
    def test_setup_backtest(self, mock_load_data, mock_create_run, mock_get_configs):
        """Test the setup_backtest function."""
        # Set up the mock return values
        mock_get_configs.return_value = (
            {'id': 1, 'field': 'tightness', 'signal': 'Ultra Tight', 'direction': 'long'},  # entry_config
            {'id': 3, 'stop_type': 'perc', 'stop_value': 0.01},  # stop_config
            {'id': 1, 'risk_per_trade': 0.01},  # risk_config
            {'id': 1, 'type': 'fixed', 'size_exit': 1.0, 'risk_reward': 2.0},  # exit_config
            {'id': 1, 'swings_allowed': 0},  # swing_config
            None  # exits_swings_config
        )
        mock_create_run.return_value = 42  # run_id
        
        # Create a mock for the entries
        expected_entries = pd.Series([True, False, False, False, True, False, False, True, False, False], 
                                    index=self.sample_df.index)
        
        mock_load_data.return_value = (self.sample_df, expected_entries, 'long')
        
        # Call the function
        result = setup_backtest('QQQ', 1, 3, 1, 1, 1)
        
        # Assert that the function returned the expected dictionary
        self.assertIsNotNone(result)
        self.assertEqual(result['run_id'], 42)
        self.assertEqual(result['symbol'], 'QQQ')
        self.assertEqual(result['direction'], 'long')
        
        # Assert that each mock was called with correct parameters
        mock_get_configs.assert_called_once_with(1, 3, 1, 1, 1, None)
        mock_create_run.assert_called_once_with(1, 3, 1, 1, 1, None)
        mock_load_data.assert_called_once()
        self.assertEqual(mock_load_data.call_args[0][0], 'QQQ')

if __name__ == '__main__':
    unittest.main() 