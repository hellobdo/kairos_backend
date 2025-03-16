import unittest
import sys
import os
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock, call

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the functions to test
from backtests.backtest_runner import calculate_risk_reward, format_trades, log_trades_to_db, run_backtest, main

class TestBacktestRunner(unittest.TestCase):
    """Test cases for the backtest_runner.py module."""
    
    def test_calculate_risk_reward_long_positive(self):
        """Test calculate_risk_reward for a winning long trade."""
        # Setup test data
        entry_price = 100.0
        exit_price = 110.0
        stop_price = 95.0
        is_long = True
        
        # Expected result: (exit - entry) / (entry - stop) = (110 - 100) / (100 - 95) = 10 / 5 = 2.0
        expected_rr = 2.0
        
        # Call the function
        result = calculate_risk_reward(entry_price, exit_price, stop_price, is_long)
        
        # Assert the result
        self.assertEqual(result, expected_rr, f"Expected R:R of {expected_rr}, got {result}")
    
    def test_calculate_risk_reward_long_negative(self):
        """Test calculate_risk_reward for a losing long trade."""
        # Setup test data
        entry_price = 100.0
        exit_price = 97.0
        stop_price = 95.0
        is_long = True
        
        # Expected result: (exit - entry) / (entry - stop) = (97 - 100) / (100 - 95) = -3 / 5 = -0.6
        expected_rr = -0.6
        
        # Call the function
        result = calculate_risk_reward(entry_price, exit_price, stop_price, is_long)
        
        # Assert the result
        self.assertAlmostEqual(result, expected_rr, places=6, 
                              msg=f"Expected R:R of {expected_rr}, got {result}")
    
    def test_calculate_risk_reward_short_positive(self):
        """Test calculate_risk_reward for a winning short trade."""
        # Setup test data
        entry_price = 100.0
        exit_price = 90.0
        stop_price = 105.0
        is_long = False
        
        # Expected result: (entry - exit) / (stop - entry) = (100 - 90) / (105 - 100) = 10 / 5 = 2.0
        expected_rr = 2.0
        
        # Call the function
        result = calculate_risk_reward(entry_price, exit_price, stop_price, is_long)
        
        # Assert the result
        self.assertEqual(result, expected_rr, f"Expected R:R of {expected_rr}, got {result}")
    
    def test_calculate_risk_reward_short_negative(self):
        """Test calculate_risk_reward for a losing short trade."""
        # Setup test data
        entry_price = 100.0
        exit_price = 103.0
        stop_price = 105.0
        is_long = False
        
        # Expected result: (entry - exit) / (stop - entry) = (100 - 103) / (105 - 100) = -3 / 5 = -0.6
        expected_rr = -0.6
        
        # Call the function
        result = calculate_risk_reward(entry_price, exit_price, stop_price, is_long)
        
        # Assert the result
        self.assertAlmostEqual(result, expected_rr, places=6, 
                              msg=f"Expected R:R of {expected_rr}, got {result}")
    
    def test_format_trades_empty_list(self):
        """Test format_trades with an empty list."""
        # Call the function with an empty list
        result = format_trades([])
        
        # Assert that the result is an empty DataFrame
        self.assertIsInstance(result, pd.DataFrame, "Result should be a pandas DataFrame")
        self.assertTrue(result.empty, "Result should be an empty DataFrame")
    
    def test_format_trades_single_trade(self):
        """Test format_trades with a single trade."""
        # Create a sample trade
        entry_time = pd.Timestamp('2023-01-01 10:00:00')
        exit_time = pd.Timestamp('2023-01-01 14:30:00')
        
        trade = {
            'entry_timestamp': entry_time,
            'exit_timestamp': exit_time,
            'entry_price': 100.0,
            'exit_price': 105.0,
            'stop_price': 98.0,
            'position_size': 50,
            'risk_size': 100.0,
            'risk_per_trade': 1.0,
            'risk_reward': 2.5,
            'perc_return': 2.5,
            'capital_required': 5000.0,
            'direction': 'long',
            'exit_type': 'Take Profit'
        }
        
        # Call the function
        result = format_trades([trade])
        
        # Assert that the result is a DataFrame with one row
        self.assertIsInstance(result, pd.DataFrame, "Result should be a pandas DataFrame")
        self.assertEqual(len(result), 1, "Result should have one row")
        
        # Check that all original fields are preserved
        for key, value in trade.items():
            if key not in ['entry_timestamp', 'exit_timestamp']:  # These are formatted as strings
                self.assertEqual(result[key].iloc[0], value, f"Field {key} should be preserved")
        
        # Check that timestamps are formatted correctly
        self.assertEqual(result['entry_timestamp'].iloc[0], entry_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "Entry timestamp should be formatted as string")
        self.assertEqual(result['exit_timestamp'].iloc[0], exit_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "Exit timestamp should be formatted as string")
        
        # Check that winning_trade is calculated correctly (1 for winning, 0 for losing)
        self.assertEqual(result['winning_trade'].iloc[0], 1, "winning_trade should be 1 for positive risk_reward")
        
        # Check that trade_duration is calculated correctly (in hours)
        expected_duration = (exit_time - entry_time).total_seconds() / 3600
        self.assertEqual(result['trade_duration'].iloc[0], expected_duration,
                        f"trade_duration should be {expected_duration} hours")
    
    def test_format_trades_multiple_trades(self):
        """Test format_trades with multiple trades."""
        # Create sample trades
        trades = [
            {
                'entry_timestamp': pd.Timestamp('2023-01-01 10:00:00'),
                'exit_timestamp': pd.Timestamp('2023-01-01 14:30:00'),
                'entry_price': 100.0,
                'exit_price': 105.0,
                'stop_price': 98.0,
                'position_size': 50,
                'risk_size': 100.0,
                'risk_per_trade': 1.0,
                'risk_reward': 2.5,
                'perc_return': 2.5,
                'capital_required': 5000.0,
                'direction': 'long',
                'exit_type': 'Take Profit'
            },
            {
                'entry_timestamp': pd.Timestamp('2023-01-02 09:30:00'),
                'exit_timestamp': pd.Timestamp('2023-01-02 11:00:00'),
                'entry_price': 105.0,
                'exit_price': 102.0,
                'stop_price': 107.0,
                'position_size': 100,
                'risk_size': 200.0,
                'risk_per_trade': 1.0,
                'risk_reward': -1.5,
                'perc_return': -1.5,
                'capital_required': 10500.0,
                'direction': 'short',
                'exit_type': 'Stop Loss'
            }
        ]
        
        # Call the function
        result = format_trades(trades)
        
        # Assert that the result is a DataFrame with the correct number of rows
        self.assertIsInstance(result, pd.DataFrame, "Result should be a pandas DataFrame")
        self.assertEqual(len(result), 2, "Result should have two rows")
        
        # Check that winning_trade is calculated correctly for each trade
        self.assertEqual(result['winning_trade'].iloc[0], 1, "First trade should be winning (1)")
        self.assertEqual(result['winning_trade'].iloc[1], 0, "Second trade should be losing (0)")
        
        # Check that trade_duration is calculated correctly for each trade
        expected_duration1 = (trades[0]['exit_timestamp'] - trades[0]['entry_timestamp']).total_seconds() / 3600
        expected_duration2 = (trades[1]['exit_timestamp'] - trades[1]['entry_timestamp']).total_seconds() / 3600
        
        self.assertEqual(result['trade_duration'].iloc[0], expected_duration1,
                        f"First trade duration should be {expected_duration1} hours")
        self.assertEqual(result['trade_duration'].iloc[1], expected_duration2,
                        f"Second trade duration should be {expected_duration2} hours")
        
        # Check that timestamps are formatted correctly
        for i in range(2):
            self.assertEqual(result['entry_timestamp'].iloc[i], 
                            trades[i]['entry_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                            f"Entry timestamp for trade {i+1} should be formatted as string")
            self.assertEqual(result['exit_timestamp'].iloc[i], 
                            trades[i]['exit_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                            f"Exit timestamp for trade {i+1} should be formatted as string")
    
    @patch('sqlite3.connect')
    def test_log_trades_to_db_empty_dataframe(self, mock_connect):
        """Test log_trades_to_db with an empty DataFrame."""
        # Create an empty DataFrame
        empty_df = pd.DataFrame()
        
        # Call the function
        log_trades_to_db(empty_df, 42, 'QQQ')
        
        # Verify that no database connection was made
        mock_connect.assert_not_called()
    
    @patch('sqlite3.connect')
    def test_log_trades_to_db_position_size_recalculation(self, mock_connect):
        """Test that missing or zero position_size values are recalculated correctly."""
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock the cursor.fetchone() to return 0 (no existing trades)
        mock_cursor.fetchone.return_value = [0]
        
        # Create a DataFrame with missing position_size
        trades_df = pd.DataFrame([
            {
                'entry_timestamp': '2023-01-01 10:00:00',
                'exit_timestamp': '2023-01-01 14:30:00',
                'entry_price': 100.0,
                'exit_price': 105.0,
                'stop_price': 98.0,
                'position_size': 0,  # Zero position_size
                'risk_size': 100.0,  # But risk_size is set
                'risk_per_trade': 1.0,
                'risk_reward': 2.5,
                'perc_return': 2.5,
                'winning_trade': 1,
                'trade_duration': 4.5,
                'capital_required': 5000.0,
            'direction': 'long',
                'exit_type': 'Take Profit'
            }
        ])
        
        # Make a copy of the DataFrame before calling the function
        original_df = trades_df.copy()
        
        # Call the function
        try:
            log_trades_to_db(trades_df, 42, 'QQQ')
        except Exception as e:
            # Even if there's an error in the database operations,
            # we still want to verify that position_size was recalculated
            pass
        
        # Expected position_size calculation: risk_size / price_diff = 100 / 2 = 50
        expected_position_size = 50
        
        # Verify that position_size was recalculated
        self.assertEqual(trades_df['position_size'].iloc[0], expected_position_size,
                        f"position_size should be recalculated to {expected_position_size}")
        
        # Verify that the database connection was made to the correct path
        mock_connect.assert_called_once_with('data/algos.db')
    
    @patch('sqlite3.connect')
    def test_log_trades_to_db_risk_size_recalculation(self, mock_connect):
        """Test that missing or zero risk_size values are recalculated correctly."""
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock the cursor.fetchone() to return 0 (no existing trades)
        mock_cursor.fetchone.return_value = [0]
        
        # Create a DataFrame with missing risk_size
        trades_df = pd.DataFrame([
            {
                'entry_timestamp': '2023-01-01 10:00:00',
                'exit_timestamp': '2023-01-01 14:30:00',
                'entry_price': 100.0,
                'exit_price': 105.0,
                'stop_price': 98.0,
                'position_size': 50,  # Position size is set
                'risk_size': 0.0,     # But risk_size is zero
                'risk_per_trade': 1.0,
                'risk_reward': 2.5,
                'perc_return': 2.5,
                'winning_trade': 1,
                'trade_duration': 4.5,
                'capital_required': 5000.0,
            'direction': 'long',
                'exit_type': 'Take Profit'
            }
        ])
        
        # Make a copy of the DataFrame before calling the function
        original_df = trades_df.copy()
        
        # Call the function
        try:
            log_trades_to_db(trades_df, 42, 'QQQ')
        except Exception as e:
            # Even if there's an error in the database operations,
            # we still want to verify that risk_size was recalculated
            pass
        
        # Expected risk_size calculation: position_size * price_diff = 50 * 2 = 100
        expected_risk_size = 100.0
        
        # Verify that risk_size was recalculated
        self.assertEqual(trades_df['risk_size'].iloc[0], expected_risk_size,
                        f"risk_size should be recalculated to {expected_risk_size}")

    @patch('sqlite3.connect')
    def test_log_trades_to_db_capital_required_recalculation(self, mock_connect):
        """Test that missing or zero capital_required values are recalculated correctly."""
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock the cursor.fetchone() to return 0 (no existing trades)
        mock_cursor.fetchone.return_value = [0]
        
        # Create a DataFrame with missing capital_required
        trades_df = pd.DataFrame([
            {
                'entry_timestamp': '2023-01-01 10:00:00',
                'exit_timestamp': '2023-01-01 14:30:00',
                'entry_price': 100.0,
                'exit_price': 105.0,
                'stop_price': 98.0,
                'position_size': 50,  # Position size is set
                'risk_size': 100.0,   # Risk size is set
                'risk_per_trade': 1.0,
                'risk_reward': 2.5,
                'perc_return': 2.5,
                'winning_trade': 1,
                'trade_duration': 4.5,
                'capital_required': 0.0,  # But capital_required is zero
                'direction': 'long',
                'exit_type': 'Take Profit'
            }
        ])
        
        # Make a copy of the DataFrame before calling the function
        original_df = trades_df.copy()
        
        # Call the function
        try:
            log_trades_to_db(trades_df, 42, 'QQQ')
        except Exception as e:
            # Even if there's an error in the database operations,
            # we still want to verify that capital_required was recalculated
            pass
        
        # Expected capital_required calculation: position_size * entry_price = 50 * 100 = 5000
        expected_capital_required = 5000.0
        
        # Verify that capital_required was recalculated
        self.assertEqual(trades_df['capital_required'].iloc[0], expected_capital_required,
                        f"capital_required should be recalculated to {expected_capital_required}")
    
    @patch('sqlite3.connect')
    def test_log_trades_to_db_database_insertion(self, mock_connect):
        """Test that trades are correctly inserted into the database."""
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock the cursor.fetchone() to return 0 (no existing trades)
        mock_cursor.fetchone.return_value = [0]
        
        # Create a sample trade DataFrame
        trades_df = pd.DataFrame([
            {
                'entry_timestamp': '2023-01-01 10:00:00',
                'exit_timestamp': '2023-01-01 14:30:00',
                'entry_price': 100.0,
                'exit_price': 105.0,
                'stop_price': 98.0,
                'position_size': 50,
                'risk_size': 100.0,
                'risk_per_trade': 1.0,
                'risk_reward': 2.5,
                'perc_return': 2.5,
                'winning_trade': 1,
                'trade_duration': 4.5,
                'capital_required': 5000.0,
                'direction': 'long',
                'exit_type': 'Take Profit'
            }
        ])
        
        # Call the function
        run_id = 42
        symbol = 'QQQ'
        log_trades_to_db(trades_df, run_id, symbol)
        
        # Verify that the database connection was made
        mock_connect.assert_called_once_with('data/algos.db')
        
        # Verify that the cursor executed the SELECT query to check for existing trades
        mock_cursor.execute.assert_any_call("SELECT COUNT(*) FROM trades WHERE run_id = ?", (run_id,))
        
        # Verify that the cursor executed the INSERT query with the correct parameters
        mock_cursor.execute.assert_any_call("""
                INSERT INTO trades (
                    run_id,
                    symbol,
                    entry_timestamp,
                    exit_timestamp,
                    entry_price,
                    exit_price,
                    stop_price,
                    position_size,
                    risk_size,
                    risk_per_trade,
                    risk_reward,
                    perc_return,
                    winning_trade,
                    trade_duration,
                    capital_required,
                    direction,
                    exit_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                symbol,
                trades_df['entry_timestamp'].iloc[0],
                trades_df['exit_timestamp'].iloc[0],
                float(trades_df['entry_price'].iloc[0]),
                float(trades_df['exit_price'].iloc[0]),
                float(trades_df['stop_price'].iloc[0]),
                int(trades_df['position_size'].iloc[0]),
                float(trades_df['risk_size'].iloc[0]),
                float(trades_df['risk_per_trade'].iloc[0]),
                float(trades_df['risk_reward'].iloc[0]),
                float(trades_df['perc_return'].iloc[0]),
                int(trades_df['winning_trade'].iloc[0]),
                float(trades_df['trade_duration'].iloc[0]),
                float(trades_df['capital_required'].iloc[0]),
                trades_df['direction'].iloc[0],
                trades_df['exit_type'].iloc[0]
            ))
        
        # Verify that the connection was committed and closed
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('sqlite3.connect')
    def test_log_trades_to_db_existing_trades(self, mock_connect):
        """Test that existing trades with the same run_id are deleted before inserting new ones."""
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock the cursor.fetchone() to return 2 (two existing trades)
        mock_cursor.fetchone.return_value = [2]
        
        # Create a sample trade DataFrame
        trades_df = pd.DataFrame([
            {
                'entry_timestamp': '2023-01-01 10:00:00',
                'exit_timestamp': '2023-01-01 14:30:00',
                'entry_price': 100.0,
                'exit_price': 105.0,
                'stop_price': 98.0,
                'position_size': 50,
                'risk_size': 100.0,
                'risk_per_trade': 1.0, 
                'risk_reward': 2.5,
                'perc_return': 2.5,
                'winning_trade': 1,
                'trade_duration': 4.5,
                'capital_required': 5000.0,
                'direction': 'long',
                'exit_type': 'Take Profit'
            }
        ])
        
        # Call the function
        run_id = 42
        symbol = 'QQQ'
        log_trades_to_db(trades_df, run_id, symbol)
        
        # Verify that the cursor executed the SELECT query to check for existing trades
        mock_cursor.execute.assert_any_call("SELECT COUNT(*) FROM trades WHERE run_id = ?", (run_id,))
        
        # Verify that the cursor executed the DELETE query to remove existing trades
        mock_cursor.execute.assert_any_call("DELETE FROM trades WHERE run_id = ?", (run_id,))
        
        # Verify that the cursor executed the INSERT query after deleting existing trades
        self.assertTrue(
            mock_cursor.execute.call_args_list.index(call("DELETE FROM trades WHERE run_id = ?", (run_id,))) <
            mock_cursor.execute.call_args_list.index(call("""
                INSERT INTO trades (
                    run_id,
                    symbol,
                    entry_timestamp,
                    exit_timestamp,
                    entry_price,
                    exit_price,
                    stop_price,
                    position_size,
                    risk_size,
                    risk_per_trade,
                    risk_reward,
                    perc_return,
                    winning_trade,
                    trade_duration,
                    capital_required,
                    direction,
                    exit_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                symbol,
                trades_df['entry_timestamp'].iloc[0],
                trades_df['exit_timestamp'].iloc[0],
                float(trades_df['entry_price'].iloc[0]),
                float(trades_df['exit_price'].iloc[0]),
                float(trades_df['stop_price'].iloc[0]),
                int(trades_df['position_size'].iloc[0]),
                float(trades_df['risk_size'].iloc[0]),
                float(trades_df['risk_per_trade'].iloc[0]),
                float(trades_df['risk_reward'].iloc[0]),
                float(trades_df['perc_return'].iloc[0]),
                int(trades_df['winning_trade'].iloc[0]),
                float(trades_df['trade_duration'].iloc[0]),
                float(trades_df['capital_required'].iloc[0]),
                trades_df['direction'].iloc[0],
                trades_df['exit_type'].iloc[0]
            )))
        )

    @patch('backtests.backtest_runner.load_backtest_config')
    @patch('backtests.backtest_runner.setup_backtest')
    @patch('backtests.backtest_runner.log_trades_to_db')
    def test_run_backtest_loads_and_setup(self, mock_log_trades, mock_setup, mock_load_config):
        """Test that run_backtest properly loads and sets up the backtest."""
        # Mock the configuration loading
        mock_load_config.return_value = (
            'AAPL',  # symbol
            1,       # entry_config_id
            2,       # stoploss_config_id
            3,       # risk_config_id
            4,       # exit_config_id
            5,       # swing_config_id
            6,       # exits_swings_config_id
            '2023',  # date_range
            100000   # init_cash
        )

        # Create mock data for setup_backtest with all required OHLC columns
        mock_df = pd.DataFrame({
            'open': [100, 101, 102, 103, 104],
            'high': [102, 103, 104, 105, 106],
            'low': [99, 100, 101, 102, 103],
            'close': [101, 102, 103, 104, 105],
            'market_session': ['regular'] * 5
        }, index=pd.date_range('2023-01-01', periods=5, freq='D'))
        
        mock_entries = pd.Series([True, False, True, False, False], index=mock_df.index)
        
        mock_setup.return_value = {
            'df': mock_df,
            'entries': mock_entries,
            'direction': 'long',
            'exit_config': {'type': 'fixed', 'risk_reward': 2.0},
            'stop_config': {'stop_type': 'perc', 'stop_value': 0.02},
            'risk_config': {'risk_per_trade': 0.01},
            'swing_config': {'swings_allowed': 1},
            'run_id': 42
        }

        # Run the backtest
        result = run_backtest('config.json')

        # Verify the configuration was loaded
        mock_load_config.assert_called_once_with('config.json')

        # Verify setup_backtest was called with correct parameters
        mock_setup.assert_called_once_with(
            'AAPL', 1, 2, 3, 4, 5, 6, '2023'
        )

        # Assert the function returned True for successful execution
        self.assertTrue(result)

    @patch('backtests.backtest_runner.load_backtest_config')
    @patch('backtests.backtest_runner.setup_backtest')
    @patch('backtests.backtest_runner.log_trades_to_db')
    def test_run_backtest_position_management(self, mock_log_trades, mock_setup, mock_load_config):
        """Test that run_backtest properly manages positions and creates trade records."""
        # Mock configuration loading
        mock_load_config.return_value = (
            'AAPL', 1, 2, 3, 4, 5, 6, '2023', 100000
        )

        # Create mock data with specific test scenarios and all required OHLC columns
        mock_df = pd.DataFrame({
            'open': [100, 98, 102, 97, 105],
            'high': [102, 100, 104, 99, 107],
            'low': [98, 96, 100, 95, 103],
            'close': [101, 98, 102, 97, 105],
            'market_session': ['regular'] * 5
        }, index=pd.date_range('2023-01-01', periods=5, freq='D'))
        
        # Entry signals on day 1 and day 3
        mock_entries = pd.Series([True, False, True, False, False], index=mock_df.index)
        
        mock_setup.return_value = {
            'df': mock_df,
            'entries': mock_entries,
            'direction': 'long',
            'exit_config': {'type': 'fixed', 'risk_reward': 2.0},
            'stop_config': {'stop_type': 'perc', 'stop_value': 0.02},
            'risk_config': {'risk_per_trade': 0.01},
            'swing_config': {'swings_allowed': 1},
            'run_id': 42
        }

        # Run the backtest
        result = run_backtest('config.json')

        # Verify trades were logged
        self.assertTrue(mock_log_trades.called)
        
        # Get the trades DataFrame that was passed to log_trades_to_db
        trades_df = mock_log_trades.call_args[0][0]
        
        # Verify trade records were created
        self.assertGreater(len(trades_df), 0, "Should have created at least one trade")
        
        # Verify trade fields
        required_fields = [
            'entry_timestamp', 'exit_timestamp', 'entry_price', 'exit_price',
            'stop_price', 'position_size', 'risk_size', 'risk_per_trade',
            'risk_reward', 'perc_return', 'direction', 'exit_type'
        ]
        for field in required_fields:
            self.assertIn(field, trades_df.columns, f"Trade record should include {field}")

    @patch('backtests.backtest_runner.load_backtest_config')
    @patch('backtests.backtest_runner.setup_backtest')
    @patch('backtests.backtest_runner.log_trades_to_db')
    def test_run_backtest_exit_types(self, mock_log_trades, mock_setup, mock_load_config):
        """Test that run_backtest handles different exit types correctly."""
        # Mock configuration loading
        mock_load_config.return_value = (
            'AAPL', 1, 2, 3, 4, 5, 6, '2023', 100000
        )

        # Create mock data to test different exit scenarios with all required OHLC columns
        mock_df = pd.DataFrame({
            'open': [100, 95, 105, 103, 102],
            'high': [102, 97, 107, 105, 104],
            'low': [98, 93, 103, 101, 100],
            'close': [101, 95, 105, 103, 102],
            'market_session': ['regular'] * 5
        }, index=pd.date_range('2023-01-01', periods=5, freq='D'))
        
        mock_entries = pd.Series([True, False, True, False, False], index=mock_df.index)
        
        mock_setup.return_value = {
            'df': mock_df,
            'entries': mock_entries,
            'direction': 'long',
            'exit_config': {'type': 'fixed', 'risk_reward': 2.0},
            'stop_config': {'stop_type': 'perc', 'stop_value': 0.02},
            'risk_config': {'risk_per_trade': 0.01},
            'swing_config': {'swings_allowed': 1},
            'run_id': 42
        }

        # Run the backtest
        result = run_backtest('config.json')

        # Verify trades were logged
        self.assertTrue(mock_log_trades.called)
        
        # Get the trades DataFrame that was passed to log_trades_to_db
        trades_df = mock_log_trades.call_args[0][0]
        
        # Verify different exit types were recorded
        exit_types = trades_df['exit_type'].unique()
        self.assertGreater(len(exit_types), 0, "Should have recorded at least one exit type")
        
        # Check for specific exit types
        expected_exit_types = {'Stop Loss', 'Take Profit', 'End of Day'}
        found_exit_types = set(exit_types)
        self.assertTrue(
            any(exit_type in found_exit_types for exit_type in expected_exit_types),
            "Should have at least one of the expected exit types"
        )

    @patch('backtests.backtest_runner.run_backtest')
    def test_main_success(self, mock_run_backtest):
        """Test main function with successful backtest execution."""
        # Mock run_backtest to return True (success)
        mock_run_backtest.return_value = True
        
        # Run main with default config
        from backtests.backtest_runner import main
        exit_code = main()
        
        # Get the expected config file path
        expected_path = os.path.join(os.path.dirname(os.path.abspath(os.path.join('backtests', 'backtest_runner.py'))), 'backtest_config.json')
        
        # Verify run_backtest was called with correct config path
        mock_run_backtest.assert_called_once_with(expected_path)
        
        # Verify successful exit code
        self.assertEqual(exit_code, 0, "Should return 0 for successful execution")

    @patch('backtests.backtest_runner.run_backtest')
    def test_main_failure(self, mock_run_backtest):
        """Test main function with failed backtest execution."""
        # Mock run_backtest to return False (failure)
        mock_run_backtest.return_value = False
        
        # Run main with default config
        from backtests.backtest_runner import main
        exit_code = main()
        
        # Get the expected config file path
        expected_path = os.path.join(os.path.dirname(os.path.abspath(os.path.join('backtests', 'backtest_runner.py'))), 'backtest_config.json')
        
        # Verify run_backtest was called with correct config path
        mock_run_backtest.assert_called_once_with(expected_path)
        
        # Verify failure exit code
        self.assertEqual(exit_code, 1, "Should return 1 for failed execution")

if __name__ == "__main__":
    # Use a simpler approach - just run the tests and print results
    print("Running tests for backtest runner functions...")
    
    # Suppress unittest output
    result = unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    
    # Print custom summary with checkmarks
    print("\n=== Test Results ===")
    tests_run = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    skipped = len(result.skipped) if hasattr(result, 'skipped') else 0
    
    print(f"Total tests: {tests_run}")
    print(f"Passed: {tests_run - failures - errors - skipped} ✓")
    
    # Print failures and errors with X marks
    if failures > 0:
        print(f"Failed: {failures} ✗")
        for i, failure in enumerate(result.failures, 1):
            print(f"  ✗ {i}. {failure[0]}")
    
    if errors > 0:
        print(f"Errors: {errors} ✗")
        for i, error in enumerate(result.errors, 1):
            print(f"  ✗ {i}. {error[0]}")
            
    if skipped > 0:
        print(f"Skipped: {skipped} ⚠")
        
    # Indicate overall success/failure
    if failures == 0 and errors == 0:
        print("\n✓ All tests passed!")
    else:
        print("\n✗ Some tests failed.")
        
    sys.exit(not result.wasSuccessful()) 