"""
Tests for the database utilities module
"""
import unittest
import sys
import os
from unittest.mock import patch, MagicMock, call
import pandas as pd
import sqlite3
from datetime import datetime
import re

# Import our test utilities from the tests package
from tests import BaseTestCase, print_summary, MockDatabaseConnection

# Import the module to test
from utils.db_utils import DatabaseManager

# Module specific test fixtures
def create_module_fixtures():
    """Create test fixtures specific to this module's tests"""
    fixtures = {}
    
    # Sample account mapping data
    fixtures['account_map_df'] = pd.DataFrame({
        'ID': [1, 2, 3],
        'account_external_ID': ['U1234567', 'U7654321', 'U9999999']
    })
    
    # Sample cash balance data
    fixtures['cash_data'] = [
        (1, '2023-05-15', 10000.50, '2023-05-15 12:00:00'),
        (2, '2023-05-15', 5000.25, '2023-05-15 12:00:00')
    ]
    
    # Sample execution data
    fixtures['execution_data'] = (
        'U1234567',  # accountId
        'T123456',   # trade_external_ID
        'O123456',   # orderID
        'AAPL',      # symbol
        100,         # quantity
        150.50,      # price
        15050.00,    # netCashWithBillable
        '2023-05-15;10:30:00',  # execution_timestamp
        7.50,        # commission
        '2023-05-15',# date
        '10:30:00',  # time_of_day
        'BUY',       # side
        1,           # trade_id
        1,           # is_entry
        0            # is_exit
    )
    
    # Sample executions dataframe
    fixtures['executions_df'] = pd.DataFrame({
        'id': [1, 2, 3],
        'accountId': ['U1234567', 'U1234567', 'U7654321'],
        'trade_external_ID': ['T123456', 'T123457', 'T123458'],
        'symbol': ['AAPL', 'AAPL', 'MSFT'],
        'quantity': [100, -100, 50],
        'price': [150.50, 155.75, 250.25],
        'netCashWithBillable': [15050.00, -15575.00, 12512.50],
        'execution_timestamp': ['2023-05-15;10:30:00', '2023-05-15;14:30:00', '2023-05-16;11:45:00'],
        'commission': [7.50, 7.50, 7.50],
        'date': ['2023-05-15', '2023-05-15', '2023-05-16'],
        'time_of_day': ['10:30:00', '14:30:00', '11:45:00'],
        'side': ['BUY', 'SELL', 'BUY'],
        'trade_id': [1, 1, 2],
        'is_entry': [1, 0, 1],
        'is_exit': [0, 1, 0]
    })
    
    # Sample cash balances dataframe
    fixtures['cash_balances_df'] = pd.DataFrame({
        'account_ID': [1, 2],
        'date': ['2023-05-15', '2023-05-15'],
        'cash_balance': [10000.50, 5000.25]
    })
    
    # Sample trades dataframe
    fixtures['trades_df'] = pd.DataFrame({
        'trade_id': [1, 2],
        'num_executions': [2, 1],
        'symbol': ['AAPL', 'MSFT'],
        'start_date': ['2023-05-15', '2023-05-16'],
        'start_time': ['10:30:00', '11:45:00'],
        'end_date': ['2023-05-15', None],
        'end_time': ['14:30:00', None],
        'duration_hours': [4.0, None],
        'quantity': [100, 50],
        'entry_price': [150.50, 250.25],
        'stop_price': [145.50, 245.25],
        'exit_price': [155.75, None],
        'capital_required': [15050.00, 12512.50],
        'exit_type': ['market', None],
        'risk_reward': [2.05, None],
        'winning_trade': [1, None],
        'perc_return': [1.025, None],
        'week': ['20', '20'],
        'month': ['5', '5'],
        'year': [2023, 2023],
        'account_id': [1, 2],
        'risk_per_trade': [0.005, 0.005],
        'status': ['closed', 'open']
    })
    
    # Sample backtest runs dataframe
    fixtures['backtest_runs_df'] = pd.DataFrame({
        'run_id': [1, 2, 3],
        'timestamp': ['2023-05-10 10:00:00', '2023-05-11 11:00:00', '2023-05-12 12:00:00'],
        'symbols_traded': ['AAPL,MSFT', 'GOOGL', 'AAPL,AMZN'],
        'direction': ['long', 'short', 'long'],
        'indicators': ['macd,rsi', 'macd', 'rsi,ema']
    })
    
    # Sample backtest dict list for non-DataFrame return
    fixtures['backtest_dict_list'] = [
        {'run_id': 1, 'timestamp': '2023-05-10 10:00:00', 'symbols_traded': 'AAPL,MSFT', 'direction': 'long'},
        {'run_id': 2, 'timestamp': '2023-05-11 11:00:00', 'symbols_traded': 'GOOGL', 'direction': 'short'}
    ]
    
    # Sample backtest data for insertion
    fixtures['backtest_data'] = {
        'timestamp': '2023-06-01 10:00:00',
        'indicators': 'macd,rsi,bollinger',
        'symbols_traded': 'AAPL,MSFT',
        'direction': 'long',
        'stop_loss': '0.02',
        'risk_reward': '2.5',
        'risk_per_trade': '0.01',
        'backtest_start_date': '2023-01-01',
        'backtest_end_date': '2023-05-31',
        'source_file': 'test_report.html',
        'is_valid': True
    }
    
    return fixtures

class TestDatabaseUtils(BaseTestCase):
    """Tests for DatabaseManager functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
        self.db_manager = DatabaseManager(':memory:')  # Use in-memory DB for tests
    
    def test_imports(self):
        """Test that imports are working correctly"""
        try:
            self.assertTrue(callable(DatabaseManager))
            self.log_case_result("DatabaseManager class is importable", True)
        except AssertionError:
            self.log_case_result("DatabaseManager class is importable", False)
            raise
    
    @patch('sqlite3.connect')
    def test_connection_context_manager(self, mock_connect):
        """Test the connection context manager"""
        # Setup mock connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Use the context manager
        with self.db_manager.connection() as conn:
            # Verify connection is established
            self.assertEqual(conn, mock_conn)
        
        # Verify connection methods were called
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
        
        self.log_case_result("Connection context manager works correctly", True)
    
    @patch('sqlite3.connect')
    def test_connection_exception_handling(self, mock_connect):
        """Test exception handling in the connection context manager"""
        # Setup mock connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock an exception
        mock_conn.commit.side_effect = sqlite3.Error("Test error")
        
        # Use the context manager with an exception
        try:
            with self.db_manager.connection() as conn:
                pass
            self.fail("Should have raised an exception")
        except sqlite3.Error:
            # Verify rollback was called
            mock_conn.rollback.assert_called_once()
            mock_conn.close.assert_called_once()
        
        self.log_case_result("Connection properly handles exceptions", True)
    
    @patch('pandas.read_sql')
    def test_get_account_map(self, mock_read_sql):
        """Test getting account mapping"""
        # Setup mock
        mock_read_sql.return_value = self.fixtures['account_map_df']
        
        # Call the method
        result = self.db_manager.get_account_map()
        
        # Verify
        self.assertIs(result, self.fixtures['account_map_df'])
        mock_read_sql.assert_called_once()
        self.assertIn("SELECT ID, account_external_ID FROM accounts", 
                     mock_read_sql.call_args[0][0])
        
        self.log_case_result("get_account_map returns correct data", True)
    
    def test_check_balance_exists(self):
        """Test checking if balance exists"""
        # Mock fetch_df to simulate record exists
        self.db_manager.fetch_df = MagicMock(return_value=pd.DataFrame({'1': [1]}))
        
        # Call the method
        result = self.db_manager.check_balance_exists(1, '2023-05-15')
        
        # Verify
        self.assertTrue(result)
        self.db_manager.fetch_df.assert_called_once()
        
        # Mock fetch_df to simulate record doesn't exist
        self.db_manager.fetch_df = MagicMock(return_value=pd.DataFrame())
        
        # Call the method
        result = self.db_manager.check_balance_exists(1, '2023-05-15')
        
        # Verify
        self.assertFalse(result)
        
        self.log_case_result("check_balance_exists works correctly", True)
    
    def test_insert_account_balances(self):
        """Test inserting account balances"""
        # Mock execute_many
        self.db_manager.execute_many = MagicMock(return_value=2)
        
        # Call the method
        result = self.db_manager.insert_account_balances(self.fixtures['cash_data'])
        
        # Verify
        self.assertEqual(result, 2)
        self.db_manager.execute_many.assert_called_once()
        
        # Verify SQL contains expected parts
        sql = self.db_manager.execute_many.call_args[0][0]
        self.assertIn("INSERT INTO accounts_balances", sql)
        self.assertIn("VALUES (?, ?, ?, ?)", sql)
        
        self.log_case_result("insert_account_balances inserts correctly", True)
    
    def test_get_existing_trade_external_ids(self):
        """Test getting existing trade external IDs"""
        # Create a context manager mock to simulate the self.connection() behavior
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [('T123456',), ('T123457',), ('T123458',)]
        
        # Setup the connection context
        with patch.object(self.db_manager, 'connection') as mock_cm:
            # When connection is called, return our mock connection
            mock_cm.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            # Call the method
            result = self.db_manager.get_existing_trade_external_ids()
            
            # Verify
            self.assertEqual(result, {'T123456', 'T123457', 'T123458'})
            
            # Verify connection was used correctly
            mock_cm.assert_called_once()
            mock_conn.cursor.assert_called_once()
            
            # Verify cursor operations
            mock_cursor.execute.assert_called_with("SELECT trade_external_ID FROM executions")
            mock_cursor.fetchall.assert_called_once()
        
        self.log_case_result("get_existing_trade_external_ids returns correct data", True)
    
    def test_get_max_trade_id(self):
        """Test getting maximum trade ID"""
        # Create a context manager mock to simulate the self.connection() behavior
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (5,)
        
        # Setup the connection context
        with patch.object(self.db_manager, 'connection') as mock_cm:
            # When connection is called, return our mock connection
            mock_cm.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            # Call the method
            result = self.db_manager.get_max_trade_id()
            
            # Verify
            self.assertEqual(result, 5)
            
            # Verify connection was used correctly
            mock_cm.assert_called_once()
            mock_conn.cursor.assert_called_once()
            
            # Verify cursor operations
            mock_cursor.execute.assert_called_with("SELECT MAX(trade_id) FROM executions")
            mock_cursor.fetchone.assert_called_once()
        
        # Test when no trades exist
        mock_cursor.fetchone.return_value = (None,)
        
        with patch.object(self.db_manager, 'connection') as mock_cm:
            # When connection is called, return our mock connection
            mock_cm.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            result = self.db_manager.get_max_trade_id()
            self.assertEqual(result, 0)  # Should default to 0
        
        self.log_case_result("get_max_trade_id returns correct value", True)
    
    def test_get_open_positions(self):
        """Test getting open positions"""
        # Create a context manager mock to simulate the self.connection() behavior
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [('AAPL', 100, 1), ('MSFT', -50, 2)]
        
        # Setup the connection context
        with patch.object(self.db_manager, 'connection') as mock_cm:
            # When connection is called, return our mock connection
            mock_cm.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            # Call the method
            result = self.db_manager.get_open_positions()
            
            # Verify
            self.assertEqual(result, [('AAPL', 100, 1), ('MSFT', -50, 2)])
            
            # Verify connection was used correctly
            mock_cm.assert_called_once()
            mock_conn.cursor.assert_called_once()
            
            # Verify cursor operations
            mock_cursor.execute.assert_called_once()
            
            # Check if query contains correct SQL
            sql = mock_cursor.execute.call_args[0][0]
            self.assertIn("FROM trades", sql)
            self.assertIn("WHERE status = 'open'", sql)
            
            mock_cursor.fetchall.assert_called_once()
        
        self.log_case_result("get_open_positions returns correct data", True)
    
    def test_insert_execution(self):
        """Test inserting execution"""
        # Create a context manager mock to simulate the self.connection() behavior
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        
        # Setup the connection context
        with patch.object(self.db_manager, 'connection') as mock_cm:
            # When connection is called, return our mock connection
            mock_cm.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            # Call the method
            result = self.db_manager.insert_execution(self.fixtures['execution_data'])
            
            # Verify
            self.assertEqual(result, 1)
            
            # Verify connection was used correctly
            mock_cm.assert_called_once()
            mock_conn.cursor.assert_called_once()
            
            # Verify cursor operations
            mock_cursor.execute.assert_called_once()
            
            # Check if query contains correct SQL
            sql = mock_cursor.execute.call_args[0][0]
            self.assertIn("INSERT INTO executions", sql)
            self.assertIn("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", sql)
        
        self.log_case_result("insert_execution inserts correctly", True)
        
    def test_get_backtest_runs(self):
        """Test getting backtest runs with various filtering options"""
        # Test 1: Test with dataframe return and no filters
        with patch.object(self.db_manager, 'fetch_df', return_value=self.fixtures['backtest_runs_df']) as mock_fetch_df:
            result = self.db_manager.get_backtest_runs()
            self.assertEqual(result.to_dict(), self.fixtures['backtest_runs_df'].to_dict())
            mock_fetch_df.assert_called_once()
            
            # Verify query has no WHERE clause and has ORDER BY
            query, params = mock_fetch_df.call_args[0]
            self.assertIn("SELECT * FROM backtest_runs", query)
            self.assertIn("ORDER BY timestamp DESC", query)
            self.assertEqual(params, [])
            
        # Test 2: Test with run_id filter
        with patch.object(self.db_manager, 'fetch_df', return_value=self.fixtures['backtest_runs_df'].iloc[[0]]) as mock_fetch_df:
            result = self.db_manager.get_backtest_runs(run_id=1)
            self.assertEqual(result.to_dict(), self.fixtures['backtest_runs_df'].iloc[[0]].to_dict())
            
            # Verify query has correct WHERE clause
            query, params = mock_fetch_df.call_args[0]
            self.assertIn("WHERE run_id = ?", query)
            self.assertEqual(params, [1])
            
        # Test 3: Test with symbol filter
        with patch.object(self.db_manager, 'fetch_df', return_value=self.fixtures['backtest_runs_df'].iloc[[0, 2]]) as mock_fetch_df:
            result = self.db_manager.get_backtest_runs(symbol='AAPL')
            self.assertEqual(result.to_dict(), self.fixtures['backtest_runs_df'].iloc[[0, 2]].to_dict())
            
            # Verify query has correct WHERE clause
            query, params = mock_fetch_df.call_args[0]
            self.assertIn("WHERE symbols_traded LIKE ?", query)
            self.assertEqual(params, ['%AAPL%'])
            
        # Test 4: Test with direction filter
        with patch.object(self.db_manager, 'fetch_df', return_value=self.fixtures['backtest_runs_df'].iloc[[1]]) as mock_fetch_df:
            result = self.db_manager.get_backtest_runs(direction='short')
            self.assertEqual(result.to_dict(), self.fixtures['backtest_runs_df'].iloc[[1]].to_dict())
            
            # Verify query has correct WHERE clause
            query, params = mock_fetch_df.call_args[0]
            self.assertIn("WHERE direction = ?", query)
            self.assertEqual(params, ['short'])
            
        # Test 5: Test with multiple filters
        with patch.object(self.db_manager, 'fetch_df', return_value=self.fixtures['backtest_runs_df'].iloc[[0]]) as mock_fetch_df:
            result = self.db_manager.get_backtest_runs(symbol='AAPL', direction='long')
            self.assertEqual(result.to_dict(), self.fixtures['backtest_runs_df'].iloc[[0]].to_dict())
            
            # Verify query has correct WHERE clause with AND
            query, params = mock_fetch_df.call_args[0]
            self.assertIn("WHERE", query)
            self.assertIn("AND", query)
            self.assertEqual(params, ['%AAPL%', 'long'])
        
        # Test 6: Test with is_valid filter
        with patch.object(self.db_manager, 'fetch_df', return_value=self.fixtures['backtest_runs_df'].iloc[[0, 2]]) as mock_fetch_df:
            result = self.db_manager.get_backtest_runs(is_valid=True)
            self.assertEqual(result.to_dict(), self.fixtures['backtest_runs_df'].iloc[[0, 2]].to_dict())
            
            # Verify query has correct WHERE clause
            query, params = mock_fetch_df.call_args[0]
            self.assertIn("WHERE is_valid = ?", query)
            self.assertEqual(params, [True])
            
        # Test 7: Test with multiple filters including is_valid
        with patch.object(self.db_manager, 'fetch_df', return_value=self.fixtures['backtest_runs_df'].iloc[[0]]) as mock_fetch_df:
            result = self.db_manager.get_backtest_runs(symbol='AAPL', is_valid=True)
            self.assertEqual(result.to_dict(), self.fixtures['backtest_runs_df'].iloc[[0]].to_dict())
            
            # Verify query has correct WHERE clause with AND
            query, params = mock_fetch_df.call_args[0]
            self.assertIn("WHERE", query)
            self.assertIn("AND", query)
            self.assertEqual(params, ['%AAPL%', True])
            
        # Test 8: Test with list of dictionaries return
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_cursor.fetchall.return_value = self.fixtures['backtest_dict_list']
        
        with patch.object(self.db_manager, 'connection') as mock_cm:
            # When connection is called, return our mock connection
            mock_cm.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            # Call with as_df=False
            result = self.db_manager.get_backtest_runs(as_df=False)
            
            # Verify correct SQL and row_factory was set
            self.assertTrue(hasattr(mock_conn, 'row_factory'))
            self.assertEqual(mock_conn.row_factory, sqlite3.Row)
            
            # Verify cursor operations and result conversion
            sql = mock_cursor.execute.call_args[0][0]
            self.assertIn("SELECT * FROM backtest_runs", sql)
            self.assertIn("ORDER BY timestamp DESC", sql)
            mock_cursor.fetchall.assert_called_once()
        
        self.log_case_result("get_backtest_runs works correctly with various filters", True)
    
    def test_save_to_backtest_runs(self):
        """Test saving backtest run data to the database"""
        # Create mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 42  # Simulate the returned run_id
        
        # Setup the connection context
        with patch.object(self.db_manager, 'connection') as mock_cm:
            # When connection is called, return our mock connection
            mock_cm.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            # Call the method
            run_id = self.db_manager.save_to_backtest_runs(self.fixtures['backtest_data'])
            
            # Verify the result
            self.assertEqual(run_id, 42)
            
            # Verify connection was used correctly
            mock_cm.assert_called_once()
            mock_conn.cursor.assert_called_once()
            
            # Verify cursor operations
            mock_cursor.execute.assert_called_once()
            
            # Check if query contains correct SQL
            sql = mock_cursor.execute.call_args[0][0]
            self.assertIn("INSERT INTO backtest_runs", sql)
            self.assertIn("VALUES", sql)
            
            # Verify the data was passed correctly
            data_param = mock_cursor.execute.call_args[0][1]
            self.assertEqual(data_param, self.fixtures['backtest_data'])
            
            # Verify commit was called (important since we modified this in the implementation)
            mock_conn.commit.assert_called_once()
        
        self.log_case_result("save_to_backtest_runs inserts data correctly", True)


if __name__ == '__main__':
    print("\n🔍 Running tests for db_utils.py...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary
    print_summary() 