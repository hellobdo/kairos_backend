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
from data.db_utils import DatabaseManager

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
    
    return fixtures

class TestDatabaseManagerImports(BaseTestCase):
    """Test basic imports and module setup"""
    
    def test_imports(self):
        """Test that imports are working correctly"""
        try:
            self.assertTrue(callable(DatabaseManager))
            self.log_case_result("DatabaseManager class is importable", True)
        except AssertionError:
            self.log_case_result("DatabaseManager class is importable", False)
            raise

class TestDatabaseManagerBasics(BaseTestCase):
    """Test basic functionality of DatabaseManager"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
        self.db_manager = DatabaseManager(':memory:')  # Use in-memory DB for tests
    
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

class TestAccountBalanceOperations(BaseTestCase):
    """Test account balance related operations"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
        self.db_manager = DatabaseManager(':memory:')
    
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

class TestExecutionOperations(BaseTestCase):
    """Test execution related operations"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
        self.db_manager = DatabaseManager(':memory:')
    
    def test_get_existing_trade_ids(self):
        """Test getting existing trade IDs"""
        # Setup mock cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [('T123456',), ('T234567',)]
        
        # Mock execute_query
        self.db_manager.execute_query = MagicMock(return_value=mock_cursor)
        
        # Call the method
        result = self.db_manager.get_existing_trade_ids()
        
        # Verify
        self.assertEqual(result, {'T123456', 'T234567'})
        self.db_manager.execute_query.assert_called_once_with(
            "SELECT trade_external_ID FROM executions")
        
        self.log_case_result("get_existing_trade_ids returns correct data", True)
    
    def test_get_max_trade_id(self):
        """Test getting maximum trade ID"""
        # Setup mock cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (5,)
        
        # Mock execute_query
        self.db_manager.execute_query = MagicMock(return_value=mock_cursor)
        
        # Call the method
        result = self.db_manager.get_max_trade_id()
        
        # Verify
        self.assertEqual(result, 5)
        self.db_manager.execute_query.assert_called_once_with(
            "SELECT MAX(trade_id) FROM executions")
        
        # Test when no trades exist
        mock_cursor.fetchone.return_value = (None,)
        result = self.db_manager.get_max_trade_id()
        self.assertEqual(result, 0)  # Should default to 0
        
        self.log_case_result("get_max_trade_id returns correct value", True)
    
    def test_get_open_positions(self):
        """Test getting open positions"""
        # Setup mock cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [('AAPL', 100, 1), ('MSFT', -50, 2)]
        
        # Mock execute_query
        self.db_manager.execute_query = MagicMock(return_value=mock_cursor)
        
        # Call the method
        result = self.db_manager.get_open_positions()
        
        # Verify
        self.assertEqual(result, [('AAPL', 100, 1), ('MSFT', -50, 2)])
        self.db_manager.execute_query.assert_called_once()
        
        # Check if query contains correct SQL
        sql = self.db_manager.execute_query.call_args[0][0]
        self.assertIn("position_sums", sql)
        self.assertIn("SUM(quantity)", sql)
        self.assertIn("HAVING SUM(quantity) != 0", sql)
        
        self.log_case_result("get_open_positions returns correct data", True)
    
    def test_insert_execution(self):
        """Test inserting execution"""
        # Mock execute_query
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        self.db_manager.execute_query = MagicMock(return_value=mock_cursor)
        
        # Call the method
        result = self.db_manager.insert_execution(self.fixtures['execution_data'])
        
        # Verify
        self.assertEqual(result, 1)
        self.db_manager.execute_query.assert_called_once()
        
        # Check if query contains correct SQL
        sql = self.db_manager.execute_query.call_args[0][0]
        self.assertIn("INSERT INTO executions", sql)
        self.assertIn("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", sql)
        
        self.log_case_result("insert_execution inserts correctly", True)

if __name__ == '__main__':
    print("\n🔍 Running tests for db_utils.py...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary
    print_summary() 