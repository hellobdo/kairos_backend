import unittest
import sys
import os
from unittest.mock import patch, MagicMock, call
import pandas as pd
import sqlite3
from datetime import datetime
import re
from dotenv import load_dotenv

# Import our test utilities from the tests package
from tests import BaseTestCase, print_summary, MockDatabaseConnection

# Set up a mock for ibkr_connection to avoid the actual import error
sys.modules['ibkr_connection'] = MagicMock()

# Set up a mock for the actual function used by cash.py
from ibkr_connection import get_ibkr_flex_data
sys.modules['ibkr_connection'].get_ibkr_flex_data = MagicMock()

# Import the functions we want to test
from analytics.cash import process_ibkr_account, update_accounts_balances, process_account_data

# Cash module specific test fixtures
def create_cash_fixtures():
    """Create test fixtures specific to cash module tests"""
    fixtures = {}
    
    # Sample account mapping data
    fixtures['account_map_df'] = pd.DataFrame({
        'ID': [1, 2, 3],
        'account_external_ID': ['U1234567', 'U7654321', 'U9999999']
    })
    
    # Sample valid cash report data
    fixtures['valid_cash_df'] = pd.DataFrame({
        'ClientAccountID': ['U1234567', 'U7654321'],
        'EndingCash': ['10000.50', '5000.25'],
        'ToDate': ['2023-05-15', '2023-05-15']
    })
    
    # Sample empty DataFrame
    fixtures['empty_df'] = pd.DataFrame()
    
    # Sample invalid data (missing required columns)
    fixtures['missing_account_df'] = pd.DataFrame({
        'EndingCash': ['10000.50'],
        'ToDate': ['2023-05-15']
    })
    
    fixtures['missing_cash_df'] = pd.DataFrame({
        'ClientAccountID': ['U1234567'],
        'ToDate': ['2023-05-15']
    })
    
    fixtures['missing_date_df'] = pd.DataFrame({
        'ClientAccountID': ['U1234567'],
        'EndingCash': ['10000.50']
    })
    
    return fixtures

class TestCashImports(BaseTestCase):
    """Test basic imports and module setup"""
    
    def test_imports(self):
        """Test that imports are working correctly"""
        # Case 1: Check that functions are callable
        try:
            self.assertTrue(callable(process_ibkr_account))
            self.assertTrue(callable(update_accounts_balances))
            self.assertTrue(callable(process_account_data))
            self.log_case_result("Functions are callable", True)
        except AssertionError:
            self.log_case_result("Functions are callable", False)
            raise
        
        # Case 2: Check that modules are accessible
        try:
            self.assertIsNotNone(pd)
            self.assertIsNotNone(datetime)
            self.log_case_result("Modules are accessible", True)
        except AssertionError:
            self.log_case_result("Modules are accessible", False)
            raise
        
        # Case 3: Create a simple DataFrame
        try:
            test_df = pd.DataFrame({'A': [1, 2, 3]})
            self.assertEqual(len(test_df), 3)
            self.log_case_result("pandas DataFrame creation", True)
        except (AssertionError, Exception) as e:
            self.log_case_result(f"pandas DataFrame creation: {str(e)}", False)
            raise
    
    def test_environment_variables(self):
        """Test that environment variables are accessible"""
        # Load environment variables
        load_dotenv()
        
        # Get IBKR environment variables
        token_paper = os.getenv("IBKR_TOKEN_PAPER")
        query_id_paper = os.getenv("IBKR_QUERY_ID_CASH_PAPER")
        token_live = os.getenv("IBKR_TOKEN_LIVE")
        query_id_live = os.getenv("IBKR_QUERY_ID_CASH_LIVE")
        
        # Case 1: Paper trading credentials
        try:
            self.assertIsNotNone(token_paper, "IBKR_TOKEN_PAPER environment variable not set")
            self.assertIsNotNone(query_id_paper, "IBKR_QUERY_ID_CASH_PAPER environment variable not set")
            self.log_case_result("Paper trading credentials", True)
        except AssertionError as e:
            self.log_case_result(f"Paper trading credentials: {str(e)}", False)
            raise
            
        # Case 2: Live trading credentials
        try:
            self.assertIsNotNone(token_live, "IBKR_TOKEN_LIVE environment variable not set")
            self.assertIsNotNone(query_id_live, "IBKR_QUERY_ID_CASH_LIVE environment variable not set")
            self.log_case_result("Live trading credentials", True)
        except AssertionError as e:
            self.log_case_result(f"Live trading credentials: {str(e)}", False)
            raise

class TestProcessIBKRAccount(BaseTestCase):
    """Test cases for process_ibkr_account function"""
    
    @patch('analytics.cash.get_ibkr_flex_data')
    def test_successful_data_retrieval(self, mock_get_ibkr_flex_data):
        """Test successful retrieval of data from IBKR"""
        # Setup mock to return a DataFrame with sample data
        mock_df = pd.DataFrame({
            'ClientAccountID': ['U1234567'],
            'ToDate': ['2023-03-15'],
            'EndingCash': ['10000.00']
        })
        mock_get_ibkr_flex_data.return_value = mock_df
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the function
        result = process_ibkr_account('dummy_token', 'dummy_query_id')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify results
        mock_get_ibkr_flex_data.assert_called_once_with('dummy_token', 'dummy_query_id')
        self.assertIs(result, mock_df)
        output = self.captured_output.get_value()
        self.assertIn("Cash report retrieved from IBKR with 1 rows", output)
        
        self.log_case_result("Successful data retrieval", True)
    
    @patch('analytics.cash.get_ibkr_flex_data')
    def test_api_failure(self, mock_get_ibkr_flex_data):
        """Test handling of API failure"""
        # Setup mock to return False
        mock_get_ibkr_flex_data.return_value = False
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the function
        result = process_ibkr_account('dummy_token', 'dummy_query_id')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify results
        mock_get_ibkr_flex_data.assert_called_once_with('dummy_token', 'dummy_query_id')
        self.assertFalse(result)
        output = self.captured_output.get_value()
        self.assertIn("No data retrieved from IBKR", output)
        
        self.log_case_result("API failure handling", True)
    
    @patch('analytics.cash.get_ibkr_flex_data')
    def test_empty_dataframe(self, mock_get_ibkr_flex_data):
        """Test handling of empty DataFrame"""
        # Setup mock to return empty DataFrame
        mock_get_ibkr_flex_data.return_value = pd.DataFrame()
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the function
        result = process_ibkr_account('dummy_token', 'dummy_query_id')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify results
        mock_get_ibkr_flex_data.assert_called_once_with('dummy_token', 'dummy_query_id')
        self.assertFalse(result)
        output = self.captured_output.get_value()
        self.assertIn("Empty DataFrame returned from IBKR", output)
        
        self.log_case_result("Empty DataFrame handling", True)

class TestUpdateAccountsBalances(BaseTestCase):
    """Test cases for update_accounts_balances function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_cash_fixtures()
    
    @patch('analytics.cash.sqlite3.connect')
    def test_successful_insert(self, mock_connect):
        """Test successful insertion of valid data"""
        # Mock connection and cursor
        mock_conn, mock_cursor = MockDatabaseConnection.create_mock_db()
        mock_connect.return_value = mock_conn
        
        # Setup read_sql patch
        with patch('pandas.read_sql') as mock_read_sql:
            # Define a side_effect function to handle different query patterns
            def read_sql_side_effect(query, conn, params=None):
                if "SELECT ID, account_external_ID" in query:
                    return self.fixtures['account_map_df']
                elif "SELECT 1 FROM accounts_balances" in query:
                    return pd.DataFrame()  # No existing records
                return pd.DataFrame()
            
            mock_read_sql.side_effect = read_sql_side_effect
            
            # Call function
            update_accounts_balances(self.fixtures['valid_cash_df'])
            
            # Verify that read_sql was called at least twice
            self.assertGreaterEqual(mock_read_sql.call_count, 2)
            
            # Verify cursor executemany was called once (batch insert)
            mock_cursor.executemany.assert_called_once()
            
            # Extract parameters from execute calls to verify correct data was inserted
            call_args = mock_cursor.executemany.call_args[0]
            
            # Verify SQL statement
            self.assertIn("INSERT INTO accounts_balances", call_args[0])
            
            # Verify data params (should be a list of tuples)
            data_params = call_args[1]
            self.assertEqual(len(data_params), 2)  # Two records
            
            # Verify account IDs
            account_ids = set(param[0] for param in data_params)  # First item is account_ID
            self.assertEqual(account_ids, {1, 2})
            
            # Verify commit was called
            mock_conn.commit.assert_called_once()
        
        self.log_case_result("Successful insert of valid data", True)
    
    @patch('analytics.cash.sqlite3.connect')
    def test_empty_dataframe(self, mock_connect):
        """Test handling of empty DataFrame"""
        # Call function directly - it should return early without database operations
        update_accounts_balances(self.fixtures['empty_df'])
        
        # Verify connect was not called
        mock_connect.assert_not_called()
        
        self.log_case_result("Properly handles empty DataFrame", True)
    
    @patch('analytics.cash.sqlite3.connect')
    def test_missing_columns(self, mock_connect):
        """Test handling of DataFrames with missing required columns"""
        # Test with missing ClientAccountID
        update_accounts_balances(self.fixtures['missing_account_df'])
        mock_connect.assert_not_called()
        
        # Reset mock
        mock_connect.reset_mock()
        
        # Test with missing EndingCash
        update_accounts_balances(self.fixtures['missing_cash_df'])
        mock_connect.assert_not_called()
        
        # Reset mock
        mock_connect.reset_mock()
        
        # Test with missing ToDate
        update_accounts_balances(self.fixtures['missing_date_df'])
        mock_connect.assert_not_called()
        
        self.log_case_result("Handles DataFrames with missing columns", True)
    
    @patch('analytics.cash.sqlite3.connect')
    def test_duplicate_records(self, mock_connect):
        """Test handling of duplicate records"""
        # Mock connection and cursor
        mock_conn, mock_cursor = MockDatabaseConnection.create_mock_db()
        mock_connect.return_value = mock_conn
        
        # Setup mock for pd.read_sql
        with patch('pandas.read_sql') as mock_read_sql:
            # Prepare existing record
            existing_record = pd.DataFrame({
                '1': [1]  # Just need a non-empty DataFrame
            })
            
            # Define side effect to return account mapping and then existing record
            mock_read_sql.side_effect = [
                self.fixtures['account_map_df'],  # Account mapping
                existing_record                  # Existing record found
            ]
            
            # Create test data with one row
            test_data = pd.DataFrame({
                'ClientAccountID': ['U1234567'],
                'EndingCash': ['10000.50'],
                'ToDate': ['2023-05-15']
            })
            
            # Call function
            update_accounts_balances(test_data)
            
            # Verify the SQL query for checking existing records contains expected parameters
            query_call = mock_read_sql.call_args_list[1]
            self.assertEqual(query_call[1]['params'], [1, '2023-05-15'])
            
            # Verify cursor executemany is not called (no inserts)
            self.assertFalse(hasattr(mock_cursor, 'executemany') and mock_cursor.executemany.called)
            
            # Verify commit not called
            self.assertFalse(mock_conn.commit.called)
        
        self.log_case_result("Properly skips duplicate records", True)
    
    @patch('analytics.cash.sqlite3.connect')
    def test_sql_error_handling(self, mock_connect):
        """Test handling of SQL errors"""
        # Mock connection and cursor
        mock_conn, mock_cursor = MockDatabaseConnection.create_mock_db()
        mock_connect.return_value = mock_conn
        
        # Setup mock for pd.read_sql
        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.side_effect = [
                self.fixtures['account_map_df'],  # Account mapping
                pd.DataFrame()                   # No existing records
            ]
            
            # Make cursor.executemany raise an exception
            mock_cursor.executemany.side_effect = sqlite3.Error("Simulated SQL error")
            
            # Create test data
            test_data = pd.DataFrame({
                'ClientAccountID': ['U1234567'],
                'EndingCash': ['10000.50'],
                'ToDate': ['2023-05-15']
            })
            
            # Call function - we expect the exception to be caught inside the function
            with self.assertRaises(Exception):
                update_accounts_balances(test_data)
            
            # Verify rollback was called
            mock_conn.rollback.assert_called_once()
            
            # Verify close was called
            mock_conn.close.assert_called_once()
        
        self.log_case_result("Properly handles SQL errors", True)

class TestMainBlockExecution(BaseTestCase):
    """Test cases for the main block functionality"""
    
    @patch('analytics.cash.update_accounts_balances')
    @patch('analytics.cash.process_ibkr_account')
    @patch('analytics.cash.os.getenv')
    def test_main_block_error_handling(self, mock_getenv, mock_process_ibkr, mock_update_balances):
        """Test how the process_account_data function handles errors from process_ibkr_account"""
        # Setup mocks for environment variables
        mock_getenv.side_effect = lambda key: {
            'IBKR_TOKEN_PAPER': 'paper_token',
            'IBKR_QUERY_ID_CASH_PAPER': 'paper_query',
            'IBKR_TOKEN_LIVE': 'live_token',
            'IBKR_QUERY_ID_CASH_LIVE': 'live_query'
        }.get(key)
        
        # Setup mock to return False
        mock_process_ibkr.return_value = False
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the process_account_data function
        process_account_data('paper_token', 'paper_query', 'paper')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify results
        output = self.captured_output.get_value()
        self.assertIn("Failed to retrieve cash data for paper account", output)
        
        # Verify update_accounts_balances was not called
        mock_update_balances.assert_not_called()
        
        self.log_case_result("Main block handles False from process_ibkr_account", True)

class TestUtilities(BaseTestCase):
    """Test utility functions and patterns used in cash.py"""
    
    def test_date_format_regex(self):
        """Test the regex pattern used for date validation"""
        # This is the regex pattern used in cash.py
        pattern = r'^\d{4}-\d{2}-\d{2}$'
        
        # Valid dates should match
        valid_dates = [
            '2023-05-15',
            '2023-01-01',
            '2023-12-31'
        ]
        
        for date in valid_dates:
            self.assertTrue(re.match(pattern, date), 
                           f"Valid date {date} should match the pattern")
        
        # Invalid dates should not match
        invalid_dates = [
            '05/16/2023',      # MM/DD/YYYY
            '2023/05/16',      # YYYY/MM/DD
            '05-16-2023',      # MM-DD-YYYY
            '2023-5-16',       # Missing leading zero
            '2023-05-16 14:30' # With time
        ]
        
        for date in invalid_dates:
            self.assertFalse(re.match(pattern, date), 
                            f"Invalid date {date} should not match the pattern")
        
        self.log_case_result("Date format regex validation", True)

if __name__ == '__main__':
    print("\n🔍 Running tests for cash.py...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary
    print_summary() 