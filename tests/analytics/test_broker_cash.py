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
from api.ibkr import get_ibkr_report
sys.modules['api.ibkr'].get_ibkr_report = MagicMock()

# Import the functions we want to test
from analytics.broker_cash import update_accounts_balances, process_account_data

# Cash module specific test fixtures
def create_cash_fixtures():
    """Create test fixtures specific to cash module tests"""
    fixtures = {}
    
    # Sample account mapping data
    fixtures['account_map_df'] = pd.DataFrame({
        'id': [1, 2, 3],
        'account_external_id': ['U1234567', 'U7654321', 'U9999999']
    })
    
    # Sample valid cash report data - with lowercase column names
    fixtures['valid_cash_df'] = pd.DataFrame({
        'clientaccountid': ['U1234567', 'U7654321'],
        'endingcash': ['10000.50', '5000.25'],
        'todate': ['2023-05-15', '2023-05-15']
    })
    
    # Sample empty DataFrame
    fixtures['empty_df'] = pd.DataFrame()
    
    # Sample invalid data (missing required columns) - with lowercase column names
    fixtures['missing_account_df'] = pd.DataFrame({
        'endingcash': ['10000.50'],
        'todate': ['2023-05-15']
    })
    
    fixtures['missing_cash_df'] = pd.DataFrame({
        'clientaccountid': ['U1234567'],
        'todate': ['2023-05-15']
    })
    
    fixtures['missing_date_df'] = pd.DataFrame({
        'clientaccountid': ['U1234567'],
        'endingcash': ['10000.50']
    })
    
    return fixtures

class TestCashImports(BaseTestCase):
    """Test basic imports and module setup"""
    
    def test_imports(self):
        """Test that imports are working correctly"""
        # Case 1: Check that functions are callable
        try:
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

class TestUpdateAccountsBalances(BaseTestCase):
    """Test cases for update_accounts_balances function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_cash_fixtures()
    
    @patch('analytics.broker_cash.db')
    def test_successful_insert(self, mock_db):
        """Test successful insertion of valid data"""
        # Mock db.get_account_map
        mock_db.get_account_map.return_value = self.fixtures['account_map_df']
        
        # Mock db.check_balance_exists to return False (record doesn't exist)
        mock_db.check_balance_exists.return_value = False
        
        # Mock db.insert_dataframe to return the count of inserted records
        mock_db.insert_dataframe.return_value = 2
        
        # Call function
        result = update_accounts_balances(self.fixtures['valid_cash_df'])
        
        # Verify
        self.assertEqual(result, 2)
        mock_db.get_account_map.assert_called_once()
        self.assertEqual(mock_db.check_balance_exists.call_count, 2)  # Called for each row
        mock_db.insert_dataframe.assert_called_once()
        
        # Verify DataFrame was passed to insert_dataframe
        args, kwargs = mock_db.insert_dataframe.call_args
        self.assertIsInstance(args[0], pd.DataFrame)
        self.assertEqual(args[1], 'accounts_balances')
        self.assertEqual(len(args[0]), 2)  # DataFrame should have 2 rows
        
        self.log_case_result("Successful insert of valid data", True)
    
    @patch('analytics.broker_cash.db')
    def test_empty_dataframe(self, mock_db):
        """Test handling of empty DataFrame"""
        # Call function
        result = update_accounts_balances(self.fixtures['empty_df'])
        
        # Verify
        self.assertEqual(result, 0)
        mock_db.get_account_map.assert_not_called()
        mock_db.insert_dataframe.assert_not_called()
        
        self.log_case_result("Properly handles empty DataFrame", True)
    
    @patch('analytics.broker_cash.db')
    def test_missing_columns(self, mock_db):
        """Test handling of DataFrames with missing required columns"""
        # Test with missing ClientAccountID
        result = update_accounts_balances(self.fixtures['missing_account_df'])
        self.assertEqual(result, 0)
        mock_db.get_account_map.assert_not_called()
        mock_db.insert_dataframe.assert_not_called()
        
        # Reset mock
        mock_db.reset_mock()
        
        # Test with missing EndingCash
        result = update_accounts_balances(self.fixtures['missing_cash_df'])
        self.assertEqual(result, 0)
        mock_db.get_account_map.assert_not_called()
        mock_db.insert_dataframe.assert_not_called()
        
        # Reset mock
        mock_db.reset_mock()
        
        # Test with missing ToDate
        result = update_accounts_balances(self.fixtures['missing_date_df'])
        self.assertEqual(result, 0)
        mock_db.get_account_map.assert_not_called()
        mock_db.insert_dataframe.assert_not_called()
        
        self.log_case_result("Handles DataFrames with missing columns", True)
    
    @patch('analytics.broker_cash.db')
    def test_duplicate_records(self, mock_db):
        """Test handling of duplicate records"""
        # Mock db.get_account_map
        mock_db.get_account_map.return_value = self.fixtures['account_map_df']
        
        # Mock db.check_balance_exists to return True (record exists)
        mock_db.check_balance_exists.return_value = True
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Create test data with one row - using lowercase column names
        test_data = pd.DataFrame({
            'clientaccountid': ['U1234567'],
            'endingcash': ['10000.50'],
            'todate': ['2023-05-15']
        })
        
        # Call function
        result = update_accounts_balances(test_data)
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify
        self.assertEqual(result, 0)
        mock_db.get_account_map.assert_called_once()
        mock_db.check_balance_exists.assert_called_once()
        mock_db.insert_dataframe.assert_not_called()
        
        # Verify the skipped message was printed
        output = self.captured_output.get_value()
        self.assertIn("already exists in database - skipping", output)
        
        self.log_case_result("Properly skips duplicate records and prints message", True)
    
    @patch('analytics.broker_cash.db')
    def test_sql_error_handling(self, mock_db):
        """Test handling of SQL errors"""
        # Mock db.get_account_map
        mock_db.get_account_map.return_value = self.fixtures['account_map_df']
        
        # Mock db.check_balance_exists to return False (record doesn't exist)
        mock_db.check_balance_exists.return_value = False
        
        # Make insert_dataframe raise an exception
        mock_db.insert_dataframe.side_effect = sqlite3.Error("Simulated SQL error")
        
        # Create test data with lowercase column names
        test_data = pd.DataFrame({
            'clientaccountid': ['U1234567'],
            'endingcash': ['10000.50'],
            'todate': ['2023-05-15']
        })
        
        # Call function - we expect the exception to propagate
        with self.assertRaises(Exception):
            update_accounts_balances(test_data)
        
        self.log_case_result("Properly propagates database errors", True)
    
    @patch('analytics.broker_cash.db')
    def test_dataframe_structure(self, mock_db):
        """Test the structure of the DataFrame being sent to insert_dataframe"""
        # Mock db.get_account_map
        mock_db.get_account_map.return_value = self.fixtures['account_map_df']
        
        # Mock db.check_balance_exists to return False (record doesn't exist)
        mock_db.check_balance_exists.return_value = False
        
        # Mock db.insert_dataframe to return a count
        mock_db.insert_dataframe.return_value = 1
        
        # Create test data with one row - using lowercase column names
        test_data = pd.DataFrame({
            'clientaccountid': ['U1234567'],
            'endingcash': ['10000.50'],
            'todate': ['2023-05-15']
        })
        
        # Call function
        update_accounts_balances(test_data)
        
        # Get the DataFrame that was passed to insert_dataframe
        args, kwargs = mock_db.insert_dataframe.call_args
        df_arg = args[0]
        
        # Verify DataFrame structure
        self.assertIsInstance(df_arg, pd.DataFrame)
        self.assertEqual(len(df_arg), 1)
        self.assertListEqual(list(df_arg.columns), ['account_id', 'date', 'cash_balance', 'record_date'])
        self.assertEqual(df_arg['account_id'].iloc[0], 1)
        self.assertEqual(df_arg['date'].iloc[0], '2023-05-15')
        self.assertEqual(df_arg['cash_balance'].iloc[0], 10000.50)
        
        self.log_case_result("DataFrame has correct structure", True)

class TestProcessAccountData(BaseTestCase):
    """Test cases for the process_account_data function"""
    
    @patch('analytics.broker_cash.update_accounts_balances')
    @patch('analytics.broker_cash.get_ibkr_report')
    def test_successful_processing(self, mock_get_ibkr_report, mock_update_balances):
        """Test successful data processing flow"""
        # Setup mock to return a valid DataFrame with lowercase column names
        mock_df = pd.DataFrame({
            'clientaccountid': ['U1234567'],
            'endingcash': ['10000.50'],
            'todate': ['2023-05-15']
        })
        mock_get_ibkr_report.return_value = mock_df
        
        # Mock update_accounts_balances to return a count
        mock_update_balances.return_value = 1
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the process_account_data function
        process_account_data('test_token', 'test_query', 'test')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify
        mock_get_ibkr_report.assert_called_once_with('test_token', 'test_query', 'cash')
        mock_update_balances.assert_called_once_with(mock_df)
        
        # Check output
        output = self.captured_output.get_value()
        self.assertIn("Processing test trading account", output)
        self.assertIn("Updated database with 1 new cash entries", output)
        
        self.log_case_result("Successful data processing flow", True)
    
    @patch('analytics.broker_cash.update_accounts_balances')
    @patch('analytics.broker_cash.get_ibkr_report')
    def test_api_failure(self, mock_get_ibkr_report, mock_update_balances):
        """Test handling when IBKR API returns False"""
        # Setup mock to return False
        mock_get_ibkr_report.return_value = False
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the process_account_data function
        process_account_data('test_token', 'test_query', 'test')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify
        mock_get_ibkr_report.assert_called_once_with('test_token', 'test_query', 'cash')
        mock_update_balances.assert_not_called()
        
        # Check output
        output = self.captured_output.get_value()
        self.assertIn("Failed to retrieve cash data for test account", output)
        
        self.log_case_result("API failure handling", True)
    
    @patch('analytics.broker_cash.update_accounts_balances')
    @patch('analytics.broker_cash.get_ibkr_report')
    def test_no_new_data(self, mock_get_ibkr_report, mock_update_balances):
        """Test handling when no new data is inserted"""
        # Setup mock to return a valid DataFrame with lowercase column names
        mock_df = pd.DataFrame({
            'clientaccountid': ['U1234567'],
            'endingcash': ['10000.50'],
            'todate': ['2023-05-15']
        })
        mock_get_ibkr_report.return_value = mock_df
        
        # Mock update_accounts_balances to return 0 (no records inserted)
        mock_update_balances.return_value = 0
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the process_account_data function
        process_account_data('test_token', 'test_query', 'test')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify
        mock_get_ibkr_report.assert_called_once_with('test_token', 'test_query', 'cash')
        mock_update_balances.assert_called_once_with(mock_df)
        
        # Check output
        output = self.captured_output.get_value()
        self.assertIn("No new cash entries inserted", output)
        
        self.log_case_result("No new data handling", True)
    
    @patch('analytics.broker_cash.update_accounts_balances')
    @patch('analytics.broker_cash.get_ibkr_report')
    def test_exception_handling(self, mock_get_ibkr_report, mock_update_balances):
        """Test exception handling in process_account_data"""
        # Make get_ibkr_report raise an exception
        mock_get_ibkr_report.side_effect = Exception("Test exception")
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the process_account_data function
        process_account_data('test_token', 'test_query', 'test')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify
        mock_get_ibkr_report.assert_called_once_with('test_token', 'test_query', 'cash')
        mock_update_balances.assert_not_called()
        
        # Check output
        output = self.captured_output.get_value()
        self.assertIn("Error processing test account: Test exception", output)
        
        self.log_case_result("Exception handling", True)

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