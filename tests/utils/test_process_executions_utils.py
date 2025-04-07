"""
Tests for process_executions utility functions
"""
import unittest
import sys
import pandas as pd
from unittest.mock import patch, MagicMock

# Import our test utilities from the tests package
from tests import BaseTestCase, print_summary, MockDatabaseConnection

# Import the function to test
from utils.process_executions_utils import process_datetime_fields, identify_trade_ids

# Module specific test fixtures
def create_module_fixtures():
    """Create test fixtures specific to this module's tests"""
    fixtures = {}
    
    # Create sample DataFrame with semicolon delimiter
    fixtures['semicolon_df'] = pd.DataFrame({
        'date/time': ['2024-03-20;10:30:00', '2024-03-21;14:15:00', '2024-03-22;15:45:00'],
        'symbol': ['AAPL', 'MSFT', 'GOOG'],
        'price': [150.50, 300.75, 2500.25]
    })
    
    # Create sample DataFrame with space delimiter
    fixtures['space_df'] = pd.DataFrame({
        'date/time': ['2024-03-20 10:30:00', '2024-03-21 14:15:00', '2024-03-22 15:45:00'],
        'symbol': ['AAPL', 'MSFT', 'GOOG'],
        'price': [150.50, 300.75, 2500.25]
    })
    
    # Create sample DataFrame with invalid format
    fixtures['invalid_datetime_df'] = pd.DataFrame({
        'date/time': ['2024/03/20 10:30:00', '03-21-2024 14:15', 'invalid'],
        'symbol': ['AAPL', 'MSFT', 'GOOG'],
        'price': [150.50, 300.75, 2500.25]
    })
    
    # Create sample DataFrame without date/time column
    fixtures['no_datetime_df'] = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'GOOG'],
        'price': [150.50, 300.75, 2500.25]
    })
    
    # Create sample DataFrame for sorting test (in different order)
    fixtures['unsorted_semicolon_df'] = pd.DataFrame({
        'date/time': ['2024-03-22;15:45:00', '2024-03-20;10:30:00', '2024-03-21;14:15:00'],
        'symbol': ['GOOG', 'AAPL', 'MSFT'],
        'price': [2500.25, 150.50, 300.75]
    })
    
    # Create sample DataFrame for sorting test with space delimiter
    fixtures['unsorted_space_df'] = pd.DataFrame({
        'date/time': ['2024-03-22 15:45:00', '2024-03-20 10:30:00', '2024-03-21 14:15:00'],
        'symbol': ['GOOG', 'AAPL', 'MSFT'],
        'price': [2500.25, 150.50, 300.75]
    })
    
    # Create sample trades DataFrame for identify_trade_ids testing
    fixtures['trades_df'] = pd.DataFrame({
        'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT', 'AAPL'],
        'quantity': [100, -50, 200, -200, -50],  # Positive for buy, negative for sell
        'price': [150.0, 155.0, 250.0, 260.0, 160.0]
    })
    
    return fixtures

class TestProcessExecutionsUtilsImports(BaseTestCase):
    """Test basic imports and module setup"""
    
    def test_imports(self):
        """Test that imports are working correctly"""
        # Check that functions are callable
        try:
            self.assertTrue(callable(process_datetime_fields))
            self.assertTrue(callable(identify_trade_ids))
            self.log_case_result("Functions are callable", True)
        except AssertionError:
            self.log_case_result("Functions are callable", False)
            raise

class TestProcessDatetimeFields(BaseTestCase):
    """Test cases for process_datetime_fields function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
    
    def test_semicolon_delimiter_processing(self):
        """Test processing with semicolon delimiter"""
        # Get test data
        test_df = self.fixtures['semicolon_df'].copy()
        
        # Process data
        result = process_datetime_fields(test_df, datetime_column='date/time')
        
        # Verify date/time processing
        self.assertTrue('execution_timestamp' in result.columns)
        self.assertTrue('date' in result.columns)
        self.assertTrue('time_of_day' in result.columns)
        
        # Check values
        self.assertEqual(result['date'].iloc[0], '2024-03-20')
        self.assertEqual(result['time_of_day'].iloc[0], '10:30:00')
        
        # Verify original DataFrame was not modified
        self.assertNotIn('date', test_df.columns)
        self.assertNotIn('time_of_day', test_df.columns)
        
        # Verify sorting by execution_timestamp
        sorted_dates = ['2024-03-20;10:30:00', '2024-03-21;14:15:00', '2024-03-22;15:45:00']
        self.assertEqual(list(result['execution_timestamp']), sorted_dates)
        
        self.log_case_result("Successfully processes date/time with semicolon delimiter", True)
    
    def test_space_delimiter_processing(self):
        """Test processing with space delimiter"""
        # Get test data
        test_df = self.fixtures['space_df'].copy()
        
        # Process data
        result = process_datetime_fields(test_df, datetime_column='date/time')
        
        # Verify date/time processing
        self.assertTrue('execution_timestamp' in result.columns)
        self.assertTrue('date' in result.columns)
        self.assertTrue('time_of_day' in result.columns)
        
        # Check values
        self.assertEqual(result['date'].iloc[0], '2024-03-20')
        self.assertEqual(result['time_of_day'].iloc[0], '10:30:00')
        
        # Verify sorting by execution_timestamp
        sorted_dates = ['2024-03-20 10:30:00', '2024-03-21 14:15:00', '2024-03-22 15:45:00']
        self.assertEqual(list(result['execution_timestamp']), sorted_dates)
        
        self.log_case_result("Successfully processes date/time with space delimiter", True)
    
    def test_sorting_semicolon_delimiter(self):
        """Test that the function properly sorts by execution_timestamp with semicolon delimiter"""
        # Get test data with unsorted dates
        test_df = self.fixtures['unsorted_semicolon_df'].copy()
        
        # Process data
        result = process_datetime_fields(test_df, datetime_column='date/time')
        
        # Verify sorting by execution_timestamp - should be chronological order
        expected_order = ['2024-03-20;10:30:00', '2024-03-21;14:15:00', '2024-03-22;15:45:00']
        self.assertEqual(list(result['execution_timestamp']), expected_order)
        
        # Verify symbols have been reordered too
        expected_symbols = ['AAPL', 'MSFT', 'GOOG']
        self.assertEqual(list(result['symbol']), expected_symbols)
        
        self.log_case_result("Successfully sorts by execution_timestamp with semicolon delimiter", True)
    
    def test_sorting_space_delimiter(self):
        """Test that the function properly sorts by execution_timestamp with space delimiter"""
        # Get test data with unsorted dates
        test_df = self.fixtures['unsorted_space_df'].copy()
        
        # Process data
        result = process_datetime_fields(test_df, datetime_column='date/time')
        
        # Verify sorting by execution_timestamp - should be chronological order
        expected_order = ['2024-03-20 10:30:00', '2024-03-21 14:15:00', '2024-03-22 15:45:00']
        self.assertEqual(list(result['execution_timestamp']), expected_order)
        
        # Verify symbols have been reordered too
        expected_symbols = ['AAPL', 'MSFT', 'GOOG']
        self.assertEqual(list(result['symbol']), expected_symbols)
        
        self.log_case_result("Successfully sorts by execution_timestamp with space delimiter", True)
    
    def test_invalid_format_handling(self):
        """Test handling of invalid date/time formats"""
        # Get test data
        test_df = self.fixtures['invalid_datetime_df'].copy()
        
        # Process data
        result = process_datetime_fields(test_df, datetime_column='date/time')
        
        # Verify fallback behavior - should still create date and time columns
        self.assertTrue('date' in result.columns)
        self.assertTrue('time_of_day' in result.columns)
        
        # Result should still be sorted by execution_timestamp
        self.assertEqual(result.index.tolist(), sorted(result.index.tolist()))
        
        self.log_case_result("Properly handles invalid date/time formats", True)
    
    def test_missing_column_handling(self):
        """Test handling when the date/time column doesn't exist"""
        # Get test data
        test_df = self.fixtures['no_datetime_df'].copy()
        
        # Process data
        result = process_datetime_fields(test_df, datetime_column='date/time')
        
        # Verify result is empty DataFrame due to missing execution_timestamp
        self.assertTrue(result.empty)
        
        self.log_case_result("Properly handles missing date/time column", True)
    
    def test_custom_column_name(self):
        """Test using a custom column name instead of 'Date/Time'"""
        # Create test data with a custom column name
        test_df = pd.DataFrame({
            'CustomDateTime': ['2024-03-20;10:30:00', '2024-03-21;14:15:00'],
            'symbol': ['AAPL', 'MSFT']
        })
        
        # Process data with custom column name
        result = process_datetime_fields(test_df, datetime_column='CustomDateTime')
        
        # Verify date/time processing
        self.assertTrue('execution_timestamp' in result.columns)
        self.assertTrue('date' in result.columns)
        self.assertTrue('time_of_day' in result.columns)
        
        # Check values
        self.assertEqual(result['date'].iloc[0], '2024-03-20')
        self.assertEqual(result['time_of_day'].iloc[0], '10:30:00')
        
        self.log_case_result("Successfully handles custom column names", True)

class TestIdentifyTradeIds(BaseTestCase):
    """Test cases for identify_trade_ids function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
    
    @patch('utils.process_executions_utils.db')
    def test_db_validation_mode(self, mock_db):
        """Test identify_trade_ids with db_validation=True"""
        # Setup mock database responses
        mock_db.get_max_id.return_value = 10
        mock_db.get_open_positions.return_value = [
            ('AAPL', 50, 5),   # Symbol, volume, trade_id
            ('GOOG', 20, 9)    # Symbol, volume, trade_id
        ]
        
        # Get test data
        test_df = self.fixtures['trades_df'].copy()
        
        # Process data
        result = identify_trade_ids(test_df, db_validation=True)
        
        # Verify db was called
        mock_db.get_max_id.assert_called_once_with("executions", "trade_id")
        mock_db.get_open_positions.assert_called_once()
        
        # Verify required columns exist
        self.assertIn('trade_id', result.columns)
        self.assertIn('open_volume', result.columns)
        self.assertIn('is_entry', result.columns)
        self.assertIn('is_exit', result.columns)
        
        # Check AAPL trade_id assignments (should continue from existing trade_id 5)
        aapl_rows = result[result['symbol'] == 'AAPL']
        self.assertEqual(aapl_rows['trade_id'].iloc[0], 5)  # First AAPL execution continues trade_id 5
        
        # Verify end position for AAPL 
        # Initial position from DB (50) + Executions (100 - 50 - 50) = Final position (50)
        self.assertEqual(aapl_rows['open_volume'].iloc[-1], 50)
        self.assertFalse(aapl_rows['is_exit'].iloc[-1])  # Position is not closed yet
        
        # Check MSFT should get a new trade_id
        msft_rows = result[result['symbol'] == 'MSFT']
        self.assertEqual(msft_rows['trade_id'].iloc[0], 11)  # Should increment from 10
        
        # Verify MSFT position gets opened and closed
        self.assertTrue(msft_rows['is_entry'].iloc[0])
        self.assertTrue(msft_rows['is_exit'].iloc[1])
        
        self.log_case_result("Successfully identifies trade_ids with db validation", True)
    
    @patch('utils.process_executions_utils.db')
    def test_db_validation_with_none_values(self, mock_db):
        """Test identify_trade_ids with db_validation=True returning None values"""
        # Setup mock database to return None values
        mock_db.get_max_id.return_value = None
        mock_db.get_open_positions.return_value = None
        
        # Get test data
        test_df = self.fixtures['trades_df'].copy()
        
        # Process data
        result = identify_trade_ids(test_df, db_validation=True)
        
        # Verify db was called
        mock_db.get_max_id.assert_called_once_with("executions", "trade_id")
        mock_db.get_open_positions.assert_called_once()
        
        # Verify function handled None values properly
        self.assertIn('trade_id', result.columns)
        
        # First trade should start with trade_id 1
        self.assertEqual(result['trade_id'].iloc[0], 1)
        
        self.log_case_result("Successfully handles None database returns", True)
    
    def test_backtest_mode(self):
        """Test identify_trade_ids with db_validation=False (backtest mode)"""
        # Get test data
        test_df = self.fixtures['trades_df'].copy()
        
        # Process data with db_validation=False
        result = identify_trade_ids(test_df, db_validation=False)
        
        # Verify required columns exist
        self.assertIn('trade_id', result.columns)
        self.assertIn('open_volume', result.columns)
        self.assertIn('is_entry', result.columns)
        self.assertIn('is_exit', result.columns)
        
        # The first AAPL trade should be marked as entry and have trade_id 1
        aapl_first = result[result['symbol'] == 'AAPL'].iloc[0]
        self.assertEqual(aapl_first['trade_id'], 1)
        self.assertTrue(aapl_first['is_entry'])
        
        # The first MSFT trade should be marked as entry and have trade_id 2
        msft_first = result[result['symbol'] == 'MSFT'].iloc[0]
        self.assertEqual(msft_first['trade_id'], 2)
        self.assertTrue(msft_first['is_entry'])
        
        # The last AAPL trade should be marked as exit
        aapl_last = result[result['symbol'] == 'AAPL'].iloc[-1]
        self.assertTrue(aapl_last['is_exit'])
        self.assertEqual(aapl_last['open_volume'], 0)
        
        # The last MSFT trade should be marked as exit
        msft_last = result[result['symbol'] == 'MSFT'].iloc[-1]
        self.assertTrue(msft_last['is_exit'])
        self.assertEqual(msft_last['open_volume'], 0)
        
        self.log_case_result("Successfully identifies trade_ids in backtest mode", True)
    
    def test_multiple_trades_same_symbol(self):
        """Test handling multiple sequential trades for the same symbol"""
        # Create test data with multiple complete trades on same symbol
        test_df = pd.DataFrame({
            'symbol': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
            'quantity': [100, -100, 50, -50],  # Two complete trades
            'price': [150.0, 155.0, 160.0, 165.0]
        })
        
        # Process without db validation (backtest mode)
        result = identify_trade_ids(test_df, db_validation=False)
        
        # First trade should be ID 1, second trade should be ID 2
        self.assertEqual(result['trade_id'].iloc[0], 1)  # First entry
        self.assertEqual(result['trade_id'].iloc[1], 1)  # First exit
        self.assertEqual(result['trade_id'].iloc[2], 2)  # Second entry
        self.assertEqual(result['trade_id'].iloc[3], 2)  # Second exit
        
        # Verify entry and exit flags
        self.assertTrue(result['is_entry'].iloc[0])
        self.assertTrue(result['is_exit'].iloc[1])
        self.assertTrue(result['is_entry'].iloc[2])
        self.assertTrue(result['is_exit'].iloc[3])
        
        self.log_case_result("Successfully handles multiple sequential trades", True)

if __name__ == '__main__':
    print("\n🔍 Running tests for process_executions utilities...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary - this will use the built-in test_utils.py functionality
    print_summary() 