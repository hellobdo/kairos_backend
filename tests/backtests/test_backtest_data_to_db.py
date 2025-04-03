"""
Tests for backtest_data_to_db module
"""
import unittest
import pandas as pd
from unittest.mock import patch
import numpy as np

# Import our test utilities
from tests import BaseTestCase, print_summary, MockDatabaseConnection

# Import the function to test
from backtests.utils.backtest_data_to_db import insert_executions_to_db

# Module specific test fixtures
def create_module_fixtures():
    """Create test fixtures specific to this module's tests"""
    fixtures = {}
    
    # Create final result DataFrame for testing
    fixtures['final_df'] = pd.DataFrame({
        'execution_timestamp': ['2023-01-01;09:30:00', '2023-01-01;10:15:00', '2023-01-01;11:00:00'],
        'order_id': ['order1', 'order2', 'order3'],
        'symbol': ['AAPL', 'MSFT', 'GOOG'],
        'side': ['buy', 'sell', 'buy'],
        'order_type': ['market', 'market', 'market'],
        'price': [150.25, 300.50, 2500.75],
        'quantity': [100, 50, 10],
        'commission': [1.0, 1.0, 1.0],
        'date': ['2023-01-01', '2023-01-01', '2023-01-01'],
        'time_of_day': ['09:30:00', '10:15:00', '11:00:00'],
        'trade_id': [1, 2, 3],
        'is_entry': [True, True, True],
        'is_exit': [False, False, False]
    })
    
    return fixtures

class TestInsertExecutionsToDb(BaseTestCase):
    """Test the insert_executions_to_db function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
        
        # Create a mock database connection
        self.mock_db = MockDatabaseConnection()
        self.db_patcher = patch('backtests.utils.backtest_data_to_db.db', self.mock_db)
        self.db_patcher.start()
    
    def tearDown(self):
        """Clean up after tests"""
        super().tearDown()
        self.db_patcher.stop()
    
    def test_successful_insertion(self):
        """Test successful insertion of executions into database"""
        # Get a copy of our final DataFrame fixture
        df = self.fixtures['final_df'].copy()
        
        # Configure mock to return number of inserted records
        self.mock_db.insert_dataframe.return_value = len(df)
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the function
        result = insert_executions_to_db(df)
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify the result
        self.assertEqual(result, len(df))
        
        # Verify the mock was called with correct DataFrame structure
        args, _ = self.mock_db.insert_dataframe.call_args
        inserted_df = args[0]
        
        # Check that all required columns are present
        required_columns = [
            'execution_timestamp', 'identifier', 'symbol',
            'side', 'type', 'price', 'quantity', 'trade_cost',
            'date', 'time_of_day', 'trade_id', 'is_entry', 'is_exit', 
            'net_cash_with_billable'
        ]
        for col in required_columns:
            self.assertIn(col, inserted_df.columns)
            
        # Verify that boolean values are correctly converted to integers
        for i in range(len(df)):
            # Check is_entry conversion
            self.assertEqual(inserted_df['is_entry'].iloc[i], 1 if df['is_entry'].iloc[i] else 0)
            # Check is_exit conversion
            self.assertEqual(inserted_df['is_exit'].iloc[i], 1 if df['is_exit'].iloc[i] else 0)
            # Also ensure they are integer type
            self.assertIsInstance(inserted_df['is_entry'].iloc[i], (int, np.int64))
            self.assertIsInstance(inserted_df['is_exit'].iloc[i], (int, np.int64))
        
        # Verify that net_cash_with_billable was calculated correctly
        for i in range(len(df)):
            expected_net_cash = df.iloc[i]['quantity'] * df.iloc[i]['price'] + df.iloc[i]['commission']
            self.assertEqual(inserted_df.iloc[i]['net_cash_with_billable'], expected_net_cash)
        
        # Verify output message
        output = self.captured_output.get_value()
        self.assertIn(f"Successfully inserted {len(df)} records into backtest_executions table", output)
        
        self.log_case_result("Successfully inserts executions into database", True)
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame"""
        # Create empty DataFrame
        empty_df = pd.DataFrame()
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the function
        result = insert_executions_to_db(empty_df)
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify the result
        self.assertEqual(result, 0)
        
        # Verify the mock was not called
        self.mock_db.insert_dataframe.assert_not_called()
        
        self.log_case_result("Successfully handles empty DataFrame", True)
    
    def test_database_error(self):
        """Test handling of database insertion error"""
        # Get a copy of our final DataFrame fixture
        df = self.fixtures['final_df'].copy()
        
        # Configure mock to raise an exception
        self.mock_db.insert_dataframe.side_effect = Exception("Database error")
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the function and expect exception
        with self.assertRaises(Exception) as context:
            insert_executions_to_db(df)
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify error message
        self.assertEqual(str(context.exception), "Database error")
        
        # Verify output message
        output = self.captured_output.get_value()
        self.assertIn("Error inserting backtest executions into database:", output)
        
        self.log_case_result("Successfully handles database errors", True)

if __name__ == '__main__':
    print("\n🔍 Running tests for backtest_data_to_db module...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary using the existing function from test_utils
    print_summary()
