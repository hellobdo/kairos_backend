"""
Tests for process_executions module
"""
import unittest
import sys
import os
import tempfile
import pandas as pd
from unittest.mock import patch, MagicMock
import numpy as np

# Import our test utilities from the tests package
from tests import BaseTestCase, print_summary, MockDatabaseConnection

# Import the functions to test
from backtests.utils.process_executions import (
    drop_columns, 
    process_csv, 
    side_follows_qty,
    insert_executions_to_db
)
# Import csv_to_dataframe from pandas_utils since it was moved there
from utils.pandas_utils import csv_to_dataframe
# Import identify_trade_ids for testing the flow
from utils.process_executions_utils import identify_trade_ids

# Module specific test fixtures
def create_module_fixtures():
    """Create test fixtures specific to this module's tests"""
    fixtures = {}
    
    # Create sample CSV data with lowercase column names
    fixtures['valid_csv_data'] = """symbol,price,time,filled_quantity,trade_cost,side,order_type,commission,order_id
AAPL,150.25,2023-01-01;09:30:00,100,15025.00,buy,market,1.0,order1
MSFT,300.50,2023-01-01;10:15:00,50,15025.00,sell,market,1.0,order2
GOOG,2500.75,2023-01-01;11:00:00,10,25007.50,buy,market,1.0,order3"""

    # Create temporary CSV file for testing
    temp_fd, fixtures['temp_csv_path'] = tempfile.mkstemp(suffix='.csv')
    os.write(temp_fd, fixtures['valid_csv_data'].encode())
    os.close(temp_fd)
    
    # Create sample CSV data with columns to drop
    fixtures['csv_with_columns_to_drop'] = """symbol,price,strategy,status,multiplier,time_in_force,asset.strike,asset.multiplier,asset.asset_type,time,filled_quantity,trade_cost,side,order_type,commission,order_id
AAPL,150.25,strategy1,filled,1,day,0,1,stock,2023-01-01;09:30:00,100,15025.00,buy,market,1.0,order1
MSFT,300.50,strategy2,filled,1,day,0,1,stock,2023-01-01;10:15:00,50,15025.00,sell,market,1.0,order2
GOOG,2500.75,strategy3,filled,1,day,0,1,stock,2023-01-01;11:00:00,10,25007.50,buy,market,1.0,order3"""

    # Create temporary CSV file with columns to drop
    temp_fd, fixtures['temp_csv_with_columns_path'] = tempfile.mkstemp(suffix='.csv')
    os.write(temp_fd, fixtures['csv_with_columns_to_drop'].encode())
    os.close(temp_fd)
    
    # Create sample DataFrame with columns to be dropped
    fixtures['df_with_columns'] = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'GOOG'],
        'price': [150.25, 300.50, 2500.75],
        'strategy': ['strategy1', 'strategy2', 'strategy3'],
        'status': ['filled', 'filled', 'filled'],
        'multiplier': [1, 1, 1],
        'time_in_force': ['day', 'day', 'day'],
        'asset.strike': [0, 0, 0],
        'asset.multiplier': [1, 1, 1],
        'asset.asset_type': ['stock', 'stock', 'stock'],
        'time': ['2023-01-01;09:30:00', '2023-01-01;10:15:00', '2023-01-01;11:00:00'],
        'filled_quantity': [100, 50, 10],
        'trade_cost': [15025.00, 15025.00, 25007.50],
        'side': ['buy', 'sell', 'buy'],
        'order_type': ['market', 'market', 'market'],
        'commission': [1.0, 1.0, 1.0],
        'order_id': ['order1', 'order2', 'order3']
    })
    
    # Create sample DataFrame without columns to be dropped
    fixtures['df_without_columns'] = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'GOOG'],
        'price': [150.25, 300.50, 2500.75],
        'time': ['2023-01-01;09:30:00', '2023-01-01;10:15:00', '2023-01-01;11:00:00'],
        'filled_quantity': [100, 50, 10],
        'trade_cost': [15025.00, 15025.00, 25007.50],
        'side': ['buy', 'sell', 'buy'],
        'order_type': ['market', 'market', 'market'],
        'commission': [1.0, 1.0, 1.0],
        'order_id': ['order1', 'order2', 'order3']
    })
    
    # Create sample DataFrame for side_follows_qty testing with lowercase 'side'
    fixtures['df_with_side'] = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'GOOG', 'AMZN'],
        'side': ['BUY', 'sell', 'buy', 'SELL'],
        'filled_quantity': [100, 50, 25, 75]
    })
    
    # Create additional test data with various 'sell' variations
    fixtures['sell_variations_df'] = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA', 'META'],
        'side': ['sell', 'sell', 'sell', 'selling', 'presell', 'sell_to_close'],
        'filled_quantity': [100, 50, 25, 75, 30, 45]
    })
    
    # Create data with various 'buy' variations
    fixtures['buy_variations_df'] = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'GOOG', 'AMZN'],
        'side': ['buy', 'buy', 'buy', 'buying'],
        'filled_quantity': [100, 50, 25, 75]
    })
    
    # Create mock data for process_datetime_fields and identify_trade_ids
    fixtures['datetime_df'] = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'GOOG'],
        'price': [150.25, 300.50, 2500.75],
        'time': ['2023-01-01;09:30:00', '2023-01-01;10:15:00', '2023-01-01;11:00:00'],
        'filled_quantity': [100, 50, 10],
        'trade_cost': [15025.00, 15025.00, 25007.50],
        'execution_timestamp': ['2023-01-01;09:30:00', '2023-01-01;10:15:00', '2023-01-01;11:00:00'],
        'date': ['2023-01-01', '2023-01-01', '2023-01-01'],
        'time_of_day': ['09:30:00', '10:15:00', '11:00:00'],
        'side': ['buy', 'sell', 'buy'],
        'order_type': ['market', 'market', 'market'],
        'commission': [1.0, 1.0, 1.0],
        'order_id': ['order1', 'order2', 'order3']
    })
    
    # Create final result after identify_trade_ids
    fixtures['final_df'] = fixtures['datetime_df'].copy()
    fixtures['final_df']['quantity'] = fixtures['final_df']['filled_quantity'].copy()
    fixtures['final_df']['trade_id'] = [1, 2, 3]
    fixtures['final_df']['is_entry'] = [True, True, True]
    fixtures['final_df']['is_exit'] = [False, False, False]
    
    return fixtures

class TestFunctionImports(BaseTestCase):
    """Test that all functions are properly imported and callable"""
    
    def test_function_imports(self):
        """Test that all functions are properly imported and callable"""
        self.assertTrue(callable(csv_to_dataframe))
        self.assertTrue(callable(drop_columns))
        self.assertTrue(callable(process_csv))
        self.assertTrue(callable(side_follows_qty))
        self.assertTrue(callable(insert_executions_to_db))
        self.log_case_result("All functions are properly imported", True)

class TestDropColumns(BaseTestCase):
    """Test the drop_columns function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
    
    def test_drop_columns(self):
        """Test the drop_columns function"""
        self.existing_columns()
        self.nonexistent_columns()
    
    def existing_columns(self):
        """Test function drops columns that exist in the DataFrame"""
        # Get a copy of the DataFrame with columns to drop
        df = self.fixtures['df_with_columns'].copy()
        
        # Capture stdout to verify print statements
        original_stdout = self.capture_stdout()
        
        # Call the function
        result = drop_columns(df)
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify columns were dropped
        self.assertNotIn('strategy', result.columns)
        self.assertNotIn('status', result.columns)
        self.assertNotIn('multiplier', result.columns)
        self.assertNotIn('time_in_force', result.columns)
        self.assertNotIn('asset.strike', result.columns)
        self.assertNotIn('asset.multiplier', result.columns)
        self.assertNotIn('asset.asset_type', result.columns)
        
        # Verify expected columns remain
        self.assertIn('symbol', result.columns)
        self.assertIn('price', result.columns)
        self.assertIn('filled_quantity', result.columns)
        
        # Verify output message
        output = self.captured_output.get_value()
        self.assertIn("Dropped columns:", output)
        
        self.log_case_result("Successfully drops existing columns", True)
    
    def nonexistent_columns(self):
        """Test function gracefully handles non-existent columns"""
        # Get a copy of the DataFrame without columns to drop
        df = self.fixtures['df_without_columns'].copy()
        
        # Capture stdout to verify print statements
        original_stdout = self.capture_stdout()
        
        # Call the function
        result = drop_columns(df)
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify DataFrame remains unchanged
        self.assertEqual(result.shape, df.shape)
        self.assertEqual(list(result.columns), list(df.columns))
        
        # Verify output message
        output = self.captured_output.get_value()
        self.assertIn("None of the specified columns exist in the DataFrame", output)
        
        self.log_case_result("Gracefully handles non-existent columns", True)

class TestProcessCsv(BaseTestCase):
    """Test the process_csv function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
        
        # Create patches for the utility functions used in process_csv
        self.process_datetime_patcher = patch('backtests.utils.process_executions.process_datetime_fields')
        self.mock_process_datetime = self.process_datetime_patcher.start()
        
        self.identify_trade_ids_patcher = patch('backtests.utils.process_executions.identify_trade_ids')
        self.mock_identify_trade_ids = self.identify_trade_ids_patcher.start()
        
        # Create patch for insert_executions_to_db to isolate our test
        self.insert_executions_patcher = patch('backtests.utils.process_executions.insert_executions_to_db')
        self.mock_insert_executions = self.insert_executions_patcher.start()
        self.mock_insert_executions.return_value = 3  # Default to 3 records inserted
        
        # Add debug tracing for identify_trade_ids
        def trace_identify_trade_ids(df):
            print("\nDEBUG: identify_trade_ids called with DataFrame shape:", df.shape)
            print("DEBUG: DataFrame columns:", df.columns.tolist())
            result = self.fixtures['final_df'].copy()
            print("DEBUG: identify_trade_ids returning DataFrame shape:", result.shape)
            print("DEBUG: identify_trade_ids returning columns:", result.columns.tolist())
            return result
        
        self.mock_identify_trade_ids.side_effect = trace_identify_trade_ids
        
        # Configure default behavior for the mocks using our fixtures
        self.mock_process_datetime.return_value = self.fixtures['datetime_df'].copy()
        
    def tearDown(self):
        """Clean up fixtures"""
        super().tearDown()
        
        # Stop all patchers
        self.process_datetime_patcher.stop()
        self.identify_trade_ids_patcher.stop()
        self.insert_executions_patcher.stop()
        
        # Remove temporary CSV files
        if hasattr(self, 'fixtures'):
            if 'temp_csv_path' in self.fixtures:
                try:
                    os.remove(self.fixtures['temp_csv_path'])
                except:
                    pass
            if 'temp_csv_with_columns_path' in self.fixtures:
                try:
                    os.remove(self.fixtures['temp_csv_with_columns_path'])
                except:
                    pass
    
    def test_process_csv(self):
        """Test the process_csv function"""
        self.successful_processing()
        self.processing_without_columns()
        self.csv_loading_failure()
        self.test_quantity_column_mapping()
    
    def successful_processing(self):
        """Test successful processing of a CSV file with columns to drop"""
        # Capture stdout to verify print statements
        original_stdout = self.capture_stdout()
        
        # Add debug print before the call
        print("\nDEBUG: Before calling process_csv with temp_csv_with_columns_path")
        
        # Call the function with the temp CSV file that has columns to drop
        result = process_csv(self.fixtures['temp_csv_with_columns_path'])
        
        # Add debug print after the call
        print(f"\nDEBUG: After calling process_csv, result: {result}")
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Print captured output for debugging
        print("\nCaptured output during test:")
        print(self.captured_output.get_value())
        
        # Verify the result
        self.assertTrue(result)  # Now expecting True for success
        
        # Verify both mocks were called
        self.mock_process_datetime.assert_called()
        self.mock_identify_trade_ids.assert_called()
        
        # Verify output messages
        output = self.captured_output.get_value()
        self.assertIn("Processing CSV file:", output)
        self.assertIn("CSV loaded successfully", output)
        self.assertIn("Dropped columns:", output)
        
        self.log_case_result("Successfully processes CSV file with columns to drop", True)
    
    def processing_without_columns(self):
        """Test processing a CSV file without columns to drop"""
        # Reset mocks
        self.mock_process_datetime.reset_mock()
        self.mock_identify_trade_ids.reset_mock()
        
        # Capture stdout to verify print statements
        original_stdout = self.capture_stdout()
        
        # Call the function with the temp CSV file that has no columns to drop
        result = process_csv(self.fixtures['temp_csv_path'])
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify the result
        self.assertTrue(result)  # Now expecting True for success
        
        # Verify both mocks were called
        self.mock_process_datetime.assert_called()
        self.mock_identify_trade_ids.assert_called()
        
        # Verify output messages
        output = self.captured_output.get_value()
        self.assertIn("Processing CSV file:", output)
        self.assertIn("CSV loaded successfully", output)
        
        self.log_case_result("Successfully processes CSV file without columns to drop", True)
    
    def csv_loading_failure(self):
        """Test handling of CSV loading failures"""
        # Reset mocks
        self.mock_process_datetime.reset_mock()
        self.mock_identify_trade_ids.reset_mock()
        
        # Capture stdout to verify print statements
        original_stdout = self.capture_stdout()
        
        # Call the function with a non-existent file
        result = process_csv('nonexistent_file.csv')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify the result
        self.assertFalse(result)  # Now expecting False for failure
        
        # Verify mocks were not called
        self.mock_process_datetime.assert_not_called()
        self.mock_identify_trade_ids.assert_not_called()
        
        # Verify output messages
        output = self.captured_output.get_value()
        self.assertIn("Processing CSV file:", output)
        self.assertIn("Error reading CSV file:", output)
        
        self.log_case_result("Properly handles CSV loading failures", True)
    
    def test_quantity_column_mapping(self):
        """Test that filled_quantity is properly mapped to quantity for identify_trade_ids"""
        # Reset mocks
        self.mock_process_datetime.reset_mock()
        self.mock_identify_trade_ids.reset_mock()
        
        # Create captured_df variable
        self.captured_df = None
        
        # Setup the capture of DataFrame passed to identify_trade_ids
        def capture_and_trace_df(df):
            print("\nDEBUG: identify_trade_ids called with DataFrame shape:", df.shape)
            print("DEBUG: DataFrame columns:", df.columns.tolist())
            self.captured_df = df.copy()
            result = self.fixtures['final_df'].copy()
            print("DEBUG: identify_trade_ids returning DataFrame shape:", result.shape)
            return result
        
        self.mock_identify_trade_ids.side_effect = capture_and_trace_df
        
        # Capture stdout 
        original_stdout = self.capture_stdout()
        
        # Call the function
        print("\nDEBUG: Before calling process_csv for quantity_column_mapping test")
        result = process_csv(self.fixtures['temp_csv_path'])
        print(f"\nDEBUG: After calling process_csv, result: {result}")
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Print captured output for debugging
        print("\nCaptured output during test:")
        print(self.captured_output.get_value())
        
        # Verify identify_trade_ids was called
        self.mock_identify_trade_ids.assert_called_once()
        
        # Check that 'quantity' was added to the DataFrame
        self.assertIsNotNone(self.captured_df)
        self.assertIn('quantity', self.captured_df.columns)
        
        # The filled_quantity column should now be renamed to quantity
        # There should no longer be a filled_quantity column
        self.assertNotIn('filled_quantity', self.captured_df.columns)
        
        # Verify the values from original dataframe's filled_quantity are in quantity
        # We need to compare with the values from mock_process_datetime's return value
        expected_values = list(self.fixtures['datetime_df']['filled_quantity'])
        actual_values = list(self.captured_df['quantity'])
        self.assertEqual(expected_values, actual_values)
        
        self.log_case_result("Successfully maps filled_quantity to quantity for identify_trade_ids", True)

class TestSideFollowsQty(BaseTestCase):
    """Test the side_follows_qty function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
    
    def test_standardizes_sides(self):
        """Test that side_follows_qty standardizes sides to 'buy' or 'sell'"""
        # Test with buy variations
        buy_df = self.fixtures['buy_variations_df'].copy()
        buy_result = side_follows_qty(buy_df)
        
        # All sides should be standardized to 'buy'
        for i in range(len(buy_df)):
            self.assertEqual(buy_result.loc[i, 'side'], 'buy')
        
        # Test with sell variations
        sell_df = self.fixtures['sell_variations_df'].copy()
        sell_result = side_follows_qty(sell_df)
        
        # All sides should be standardized to 'sell'
        for i in range(len(sell_df)):
            self.assertEqual(sell_result.loc[i, 'side'], 'sell')
        
        self.log_case_result("Successfully standardizes sides to 'buy' or 'sell'", True)
    
    def test_multiplies_sell_by_negative_one(self):
        """Test that side_follows_qty multiplies filled_quantity by -1 for sell orders"""
        # Get a copy of the DataFrame with side column
        df = self.fixtures['df_with_side'].copy()
        
        # Call the function
        result = side_follows_qty(df)
        
        # Verify sell rows have negative quantities
        self.assertEqual(result.loc[1, 'filled_quantity'], -50)  # sell -> sell
        self.assertEqual(result.loc[3, 'filled_quantity'], -75)  # sell -> sell
        
        # Also verify sides were standardized
        self.assertEqual(result.loc[1, 'side'], 'sell')
        self.assertEqual(result.loc[3, 'side'], 'sell')
        
        self.log_case_result("Successfully multiplies sell orders by -1", True)
    
    def test_leaves_non_sell_orders_unchanged(self):
        """Test that side_follows_qty leaves non-sell orders unchanged"""
        # Get a copy of the DataFrame with side column
        df = self.fixtures['df_with_side'].copy()
        
        # Store original quantities
        buy_qty_0 = df.loc[0, 'filled_quantity']  # buy
        buy_qty_2 = df.loc[2, 'filled_quantity']  # buy
        
        # Call the function
        result = side_follows_qty(df)
        
        # Verify buy rows have unchanged quantities
        self.assertEqual(result.loc[0, 'filled_quantity'], buy_qty_0)  # buy -> buy
        self.assertEqual(result.loc[2, 'filled_quantity'], buy_qty_2)  # buy -> buy
        
        # Also verify sides were standardized
        self.assertEqual(result.loc[0, 'side'], 'buy')
        self.assertEqual(result.loc[2, 'side'], 'buy')
        
        self.log_case_result("Successfully leaves non-sell orders unchanged", True)
    
    def test_handles_various_sell_strings(self):
        """Test that side_follows_qty handles various strings containing 'sell'"""
        # Use the DataFrame with various 'sell' variations
        df = self.fixtures['sell_variations_df'].copy()
        
        # Call the function
        result = side_follows_qty(df)
        
        # All rows should have negative quantities because they all contain 'sell'
        for i in range(len(df)):
            self.assertEqual(result.loc[i, 'filled_quantity'], -df.loc[i, 'filled_quantity'])
            self.assertEqual(result.loc[i, 'side'], 'sell')  # Side should be standardized
        
        self.log_case_result("Successfully handles various strings containing 'sell'", True)
    
    def test_returns_dataframe(self):
        """Test that side_follows_qty returns a DataFrame"""
        # Get a copy of the DataFrame with side column
        df = self.fixtures['df_with_side'].copy()
        
        # Call the function
        result = side_follows_qty(df)
        
        # Verify the result is a DataFrame with the same shape
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, df.shape)
        
        self.log_case_result("Successfully returns a DataFrame", True)

class TestInsertExecutionsToDb(BaseTestCase):
    """Test the insert_executions_to_db function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
        
        # Create a mock database connection
        self.mock_db = MockDatabaseConnection()
        self.db_patcher = patch('backtests.utils.process_executions.db', self.mock_db)
        self.db_patcher.start()
    
    def tearDown(self):
        """Clean up after tests"""
        super().tearDown()
        self.db_patcher.stop()
    
    def test_successful_insertion(self):
        """Test successful insertion of executions into database"""
        # Get a copy of our final DataFrame fixture
        df = self.fixtures['final_df'].copy()
        
        # Add required columns that might be missing
        df['order_id'] = ['order1', 'order2', 'order3']
        df['order_type'] = ['market', 'market', 'market']
        df['commission'] = [1.0, 1.0, 1.0]
        # run_id is optional, so we don't need to add it
        
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
        
        # Add required columns that might be missing
        df['order_id'] = ['order1', 'order2', 'order3']
        df['order_type'] = ['market', 'market', 'market']
        df['commission'] = [1.0, 1.0, 1.0]
        
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

class TestProcessCsvWithDb(BaseTestCase):
    """Test the process_csv function with database integration"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
        
        # Create patches for all the utility functions used in process_csv
        self.process_datetime_patcher = patch('backtests.utils.process_executions.process_datetime_fields')
        self.mock_process_datetime = self.process_datetime_patcher.start()
        
        self.identify_trade_ids_patcher = patch('backtests.utils.process_executions.identify_trade_ids')
        self.mock_identify_trade_ids = self.identify_trade_ids_patcher.start()
        
        # Create a mock database connection with insert_dataframe method
        self.mock_db = MagicMock()
        self.mock_db.insert_dataframe = MagicMock(return_value=3)  # Default to 3 records
        
        # Patch the db object in process_executions.py
        self.db_patcher = patch('backtests.utils.process_executions.db', self.mock_db)
        self.db_patcher.start()
        
        # Add debug tracing for identify_trade_ids
        def trace_identify_trade_ids(df):
            print("\nDEBUG: TestProcessCsvWithDb.identify_trade_ids called with DataFrame shape:", df.shape)
            print("DEBUG: DataFrame columns:", df.columns.tolist())
            
            # Create a result DataFrame with all required columns
            df_with_required = self.fixtures['datetime_df'].copy()
            df_with_required['quantity'] = df_with_required['filled_quantity'].copy()
            df_with_required['order_id'] = ['order1', 'order2', 'order3']
            df_with_required['order_type'] = ['market', 'market', 'market']
            df_with_required['commission'] = [1.0, 1.0, 1.0]
            df_with_required['side'] = ['buy', 'sell', 'buy']
            df_with_required['trade_id'] = [1, 2, 3]
            df_with_required['is_entry'] = [True, True, True]
            df_with_required['is_exit'] = [False, False, False]
            
            print("DEBUG: identify_trade_ids returning DataFrame with columns:", df_with_required.columns.tolist())
            return df_with_required
            
        self.mock_identify_trade_ids.side_effect = trace_identify_trade_ids
        
        # Configure mock_process_datetime to return DataFrame with required columns
        df_with_required = self.fixtures['datetime_df'].copy()
        df_with_required['quantity'] = df_with_required['filled_quantity'].copy()
        df_with_required['order_id'] = ['order1', 'order2', 'order3']
        df_with_required['order_type'] = ['market', 'market', 'market']
        df_with_required['commission'] = [1.0, 1.0, 1.0]
        df_with_required['side'] = ['buy', 'sell', 'buy']
        
        self.mock_process_datetime.return_value = df_with_required
    
    def tearDown(self):
        """Clean up fixtures"""
        super().tearDown()
        
        # Stop all patchers
        self.process_datetime_patcher.stop()
        self.identify_trade_ids_patcher.stop()
        self.db_patcher.stop()
        
        # Remove temporary CSV files
        if hasattr(self, 'fixtures'):
            if 'temp_csv_path' in self.fixtures:
                try:
                    os.remove(self.fixtures['temp_csv_path'])
                except:
                    pass
    
    def test_successful_flow(self):
        """Test successful end-to-end flow with database insertion"""
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the function with run_id
        print("\nDEBUG: Before calling process_csv with run_id=test_run")
        result = process_csv(self.fixtures['temp_csv_path'], run_id='test_run')
        print(f"\nDEBUG: After calling process_csv, result: {result}")
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Print captured output for debugging
        print("\nCaptured output during test:")
        print(self.captured_output.get_value())
        
        # Verify the result is True (successful insertion)
        self.assertTrue(result)
        
        # Verify all mocks were called in the correct order
        self.mock_process_datetime.assert_called_once()
        self.mock_identify_trade_ids.assert_called_once()
        self.mock_db.insert_dataframe.assert_called_once()
        
        # Verify output messages
        output = self.captured_output.get_value()
        self.assertIn("Processing CSV file:", output)
        self.assertIn("CSV loaded successfully", output)
        
        self.log_case_result("Successfully processes CSV and inserts into database", True)
    
    def test_database_error_handling(self):
        """Test handling of database errors during processing"""
        # Configure mock to raise an exception
        self.mock_db.insert_dataframe.side_effect = Exception("Database error")
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call the function with run_id
        print("\nDEBUG: Before calling process_csv with database error")
        result = process_csv(self.fixtures['temp_csv_path'], run_id='test_run')
        print(f"\nDEBUG: After calling process_csv, result: {result}")
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Print captured output for debugging
        print("\nCaptured output during test:")
        print(self.captured_output.get_value())
        
        # Verify the result is False (failed insertion)
        self.assertFalse(result)
        
        # Verify mocks were called
        self.mock_process_datetime.assert_called_once()
        self.mock_identify_trade_ids.assert_called_once()
        self.mock_db.insert_dataframe.assert_called_once()
        
        # Verify output messages
        output = self.captured_output.get_value()
        self.assertIn("Processing CSV file:", output)
        self.assertIn("Error during database insertion:", output)
        
        self.log_case_result("Successfully handles database errors during processing", True)

if __name__ == '__main__':
    print("\n🔍 Running tests for process_executions module...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary using the existing function from test_utils
    print_summary() 