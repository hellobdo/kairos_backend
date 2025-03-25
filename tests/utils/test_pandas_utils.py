"""
Test module for pandas_utils.py
"""
import unittest
import sys
import os
import tempfile
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

# Import our test utilities from the tests package
from tests import BaseTestCase, print_summary, MockDatabaseConnection

# Import the functions we want to test
from utils.pandas_utils import convert_to_numeric, csv_to_dataframe

# Module specific test fixtures
def create_pandas_utils_fixtures():
    """Create test fixtures specific to pandas_utils tests"""
    fixtures = {}
    
    # Create test DataFrames
    fixtures['numeric_string_df'] = pd.DataFrame({
        'int_col': ['1', '2', '3', 'invalid'],
        'float_col': ['1.5', '2.5', '3.5', 'invalid'],
        'mixed_col': ['1', '2.5', 'invalid', '4'],
        'text_col': ['a', 'b', 'c', 'd']
    })
    
    # Expected results after conversion
    expected_df = fixtures['numeric_string_df'].copy()
    expected_df['int_col'] = pd.to_numeric(expected_df['int_col'], errors='coerce')
    expected_df['float_col'] = pd.to_numeric(expected_df['float_col'], errors='coerce')
    expected_df['mixed_col'] = pd.to_numeric(expected_df['mixed_col'], errors='coerce')
    fixtures['expected_df'] = expected_df
    
    # Create sample CSV data
    fixtures['valid_csv_data'] = """symbol,price,quantity
AAPL,150.25,100
MSFT,300.50,50
GOOG,2500.75,10"""

    # Create temporary CSV file for testing
    temp_fd, fixtures['temp_csv_path'] = tempfile.mkstemp(suffix='.csv')
    os.write(temp_fd, fixtures['valid_csv_data'].encode())
    os.close(temp_fd)
    
    return fixtures

class TestPandasUtilsImports(BaseTestCase):
    """Test basic imports and module setup"""
    
    def test_imports(self):
        """Test that imports are working correctly"""
        # Case 1: Check that functions are callable
        try:
            self.assertTrue(callable(convert_to_numeric))
            self.assertTrue(callable(csv_to_dataframe))
            self.log_case_result("Functions are callable", True)
        except AssertionError:
            self.log_case_result("Functions are callable", False)
            raise

class TestConvertToNumeric(BaseTestCase):
    """Test cases for convert_to_numeric function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_pandas_utils_fixtures()
    
    def test_basic_conversion(self):
        """Test basic numeric conversion functionality"""
        # Get test data
        test_df = self.fixtures['numeric_string_df'].copy()
        numeric_fields = ['int_col', 'float_col', 'mixed_col']
        
        # Run the function
        result_df = convert_to_numeric(test_df, numeric_fields)
        
        # Verify all numeric columns have been converted
        for col in numeric_fields:
            self.assertTrue(pd.api.types.is_numeric_dtype(result_df[col]))
            # Compare with expected values from fixture
            pd.testing.assert_series_equal(result_df[col], self.fixtures['expected_df'][col])
        
        # Verify non-numeric column remained unchanged
        self.assertFalse(pd.api.types.is_numeric_dtype(result_df['text_col']))
        pd.testing.assert_series_equal(result_df['text_col'], test_df['text_col'])
        
        self.log_case_result("Successfully converts numeric fields", True)
    
    def test_missing_columns(self):
        """Test handling of columns not in the DataFrame"""
        # Get test data
        test_df = self.fixtures['numeric_string_df'].copy()
        numeric_fields = ['int_col', 'nonexistent_col']
        
        # Run the function
        result_df = convert_to_numeric(test_df, numeric_fields)
        
        # Verify existing column was converted
        self.assertTrue(pd.api.types.is_numeric_dtype(result_df['int_col']))
        
        # Verify function didn't crash due to missing column
        self.assertNotIn('nonexistent_col', result_df.columns)
        
        self.log_case_result("Handles missing columns gracefully", True)
    
    def test_empty_dataframe(self):
        """Test with empty DataFrame"""
        # Create empty DataFrame
        empty_df = pd.DataFrame()
        numeric_fields = ['col1', 'col2']
        
        # Run the function
        result_df = convert_to_numeric(empty_df, numeric_fields)
        
        # Verify the result is still an empty DataFrame
        self.assertTrue(result_df.empty)
        self.assertEqual(len(result_df.columns), 0)
        
        self.log_case_result("Handles empty DataFrames gracefully", True)
    
    def test_no_fields_to_convert(self):
        """Test with no fields to convert"""
        # Get test data
        test_df = self.fixtures['numeric_string_df'].copy()
        numeric_fields = []
        
        # Run the function
        result_df = convert_to_numeric(test_df, numeric_fields)
        
        # Verify DataFrame is unchanged
        pd.testing.assert_frame_equal(result_df, test_df)
        
        self.log_case_result("Returns original DataFrame when no fields to convert", True)
    
    def test_all_invalid_values(self):
        """Test with column having all invalid values"""
        # Create DataFrame with invalid values
        invalid_df = pd.DataFrame({
            'all_invalid': ['a', 'b', 'c', 'd']
        })
        numeric_fields = ['all_invalid']
        
        # Run the function
        result_df = convert_to_numeric(invalid_df, numeric_fields)
        
        # Verify column was converted to numeric with NaN values
        self.assertTrue(pd.api.types.is_numeric_dtype(result_df['all_invalid']))
        self.assertTrue(result_df['all_invalid'].isna().all())
        
        self.log_case_result("Handles columns with all invalid values", True)
    
    def test_already_numeric(self):
        """Test with already numeric columns"""
        # Create DataFrame with numeric columns
        numeric_df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.5, 2.5, 3.5]
        })
        numeric_fields = ['int_col', 'float_col']
        
        # Run the function
        result_df = convert_to_numeric(numeric_df, numeric_fields)
        
        # Verify columns remain numeric and unchanged
        for col in numeric_fields:
            self.assertTrue(pd.api.types.is_numeric_dtype(result_df[col]))
            pd.testing.assert_series_equal(result_df[col], numeric_df[col])
        
        self.log_case_result("Handles already numeric columns correctly", True)

class TestCsvToDataFrame(BaseTestCase):
    """Test the csv_to_dataframe function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_pandas_utils_fixtures()
    
    def tearDown(self):
        """Clean up fixtures"""
        super().tearDown()
        # Remove temporary CSV file
        if hasattr(self, 'fixtures') and 'temp_csv_path' in self.fixtures:
            try:
                os.remove(self.fixtures['temp_csv_path'])
            except:
                pass
    
    def test_csv_to_dataframe(self):
        """Test the csv_to_dataframe function"""
        self.valid_csv_handling()
        self.error_handling()
    
    def valid_csv_handling(self):
        """Test function successfully reads a valid CSV file and returns a DataFrame"""
        # Call the function with the temp CSV file
        result = csv_to_dataframe(self.fixtures['temp_csv_path'])
        
        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (3, 3))
        self.assertEqual(list(result.columns), ['symbol', 'price', 'quantity'])
        self.assertEqual(result['symbol'].tolist(), ['AAPL', 'MSFT', 'GOOG'])
        
        self.log_case_result("Successfully reads CSV into DataFrame", True)
    
    def error_handling(self):
        """Test function returns None when an error occurs"""
        # Capture stdout to check error messages
        original_stdout = self.capture_stdout()
        
        # Call the function with a non-existent file
        result = csv_to_dataframe('nonexistent_file.csv')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify the function returns None
        self.assertIsNone(result)
        
        # Verify error message is appropriate
        output = self.captured_output.get_value()
        self.assertIn("Error reading CSV file:", output)
        self.assertIn("No such file or directory", output)
        
        self.log_case_result("Returns None on error", True)

if __name__ == '__main__':
    print("\n🔍 Running tests for pandas_utils.py...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary
    print_summary() 