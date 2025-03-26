import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
import numpy as np
from datetime import datetime
from analytics.process_trades import TradeProcessor

# Import the BaseTestCase class and print_summary from test_utils
from tests._utils.test_utils import BaseTestCase, print_summary

class TestInit(BaseTestCase):
    """Test cases for TradeProcessor.__init__ method"""
    
    def test_init(self):
        """Test proper initialization of TradeProcessor"""
        # Create a sample executions DataFrame
        executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade2'],
            'quantity': [100, -100],
            'price': [150.0, 160.0],
            'symbol': ['AAPL', 'AAPL'],
            'date': ['2023-01-01', '2023-01-02'],
            'time_of_day': ['09:30:00', '10:30:00'],
            'execution_timestamp': pd.to_datetime(['2023-01-01 09:30:00', '2023-01-02 10:30:00']),
            'is_entry': [1, 0],
            'is_exit': [0, 1]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Verify instance variables are correctly initialized
        pd.testing.assert_frame_equal(processor.executions_df, executions_df)  # Same DataFrame content
        self.assertIsNone(processor.entry_execs)
        self.assertIsNone(processor.exit_execs)
        self.assertEqual(processor.trade_directions, {})
        
        self.log_case_result("Correctly initializes instance variables", True)
    
    def test_init_with_empty_dataframe(self):
        """Test initialization with an empty DataFrame"""
        # Create an empty DataFrame
        empty_df = pd.DataFrame()
        
        # Initialize the processor with empty DataFrame
        processor = TradeProcessor(empty_df)
        
        # Verify instance variables are correctly initialized
        pd.testing.assert_frame_equal(processor.executions_df, empty_df)
        self.assertIsNone(processor.entry_execs)
        self.assertIsNone(processor.exit_execs)
        self.assertEqual(processor.trade_directions, {})
        
        self.log_case_result("Correctly initializes with empty DataFrame", True)

class TestValidate(BaseTestCase):
    """Test cases for TradeProcessor.validate method"""
    
    def test_validate_with_valid_data(self):
        """Test validation with valid execution data"""
        # Create a DataFrame with all required columns
        executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade2'],
            'symbol': ['AAPL', 'AAPL'],
            'date': ['2023-01-01', '2023-01-02'],
            'time_of_day': ['09:30:00', '10:30:00'],
            'is_entry': [1, 0],
            'is_exit': [0, 1],
            'quantity': [100, -100],
            'execution_timestamp': pd.to_datetime(['2023-01-01 09:30:00', '2023-01-02 10:30:00']),
            'price': [150.0, 160.0]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Validate the data
        result = processor.validate()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify validation passed
        self.assertTrue(result)
        self.assertEqual("", self.captured_output.get_value())  # No error messages
        
        self.log_case_result("Correctly validates data with all required columns", True)
    
    def test_validate_with_empty_dataframe(self):
        """Test validation with empty DataFrame"""
        # Create an empty DataFrame
        empty_df = pd.DataFrame()
        
        # Initialize the processor
        processor = TradeProcessor(empty_df)
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Validate the data
        result = processor.validate()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify validation failed
        self.assertFalse(result)
        self.assertIn("Executions DataFrame is empty", self.captured_output.get_value())
        
        self.log_case_result("Correctly fails validation with empty DataFrame", True)
    
    def test_validate_with_missing_columns(self):
        """Test validation with missing required columns"""
        # Create a DataFrame with missing required columns
        executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade2'],
            'symbol': ['AAPL', 'AAPL'],
            # Missing: date, time_of_day, is_entry, is_exit, execution_timestamp, price
            'quantity': [100, -100]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Validate the data
        result = processor.validate()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify validation failed
        self.assertFalse(result)
        output = self.captured_output.get_value()
        self.assertIn("Missing required columns", output)
        
        # Check that it reports the missing columns
        self.assertIn("date", output)
        self.assertIn("time_of_day", output)
        self.assertIn("is_entry", output)
        self.assertIn("is_exit", output)
        self.assertIn("execution_timestamp", output)
        self.assertIn("price", output)
        
        self.log_case_result("Correctly fails validation with missing required columns", True)

# Existing test classes follow...


# If running the tests directly, print summary
if __name__ == '__main__':
    unittest.main(exit=False)  # Run tests without exiting
    print_summary()  # Print detailed summary of test results
