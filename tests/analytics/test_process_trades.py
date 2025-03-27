import unittest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch
from io import StringIO
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

class TestPreprocess(BaseTestCase):
    """Test cases for TradeProcessor.preprocess method"""
    
    def test_successful_preprocessing(self):
        """Test preprocessing with valid data containing both entry and exit executions"""
        # Create a DataFrame with both entry and exit executions
        executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade2', 'trade2'],
            'quantity': [100, -100, 200, -200],  # Positive for entries, negative for exits
            'execution_timestamp': pd.to_datetime([
                '2023-01-01 09:30:00', '2023-01-02 16:30:00',
                '2023-01-03 09:30:00', '2023-01-04 16:30:00'
            ]),
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            'time_of_day': ['09:30:00', '16:30:00', '09:30:00', '16:30:00'],
            'price': [150.0, 160.0, 250.0, 260.0],
            'is_entry': [1, 0, 1, 0],
            'is_exit': [0, 1, 0, 1]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Run preprocess
        result = processor.preprocess()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify preprocessing succeeded
        self.assertTrue(result)
        self.assertEqual("", self.captured_output.get_value())  # No error messages
        
        # Verify entry_execs was correctly filtered
        self.assertEqual(2, len(processor.entry_execs))
        self.assertTrue(all(processor.entry_execs['is_entry'] == 1))
        self.assertTrue(all(processor.entry_execs['quantity'] > 0))
        
        # Verify exit_execs was correctly filtered
        self.assertEqual(2, len(processor.exit_execs))
        self.assertTrue(all(processor.exit_execs['is_exit'] == 1))
        self.assertTrue(all(processor.exit_execs['quantity'] < 0))
        
        # Verify trade_directions were correctly analyzed
        self.assertEqual(2, len(processor.trade_directions))
        self.assertEqual('bullish', processor.trade_directions['trade1']['direction'])
        self.assertEqual('bullish', processor.trade_directions['trade2']['direction'])
        
        self.log_case_result("Successfully preprocesses valid data", True)
    
    def test_no_entry_executions(self):
        """Test preprocessing with no entry executions"""
        # Create a DataFrame with only exit executions
        executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade2'],
            'quantity': [-100, -200],  # Only negative quantities (exits)
            'execution_timestamp': pd.to_datetime(['2023-01-02 16:30:00', '2023-01-04 16:30:00']),
            'symbol': ['AAPL', 'MSFT'],
            'date': ['2023-01-02', '2023-01-04'],
            'time_of_day': ['16:30:00', '16:30:00'],
            'price': [160.0, 260.0],
            'is_entry': [0, 0],
            'is_exit': [1, 1]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Run preprocess
        result = processor.preprocess()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify preprocessing failed
        self.assertFalse(result)
        self.assertIn("No entry executions found", self.captured_output.get_value())
        
        # Verify entry_execs is empty
        self.assertEqual(0, len(processor.entry_execs))
        
        # Verify exit_execs was correctly filtered
        self.assertEqual(2, len(processor.exit_execs))
        
        self.log_case_result("Correctly handles case with no entry executions", True)
    
    def test_mixed_trade_directions(self):
        """Test preprocessing with both bullish and bearish trades"""
        # Create a DataFrame with bullish and bearish trades
        executions_df = pd.DataFrame({
            'trade_id': ['bullish', 'bullish', 'bearish', 'bearish'],
            'quantity': [100, -100, -100, 100],  # Bullish: +entry/-exit, Bearish: -entry/+exit
            'execution_timestamp': pd.to_datetime([
                '2023-01-01 09:30:00', '2023-01-02 16:30:00',
                '2023-01-03 09:30:00', '2023-01-04 16:30:00'
            ]),
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            'time_of_day': ['09:30:00', '16:30:00', '09:30:00', '16:30:00'],
            'price': [150.0, 160.0, 250.0, 240.0],
            'is_entry': [1, 0, 1, 0],
            'is_exit': [0, 1, 0, 1]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Run preprocess
        result = processor.preprocess()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify preprocessing succeeded
        self.assertTrue(result)
        
        # Verify entry_execs and exit_execs are correctly filtered
        self.assertEqual(2, len(processor.entry_execs))
        self.assertEqual(2, len(processor.exit_execs))
        
        # Verify trade directions were correctly analyzed
        self.assertEqual(2, len(processor.trade_directions))
        self.assertEqual('bullish', processor.trade_directions['bullish']['direction'])
        self.assertEqual('bearish', processor.trade_directions['bearish']['direction'])
        
        # Check that the initial quantities were correctly stored
        self.assertEqual(100, processor.trade_directions['bullish']['initial_quantity'])
        self.assertEqual(-100, processor.trade_directions['bearish']['initial_quantity'])
        
        # Check that absolute initial quantities were correctly stored
        self.assertEqual(100, processor.trade_directions['bullish']['abs_initial_quantity'])
        self.assertEqual(100, processor.trade_directions['bearish']['abs_initial_quantity'])
        
        self.log_case_result("Correctly processes mixed bullish and bearish trades", True)
    
    def test_invalid_zero_quantity_entry(self):
        """Test preprocessing with an invalid zero quantity entry"""
        # Create a DataFrame with a zero quantity entry
        executions_df = pd.DataFrame({
            'trade_id': ['valid', 'valid', 'invalid', 'invalid'],
            'quantity': [100, -100, 0, -50],  # Invalid has zero quantity entry
            'execution_timestamp': pd.to_datetime([
                '2023-01-01 09:30:00', '2023-01-02 16:30:00',
                '2023-01-03 09:30:00', '2023-01-04 16:30:00'
            ]),
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            'time_of_day': ['09:30:00', '16:30:00', '09:30:00', '16:30:00'],
            'price': [150.0, 160.0, 250.0, 240.0],
            'is_entry': [1, 0, 1, 0],
            'is_exit': [0, 1, 0, 1]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Run preprocess
        result = processor.preprocess()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify preprocessing failed
        self.assertFalse(result)
        self.assertIn("has zero quantity entry which is invalid", self.captured_output.get_value())
        
        self.log_case_result("Correctly identifies invalid zero quantity entries", True)
    
    def test_only_entry_executions(self):
        """Test preprocessing with only entry executions (no exits)"""
        # Create a DataFrame with only entry executions
        executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade2'],
            'quantity': [100, 200],  # Only positive quantities (entries)
            'execution_timestamp': pd.to_datetime(['2023-01-01 09:30:00', '2023-01-03 09:30:00']),
            'symbol': ['AAPL', 'MSFT'],
            'date': ['2023-01-01', '2023-01-03'],
            'time_of_day': ['09:30:00', '09:30:00'],
            'price': [150.0, 250.0],
            'is_entry': [1, 1],
            'is_exit': [0, 0]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Run preprocess
        result = processor.preprocess()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify preprocessing succeeded
        self.assertTrue(result)
        self.assertEqual("", self.captured_output.get_value())  # No error messages
        
        # Verify entry_execs contains both trades
        self.assertEqual(2, len(processor.entry_execs))
        
        # Verify exit_execs is empty
        self.assertEqual(0, len(processor.exit_execs))
        
        # Verify trade_directions were correctly analyzed
        self.assertEqual(2, len(processor.trade_directions))
        self.assertEqual('bullish', processor.trade_directions['trade1']['direction'])
        self.assertEqual('bullish', processor.trade_directions['trade2']['direction'])
        
        self.log_case_result("Successfully preprocesses with only entry executions", True)
    
    def test_multiple_entries_per_trade(self):
        """Test preprocessing with multiple entry executions per trade"""
        # Create a DataFrame with multiple entries per trade
        executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade1', 'trade2', 'trade2'],
            'quantity': [50, 50, -100, 100, -100],  # Multiple entries for trade1
            'execution_timestamp': pd.to_datetime([
                '2023-01-01 09:30:00', '2023-01-01 10:30:00', '2023-01-02 16:30:00',
                '2023-01-03 09:30:00', '2023-01-04 16:30:00'
            ]),
            'symbol': ['AAPL', 'AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'date': ['2023-01-01', '2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            'time_of_day': ['09:30:00', '10:30:00', '16:30:00', '09:30:00', '16:30:00'],
            'price': [150.0, 152.0, 160.0, 250.0, 260.0],
            'is_entry': [1, 1, 0, 1, 0],
            'is_exit': [0, 0, 1, 0, 1]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Run preprocess
        result = processor.preprocess()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify preprocessing succeeded
        self.assertTrue(result)
        
        # Verify entry_execs contains all entry executions
        self.assertEqual(3, len(processor.entry_execs))
        
        # Verify exit_execs contains all exit executions
        self.assertEqual(2, len(processor.exit_execs))
        
        # Verify trade_directions were determined by the first entry's quantity
        self.assertEqual('bullish', processor.trade_directions['trade1']['direction'])
        self.assertEqual('bullish', processor.trade_directions['trade2']['direction'])
        
        # The initial quantity for trade1 should be 50 (first entry)
        self.assertEqual(50, processor.trade_directions['trade1']['initial_quantity'])
        
        self.log_case_result("Correctly handles multiple entries per trade", True)

class TestAnalyzeTradeDirections(BaseTestCase):
    """Test cases for TradeProcessor._analyze_trade_directions method."""
    
    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a sample executions DataFrame
        self.executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade2', 'trade2'],
            'execution_id': ['exec1', 'exec2', 'exec3', 'exec4'],
            'quantity': [100, -100, -50, 50],
            'price': [10.0, 11.0, 20.0, 21.0],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'execution_timestamp': pd.to_datetime([
                '2023-01-01 09:30:00', 
                '2023-01-01 16:30:00',
                '2023-01-02 09:30:00', 
                '2023-01-02 16:30:00'
            ]),
            'is_entry': [1, 0, 1, 0],
            'is_exit': [0, 1, 0, 1]
        })
        
        # Initialize the processor
        self.processor = TradeProcessor(self.executions_df)
    
    def test_bullish_trade_direction(self):
        """Test bullish trade direction detection."""
        # Create a DataFrame with a bullish trade (positive quantity entry)
        executions_df = pd.DataFrame({
            'trade_id': ['trade1'],
            'quantity': [100],  # Positive quantity = bullish
            'execution_timestamp': pd.to_datetime(['2023-01-01 09:30:00']),
            'symbol': ['AAPL'],
            'date': ['2023-01-01'],
            'time_of_day': ['09:30:00'],
            'price': [150.0],
            'is_entry': [1],
            'is_exit': [0]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Set entry_execs manually since we're testing _analyze_trade_directions directly
        processor.entry_execs = executions_df.copy()
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Run analyze_trade_directions
        result = processor._analyze_trade_directions()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify analysis succeeded
        self.assertTrue(result)
        self.assertEqual("", self.captured_output.get_value())  # No error messages
        
        # Verify trade direction was correctly analyzed
        self.assertIn('trade1', processor.trade_directions)
        self.assertEqual('bullish', processor.trade_directions['trade1']['direction'])
        self.assertEqual(100, processor.trade_directions['trade1']['initial_quantity'])
        self.assertEqual(100, processor.trade_directions['trade1']['abs_initial_quantity'])
        
        self.log_case_result("Correctly identifies bullish trade direction", True)
    
    def test_bearish_trade_direction(self):
        """Test analysis of bearish trade direction"""
        # Create a DataFrame with a bearish trade (negative quantity entry)
        executions_df = pd.DataFrame({
            'trade_id': ['bearish'],
            'quantity': [-100],  # Negative quantity = bearish
            'execution_timestamp': pd.to_datetime(['2023-01-01 09:30:00']),
            'symbol': ['AAPL'],
            'date': ['2023-01-01'],
            'time_of_day': ['09:30:00'],
            'price': [150.0],
            'is_entry': [1],
            'is_exit': [0]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Set entry_execs manually
        processor.entry_execs = executions_df.copy()
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Run analyze_trade_directions
        result = processor._analyze_trade_directions()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify analysis succeeded
        self.assertTrue(result)
        
        # Verify trade direction was correctly analyzed
        self.assertIn('bearish', processor.trade_directions)
        self.assertEqual('bearish', processor.trade_directions['bearish']['direction'])
        self.assertEqual(-100, processor.trade_directions['bearish']['initial_quantity'])
        self.assertEqual(100, processor.trade_directions['bearish']['abs_initial_quantity'])
        
        self.log_case_result("Correctly identifies bearish trade direction", True)
    
    def test_multiple_trades_different_directions(self):
        """Test analysis of multiple trades with different directions"""
        # Create a DataFrame with multiple trades of different directions
        executions_df = pd.DataFrame({
            'trade_id': ['bullish1', 'bearish1', 'bullish2', 'bearish2'],
            'quantity': [100, -200, 300, -400],
            'execution_timestamp': pd.to_datetime([
                '2023-01-01 09:30:00', '2023-01-02 09:30:00',
                '2023-01-03 09:30:00', '2023-01-04 09:30:00'
            ]),
            'symbol': ['AAPL', 'MSFT', 'TSLA', 'GOOG'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            'time_of_day': ['09:30:00', '09:30:00', '09:30:00', '09:30:00'],
            'price': [150.0, 250.0, 350.0, 450.0],
            'is_entry': [1, 1, 1, 1],
            'is_exit': [0, 0, 0, 0]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Set entry_execs manually
        processor.entry_execs = executions_df.copy()
        
        # Run analyze_trade_directions
        result = processor._analyze_trade_directions()
        
        # Verify analysis succeeded
        self.assertTrue(result)
        
        # Verify all trade directions were correctly analyzed
        self.assertEqual(4, len(processor.trade_directions))
        
        # Check bullish trades
        self.assertEqual('bullish', processor.trade_directions['bullish1']['direction'])
        self.assertEqual('bullish', processor.trade_directions['bullish2']['direction'])
        self.assertEqual(100, processor.trade_directions['bullish1']['initial_quantity'])
        self.assertEqual(300, processor.trade_directions['bullish2']['initial_quantity'])
        
        # Check bearish trades
        self.assertEqual('bearish', processor.trade_directions['bearish1']['direction'])
        self.assertEqual('bearish', processor.trade_directions['bearish2']['direction'])
        self.assertEqual(-200, processor.trade_directions['bearish1']['initial_quantity'])
        self.assertEqual(-400, processor.trade_directions['bearish2']['initial_quantity'])
        
        self.log_case_result("Correctly analyzes multiple trades with different directions", True)
    
    def test_empty_entry_execs(self):
        """Test analysis with empty entry_execs DataFrame"""
        # Create an empty DataFrame
        empty_df = pd.DataFrame(columns=[
            'trade_id', 'quantity', 'execution_timestamp', 
            'symbol', 'date', 'time_of_day', 'price', 'is_entry', 'is_exit'
        ])
        
        # Initialize the processor
        processor = TradeProcessor(empty_df)
        
        # Set entry_execs to empty DataFrame
        processor.entry_execs = empty_df.copy()
        
        # Run analyze_trade_directions
        result = processor._analyze_trade_directions()
        
        # Empty entry_execs should still return True (nothing to analyze)
        self.assertTrue(result)
        
        # Verify trade_directions is empty
        self.assertEqual(0, len(processor.trade_directions))
        
        self.log_case_result("Handles empty entry_execs gracefully", True)
    
    def test_zero_quantity_entry(self):
        """Test analysis with a zero quantity entry"""
        # Create a DataFrame with a zero quantity entry
        executions_df = pd.DataFrame({
            'trade_id': ['invalid'],
            'quantity': [0],  # Zero quantity (invalid)
            'execution_timestamp': pd.to_datetime(['2023-01-01 09:30:00']),
            'symbol': ['AAPL'],
            'date': ['2023-01-01'],
            'time_of_day': ['09:30:00'],
            'price': [150.0],
            'is_entry': [1],
            'is_exit': [0]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Set entry_execs manually
        processor.entry_execs = executions_df.copy()
        
        # Capture stdout to check messages
        original_stdout = self.capture_stdout()
        
        # Run analyze_trade_directions
        result = processor._analyze_trade_directions()
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify analysis failed
        self.assertFalse(result)
        
        # Check error message
        error_msg = self.captured_output.get_value()
        self.assertIn("has zero quantity entry which is invalid", error_msg)
        
        self.log_case_result("Correctly detects invalid zero quantity entries", True)
    
    def test_chronological_ordering(self):
        """Test that direction is determined by the earliest entry execution"""
        # Create a DataFrame with multiple entries in non-chronological order
        executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade1'],
            'quantity': [100, 200, 300],  # All bullish, but with different quantities
            'execution_timestamp': pd.to_datetime([
                '2023-01-02 09:30:00',  # Not the earliest
                '2023-01-01 09:30:00',  # Earliest - should determine direction
                '2023-01-03 09:30:00'   # Latest
            ]),
            'symbol': ['AAPL', 'AAPL', 'AAPL'],
            'date': ['2023-01-02', '2023-01-01', '2023-01-03'],
            'time_of_day': ['09:30:00', '09:30:00', '09:30:00'],
            'price': [160.0, 150.0, 170.0],
            'is_entry': [1, 1, 1],
            'is_exit': [0, 0, 0]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Set entry_execs manually
        processor.entry_execs = executions_df.copy()
        
        # Run analyze_trade_directions
        result = processor._analyze_trade_directions()
        
        # Verify analysis succeeded
        self.assertTrue(result)
        
        # Verify direction is determined by the earliest entry (quantity=200)
        self.assertEqual('bullish', processor.trade_directions['trade1']['direction'])
        self.assertEqual(200, processor.trade_directions['trade1']['initial_quantity'])
        
        self.log_case_result("Correctly uses earliest entry execution for direction", True)
    
    def test_trade_direction_structure(self):
        """Test the structure of the trade_directions dictionary"""
        # Create a simple DataFrame with one trade
        executions_df = pd.DataFrame({
            'trade_id': ['trade1'],
            'quantity': [100],
            'execution_timestamp': pd.to_datetime(['2023-01-01 09:30:00']),
            'symbol': ['AAPL'],
            'date': ['2023-01-01'],
            'time_of_day': ['09:30:00'],
            'price': [150.0],
            'is_entry': [1],
            'is_exit': [0]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Set entry_execs manually
        processor.entry_execs = executions_df.copy()
        
        # Run analyze_trade_directions
        result = processor._analyze_trade_directions()
        
        # Verify analysis succeeded
        self.assertTrue(result)
        
        # Verify trade_directions has the correct structure
        self.assertIn('trade1', processor.trade_directions)
        
        trade_info = processor.trade_directions['trade1']
        self.assertIn('direction', trade_info)
        self.assertIn('initial_quantity', trade_info)
        self.assertIn('abs_initial_quantity', trade_info)
        
        # Check that values are of the correct type
        self.assertIsInstance(trade_info['direction'], str)
        self.assertIsInstance(trade_info['initial_quantity'], (int, float))
        self.assertIsInstance(trade_info['abs_initial_quantity'], (int, float))
        
        self.log_case_result("Creates trade_directions with correct structure", True)
    
    def test_exception_handling(self):
        """Test exception handling during trade direction analysis"""
        # Create a valid DataFrame
        executions_df = pd.DataFrame({
            'trade_id': ['trade1'],
            'quantity': [100],
            'execution_timestamp': pd.to_datetime(['2023-01-01 09:30:00']),
            'symbol': ['AAPL'],
            'date': ['2023-01-01'],
            'time_of_day': ['09:30:00'],
            'price': [150.0],
            'is_entry': [1],
            'is_exit': [0]
        })
        
        # Initialize the processor
        processor = TradeProcessor(executions_df)
        
        # Set entry_execs manually, but make it invalid (missing required column)
        invalid_df = executions_df.copy()
        invalid_df = invalid_df.drop(columns=['quantity'])  # Remove quantity column to cause exception
        processor.entry_execs = invalid_df
        
        # Run analyze_trade_directions - it should handle the KeyError gracefully
        result = processor._analyze_trade_directions()
        
        # Verify analysis failed
        self.assertFalse(result)
        
        self.log_case_result("Gracefully handles exceptions during analysis", True)
    
    def test_end_to_end_processing(self):
        """Test the complete process_trades flow with real data processing."""
        # Setup a more complete executions DataFrame
        executions_df = pd.DataFrame({
            'trade_id': [1, 1],
            'execution_id': [1, 2],
            'quantity': [100, -100],
            'price': [10.0, 15.0],
            'ticker': ['AAPL', 'AAPL'],
            'execution_timestamp': [pd.to_datetime('2023-01-01 10:00:00'), pd.to_datetime('2023-01-01 11:00:00')],
            'is_entry': [1, 0],
            'is_exit': [0, 1],
            'commission': [1.0, 1.0]
        })
        
        # Initialize processor with the test data
        processor = TradeProcessor(executions_df)
        
        # We'll patch internal methods to ensure a predictable flow
        with patch.object(processor, 'validate', return_value=True), \
             patch.object(processor, 'preprocess', return_value=True), \
             patch.object(processor, '_get_all_aggregations', return_value={
                 'entry_counts': pd.Series({1: 1}),
                 'exit_counts': pd.Series({1: 1}),
                 'quantities': pd.Series({1: 100}),
                 'entry_prices': pd.Series({1: 10.0}),
                 'exit_prices': pd.Series({1: 15.0}),
                 'directions': pd.Series({1: 'bullish'}),
                 'tickers': pd.Series({1: 'AAPL'}),
                 'entry_dates': pd.Series({1: pd.to_datetime('2023-01-01').date()}),
                 'entry_times': pd.Series({1: pd.to_datetime('2023-01-01 10:00:00').time()}),
                 'exit_dates': pd.Series({1: pd.to_datetime('2023-01-01').date()}),
                 'exit_times': pd.Series({1: pd.to_datetime('2023-01-01 11:00:00').time()})
             }):
            
            # We'll also patch _build_trades_dataframe to return a known DataFrame
            expected_df = pd.DataFrame({
                'trade_id': [1],
                'ticker': ['AAPL'],
            'quantity': [100],
                'direction': ['bullish'],
                'entry_price': [10.0],
                'exit_price': [15.0],
                'entry_date': [pd.to_datetime('2023-01-01').date()],
                'entry_time': [pd.to_datetime('2023-01-01 10:00:00').time()],
                'exit_date': [pd.to_datetime('2023-01-01').date()],
                'exit_time': [pd.to_datetime('2023-01-01 11:00:00').time()],
            })
            
            with patch.object(processor, '_build_trades_dataframe', return_value=expected_df):
                # Call the method
                result_df = processor.process_trades()
                
                # Verify we get the expected DataFrame
                self.assertIsNotNone(result_df)
                pd.testing.assert_frame_equal(result_df, expected_df)
                
                # Log test result
                self.log_case_result("End-to-end processing with valid data returns expected DataFrame", True)
    
    def test_multiple_trades(self):
        """Test processing multiple trades with different directions."""
        # Setup test data for both bullish and bearish trades
        entry_prices = pd.Series({1: 10.0, 2: 20.0})
        exit_prices = pd.Series({1: 15.0, 2: 15.0})
        stop_prices = pd.Series({1: 8.0, 2: 22.0})
        
        # Mock the trade_directions dictionary that would normally be set during preprocessing
        self.processor.trade_directions = {
            1: {'direction': 'bullish', 'initial_quantity': 100, 'abs_initial_quantity': 100},
            2: {'direction': 'bearish', 'initial_quantity': -100, 'abs_initial_quantity': 100}
        }
        
        # Calculate expected results
        # Bullish: (15 - 10) / (10 - 8) = 5 / 2 = 2.5
        # Bearish: (20 - 15) / (22 - 20) = 5 / 2 = 2.5
        expected_rr = pd.Series({1: 2.5, 2: 2.5})
        
        # Call the method
        result = self.processor._calculate_risk_reward_ratio(
            entry_prices=entry_prices,
            exit_prices=exit_prices,
            stop_prices=stop_prices
        )
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_rr)
        
        # Log test result
        self.log_case_result("Correctly processes multiple trades with different directions", True)
    
    def test_none_nan_handling(self):
        """Test handling of None and NaN risk-reward values."""
        # Create test data with None and NaN risk-reward values
        risk_reward = pd.Series({
            1: None,              # None value -> Loser
            2: float('nan'),      # NaN value -> Loser
            3: 1.5                # Positive RR -> Winner
        })
        
        # Expected result
        expected_winners = pd.Series({1: 0, 2: 0, 3: 1})
        
        # Call the method
        result = self.processor._get_winning_trades(risk_reward)
        
        # Verify results - for None/NaN we check individually
        self.assertEqual(result[1], 0)
        self.assertEqual(result[2], 0)
        self.assertEqual(result[3], 1)
        
        # Log test result
        self.log_case_result("Correctly handles None and NaN values", True)
    
    def test_mixed_trades(self):
        """Test with a combination of winning and losing trades."""
        # Create test data with mixed risk-reward ratios
        risk_reward = pd.Series({
            1: 2.5,     # Positive RR -> Winner
            2: -1.0,    # Negative RR -> Loser
            3: 0.0,     # Zero RR -> Loser
            4: 0.1,     # Small positive RR -> Winner
            5: None     # None value -> Loser
        })
        
        # Expected result
        expected_winners = pd.Series({1: 1, 2: 0, 3: 0, 4: 1, 5: 0})
        
        # Call the method
        result = self.processor._get_winning_trades(risk_reward)
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_winners)
        
        # Log test result
        self.log_case_result("Correctly processes mixed winning and losing trades", True)
    
    def test_return_type_and_structure(self):
        """Test the return type and structure of the method."""
        # Create test data
        risk_reward = pd.Series({1: 2.5, 2: -1.0, 3: 1.5})
        
        # Call the method
        result = self.processor._get_winning_trades(risk_reward)
        
        # Verify result type and structure
        self.assertIsInstance(result, pd.Series)
        self.assertEqual(result.index.tolist(), [1, 2, 3])  # Index should be trade_id
        self.assertEqual(result.iloc[0], 1)  # Value should be 1 for winning trades
        self.assertEqual(result.iloc[1], 0)  # Value should be 0 for losing trades
        self.assertEqual(result.iloc[2], 1)  # Value should be 1 for winning trades
        
        # Log test result
        self.log_case_result("Returns correct type and structure", True)
    
    def test_edge_cases(self):
        """Test handling of edge cases with very small values."""
        # Create test data with very small positive and negative values
        risk_reward = pd.Series({
            1: 1e-10,   # Very small positive -> Winner
            2: -1e-10,  # Very small negative -> Loser
            3: 1e10,    # Very large positive -> Winner
            4: -1e10    # Very large negative -> Loser
        })
        
        # Expected result
        expected_winners = pd.Series({1: 1, 2: 0, 3: 1, 4: 0})
        
        # Call the method
        result = self.processor._get_winning_trades(risk_reward)
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_winners)
        
        # Log test result
        self.log_case_result("Correctly handles edge case values", True)
    
    def test_return_type(self):
        """Test that the method returns a pandas Series with correct indices."""
        # Setup test data
        risk_reward = pd.Series({1: 2.5, 2: -1.0, 3: 1.5})
        
        # Call the method
        result = self.processor._get_winning_trades(risk_reward)
        
        # Verify result type and structure
        self.assertIsInstance(result, pd.Series)
        
        # Verify indices match input
        self.assertEqual(set(result.index), {1, 2, 3})
        
        # Verify values are either 0 or 1
        for value in result.values:
            self.assertIn(value, [0, 1])
        
        # Log test result
        self.log_case_result("Returns correct structure and types", True)

class TestCalculatePercReturn(BaseTestCase):
    """Test cases for the _calculate_perc_return method of TradeProcessor."""
    
    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a minimal executions DataFrame
        self.executions_df = pd.DataFrame({
            'trade_id': [1, 2, 3],
            'execution_id': [1, 2, 3],
            'quantity': [100, -100, 50],
            'price': [10.0, 20.0, 30.0],
            'ticker': ['AAPL', 'MSFT', 'TSLA'],
            'execution_timestamp': [pd.to_datetime('2023-01-01')] * 3,
            'is_entry': [1, 1, 1],
            'is_exit': [0, 0, 0],
            'commission': [1.0, 1.0, 1.0]
        })
        
        # Initialize the processor
        self.processor = TradeProcessor(self.executions_df)
        
        # Setup trade directions dictionary
        self.processor.trade_directions = {
            1: {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0},
            2: {'direction': 'bearish', 'initial_quantity': -100.0, 'abs_initial_quantity': 100.0},
            3: {'direction': 'bullish', 'initial_quantity': 50.0, 'abs_initial_quantity': 50.0}
        }
    
    def test_basic_calculation(self):
        """Test basic percentage return calculation."""
        # Setup test data
        risk_per_trade = pd.Series({1: 0.02, 2: 0.03, 3: 0.01})  # 2%, 3%, 1% risk
        risk_reward = pd.Series({1: 2.5, 2: 2.0, 3: 1.5})       # 2.5R, 2.0R, 1.5R
        
        # Expected result: risk_per_trade * risk_reward
        # 1: 0.02 * 2.5 = 0.05 (5%)
        # 2: 0.03 * 2.0 = 0.06 (6%)
        # 3: 0.01 * 1.5 = 0.015 (1.5%)
        expected_result = pd.Series({1: 0.05, 2: 0.06, 3: 0.015})
        
        # Call the method
        result = self.processor._calculate_perc_return(risk_per_trade, risk_reward)
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_result)
        
        # Log test result
        self.log_case_result("Correctly calculates basic percentage returns", True)
    
    def test_bullish_bearish_trades(self):
        """Test calculation for both bullish and bearish trades."""
        # Setup test data for bullish and bearish trades
        risk_per_trade = pd.Series({1: 0.02, 2: 0.03, 3: 0.01})  # Trade 1 is bullish, Trade 2 is bearish
        risk_reward = pd.Series({1: 2.0, 2: -1.0, 3: 1.5})     # Bullish win, Bearish loss, Bullish win
        
        # Expected result:
        # 1: 0.02 * 2.0 = 0.04 (4% gain)
        # 2: 0.03 * -1.0 = -0.03 (3% loss)
        # 3: 0.01 * 1.5 = 0.015 (1.5% gain)
        expected_result = pd.Series({1: 0.04, 2: -0.03, 3: 0.015})
        
        # Call the method
        result = self.processor._calculate_perc_return(risk_per_trade, risk_reward)
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_result)
        
        # Log test result
        self.log_case_result("Correctly calculates returns for bullish and bearish trades", True)
    
    def test_missing_values(self):
        """Test with missing values in either Series."""
        # Setup test data with missing values
        risk_per_trade = pd.Series({1: 0.02, 2: None, 3: 0.01})
        risk_reward = pd.Series({1: None, 2: 2.0, 3: 1.5})
        
        # Call the method
        result = self.processor._calculate_perc_return(risk_per_trade, risk_reward)
        
        # Verify results - trades 1 and 2 should have None or NaN
        for trade_id in [1, 2]:
            self.assertTrue(pd.isna(result[trade_id]), 
                           f"Expected NaN for trade {trade_id}, got {result[trade_id]}")
        self.assertEqual(result[3], 0.015)  # Only trade 3 has valid values
        
        # Log test result
        self.log_case_result("Correctly handles missing values", True)
    
    def test_zero_risk(self):
        """Test with zero risk per trade values."""
        # Setup test data with zero risk
        risk_per_trade = pd.Series({1: 0.0, 2: 0.0, 3: 0.0})
        risk_reward = pd.Series({1: 2.5, 2: -1.0, 3: 1.5})
        
        # Expected result: 0% return regardless of risk-reward
        expected_result = pd.Series({1: 0.0, 2: 0.0, 3: 0.0})
        
        # Call the method
        result = self.processor._calculate_perc_return(risk_per_trade, risk_reward)
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_result)
        
        # Log test result
        self.log_case_result("Correctly calculates returns with zero risk", True)
    
    def test_various_value_combinations(self):
        """Test with various combinations of risk_per_trade and risk_reward values."""
        # Setup test data with various value combinations
        risk_per_trade = pd.Series({
            1: 0.02,   # Positive risk
            2: 0.03,   # Positive risk
            3: 0.01    # Positive risk
        })
        
        risk_reward = pd.Series({
            1: 2.5,    # Positive reward (winning trade)
            2: -1.0,   # Negative reward (losing trade)
            3: 0.0     # Zero reward (breakeven trade)
        })
        
        # Expected results:
        # 1: 0.02 * 2.5 = 0.05 (5% gain)
        # 2: 0.03 * -1.0 = -0.03 (3% loss)
        # 3: 0.01 * 0.0 = 0.0 (0% - breakeven)
        expected_result = pd.Series({1: 0.05, 2: -0.03, 3: 0.0})
        
        # Call the method
        result = self.processor._calculate_perc_return(risk_per_trade, risk_reward)
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_result)
        
        # Log test result
        self.log_case_result("Correctly handles various value combinations", True)
    
    def test_return_type(self):
        """Test that the method returns a pandas Series with correct indices."""
        # Setup test data
        risk_per_trade = pd.Series({1: 0.02, 2: 0.03, 3: 0.01})
        risk_reward = pd.Series({1: 2.5, 2: -1.0, 3: 1.5})
        
        # Call the method
        result = self.processor._calculate_perc_return(risk_per_trade, risk_reward)
        
        # Verify result type and indices
        self.assertIsInstance(result, pd.Series)
        self.assertEqual(set(result.index), {1, 2, 3})
        
        # Log test result
        self.log_case_result("Returns pandas Series with correct indices", True)
    
    def test_nan_values(self):
        """Test handling of NaN values."""
        # Setup test data with NaN values
        risk_per_trade = pd.Series({1: 0.02, 2: float('nan'), 3: 0.01})
        risk_reward = pd.Series({1: float('nan'), 2: 2.0, 3: 1.5})
        
        # Call the method
        result = self.processor._calculate_perc_return(risk_per_trade, risk_reward)
        
        # Verify results - both trades should have NaN values
        self.assertTrue(pd.isna(result[1]), f"Expected NaN for trade 1, got {result[1]}")
        self.assertTrue(pd.isna(result[2]), f"Expected NaN for trade 2, got {result[2]}")
        
        # Log test result
        self.log_case_result("Correctly handles NaN values", True)
    
    def test_extreme_values(self):
        """Test with extreme values (very large or very small)."""
        # Setup test data with extreme values
        risk_per_trade = pd.Series({
            1: 0.0001,    # Very small risk (0.01%)
            2: 0.5,       # Very large risk (50%)
            3: 1.0        # Maximum risk (100%)
        })
        
        risk_reward = pd.Series({
            1: 1000.0,    # Very large reward (1000R)
            2: 0.00001,   # Very small reward (0.00001R)
            3: -10.0      # Large loss (-10R)
        })
        
        # Expected results:
        # 1: 0.0001 * 1000.0 = 0.1 (10% gain from a tiny risk)
        # 2: 0.5 * 0.00001 = 0.000005 (0.0005% gain from a large risk)
        # 3: 1.0 * -10.0 = -10.0 (1000% loss, which is extreme but valid)
        expected_result = pd.Series({1: 0.1, 2: 0.000005, 3: -10.0})
        
        # Call the method
        result = self.processor._calculate_perc_return(risk_per_trade, risk_reward)
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_result)
        
        # Log test result
        self.log_case_result("Correctly handles extreme values", True)

class TestGetTradeStatus(BaseTestCase):
    """Test cases for the _get_trade_status method of TradeProcessor."""
    
    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
    def test_closed_trades(self):
        """Test identification of closed trades (net position = 0)."""
        # Create executions with closed trades (quantities sum to 0)
        executions_df = pd.DataFrame({
            'trade_id': [1, 1, 2, 2],
            'execution_id': [1, 2, 3, 4],
            'quantity': [100, -100, -50, 50],  # Both trades sum to 0
            'price': [10.0, 11.0, 20.0, 21.0],
            'ticker': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'execution_timestamp': [
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00'),
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00')
            ],
            'is_entry': [1, 0, 1, 0],
            'is_exit': [0, 1, 0, 1],
            'commission': [1.0, 1.0, 1.0, 1.0]
        })
        
        # Initialize processor with test data
        processor = TradeProcessor(executions_df)
        
        # Setup trade directions
        processor.trade_directions = {
            1: {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0},
            2: {'direction': 'bearish', 'initial_quantity': -50.0, 'abs_initial_quantity': 50.0}
        }
        
        # Call the method
        result = processor._get_trade_status()
        
        # Expected: both trades are closed
        expected_result = pd.Series({1: 'closed', 2: 'closed'})
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_result)
        
        # Log test result
        self.log_case_result("Correctly identifies closed trades", True)
    
    def test_open_bullish_trades(self):
        """Test identification of open bullish trades (positive net position)."""
        # Create executions with an open bullish trade
        executions_df = pd.DataFrame({
            'trade_id': [1, 1],
            'execution_id': [1, 2],
            'quantity': [100, -40],  # Net position = 60, still open
            'price': [10.0, 11.0],
            'ticker': ['AAPL', 'AAPL'],
            'execution_timestamp': [
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00')
            ],
            'is_entry': [1, 0],
            'is_exit': [0, 1],
            'commission': [1.0, 1.0]
        })
        
        # Initialize processor with test data
        processor = TradeProcessor(executions_df)
        
        # Setup trade directions
        processor.trade_directions = {
            1: {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0}
        }
        
        # Call the method
        result = processor._get_trade_status()
        
        # Expected: trade is open
        expected_result = pd.Series({1: 'open'})
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_result)
        
        # Log test result
        self.log_case_result("Correctly identifies open bullish trades", True)
    
    def test_open_bearish_trades(self):
        """Test identification of open bearish trades (negative net position)."""
        # Create executions with an open bearish trade
        executions_df = pd.DataFrame({
            'trade_id': [1, 1],
            'execution_id': [1, 2],
            'quantity': [-100, 40],  # Net position = -60, still open
            'price': [10.0, 11.0],
            'ticker': ['AAPL', 'AAPL'],
            'execution_timestamp': [
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00')
            ],
            'is_entry': [1, 0],
            'is_exit': [0, 1],
            'commission': [1.0, 1.0]
        })
        
        # Initialize processor with test data
        processor = TradeProcessor(executions_df)
        
        # Setup trade directions
        processor.trade_directions = {
            1: {'direction': 'bearish', 'initial_quantity': -100.0, 'abs_initial_quantity': 100.0}
        }
        
        # Call the method
        result = processor._get_trade_status()
        
        # Expected: trade is open
        expected_result = pd.Series({1: 'open'})
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_result)
        
        # Log test result
        self.log_case_result("Correctly identifies open bearish trades", True)
    
    def test_mixed_status_trades(self):
        """Test a combination of open and closed trades."""
        # Create executions with mixed trade statuses
        executions_df = pd.DataFrame({
            'trade_id': [1, 1, 2, 2, 3, 3],
            'execution_id': [1, 2, 3, 4, 5, 6],
            'quantity': [100, -100, -50, 20, 100, -40],  # Trade 1: closed, Trade 2: partially closed (bearish), Trade 3: partially closed (bullish)
            'price': [10.0, 11.0, 20.0, 21.0, 30.0, 31.0],
            'ticker': ['AAPL', 'AAPL', 'MSFT', 'MSFT', 'TSLA', 'TSLA'],
            'execution_timestamp': [
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00'),
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00'),
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00')
            ],
            'is_entry': [1, 0, 1, 0, 1, 0],
            'is_exit': [0, 1, 0, 1, 0, 1],
            'commission': [1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        })
        
        # Initialize processor with test data
        processor = TradeProcessor(executions_df)
        
        # Setup trade directions
        processor.trade_directions = {
            1: {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0},
            2: {'direction': 'bearish', 'initial_quantity': -50.0, 'abs_initial_quantity': 50.0},
            3: {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0}
        }
        
        # Call the method
        result = processor._get_trade_status()
        
        # Expected results:
        # Trade 1: closed (net = 0)
        # Trade 2: open (bearish, net = -30)
        # Trade 3: open (bullish, net = 60)
        expected_result = pd.Series({1: 'closed', 2: 'open', 3: 'open'})
        
        # Verify results
        pd.testing.assert_series_equal(result, expected_result)
        
        # Log test result
        self.log_case_result("Correctly identifies mixed trade statuses", True)
    
    def test_invalid_bullish_status(self):
        """Test that ValueError is raised for invalid bullish trade status."""
        # Create executions with invalid bullish trade (negative net position for bullish)
        executions_df = pd.DataFrame({
            'trade_id': [1, 1],
            'execution_id': [1, 2],
            'quantity': [100, -150],  # Net position = -50, invalid for bullish
            'price': [10.0, 11.0],
            'ticker': ['AAPL', 'AAPL'],
            'execution_timestamp': [
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00')
            ],
            'is_entry': [1, 0],
            'is_exit': [0, 1],
            'commission': [1.0, 1.0]
        })
        
        # Initialize processor with test data
        processor = TradeProcessor(executions_df)
        
        # Setup trade directions
        processor.trade_directions = {
            1: {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0}
        }
        
        # Expect ValueError when calling the method
        with self.assertRaises(ValueError) as context:
            processor._get_trade_status()
        
        # Verify error message
        self.assertTrue("invalid net position" in str(context.exception).lower())
        
        # Log test result
        self.log_case_result("Raises error for invalid bullish status", True)
    
    def test_invalid_bearish_status(self):
        """Test that ValueError is raised for invalid bearish trade status."""
        # Create executions with invalid bearish trade (positive net position for bearish)
        executions_df = pd.DataFrame({
            'trade_id': [1, 1],
            'execution_id': [1, 2],
            'quantity': [-100, 150],  # Net position = 50, invalid for bearish
            'price': [10.0, 11.0],
            'ticker': ['AAPL', 'AAPL'],
            'execution_timestamp': [
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00')
            ],
            'is_entry': [1, 0],
            'is_exit': [0, 1],
            'commission': [1.0, 1.0]
        })
        
        # Initialize processor with test data
        processor = TradeProcessor(executions_df)
        
        # Setup trade directions
        processor.trade_directions = {
            1: {'direction': 'bearish', 'initial_quantity': -100.0, 'abs_initial_quantity': 100.0}
        }
        
        # Expect ValueError when calling the method
        with self.assertRaises(ValueError) as context:
            processor._get_trade_status()
        
        # Verify error message
        self.assertTrue("invalid net position" in str(context.exception).lower())
        
        # Log test result
        self.log_case_result("Raises error for invalid bearish status", True)
    
    def test_missing_net_position(self):
        """Test that ValueError is raised when trade has no net position."""
        # Create executions with a trade ID that has no executions
        executions_df = pd.DataFrame({
            'trade_id': [1],
            'execution_id': [1],
            'quantity': [100],
            'price': [10.0],
            'ticker': ['AAPL'],
            'execution_timestamp': [pd.to_datetime('2023-01-01 10:00:00')],
            'is_entry': [1],
            'is_exit': [0],
            'commission': [1.0]
        })
        
        # Initialize processor with test data
        processor = TradeProcessor(executions_df)
        
        # Setup trade directions with an extra trade that doesn't exist in the executions
        processor.trade_directions = {
            1: {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0},
            2: {'direction': 'bearish', 'initial_quantity': -100.0, 'abs_initial_quantity': 100.0}  # No executions for trade 2
        }
        
        # Expect ValueError when calling the method
        with self.assertRaises(ValueError) as context:
            processor._get_trade_status()
        
        # Verify error message contains trade ID 2
        self.assertTrue("2" in str(context.exception))
        
        # Log test result
        self.log_case_result("Raises error for missing net position", True)
    
    def test_return_type(self):
        """Test the return type and structure of the method."""
        # Create simple executions for one trade
        executions_df = pd.DataFrame({
            'trade_id': [1, 1],
            'execution_id': [1, 2],
            'quantity': [100, -100],
            'price': [10.0, 11.0],
            'ticker': ['AAPL', 'AAPL'],
            'execution_timestamp': [
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00')
            ],
            'is_entry': [1, 0],
            'is_exit': [0, 1],
            'commission': [1.0, 1.0]
        })
        
        # Initialize processor with test data
        processor = TradeProcessor(executions_df)
        
        # Setup trade directions
        processor.trade_directions = {
            1: {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0}
        }
        
        # Call the method
        result = processor._get_trade_status()
        
        # Verify result type and structure
        self.assertIsInstance(result, pd.Series)
        self.assertEqual(result.index.tolist(), [1])  # Index should be trade_id (integer)
        self.assertEqual(result.iloc[0], 'closed')  # Value should be status string
        
        # Log test result
        self.log_case_result("Returns correct type and structure", True)

class TestGetExitType(BaseTestCase):
    """Test cases for the _get_exit_type method of TradeProcessor."""
    
    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a minimal executions DataFrame
        self.executions_df = pd.DataFrame({
            'trade_id': [1],
            'execution_id': [1],
            'quantity': [100],
            'price': [10.0],
            'ticker': ['AAPL'],
            'execution_timestamp': [pd.to_datetime('2023-01-01')],
            'is_entry': [1],
            'is_exit': [0],
            'commission': [1.0]
        })
        
        # Initialize the processor
        self.processor = TradeProcessor(self.executions_df)
    
    def test_stop_loss_exits(self):
        """Test identification of stop loss exits (risk-reward <= -1)."""
        # Setup test data
        risk_reward = pd.Series({
            1: -1.0,     # Exactly -1 -> stop_loss
            2: -1.5,     # Less than -1 -> stop_loss
            3: -0.5      # Greater than -1 -> not stop_loss
        })
        status = pd.Series({1: 'closed', 2: 'closed', 3: 'closed'})
        risk_reward_goal = 2.0
        
        # Call the method
        result = self.processor._get_exit_type(risk_reward, risk_reward_goal, status)
        
        # Verify results
        self.assertEqual(result[1], 'stop_loss')
        self.assertEqual(result[2], 'stop_loss')
        self.assertEqual(result[3], 'other')  # Not stop_loss, not take_profit -> other
        
        # Log test result
        self.log_case_result("Correctly identifies stop loss exits", True)
    
    def test_take_profit_exits(self):
        """Test identification of take profit exits (risk-reward >= goal)."""
        # Setup test data
        risk_reward = pd.Series({
            1: 2.0,      # Exactly goal -> take_profit
            2: 2.5,      # Above goal -> take_profit
            3: 1.5       # Below goal -> not take_profit
        })
        status = pd.Series({1: 'closed', 2: 'closed', 3: 'closed'})
        risk_reward_goal = 2.0
        
        # Call the method
        result = self.processor._get_exit_type(risk_reward, risk_reward_goal, status)
        
        # Verify results
        self.assertEqual(result[1], 'take_profit')
        self.assertEqual(result[2], 'take_profit')
        self.assertEqual(result[3], 'other')  # Not stop_loss, not take_profit -> other
        
        # Log test result
        self.log_case_result("Correctly identifies take profit exits", True)
    
    def test_other_exit_types(self):
        """Test identification of 'other' exit types (-1 < risk-reward < goal)."""
        # Setup test data
        risk_reward = pd.Series({
            1: -0.5,     # Between -1 and goal -> other
            2: 0.0,      # Between -1 and goal -> other
            3: 1.5       # Between -1 and goal -> other
        })
        status = pd.Series({1: 'closed', 2: 'closed', 3: 'closed'})
        risk_reward_goal = 2.0
        
        # Call the method
        result = self.processor._get_exit_type(risk_reward, risk_reward_goal, status)
        
        # Verify results
        self.assertEqual(result[1], 'other')
        self.assertEqual(result[2], 'other')
        self.assertEqual(result[3], 'other')
        
        # Log test result
        self.log_case_result("Correctly identifies 'other' exit types", True)
    
    def test_open_trades(self):
        """Test that open trades have exit_type = None."""
        # Setup test data
        risk_reward = pd.Series({
            1: 2.5,      # Good risk-reward but open -> None
            2: -1.5,     # Bad risk-reward but open -> None
            3: 1.0       # Average risk-reward and closed -> other
        })
        status = pd.Series({1: 'open', 2: 'open', 3: 'closed'})
        risk_reward_goal = 2.0
        
        # Call the method
        result = self.processor._get_exit_type(risk_reward, risk_reward_goal, status)
        
        # Verify results
        self.assertIsNone(result[1])
        self.assertIsNone(result[2])
        self.assertEqual(result[3], 'other')
        
        # Log test result
        self.log_case_result("Correctly handles open trades", True)
    
    def test_missing_risk_reward_values(self):
        """Test that trades with None/NaN risk-reward have exit_type = None."""
        # Setup test data
        risk_reward = pd.Series({
            1: None,
            2: float('nan'),
            3: 1.0
        })
        status = pd.Series({1: 'closed', 2: 'closed', 3: 'closed'})
        risk_reward_goal = 2.0
        
        # Call the method
        result = self.processor._get_exit_type(risk_reward, risk_reward_goal, status)
        
        # Verify results
        self.assertIsNone(result[1])
        self.assertIsNone(result[2])
        self.assertEqual(result[3], 'other')
        
        # Log test result
        self.log_case_result("Correctly handles missing risk-reward values", True)
    
    def test_missing_risk_reward_goal(self):
        """Test that when risk_reward_goal is None, no trades are classified as take_profit."""
        # Setup test data
        risk_reward = pd.Series({
            1: 5.0,      # Very high risk-reward but no goal -> other
            2: -1.5,     # Bad risk-reward -> stop_loss
            3: 1.0       # Average risk-reward -> other
        })
        status = pd.Series({1: 'closed', 2: 'closed', 3: 'closed'})
        risk_reward_goal = None
        
        # Call the method
        result = self.processor._get_exit_type(risk_reward, risk_reward_goal, status)
        
        # Verify results
        self.assertEqual(result[1], 'other')  # No take_profit classification with None goal
        self.assertEqual(result[2], 'stop_loss')
        self.assertEqual(result[3], 'other')
        
        # Log test result
        self.log_case_result("Correctly handles missing risk-reward goal", True)
    
    def test_mixed_exit_types(self):
        """Test handling of multiple trades with different exit types."""
        # Setup test data
        risk_reward = pd.Series({
            1: 2.5,      # Above goal -> take_profit
            2: -1.5,     # Below -1 -> stop_loss
            3: 1.0,      # Between -1 and goal -> other
            4: None,     # None risk-reward -> None exit type
            5: 3.0       # Above goal but open -> None exit type
        })
        status = pd.Series({1: 'closed', 2: 'closed', 3: 'closed', 4: 'closed', 5: 'open'})
        risk_reward_goal = 2.0
        
        # Call the method
        result = self.processor._get_exit_type(risk_reward, risk_reward_goal, status)
        
        # Verify results
        self.assertEqual(result[1], 'take_profit')
        self.assertEqual(result[2], 'stop_loss')
        self.assertEqual(result[3], 'other')
        self.assertIsNone(result[4])
        self.assertIsNone(result[5])
        
        # Log test result
        self.log_case_result("Correctly handles mixed exit types", True)
    
    def test_return_type(self):
        """Test that the method returns a pandas Series with correct indices."""
        # Setup test data
        risk_reward = pd.Series({1: 1.0, 2: -1.5, 3: 2.5})
        status = pd.Series({1: 'closed', 2: 'closed', 3: 'closed'})
        risk_reward_goal = 2.0
        
        # Call the method
        result = self.processor._get_exit_type(risk_reward, risk_reward_goal, status)
        
        # Verify result type and indices
        self.assertIsInstance(result, pd.Series)
        self.assertEqual(set(result.index), {1, 2, 3})
        
        # Log test result
        self.log_case_result("Returns correct type with correct indices", True)

class TestGetAllAggregations(BaseTestCase):
    """Test cases for the _get_all_aggregations method of TradeProcessor."""
    
    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a complete executions DataFrame with both entry and exit executions
        self.executions_df = pd.DataFrame({
            'trade_id': [1, 1, 2, 2],
            'execution_id': [1, 2, 3, 4],
            'quantity': [100, -100, -50, 50],
            'price': [10.0, 15.0, 20.0, 18.0],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],  # Used only symbol, removed ticker
            'execution_timestamp': [
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00'),
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-01 11:00:00')
            ],
            'is_entry': [1, 0, 1, 0],
            'is_exit': [0, 1, 0, 1],
            'commission': [1.0, 1.0, 1.0, 1.0],
            'date': ['2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01'],
            'time_of_day': ['10:00:00', '11:00:00', '10:00:00', '11:00:00']
        })
        
        # Initialize the processor
        self.processor = TradeProcessor(self.executions_df)
        
        # For preprocessing
        self.processor.preprocess()
    
    def test_successful_aggregation(self):
        """Test that _get_all_aggregations successfully returns all expected aggregations."""
        # Call the method
        result = self.processor._get_all_aggregations()
        
        # Expected keys in the result dictionary
        expected_keys = [
            'num_executions', 'symbol', 'direction', 'quantity', 
            'entry_price', 'capital_required', 'exit_price', 'stop_price',
            'take_profit_price', 'risk_reward', 'risk_amount_per_share',
            'is_winner', 'risk_per_trade', 'perc_return', 'status',
            'exit_type', 'end_date', 'end_time', 'duration_hours',
            'commission', 'start_date', 'start_time', 'week', 'month', 'year'
        ]
        
        # Verify that all expected keys are present
        for key in expected_keys:
            self.assertIn(key, result, f"Expected key '{key}' missing from result")
        
        # Verify the total number of keys
        self.assertEqual(len(result), len(expected_keys), 
                         f"Expected {len(expected_keys)} keys, got {len(result)}")
        
        # Verify types of results (each should be a Series or DataFrame)
        for key, value in result.items():
            self.assertTrue(isinstance(value, (pd.Series, pd.DataFrame)), 
                           f"Value for '{key}' is not a Series or DataFrame")
        
        # Log test result
        self.log_case_result("Successfully returns all expected aggregations", True)
    
    def test_exception_handling(self):
        """Test that exceptions during aggregation are caught and an empty dict is returned."""
        # Mock _get_num_executions to raise an exception
        with patch.object(self.processor, '_get_num_executions', side_effect=Exception("Test exception")):
            # Redirect stdout to capture print statements
            with patch('sys.stdout', new=StringIO()) as fake_out:
                # Call the method
                result = self.processor._get_all_aggregations()
                
                # Verify that an error message was printed
                self.assertIn("Error in _get_all_aggregations", fake_out.getvalue())
                
                # Verify that an empty dictionary was returned
                self.assertEqual(result, {})
        
        # Log test result
        self.log_case_result("Properly handles exceptions during aggregation", True)
    
    def test_internal_dependency_calls(self):
        """Test that all internal helper methods are called with correct parameters."""
        # Mock all the helper methods that _get_all_aggregations depends on
        with patch.object(self.processor, '_get_num_executions', return_value=pd.Series()) as mock_num_executions, \
             patch.object(self.processor, '_get_symbols', return_value=pd.Series()) as mock_symbols, \
             patch.object(self.processor, '_get_entry_date_time_info', return_value=pd.DataFrame({
                 'start_date': [], 'start_time': [], 'week': [], 'month': [], 'year': []
             })) as mock_entry_info, \
             patch.object(self.processor, '_get_quantity_and_entry_price', 
                          return_value=(pd.Series(), pd.Series(), pd.Series())) as mock_quantity_entry, \
             patch.object(self.processor, '_get_exit_price', return_value=pd.Series()) as mock_exit_price, \
             patch.object(self.processor, '_get_stop_prices', return_value=pd.Series()) as mock_stop_prices, \
             patch.object(self.processor, '_calculate_risk_reward_ratio', return_value=pd.Series()) as mock_risk_reward, \
             patch.object(self.processor, '_get_risk_amount_per_share', return_value=pd.Series()) as mock_risk_amount, \
             patch.object(self.processor, '_get_risk_per_trade', return_value=pd.Series()) as mock_risk_per_trade, \
             patch.object(self.processor, '_calculate_perc_return', return_value=pd.Series()) as mock_perc_return, \
             patch.object(self.processor, '_get_winning_trades', return_value=pd.Series()) as mock_winning_trades, \
             patch.object(self.processor, '_get_trade_status', return_value=pd.Series()) as mock_trade_status, \
             patch.object(self.processor, '_get_take_profit_price', return_value=pd.Series()) as mock_take_profit, \
             patch.object(self.processor, '_get_exit_type', return_value=pd.Series()) as mock_exit_type, \
             patch.object(self.processor, '_get_end_date_and_time', 
                          return_value=(pd.Series(), pd.Series())) as mock_end_date_time, \
             patch.object(self.processor, '_get_duration_hours', return_value=pd.Series()) as mock_duration, \
             patch.object(self.processor, '_get_commission', return_value=pd.Series()) as mock_commission:
             
            # Call the method
            self.processor._get_all_aggregations()
            
            # Verify that all methods were called
            mock_num_executions.assert_called_once()
            mock_symbols.assert_called_once()
            mock_entry_info.assert_called_once()
            mock_quantity_entry.assert_called_once()
            mock_exit_price.assert_called_once()
            mock_stop_prices.assert_called_once()
            mock_risk_reward.assert_called_once()
            mock_risk_amount.assert_called_once()
            mock_risk_per_trade.assert_called_once()
            mock_perc_return.assert_called_once()
            mock_winning_trades.assert_called_once()
            mock_trade_status.assert_called_once()
            mock_take_profit.assert_called_once()
            mock_exit_type.assert_called_once()
            mock_end_date_time.assert_called_once()
            mock_duration.assert_called_once()
            mock_commission.assert_called_once()
            
            # Verify specific parameters for methods that take them
            mock_stop_prices.assert_called_with(stop_loss_amount=0.02, entry_prices=mock_quantity_entry.return_value[1])
            mock_risk_reward.assert_called_with(
                entry_prices=mock_quantity_entry.return_value[1],
                exit_prices=mock_exit_price.return_value,
                stop_prices=mock_stop_prices.return_value
            )
            mock_risk_amount.assert_called_with(
                entry_prices=mock_quantity_entry.return_value[1],
                stop_prices=mock_stop_prices.return_value
            )
        
        # Log test result
        self.log_case_result("Correctly calls all dependency methods with proper parameters", True)
    
    def test_missing_exit_executions(self):
        """Test behavior when there are no exit executions."""
        # Create a processor with only entry executions
        entry_only_df = self.executions_df[self.executions_df['is_entry'] == 1].copy()
        processor = TradeProcessor(entry_only_df)
        
        # Set up trade directions manually
        processor.entry_execs = entry_only_df
        processor.exit_execs = pd.DataFrame()  # Empty DataFrame for exit executions
        processor.trade_directions = {
            1: {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0},
            2: {'direction': 'bearish', 'initial_quantity': -50.0, 'abs_initial_quantity': 50.0}
        }
        
        # Call the method
        result = processor._get_all_aggregations()
        
        # Verify that the result contains most keys, but end_date and end_time are None
        self.assertIsNone(result['end_date'])
        self.assertIsNone(result['end_time'])
        
        # Other keys should still be present
        self.assertIn('num_executions', result)
        self.assertIn('symbol', result)
        self.assertIn('direction', result)
        
        # Log test result
        self.log_case_result("Correctly handles missing exit executions", True)
    
    def test_take_profit_error_handling(self):
        """Test behavior when _get_take_profit_price raises an exception."""
        # Mock _get_take_profit_price to raise an exception
        with patch.object(self.processor, '_get_take_profit_price', 
                          side_effect=Exception("Test take profit exception")):
            # Redirect stdout to capture print statements
            with patch('sys.stdout', new=StringIO()) as fake_out:
                # Call the method
                result = self.processor._get_all_aggregations()
                
                # Verify that an error message about take profit was printed
                self.assertIn("Error calculating take profit prices", fake_out.getvalue())
                
                # Verify that take_profit_price is an empty Series but other keys exist
                self.assertIn('take_profit_price', result)
                self.assertTrue(result['take_profit_price'].empty)
                self.assertIn('num_executions', result)  # Other keys should still be present
        
        # Log test result
        self.log_case_result("Properly handles take profit calculation errors", True)
    
    def test_exit_type_error_handling(self):
        """Test behavior when _get_exit_type raises an exception."""
        # Mock _get_exit_type to raise an exception
        with patch.object(self.processor, '_get_exit_type', 
                          side_effect=Exception("Test exit type exception")):
            # Redirect stdout to capture print statements
            with patch('sys.stdout', new=StringIO()) as fake_out:
                # Call the method
                result = self.processor._get_all_aggregations()
                
                # Verify that an error message about exit type was printed
                self.assertIn("Error determining exit types", fake_out.getvalue())
                
                # Verify that exit_type is an empty Series but other keys exist
                self.assertIn('exit_type', result)
                self.assertTrue(result['exit_type'].empty)
                self.assertIn('num_executions', result)  # Other keys should still be present
        
        # Log test result
        self.log_case_result("Properly handles exit type determination errors", True)
    
    def test_direction_series_creation(self):
        """Test that the direction Series is created correctly."""
        # Call the method
        result = self.processor._get_all_aggregations()
        
        # Verify the direction Series matches the trade_directions dictionary
        expected_directions = pd.Series({
            trade_id: info['direction'] 
            for trade_id, info in self.processor.trade_directions.items()
        })
        pd.testing.assert_series_equal(result['direction'], expected_directions)
        
        # Log test result
        self.log_case_result("Correctly creates direction Series from trade_directions", True)
    
    def test_value_propagation(self):
        """Test that values flow correctly from one calculation to dependent ones."""
        # Create a controlled test environment with known values
        with patch.object(self.processor, '_get_quantity_and_entry_price', 
                          return_value=(pd.Series({1: 100}), pd.Series({1: 10.0}), pd.Series({1: 1000.0}))) as mock_quantity, \
             patch.object(self.processor, '_get_stop_prices', 
                          return_value=pd.Series({1: 9.0})) as mock_stop, \
             patch.object(self.processor, '_get_exit_price', 
                          return_value=pd.Series({1: 11.0})) as mock_exit:
             
            # Call the method with any other method mocked to prevent unrelated errors
            with patch.multiple(self.processor,
                              _get_num_executions=MagicMock(return_value=pd.DataFrame({'trade_id': [1], 'num_executions': [2]})),
                              _get_symbols=MagicMock(return_value=pd.Series({1: 'AAPL'})),
                              _get_entry_date_time_info=MagicMock(return_value=pd.DataFrame({
                                  'start_date': {1: '2023-01-01'}, 
                                  'start_time': {1: '10:00:00'}, 
                                  'week': {1: 1}, 
                                  'month': {1: 1}, 
                                  'year': {1: 2023}
                              })),
                              _get_trade_status=MagicMock(return_value=pd.Series({1: 'closed'})),
                              _get_end_date_and_time=MagicMock(return_value=(pd.Series({1: '2023-01-01'}), pd.Series({1: '11:00:00'}))),
                              _get_duration_hours=MagicMock(return_value=pd.Series({1: 1.0})),
                              _get_commission=MagicMock(return_value=pd.Series({1: 2.0}))):
                
                # We're not mocking these to test the value propagation
                # _calculate_risk_reward_ratio
                # _get_risk_amount_per_share
                # _get_risk_per_trade
                # _calculate_perc_return
                # _get_winning_trades
                # _get_take_profit_price
                # _get_exit_type
                
                # Call the method
                result = self.processor._get_all_aggregations()
                
                # Verify that _get_stop_prices was called
                self.assertTrue(mock_stop.called, "The _get_stop_prices method was not called")
                
                # Check the arguments manually instead of using assert_called_with
                call_args = mock_stop.call_args[1]  # Get the keyword arguments
                self.assertEqual(call_args['stop_loss_amount'], 0.02)
                self.assertTrue(isinstance(call_args['entry_prices'], pd.Series))
                self.assertEqual(list(call_args['entry_prices'].index), [1])
                self.assertEqual(call_args['entry_prices'][1], 10.0)
                
                # Verify risk_reward calculation used the correct values
                # For a bullish trade: (exit - entry) / (entry - stop) = (11 - 10) / (10 - 9) = 1.0
                self.assertEqual(result['risk_reward'].get(1), 1.0)
                
                # Verify risk_amount_per_share calculation
                # For a bullish trade: entry - stop = 10 - 9 = 1.0
                self.assertEqual(result['risk_amount_per_share'].get(1), 1.0)
                
                # Verify is_winner calculation
                # risk_reward > 0 -> winner
                self.assertEqual(result['is_winner'].get(1), 1)
        
        # Log test result
        self.log_case_result("Correctly propagates values between dependent calculations", True)

class TestBuildTradesDataframe(BaseTestCase):
    """Test cases for the _build_trades_dataframe method of TradeProcessor."""
    
    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a basic executions DataFrame
        self.executions_df = pd.DataFrame({
            'trade_id': [1, 2, 3],
            'execution_id': [1, 2, 3],
            'quantity': [100, -50, 200],
            'price': [10.0, 20.0, 15.0],
            'symbol': ['AAPL', 'MSFT', 'GOOGL'],
            'execution_timestamp': [
                pd.to_datetime('2023-01-01 10:00:00'),
                pd.to_datetime('2023-01-02 11:00:00'),
                pd.to_datetime('2023-01-03 12:00:00')
            ],
            'is_entry': [1, 1, 1],
            'is_exit': [0, 0, 0],
            'commission': [1.0, 1.0, 1.0],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'time_of_day': ['10:00:00', '11:00:00', '12:00:00']
        })
        
        # Initialize processor
        self.processor = TradeProcessor(self.executions_df)
        
        # Create a basic num_executions DataFrame
        self.num_executions = pd.DataFrame({
            'trade_id': [1, 2, 3],
            'num_executions': [2, 1, 3]
        })
    
    def test_basic_functionality(self):
        """Test that the method correctly builds a DataFrame from a dictionary of aggregations."""
        # Create a simple aggregations dictionary
        aggs = {
            'num_executions': self.num_executions,
            'symbol': pd.Series({1: 'AAPL', 2: 'MSFT', 3: 'GOOGL'}),
            'quantity': pd.Series({1: 100, 2: 50, 3: 200}),
            'entry_price': pd.Series({1: 10.0, 2: 20.0, 3: 15.0})
        }
        
        # Call the method
        result = self.processor._build_trades_dataframe(aggs)
        
        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)  # Should have 3 rows
        self.assertEqual(list(result.columns), ['trade_id', 'num_executions', 'symbol', 'quantity', 'entry_price'])
        
        # Check values
        self.assertEqual(result.loc[0, 'trade_id'], 1)
        self.assertEqual(result.loc[0, 'num_executions'], 2)
        self.assertEqual(result.loc[0, 'symbol'], 'AAPL')
        self.assertEqual(result.loc[0, 'quantity'], 100)
        self.assertEqual(result.loc[0, 'entry_price'], 10.0)
        
        # Log test result
        self.log_case_result("Correctly builds DataFrame from aggregations", True)
        
    def test_column_mapping(self):
        """Test that all Series in the aggregations dictionary are correctly mapped as columns."""
        # Create a dictionary with multiple metrics
        aggs = {
            'num_executions': self.num_executions,
            'symbol': pd.Series({1: 'AAPL', 2: 'MSFT', 3: 'GOOGL'}),
            'direction': pd.Series({1: 'bullish', 2: 'bearish', 3: 'bullish'}),
            'quantity': pd.Series({1: 100, 2: 50, 3: 200}),
            'entry_price': pd.Series({1: 10.0, 2: 20.0, 3: 15.0}),
            'exit_price': pd.Series({1: 11.0, 2: 19.0, 3: 16.0}),
            'risk_reward': pd.Series({1: 1.5, 2: 0.8, 3: 1.2}),
            'is_winner': pd.Series({1: 1, 2: 0, 3: 1})
        }
        
        # Call the method
        result = self.processor._build_trades_dataframe(aggs)
        
        # Verify all columns are present
        expected_columns = ['trade_id', 'num_executions', 'symbol', 'direction', 'quantity', 
                           'entry_price', 'exit_price', 'risk_reward', 'is_winner']
        self.assertEqual(list(result.columns), expected_columns)
        
        # Verify mapping is correct for each row
        for i, trade_id in enumerate([1, 2, 3]):
            for col in aggs.keys():
                if col == 'num_executions':
                    self.assertEqual(result.loc[i, col], aggs[col].loc[aggs[col]['trade_id'] == trade_id, col].iloc[0])
                else:
                    self.assertEqual(result.loc[i, col], aggs[col].get(trade_id))
        
        # Log test result
        self.log_case_result("Correctly maps all Series as columns in the DataFrame", True)
        
    def test_empty_aggregations(self):
        """Test behavior with minimal or empty aggregations."""
        # Test with only num_executions
        aggs = {'num_executions': self.num_executions}
        result = self.processor._build_trades_dataframe(aggs)
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ['trade_id', 'num_executions'])
        self.assertEqual(len(result), 3)
        
        # Log test result
        self.log_case_result("Handles minimal aggregations correctly", True)
        
    def test_type_conversion(self):
        """Test handling of different data types in aggregations."""
        # Create aggregations with different data types
        aggs = {
            'num_executions': self.num_executions,
            'symbol': pd.Series({1: 'AAPL', 2: 'MSFT', 3: 'GOOGL'}),  # strings
            'quantity': pd.Series({1: 100, 2: 50, 3: 200}),  # integers
            'entry_price': pd.Series({1: 10.0, 2: 20.0, 3: 15.0}),  # floats
            'entry_date': pd.Series({1: pd.Timestamp('2023-01-01'), 
                                    2: pd.Timestamp('2023-01-02'), 
                                    3: pd.Timestamp('2023-01-03')}),  # timestamps
            'is_active': pd.Series({1: True, 2: False, 3: True})  # booleans
        }
        
        # Call the method
        result = self.processor._build_trades_dataframe(aggs)
        
        # Verify types are preserved
        self.assertEqual(result['symbol'].dtype, 'object')  # strings stored as objects
        self.assertTrue(np.issubdtype(result['quantity'].dtype, np.integer))
        self.assertTrue(np.issubdtype(result['entry_price'].dtype, np.floating))
        self.assertTrue(pd.api.types.is_datetime64_dtype(result['entry_date']))
        self.assertTrue(pd.api.types.is_bool_dtype(result['is_active']))
        
        # Log test result
        self.log_case_result("Correctly preserves data types", True)
        
    def test_missing_values(self):
        """Test handling of missing values in Series."""
        # Create Series with missing values
        symbol_series = pd.Series({1: 'AAPL', 3: 'GOOGL'})  # Missing trade_id 2
        price_series = pd.Series({1: 10.0, 2: 20.0})  # Missing trade_id 3
        
        aggs = {
            'num_executions': self.num_executions,
            'symbol': symbol_series,
            'entry_price': price_series
        }
        
        # Call the method
        result = self.processor._build_trades_dataframe(aggs)
        
        # Verify missing values are handled correctly
        self.assertTrue(pd.isna(result.loc[1, 'symbol']))  # trade_id 2 has missing symbol
        self.assertTrue(pd.isna(result.loc[2, 'entry_price']))  # trade_id 3 has missing entry_price
        
        # Non-missing values should still be present
        self.assertEqual(result.loc[0, 'symbol'], 'AAPL')
        self.assertEqual(result.loc[0, 'entry_price'], 10.0)
        
        # Log test result
        self.log_case_result("Correctly handles missing values", True)
        
    def test_dataframe_in_aggregations(self):
        """Test handling of DataFrame objects in aggregations."""
        # Create a DataFrame for inclusion in aggregations
        entry_info = pd.DataFrame({
            'start_date': {1: '2023-01-01', 2: '2023-01-02', 3: '2023-01-03'},
            'start_time': {1: '10:00:00', 2: '11:00:00', 3: '12:00:00'},
            'week': {1: 1, 2: 1, 3: 1},
            'month': {1: 1, 2: 1, 3: 1},
            'year': {1: 2023, 2: 2023, 3: 2023}
        })
        
        aggs = {
            'num_executions': self.num_executions,
            'symbol': pd.Series({1: 'AAPL', 2: 'MSFT', 3: 'GOOGL'}),
            'entry_info': entry_info  # This is a DataFrame, not a Series
        }
        
        # We expect this to fail since _build_trades_dataframe expects Series, not DataFrames
        with self.assertRaises(Exception):
            self.processor._build_trades_dataframe(aggs)
        
        # Log test result
        self.log_case_result("Correctly raises exception for DataFrame in aggregations", True)
        
    def test_return_type_verification(self):
        """Test that the return type is always a DataFrame with the expected structure."""
        # Basic aggregations
        aggs = {
            'num_executions': self.num_executions,
            'symbol': pd.Series({1: 'AAPL', 2: 'MSFT', 3: 'GOOGL'})
        }
        
        # Call the method
        result = self.processor._build_trades_dataframe(aggs)
        
        # Verify return type
        self.assertIsInstance(result, pd.DataFrame)
        
        # Verify structure
        self.assertEqual(result.index.name, None)  # Index is not named
        self.assertEqual(result.index.dtype, np.dtype('int64'))  # Index is integer
        self.assertTrue('trade_id' in result.columns)
        self.assertTrue('num_executions' in result.columns)
        
        # Verify data types
        self.assertTrue(np.issubdtype(result['trade_id'].dtype, np.integer))
        self.assertTrue(np.issubdtype(result['num_executions'].dtype, np.integer))
        
        # Log test result
        self.log_case_result("Returns DataFrame with correct structure", True)

class TestGetNumExecutions(BaseTestCase):
    """Test cases for the _get_num_executions method of TradeProcessor."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()

    def test_basic_functionality(self):
        """Test that the method correctly counts executions for each trade_id."""
        # Create a test DataFrame with multiple trade_ids and varying numbers of executions
        executions_df = pd.DataFrame({
            'trade_id': [1, 1, 1, 2, 2, 3],  # 3 executions for trade_id 1, 2 for trade_id 2, 1 for trade_id 3
            'execution_id': range(1, 7),
            'quantity': [100, 50, -150, 200, -200, 300],
            'price': [10.0, 10.5, 11.0, 20.0, 21.0, 30.0],
            'symbol': ['AAPL'] * 6,
            'date': ['2023-01-01'] * 6,
            'time_of_day': ['10:00:00'] * 6,
            'execution_timestamp': pd.to_datetime(['2023-01-01 10:00:00'] * 6),
            'is_entry': [1, 1, 0, 1, 0, 1],
            'is_exit': [0, 0, 1, 0, 1, 0]
        })

        # Initialize processor
        processor = TradeProcessor(executions_df)

        # Call the method
        result = processor._get_num_executions()

        # Verify result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)  # 3 unique trade_ids

        # Check counts are correct
        counts = result.set_index('trade_id')['num_executions'].to_dict()
        self.assertEqual(counts[1], 3)
        self.assertEqual(counts[2], 2)
        self.assertEqual(counts[3], 1)

        # Log test result
        self.log_case_result("Correctly counts executions per trade_id", True)

    def test_empty_dataframe(self):
        """Test behavior when the executions DataFrame is empty."""
        # Create an empty DataFrame
        empty_df = pd.DataFrame(columns=['trade_id', 'execution_id', 'quantity', 'price', 'symbol'])

        # Initialize processor
        processor = TradeProcessor(empty_df)

        # Call the method
        result = processor._get_num_executions()

        # Verify result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)  # Result should be empty

        # Log test result
        self.log_case_result("Returns empty DataFrame for empty input", True)

    def test_duplicate_trade_ids(self):
        """Test that it correctly counts when multiple executions have the same trade_id."""
        # Create a test DataFrame with repeated trade_ids
        executions_df = pd.DataFrame({
            'trade_id': ['A', 'A', 'A', 'A', 'A', 'B'],  # 5 executions for trade_id 'A', 1 for 'B'
            'execution_id': range(1, 7),
            'quantity': [100, 50, 75, 25, -250, 300],
            'price': [10.0, 10.5, 11.0, 11.5, 12.0, 20.0],
            'symbol': ['AAPL'] * 6,
            'date': ['2023-01-01'] * 6,
            'time_of_day': ['10:00:00'] * 6,
            'execution_timestamp': pd.to_datetime(['2023-01-01 10:00:00'] * 6),
            'is_entry': [1, 1, 1, 1, 0, 1],
            'is_exit': [0, 0, 0, 0, 1, 0]
        })

        # Initialize processor
        processor = TradeProcessor(executions_df)

        # Call the method
        result = processor._get_num_executions()

        # Verify result
        counts = result.set_index('trade_id')['num_executions'].to_dict()
        self.assertEqual(counts['A'], 5)
        self.assertEqual(counts['B'], 1)

        # Log test result
        self.log_case_result("Correctly counts duplicate trade_ids", True)

    def test_return_type(self):
        """Verify that the method returns a DataFrame with the expected structure."""
        # Create a basic test DataFrame
        executions_df = pd.DataFrame({
            'trade_id': [1, 2, 3],
            'execution_id': [1, 2, 3],
            'quantity': [100, 200, 300],
            'price': [10.0, 20.0, 30.0],
            'symbol': ['AAPL', 'MSFT', 'GOOGL'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'time_of_day': ['10:00:00', '11:00:00', '12:00:00'],
            'execution_timestamp': pd.to_datetime(['2023-01-01 10:00:00', 
                                                  '2023-01-02 11:00:00',
                                                  '2023-01-03 12:00:00']),
            'is_entry': [1, 1, 1],
            'is_exit': [0, 0, 0]
        })

        # Initialize processor
        processor = TradeProcessor(executions_df)

        # Call the method
        result = processor._get_num_executions()

        # Verify it's a DataFrame
        self.assertIsInstance(result, pd.DataFrame)

        # Verify it has exactly two columns
        self.assertEqual(list(result.columns), ['trade_id', 'num_executions'])

        # Verify data types
        self.assertTrue(np.issubdtype(result['num_executions'].dtype, np.integer))

        # Verify it has the same trade_id type as input
        self.assertEqual(result['trade_id'].dtype, executions_df['trade_id'].dtype)

        # Log test result
        self.log_case_result("Returns DataFrame with correct structure", True)

    def test_large_dataset(self):
        """Test performance with a large number of executions."""
        # Create a larger dataset with many trades and executions
        trades = list(range(1, 101))  # 100 different trade_ids
        trade_ids = []
        for trade_id in trades:
            # Each trade has between 1 and 10 executions
            num_execs = (trade_id % 10) + 1
            trade_ids.extend([trade_id] * num_execs)
        
        executions_df = pd.DataFrame({
            'trade_id': trade_ids,
            'execution_id': range(1, len(trade_ids) + 1),
            'quantity': [100] * len(trade_ids),
            'price': [10.0] * len(trade_ids),
            'symbol': ['AAPL'] * len(trade_ids),
            'date': ['2023-01-01'] * len(trade_ids),
            'time_of_day': ['10:00:00'] * len(trade_ids),
            'execution_timestamp': pd.to_datetime(['2023-01-01 10:00:00'] * len(trade_ids)),
            'is_entry': [1] * len(trade_ids),
            'is_exit': [0] * len(trade_ids)
        })

        # Initialize processor
        processor = TradeProcessor(executions_df)

        # Call the method
        result = processor._get_num_executions()

        # Verify basic properties
        self.assertEqual(len(result), 100)  # 100 unique trade_ids

        # Verify some counts
        counts = result.set_index('trade_id')['num_executions'].to_dict()
        for trade_id in trades:
            expected_count = (trade_id % 10) + 1
            self.assertEqual(counts[trade_id], expected_count)

        # Log test result
        self.log_case_result("Correctly handles large datasets", True)

    def test_integration_with_build_trades_dataframe(self):
        """Ensure the output of _get_num_executions can be properly used by _build_trades_dataframe."""
        # Create a basic test DataFrame
        executions_df = pd.DataFrame({
            'trade_id': [1, 1, 2, 3],
            'execution_id': [1, 2, 3, 4],
            'quantity': [100, -100, 200, 300],
            'price': [10.0, 11.0, 20.0, 30.0],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'GOOGL'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            'time_of_day': ['10:00:00', '16:00:00', '11:00:00', '12:00:00'],
            'execution_timestamp': pd.to_datetime([
                '2023-01-01 10:00:00', '2023-01-02 16:00:00', 
                '2023-01-03 11:00:00', '2023-01-04 12:00:00'
            ]),
            'is_entry': [1, 0, 1, 1],
            'is_exit': [0, 1, 0, 0]
        })

        # Initialize processor
        processor = TradeProcessor(executions_df)

        # Get num_executions
        num_executions = processor._get_num_executions()

        # Create a simple aggregations dictionary
        aggs = {
            'num_executions': num_executions,
            'symbol': pd.Series({1: 'AAPL', 2: 'MSFT', 3: 'GOOGL'})
        }

        # Build trades DataFrame
        result = processor._build_trades_dataframe(aggs)

        # Verify the result has the correct structure
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ['trade_id', 'num_executions', 'symbol'])
        self.assertEqual(len(result), 3)  # 3 unique trade_ids

        # Verify values are correctly mapped
        self.assertEqual(result.loc[0, 'trade_id'], 1)
        self.assertEqual(result.loc[0, 'num_executions'], 2)
        self.assertEqual(result.loc[0, 'symbol'], 'AAPL')

        # Log test result
        self.log_case_result("Integrates correctly with _build_trades_dataframe", True)

    def test_column_naming(self):
        """Verify that the column containing the counts is correctly named 'num_executions'."""
        # Create a simple DataFrame
        executions_df = pd.DataFrame({
            'trade_id': [1, 2, 3],
            'execution_id': [1, 2, 3],
            'quantity': [100, 200, 300],
            'price': [10.0, 20.0, 30.0],
            'symbol': ['AAPL', 'MSFT', 'GOOGL']
        })

        # Initialize processor
        processor = TradeProcessor(executions_df)

        # Call the method
        result = processor._get_num_executions()

        # Verify the column name is exactly 'num_executions'
        self.assertIn('num_executions', result.columns)
        self.assertEqual(result.columns[1], 'num_executions')  # Should be the second column

        # Log test result
        self.log_case_result("Correctly names the count column 'num_executions'", True)

    def test_reset_index_operation(self):
        """Verify that the trade_id becomes a column, not an index."""
        # Create a test DataFrame
        executions_df = pd.DataFrame({
            'trade_id': ['A', 'B', 'C'],
            'execution_id': [1, 2, 3],
            'quantity': [100, 200, 300],
            'price': [10.0, 20.0, 30.0],
            'symbol': ['AAPL', 'MSFT', 'GOOGL']
        })

        # Initialize processor
        processor = TradeProcessor(executions_df)

        # Call the method
        result = processor._get_num_executions()

        # Verify that trade_id is a column, not the index
        self.assertIn('trade_id', result.columns)
        
        # Check that the index is a simple RangeIndex (0, 1, 2, ...)
        self.assertTrue(isinstance(result.index, pd.RangeIndex))
        self.assertEqual(list(result.index), [0, 1, 2])

        # Log test result
        self.log_case_result("Correctly converts trade_id to column with reset_index", True)

    def test_data_integrity(self):
        """Ensure no data is lost or modified during the transformations."""
        # Create a test DataFrame with known trade_ids and execution counts
        executions_df = pd.DataFrame({
            'trade_id': ['X', 'X', 'Y', 'Y', 'Y', 'Z'],
            'execution_id': range(1, 7),
            'quantity': [100, 200, 300, 400, 500, 600],
            'price': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT', 'MSFT', 'GOOGL']
        })

        # Initialize processor
        processor = TradeProcessor(executions_df)

        # Call the method
        result = processor._get_num_executions()

        # Create expected result - trade_id X has 2 executions, Y has 3, Z has 1
        expected_counts = {'X': 2, 'Y': 3, 'Z': 1}

        # Verify counts match exactly as expected for each trade_id
        for index, row in result.iterrows():
            trade_id = row['trade_id']
            count = row['num_executions']
            self.assertEqual(count, expected_counts[trade_id])

        # Verify all trade_ids are present
        self.assertEqual(set(result['trade_id']), set(expected_counts.keys()))

        # Log test result
        self.log_case_result("Maintains data integrity through transformations", True)

    def test_preserve_trade_id_type(self):
        """Ensure the trade_id column retains its original data type."""
        # Test with different types of trade_ids
        test_cases = [
            # Case 1: Integer trade_ids
            pd.DataFrame({'trade_id': [1, 2, 3], 'execution_id': [1, 2, 3]}),
            
            # Case 2: String trade_ids
            pd.DataFrame({'trade_id': ['A', 'B', 'C'], 'execution_id': [1, 2, 3]}),
            
            # Case 3: Float trade_ids
            pd.DataFrame({'trade_id': [1.1, 2.2, 3.3], 'execution_id': [1, 2, 3]})
        ]

        for i, df in enumerate(test_cases):
            # Add required columns
            df['quantity'] = [100, 200, 300]
            df['price'] = [10.0, 20.0, 30.0]
            df['symbol'] = ['AAPL', 'MSFT', 'GOOGL']

            # Initialize processor
            processor = TradeProcessor(df)

            # Call the method
            result = processor._get_num_executions()

            # Verify the type in the result matches the input type
            self.assertEqual(result['trade_id'].dtype, df['trade_id'].dtype)

            # Log test result
            self.log_case_result(f"Preserves trade_id data type (case {i+1})", True)

    def test_nonstandard_trade_ids(self):
        """Check how it handles unusual trade_id values."""
        # Create a test DataFrame with unusual trade_id values
        executions_df = pd.DataFrame({
            'trade_id': [
                'trade-123',               # With hyphen
                'trade_with_underscores',  # With underscores
                '!!!Special@@@',           # Special characters
                'a' * 50,                  # Very long string
                '   ',                     # Just spaces
                '0'                        # Single digit
            ],
            'execution_id': range(1, 7),
            'quantity': [100] * 6,
            'price': [10.0] * 6,
            'symbol': ['AAPL'] * 6
        })

        # Initialize processor
        processor = TradeProcessor(executions_df)

        # Call the method
        result = processor._get_num_executions()

        # Verify all trade_ids are preserved correctly
        result_trade_ids = set(result['trade_id'])
        input_trade_ids = set(executions_df['trade_id'])
        self.assertEqual(result_trade_ids, input_trade_ids)

        # Check counts (should all be 1)
        for _, row in result.iterrows():
            self.assertEqual(row['num_executions'], 1)

        # Log test result
        self.log_case_result("Correctly handles non-standard trade_id values", True)

class TestGetEndDateAndTime(BaseTestCase):
    """Test cases for the _get_end_date_and_time method of TradeProcessor."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a sample DataFrame for testing
        timestamps = [
            pd.Timestamp('2023-01-01 10:00:00'),  # trade1 entry
            pd.Timestamp('2023-01-01 14:30:00'),  # trade1 exit
            pd.Timestamp('2023-01-02 09:15:00'),  # trade2 entry
            pd.Timestamp('2023-01-02 16:45:00'),  # trade2 exit
            pd.Timestamp('2023-01-03 11:30:00'),  # trade3 entry, no exit
            pd.Timestamp('2023-01-04 09:00:00'),  # trade4 entry
            pd.Timestamp('2023-01-04 12:30:00'),  # trade4 exit
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        self.executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade2', 'trade2', 'trade3', 'trade4', 'trade4'],
            'execution_id': ['exec1', 'exec2', 'exec3', 'exec4', 'exec5', 'exec6', 'exec7'],
            'quantity': [100, -100, 200, -200, 300, 400, -400],
            'price': [10.0, 11.0, 20.0, 21.0, 30.0, 40.0, 41.0],
            'execution_timestamp': timestamps,
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT', 'GOOG', 'AMZN', 'AMZN'],
            'is_entry': [1, 0, 1, 0, 1, 1, 0],  # 1 for entries, 0 for exits
            'is_exit': [0, 1, 0, 1, 0, 0, 1],    # 0 for entries, 1 for exits
            'date': dates,  # Add date column
            'time_of_day': times  # Add time_of_day column
        })
        
        # Create trade directions for testing
        self.trade_directions = {
            'trade1': 'bullish',
            'trade2': 'bearish',
            'trade3': 'bullish',
            'trade4': 'bullish',
            'trade5': 'bearish'  # Trade without executions
        }
        
        # Create an instance of TradeProcessor with the test data
        self.processor = TradeProcessor(self.executions_df)
        self.processor.trade_directions = self.trade_directions
        
        # Initialize processor state by calling necessary methods
        # We need entry and exit executions to be properly separated
        self.processor.preprocess()
        
        # Get entry and exit executions for testing
        self.entry_executions = self.processor.entry_execs
        self.exit_executions = self.processor.exit_execs

    def test_basic_functionality(self):
        """Verify that end date and time are correctly extracted from exit executions."""
        # Call the method
        end_date_series, end_time_series = self.processor._get_end_date_and_time()
        
        # Verify types and structures
        self.assertIsInstance(end_date_series, pd.Series)
        self.assertIsInstance(end_time_series, pd.Series)
        
        # The method returns Series with names 'date' and 'time_of_day', not 'end_date' and 'end_time'
        self.assertEqual(end_date_series.name, 'date')
        self.assertEqual(end_time_series.name, 'time_of_day')
        self.assertEqual(end_date_series.index.name, 'trade_id')
        self.assertEqual(end_time_series.index.name, 'trade_id')
        
        # Check specific values for date
        self.assertEqual(end_date_series['trade1'], '2023-01-01')
        self.assertEqual(end_date_series['trade2'], '2023-01-02')
        self.assertNotIn('trade3', end_date_series.index)  # No exit for trade3
        self.assertEqual(end_date_series['trade4'], '2023-01-04')
        
        # Check specific values for time
        self.assertEqual(end_time_series['trade1'], '14:30:00')
        self.assertEqual(end_time_series['trade2'], '16:45:00')
        self.assertNotIn('trade3', end_time_series.index)  # No exit for trade3
        self.assertEqual(end_time_series['trade4'], '12:30:00')
        
        # trade5 should not be in the results as it has no executions
        self.assertNotIn('trade5', end_date_series.index)
        self.assertNotIn('trade5', end_time_series.index)
        
        # Log test result
        self.log_case_result("Correctly extracts end date and time from exit executions", True)

    def test_no_exit_executions(self):
        """Verify behavior when a trade has no exit executions."""
        # Use the test case already in the data: trade3 has no exit executions
        end_date_series, end_time_series = self.processor._get_end_date_and_time()
        
        # Verify trade3 is not in the results
        self.assertNotIn('trade3', end_date_series.index)
        self.assertNotIn('trade3', end_time_series.index)
        
        # Log test result
        self.log_case_result("Excludes trades with no exit executions", True)

    def test_empty_exit_executions(self):
        """Verify behavior with empty exit executions DataFrame."""
        # Create a processor with no exit executions
        processor = TradeProcessor(self.executions_df[self.executions_df['quantity'] > 0])  # Only entries
        processor.trade_directions = self.trade_directions
        processor.preprocess()
        
        # Call the method
        end_date_series, end_time_series = processor._get_end_date_and_time()
        
        # The method returns None when there are no exit executions
        self.assertIsNone(end_date_series)
        self.assertIsNone(end_time_series)
        
        # Log test result
        self.log_case_result("Returns None when there are no exit executions", True)

    def test_multiple_exit_executions(self):
        """Verify behavior with multiple exit executions for the same trade."""
        # Create DataFrame with multiple exit executions for a trade
        timestamps = [
            pd.Timestamp('2023-01-05 09:00:00'),  # Entry
            pd.Timestamp('2023-01-05 10:00:00'),  # First exit
            pd.Timestamp('2023-01-05 11:30:00'),  # Second exit
            pd.Timestamp('2023-01-05 13:45:00'),  # Last exit
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        multi_exit_df = pd.DataFrame({
            'trade_id': ['multi1', 'multi1', 'multi1', 'multi1'],
            'execution_id': ['mexec1', 'mexec2', 'mexec3', 'mexec4'],
            'quantity': [100, -25, -25, -50],
            'price': [10.0, 11.0, 12.0, 13.0],
            'execution_timestamp': timestamps,
            'symbol': ['TSLA', 'TSLA', 'TSLA', 'TSLA'],
            'is_entry': [1, 0, 0, 0],  # 1 for entry, 0 for exits
            'is_exit': [0, 1, 1, 1],   # 0 for entry, 1 for exits
            'date': dates,  # Add date column
            'time_of_day': times  # Add time_of_day column
        })
        
        # Create processor with multi-exit data
        processor = TradeProcessor(multi_exit_df)
        processor.trade_directions = {'multi1': 'bullish'}
        processor.preprocess()
        
        # Call the method
        end_date_series, end_time_series = processor._get_end_date_and_time()
        
        # Verify we get the last exit timestamp
        self.assertEqual(end_date_series['multi1'], '2023-01-05')
        self.assertEqual(end_time_series['multi1'], '13:45:00')
        
        # Log test result
        self.log_case_result("Uses the last exit execution for end date and time", True)

    def test_out_of_order_timestamps(self):
        """Verify behavior when exit executions have timestamps in random order."""
        # Create DataFrame with out-of-order exit timestamps
        timestamps = [
            pd.Timestamp('2023-01-05 09:00:00'),  # Entry
            pd.Timestamp('2023-01-05 13:45:00'),  # Exit (latest)
            pd.Timestamp('2023-01-05 10:00:00'),  # Exit (earliest)
            pd.Timestamp('2023-01-05 11:30:00'),  # Exit (middle)
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        out_of_order_df = pd.DataFrame({
            'trade_id': ['order1', 'order1', 'order1', 'order1'],
            'execution_id': ['oexec1', 'oexec2', 'oexec3', 'oexec4'],
            'quantity': [25, -25, -25, -25],
            'price': [10.0, 11.0, 12.0, 13.0],
            'execution_timestamp': timestamps,
            'symbol': ['XYZ', 'XYZ', 'XYZ', 'XYZ'],
            'is_entry': [1, 0, 0, 0],  # 1 for entry, 0 for exits
            'is_exit': [0, 1, 1, 1],   # 0 for entry, 1 for exits
            'date': dates,  # Add date column
            'time_of_day': times  # Add time_of_day column
        })
        
        # Create processor with out-of-order data
        processor = TradeProcessor(out_of_order_df)
        processor.trade_directions = {'order1': 'bullish'}
        processor.preprocess()
        
        # Call the method
        end_date_series, end_time_series = processor._get_end_date_and_time()
        
        # Verify we get the latest timestamp regardless of order in DataFrame
        self.assertEqual(end_date_series['order1'], '2023-01-05')
        self.assertEqual(end_time_series['order1'], '13:45:00')
        
        # Log test result
        self.log_case_result("Correctly identifies latest timestamp regardless of row order", True)

    def test_return_type_verification(self):
        """Verify the method returns pandas Series with expected properties."""
        # Call the method
        end_date_series, end_time_series = self.processor._get_end_date_and_time()
        
        # Verify both are pandas Series
        self.assertIsInstance(end_date_series, pd.Series)
        self.assertIsInstance(end_time_series, pd.Series)
        
        # Verify index is trade_id
        self.assertEqual(end_date_series.index.name, 'trade_id')
        self.assertEqual(end_time_series.index.name, 'trade_id')
        
        # Verify Series names
        self.assertEqual(end_date_series.name, 'date')
        self.assertEqual(end_time_series.name, 'time_of_day')
        
        # Verify data types (should be string/object)
        self.assertTrue(pd.api.types.is_string_dtype(end_date_series.dtype) or 
                        pd.api.types.is_object_dtype(end_date_series.dtype))
        self.assertTrue(pd.api.types.is_string_dtype(end_time_series.dtype) or 
                        pd.api.types.is_object_dtype(end_time_series.dtype))
        
        # Log test result
        self.log_case_result("Returns Series with correct structure and data types", True)

    def test_integration(self):
        """Test integration with other methods that depend on end_date and end_time."""
        # Make sure required state is set up
        self.processor.preprocess()
        
        # Get end date and time
        end_date_series, end_time_series = self.processor._get_end_date_and_time()
        
        # Get all aggregations (which should include end_date and end_time)
        aggs = self.processor._get_all_aggregations()
        
        # Verify end_date and end_time are properly included in aggregations
        self.assertIn('end_date', aggs)
        self.assertIn('end_time', aggs)
        
        # Verify the values match
        pd.testing.assert_series_equal(aggs['end_date'], end_date_series)
        pd.testing.assert_series_equal(aggs['end_time'], end_time_series)
        
        # Log test result
        self.log_case_result("Integrates properly with dependent methods", True)

    def test_edge_cases(self):
        """Test with various edge cases like missing timestamps, etc."""
        # Create DataFrame with edge cases
        timestamps = [
            pd.Timestamp('2023-01-01 10:00:00'),  # edge1 entry
            pd.NaT,                              # edge1 exit - NaT timestamp
            pd.Timestamp('2023-01-02 09:15:00'),  # edge2 entry
            pd.Timestamp('2023-01-02 16:45:00'),  # edge2 exit
        ]
        
        # Create dates and times from timestamps, handling NaT values
        dates = []
        times = []
        for ts in timestamps:
            if pd.isna(ts):
                dates.append(None)
                times.append(None)
            else:
                dates.append(ts.strftime('%Y-%m-%d'))
                times.append(ts.strftime('%H:%M:%S'))
        
        edge_df = pd.DataFrame({
            'trade_id': ['edge1', 'edge1', 'edge2', 'edge2'],
            'execution_id': ['edge_exec1', 'edge_exec2', 'edge_exec3', 'edge_exec4'],
            'quantity': [100, -100, 200, -200],
            'price': [10.0, 11.0, 20.0, 21.0],
            'execution_timestamp': timestamps,
            'symbol': ['EDGE', 'EDGE', 'EDGE', 'EDGE'],
            'is_entry': [1, 0, 1, 0],  # 1 for entries, 0 for exits
            'is_exit': [0, 1, 0, 1],   # 0 for entries, 1 for exits
            'date': dates,  # Add date column with None for NaT
            'time_of_day': times  # Add time_of_day column with None for NaT
        })
        
        # Create processor with edge case data
        processor = TradeProcessor(edge_df)
        processor.trade_directions = {'edge1': 'bullish', 'edge2': 'bullish'}
        processor.preprocess()
        
        # Call the method
        end_date_series, end_time_series = processor._get_end_date_and_time()
        
        # edge1 should not be in the results due to NaT timestamp
        self.assertNotIn('edge1', end_date_series.index)
        self.assertNotIn('edge1', end_time_series.index)
        
        # edge2 should have normal values
        self.assertEqual(end_date_series['edge2'], '2023-01-02')
        self.assertEqual(end_time_series['edge2'], '16:45:00')
        
        # Log test result
        self.log_case_result("Handles edge cases correctly", True)

class TestGetEntryDateTimeInfo(BaseTestCase):
    """Test cases for the _get_entry_date_time_info method of TradeProcessor."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a sample DataFrame for testing
        timestamps = [
            pd.Timestamp('2023-01-01 10:00:00'),  # trade1 entry
            pd.Timestamp('2023-01-01 14:30:00'),  # trade1 exit
            pd.Timestamp('2023-01-02 09:15:00'),  # trade2 entry - different month
            pd.Timestamp('2023-01-02 16:45:00'),  # trade2 exit
            pd.Timestamp('2023-01-03 11:30:00'),  # trade3 entry - different month
            pd.Timestamp('2023-01-04 09:00:00'),  # trade4 entry - different month
            pd.Timestamp('2023-01-04 12:30:00'),  # trade4 exit
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        self.executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade2', 'trade2', 'trade3', 'trade4', 'trade4'],
            'execution_id': ['exec1', 'exec2', 'exec3', 'exec4', 'exec5', 'exec6', 'exec7'],
            'quantity': [100, -100, 200, -200, 300, 400, -400],
            'price': [10.0, 11.0, 20.0, 21.0, 30.0, 40.0, 41.0],
            'execution_timestamp': timestamps,
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT', 'GOOG', 'AMZN', 'AMZN'],
            'is_entry': [1, 0, 1, 0, 1, 1, 0],  # 1 for entries, 0 for exits
            'is_exit': [0, 1, 0, 1, 0, 0, 1],   # 0 for entries, 1 for exits
            'date': dates,  # Add date column
            'time_of_day': times  # Add time_of_day column
        })
        
        # Create trade directions for testing
        self.trade_directions = {
            'trade1': 'bullish',
            'trade2': 'bearish',
            'trade3': 'bullish',
            'trade4': 'bullish',
            'trade5': 'bearish'  # Trade without executions
        }
        
        # Create an instance of TradeProcessor with the test data
        self.processor = TradeProcessor(self.executions_df)
        self.processor.trade_directions = self.trade_directions
        
        # Initialize processor state by calling necessary methods
        self.processor.preprocess()
        
        # Get entry and exit executions directly from processor
        self.entry_executions = self.processor.entry_execs
        self.exit_executions = self.processor.exit_execs

    def test_basic_functionality(self):
        """Verify that entry date and time info is correctly extracted from entry executions."""
        # Call the method
        entry_info_df = self.processor._get_entry_date_time_info()
        
        # Verify type and structure
        self.assertIsInstance(entry_info_df, pd.DataFrame)
        self.assertEqual(entry_info_df.index.name, 'trade_id')
        
        # Check for expected columns
        expected_columns = ['start_date', 'start_time', 'day', 'week', 'month', 'year']
        for col in expected_columns:
            self.assertIn(col, entry_info_df.columns)
        
        # Check specific values for trade1
        self.assertEqual(entry_info_df.loc['trade1', 'start_date'], '2023-01-01')
        self.assertEqual(entry_info_df.loc['trade1', 'start_time'], '10:00:00')
        self.assertEqual(entry_info_df.loc['trade1', 'day'], 1)  # 1st of month
        # Week may vary slightly based on calendar settings, so we'll check it's within a reasonable range
        self.assertTrue(1 <= entry_info_df.loc['trade1', 'week'] <= 5)  
        self.assertEqual(entry_info_df.loc['trade1', 'month'], 1)  # January
        self.assertEqual(entry_info_df.loc['trade1', 'year'], 2023)
        
        # Check that different months are handled correctly
        self.assertEqual(entry_info_df.loc['trade2', 'month'], 2)  # February
        self.assertEqual(entry_info_df.loc['trade3', 'month'], 3)  # March
        self.assertEqual(entry_info_df.loc['trade4', 'month'], 4)  # April
        
        # Trade5 should not be in the results as it has no executions
        self.assertNotIn('trade5', entry_info_df.index)
        
        # Log test result
        self.log_case_result("Correctly extracts entry date and time info", True)

    def test_data_type_verification(self):
        """Verify the data types of the returned DataFrame columns."""
        # Call the method
        entry_info_df = self.processor._get_entry_date_time_info()
        
        # Check data types
        self.assertTrue(pd.api.types.is_string_dtype(entry_info_df['start_date']) or 
                        pd.api.types.is_object_dtype(entry_info_df['start_date']))
        self.assertTrue(pd.api.types.is_string_dtype(entry_info_df['start_time']) or 
                        pd.api.types.is_object_dtype(entry_info_df['start_time']))
        self.assertTrue(pd.api.types.is_integer_dtype(entry_info_df['day']))
        self.assertTrue(pd.api.types.is_integer_dtype(entry_info_df['week']))
        self.assertTrue(pd.api.types.is_integer_dtype(entry_info_df['month']))
        self.assertTrue(pd.api.types.is_integer_dtype(entry_info_df['year']))
        
        # Log test result
        self.log_case_result("Returns correct data types for each column", True)

    def test_no_entry_executions(self):
        """Verify behavior when a trade has no entry executions."""
        # Create a processor with no entry executions for a specific trade
        # Keep the original executions but remove trade1's entry execution
        modified_df = self.executions_df[self.executions_df['execution_id'] != 'exec1']
        processor = TradeProcessor(modified_df)
        processor.trade_directions = self.trade_directions
        processor.preprocess()
        
        # Call the method
        entry_info_df = processor._get_entry_date_time_info()
        
        # Verify trade1 is not in the results since it has no entry
        self.assertNotIn('trade1', entry_info_df.index)
        
        # Other trades should still be present
        self.assertIn('trade2', entry_info_df.index)
        self.assertIn('trade3', entry_info_df.index)
        self.assertIn('trade4', entry_info_df.index)
        
        # Log test result
        self.log_case_result("Excludes trades with no entry executions", True)

    def test_empty_entry_executions(self):
        """Verify behavior with empty entry executions DataFrame."""
        # Create a processor with no entry executions at all
        processor = TradeProcessor(self.executions_df[self.executions_df['quantity'] < 0])  # Only exits
        processor.trade_directions = self.trade_directions
        processor.preprocess()
        
        # Call the method
        entry_info_df = processor._get_entry_date_time_info()
        
        # Verify the DataFrame is empty
        self.assertEqual(len(entry_info_df), 0)
        
        # Log test result
        self.log_case_result("Returns empty DataFrame when there are no entry executions", True)

    def test_multiple_entry_executions(self):
        """Verify behavior with multiple entry executions for the same trade."""
        # Create DataFrame with multiple entry executions for a trade
        timestamps = [
            pd.Timestamp('2023-01-05 09:00:00'),  # First entry
            pd.Timestamp('2023-01-05 10:30:00'),  # Second entry
            pd.Timestamp('2023-01-05 14:00:00'),  # First exit
            pd.Timestamp('2023-01-05 15:30:00'),  # Second exit
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        multi_entry_df = pd.DataFrame({
            'trade_id': ['multi1', 'multi1', 'multi1', 'multi1'],
            'execution_id': ['mexec1', 'mexec2', 'mexec3', 'mexec4'],
            'quantity': [50, 50, -50, -50],
            'price': [10.0, 11.0, 12.0, 13.0],
            'execution_timestamp': timestamps,
            'symbol': ['TSLA', 'TSLA', 'TSLA', 'TSLA'],
            'is_entry': [1, 1, 0, 0],  # 1 for entries, 0 for exits
            'is_exit': [0, 0, 1, 1],   # 0 for entries, 1 for exits
            'date': dates,  # Add date column
            'time_of_day': times  # Add time_of_day column
        })
        
        # Create processor with multi-entry data
        processor = TradeProcessor(multi_entry_df)
        processor.trade_directions = {'multi1': 'bullish'}
        processor.preprocess()
        
        # Call the method
        entry_info_df = processor._get_entry_date_time_info()
        
        # Verify we get the first entry timestamp
        self.assertEqual(entry_info_df.loc['multi1', 'start_date'], '2023-01-05')
        self.assertEqual(entry_info_df.loc['multi1', 'start_time'], '09:00:00')
        
        # Log test result
        self.log_case_result("Uses the first entry execution for start date and time", True)

    def test_out_of_order_timestamps(self):
        """Verify behavior when entry executions have timestamps in random order."""
        # Create DataFrame with out-of-order entry timestamps
        timestamps = [
            pd.Timestamp('2023-01-05 10:30:00'),  # Entry (later)
            pd.Timestamp('2023-01-05 09:00:00'),  # Entry (earlier)
            pd.Timestamp('2023-01-05 14:00:00'),  # Exit
            pd.Timestamp('2023-01-05 15:30:00'),  # Exit
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        out_of_order_df = pd.DataFrame({
            'trade_id': ['order1', 'order1', 'order1', 'order1'],
            'execution_id': ['oexec1', 'oexec2', 'oexec3', 'oexec4'],
            'quantity': [25, 25, -25, -25],
            'price': [10.0, 11.0, 12.0, 13.0],
            'execution_timestamp': timestamps,
            'symbol': ['XYZ', 'XYZ', 'XYZ', 'XYZ'],
            'is_entry': [1, 1, 0, 0],  # 1 for entries, 0 for exits
            'is_exit': [0, 0, 1, 1],   # 0 for entries, 1 for exits
            'date': dates,  # Add date column
            'time_of_day': times  # Add time_of_day column
        })
        
        # Create processor with out-of-order data
        processor = TradeProcessor(out_of_order_df)
        processor.trade_directions = {'order1': 'bullish'}
        processor.preprocess()
        
        # Call the method
        entry_info_df = processor._get_entry_date_time_info()
        
        # Verify we get the earliest timestamp regardless of order in DataFrame
        self.assertEqual(entry_info_df.loc['order1', 'start_date'], '2023-01-05')
        self.assertEqual(entry_info_df.loc['order1', 'start_time'], '09:00:00')
        
        # Log test result
        self.log_case_result("Correctly identifies earliest timestamp regardless of row order", True)

    def test_return_dataframe_structure(self):
        """Verify the structure of the returned DataFrame."""
        # Call the method
        entry_info_df = self.processor._get_entry_date_time_info()
        
        # Verify the DataFrame index is trade_id
        self.assertEqual(entry_info_df.index.name, 'trade_id')
        
        # Verify the DataFrame has exactly the expected columns
        expected_columns = ['start_date', 'start_time', 'day', 'week', 'month', 'year']
        self.assertEqual(set(entry_info_df.columns), set(expected_columns))
        
        # Verify the DataFrame has the correct number of rows
        # Should have one row per trade with entry executions
        self.assertEqual(len(entry_info_df), 4)  # trade1, trade2, trade3, trade4
        
        # Log test result
        self.log_case_result("Returns DataFrame with correct structure", True)

    def test_integration_with_get_all_aggregations(self):
        """Test integration with other methods that depend on entry date/time info."""
        # Make sure required state is set up
        self.processor.preprocess()
        
        # Get entry date/time info
        entry_info_df = self.processor._get_entry_date_time_info()
        
        # Get all aggregations
        aggs = self.processor._get_all_aggregations()
        
        # Verify all entry date/time columns are included in aggregations
        for col in ['start_date', 'start_time', 'day', 'week', 'month', 'year']:
            self.assertIn(col, aggs)
            
            # Convert DataFrame column to Series for comparison
            series_from_df = entry_info_df[col]
            
            # Verify the values match
            pd.testing.assert_series_equal(aggs[col], series_from_df)
        
        # Log test result
        self.log_case_result("Integrates properly with dependent methods", True)

    def test_edge_cases(self):
        """Test with various edge cases like missing timestamps, etc."""
        # Create DataFrame with edge cases
        edge_df = pd.DataFrame({
            'trade_id': ['edge1', 'edge1', 'edge2', 'edge2'],
            'execution_id': ['edge_exec1', 'edge_exec2', 'edge_exec3', 'edge_exec4'],
            'quantity': [100, -100, 200, -200],
            'price': [10.0, 11.0, 20.0, 21.0],
            'execution_timestamp': [
                pd.NaT,                              # edge1 entry - NaT timestamp
                pd.Timestamp('2023-01-01 14:30:00'),  # edge1 exit
                pd.Timestamp('2023-01-02 09:15:00'),  # edge2 entry
                pd.Timestamp('2023-01-02 16:45:00'),  # edge2 exit
            ],
            'symbol': ['EDGE', 'EDGE', 'EDGE', 'EDGE']
        })
        
        # Create processor with edge case data
        processor = TradeProcessor(edge_df)
        processor.trade_directions = {'edge1': 'bullish', 'edge2': 'bullish'}
        processor.preprocess()
        
        # Call the method
        entry_info_df = processor._get_entry_date_time_info()
        
        # edge1 should not be in the results due to NaT timestamp
        self.assertNotIn('edge1', entry_info_df.index)
        
        # edge2 should have normal values
        self.assertEqual(entry_info_df.loc['edge2', 'start_date'], '2023-01-02')
        self.assertEqual(entry_info_df.loc['edge2', 'start_time'], '09:15:00')
        
        # Log test result
        self.log_case_result("Handles edge cases correctly", True)

    def test_cross_year_dates(self):
        """Test with dates that span different years."""
        # Create DataFrame with entries in different years
        timestamps = [
            pd.Timestamp('2022-12-31 23:59:59'),  # year1 entry - 2022
            pd.Timestamp('2023-01-01 00:00:01'),  # year2 entry - 2023
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        year_df = pd.DataFrame({
            'trade_id': ['year1', 'year2'],
            'execution_id': ['year_exec1', 'year_exec2'],
            'quantity': [100, 100],
            'price': [10.0, 20.0],
            'execution_timestamp': timestamps,
            'symbol': ['YEAR', 'YEAR'],
            'is_entry': [1, 1],  # Both are entries
            'is_exit': [0, 0],   # Both are not exits
            'date': dates,  # Add date column
            'time_of_day': times  # Add time_of_day column
        })
        
        # Create processor with year-spanning data
        processor = TradeProcessor(year_df)
        processor.trade_directions = {'year1': 'bullish', 'year2': 'bullish'}
        processor.preprocess()
        
        # Call the method
        entry_info_df = processor._get_entry_date_time_info()
        
        # Verify correct years are extracted
        self.assertEqual(entry_info_df.loc['year1', 'year'], 2022)
        self.assertEqual(entry_info_df.loc['year2', 'year'], 2023)
        
        # Log test result
        self.log_case_result("Correctly handles dates spanning different years", True)

# If running the tests directly, print summary
if __name__ == '__main__':
    unittest.main(exit=False)  # Run tests without exiting
    print_summary()  # Print detailed summary of test results
    