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
        
        # Verify that the result contains most keys, but end_date and end_time are empty Series
        # This is the updated behavior - returning empty Series instead of None
        self.assertIn('end_date', result)
        self.assertIn('end_time', result)
        self.assertTrue(isinstance(result['end_date'], pd.Series), "end_date should be a Series")
        self.assertTrue(isinstance(result['end_time'], pd.Series), "end_time should be a Series")
        
        # Check that the Series have entries for each trade ID
        expected_trade_ids = list(processor.trade_directions.keys())
        self.assertEqual(set(result['end_date'].index), set(expected_trade_ids))
        self.assertEqual(set(result['end_time'].index), set(expected_trade_ids))
        
        # Check that all values are NaN
        self.assertTrue(result['end_date'].isna().all())
        self.assertTrue(result['end_time'].isna().all())
        
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


class TestProcessTrades(BaseTestCase):
    """Test cases for the process_trades method of TradeProcessor"""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a sample executions DataFrame for testing
        timestamps = [
            pd.Timestamp('2023-01-01 10:00:00'),  # trade1 entry
            pd.Timestamp('2023-01-01 14:30:00'),  # trade1 exit
            pd.Timestamp('2023-02-02 09:15:00'),  # trade2 entry
            pd.Timestamp('2023-02-02 16:45:00'),  # trade2 exit
            pd.Timestamp('2023-03-03 11:30:00'),  # trade3 entry
            pd.Timestamp('2023-04-04 09:00:00'),  # trade4 entry
            pd.Timestamp('2023-04-04 12:30:00'),  # trade4 exit
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        self.valid_executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade2', 'trade2', 'trade3', 'trade4', 'trade4'],
            'execution_id': ['exec1', 'exec2', 'exec3', 'exec4', 'exec5', 'exec6', 'exec7'],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT', 'GOOG', 'AMZN', 'AMZN'],
            'date': dates,
            'time_of_day': times,
            'is_entry': [1, 0, 1, 0, 1, 1, 0],  # 1 for entries, 0 for exits
            'is_exit': [0, 1, 0, 1, 0, 0, 1],   # 0 for entries, 1 for exits
            'quantity': [100, -100, -200, 200, 300, 400, -400],  # positive for buys, negative for sells
            'execution_timestamp': timestamps,
            'price': [10.0, 11.0, 20.0, 21.0, 30.0, 40.0, 41.0],
            'commission': [1.0, 1.0, 2.0, 2.0, 3.0, 4.0, 4.0]
        })
        
        # Create an invalid DataFrame (missing required columns)
        self.invalid_executions_df = self.valid_executions_df.drop(columns=['is_entry', 'is_exit'])
        
        # Create an empty DataFrame
        self.empty_executions_df = pd.DataFrame(columns=self.valid_executions_df.columns)
        
        # Set up mock for db.get_account_balances() 
        self.account_balances_patcher = patch('utils.db_utils.DatabaseManager.get_account_balances')
        self.mock_get_account_balances = self.account_balances_patcher.start()
        
        # Set up mock account balances return value
        self.mock_get_account_balances.return_value = pd.DataFrame({
            'date': ['2023-01-01', '2023-02-02', '2023-03-03', '2023-04-04'],
            'cash_balance': [10000.0, 10500.0, 11000.0, 12000.0]
        })

    def tearDown(self):
        """Clean up after each test"""
        super().tearDown()
        self.account_balances_patcher.stop()

    def test_basic_functionality(self):
        """Test that valid inputs produce a non-None DataFrame with expected columns."""
        # Create processor with valid data
        processor = TradeProcessor(self.valid_executions_df)
        
        # Process trades
        result_df = processor.process_trades()
        
        # Verify the result
        self.assertIsNotNone(result_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        
        # Check that the result has expected columns
        expected_columns = [
            'trade_id', 'num_executions', 'symbol', 'direction', 'quantity', 
            'entry_price', 'capital_required', 'exit_price', 'risk_reward',
            'risk_per_trade', 'commission', 'status'
        ]
        
        for col in expected_columns:
            self.assertIn(col, result_df.columns)
        
        # Check that all trades are present
        expected_trade_ids = ['trade1', 'trade2', 'trade3', 'trade4']
        self.assertListEqual(sorted(result_df['trade_id'].tolist()), sorted(expected_trade_ids))
        
        # Log test result
        self.log_case_result("Basic functionality with valid data works correctly", True)

    def test_validation_failure(self):
        """Test that invalid data causes validate() to return False and process_trades() to return None."""
        # Create processor with invalid data (missing required columns)
        processor = TradeProcessor(self.invalid_executions_df)
        
        # Process trades (should return None due to validation failure)
        result_df = processor.process_trades()
        
        # Verify the result
        self.assertIsNone(result_df)
        
        # Log test result
        self.log_case_result("Returns None when validation fails", True)

    def test_preprocessing_failure(self):
        """Test that issues in preprocessing cause preprocess() to return False and process_trades() to return None."""
        # Create a processor with valid structure but no entry executions
        no_entry_df = self.valid_executions_df.copy()
        no_entry_df['is_entry'] = 0  # Set all is_entry to 0
        
        processor = TradeProcessor(no_entry_df)
        
        # Mock the preprocess method to return False
        with patch.object(TradeProcessor, 'preprocess', return_value=False):
            # Process trades (should return None due to preprocessing failure)
            result_df = processor.process_trades()
            
            # Verify the result
            self.assertIsNone(result_df)
        
        # Log test result
        self.log_case_result("Returns None when preprocessing fails", True)

    def test_exception_handling(self):
        """Test that exceptions in _get_all_aggregations() or _build_trades_dataframe() are caught."""
        # Create processor with valid data
        processor = TradeProcessor(self.valid_executions_df)
        
        # Test exception in _get_all_aggregations
        with patch.object(TradeProcessor, '_get_all_aggregations', side_effect=Exception("Test exception")):
            # Process trades (should return None due to exception)
            result_df = processor.process_trades()
            
            # Verify the result
            self.assertIsNone(result_df)
        
        # Test exception in _build_trades_dataframe
        with patch.object(TradeProcessor, '_build_trades_dataframe', side_effect=Exception("Test exception")):
            # Process trades (should return None due to exception)
            result_df = processor.process_trades()
            
            # Verify the result
            self.assertIsNone(result_df)
        
        # Log test result
        self.log_case_result("Catches exceptions and returns None", True)

    def test_empty_dataframe(self):
        """Test that an empty input DataFrame returns None."""
        # Create processor with empty data
        processor = TradeProcessor(self.empty_executions_df)
        
        # Process trades (should return None due to empty data)
        result_df = processor.process_trades()
        
        # Verify the result
        self.assertIsNone(result_df)
        
        # Log test result
        self.log_case_result("Returns None for empty input DataFrame", True)

    def test_single_execution_trade(self):
        """Test processing a trade with only a single execution (entry only)."""
        # Create DataFrame with single execution
        single_exec_df = self.valid_executions_df.iloc[[0]].copy()  # Only the first row (entry for trade1)
        
        # Create processor with single execution data
        processor = TradeProcessor(single_exec_df)
        
        # Manually initialize trade directions - needed since we normally need entry executions processed
        processor.trade_directions = {'trade1': {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0}}
        
        # Process trades
        result_df = processor.process_trades()
        
        # Verify the result
        self.assertIsNotNone(result_df)
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]['trade_id'], 'trade1')
        self.assertEqual(result_df.iloc[0]['status'], 'open')  # Should be open since there's no exit
        
        # Log test result
        self.log_case_result("Correctly processes single execution trade", True)

    def test_multiple_entries_exits(self):
        """Test processing a trade with multiple entry and exit executions."""
        # Create DataFrame with multiple entries/exits for the same trade
        timestamps = [
            pd.Timestamp('2023-05-01 09:00:00'),  # First entry
            pd.Timestamp('2023-05-01 10:30:00'),  # Second entry
            pd.Timestamp('2023-05-01 14:00:00'),  # First exit
            pd.Timestamp('2023-05-01 15:30:00'),  # Second exit
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        multi_exec_df = pd.DataFrame({
            'trade_id': ['multi1', 'multi1', 'multi1', 'multi1'],
            'execution_id': ['mexec1', 'mexec2', 'mexec3', 'mexec4'],
            'symbol': ['TSLA', 'TSLA', 'TSLA', 'TSLA'],
            'date': dates,
            'time_of_day': times,
            'is_entry': [1, 1, 0, 0],  # 1 for entries, 0 for exits
            'is_exit': [0, 0, 1, 1],   # 0 for entries, 1 for exits
            'quantity': [50, 50, -50, -50],  # positive for buys, negative for sells
            'execution_timestamp': timestamps,
            'price': [100.0, 101.0, 102.0, 103.0],
            'commission': [5.0, 5.0, 5.0, 5.0]
        })
        
        # Add account balance for this date
        self.mock_get_account_balances.return_value = pd.concat([
            self.mock_get_account_balances.return_value,
            pd.DataFrame({'date': ['2023-05-01'], 'cash_balance': [12500.0]})
        ])
        
        # Create processor with multiple execution data
        processor = TradeProcessor(multi_exec_df)
        
        # Manually initialize trade directions
        processor.trade_directions = {'multi1': {'direction': 'bullish', 'initial_quantity': 50.0, 'abs_initial_quantity': 50.0}}
        
        # Process trades
        result_df = processor.process_trades()
        
        # Verify the result
        self.assertIsNotNone(result_df)
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]['trade_id'], 'multi1')
        self.assertEqual(result_df.iloc[0]['quantity'], 100)  # Sum of entry quantities
        
        # Calculate expected VWAP for entries and exits
        expected_entry_vwap = (50*100.0 + 50*101.0) / 100  # (qty1*price1 + qty2*price2) / total_qty
        expected_exit_vwap = (50*102.0 + 50*103.0) / 100  # (qty1*price1 + qty2*price2) / total_qty
        
        # Check that the entry and exit prices are correct VWAPs
        self.assertAlmostEqual(result_df.iloc[0]['entry_price'], expected_entry_vwap)
        self.assertAlmostEqual(result_df.iloc[0]['exit_price'], expected_exit_vwap)
        
        # Log test result
        self.log_case_result("Correctly processes trades with multiple entries and exits", True)

    def test_trades_spanning_multiple_days(self):
        """Test processing trades that span multiple days."""
        # Create DataFrame with trades spanning multiple days
        timestamps = [
            pd.Timestamp('2023-06-01 10:00:00'),  # span1 entry
            pd.Timestamp('2023-06-05 14:30:00'),  # span1 exit (4 days later)
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        span_df = pd.DataFrame({
            'trade_id': ['span1', 'span1'],
            'execution_id': ['sexec1', 'sexec2'],
            'symbol': ['META', 'META'],
            'date': dates,
            'time_of_day': times,
            'is_entry': [1, 0],  # 1 for entries, 0 for exits
            'is_exit': [0, 1],   # 0 for entries, 1 for exits
            'quantity': [200, -200],  # positive for buys, negative for sells
            'execution_timestamp': timestamps,
            'price': [300.0, 310.0],
            'commission': [10.0, 10.0]
        })
        
        # Add account balance for these dates
        self.mock_get_account_balances.return_value = pd.concat([
            self.mock_get_account_balances.return_value,
            pd.DataFrame({'date': ['2023-06-01', '2023-06-05'], 'cash_balance': [13000.0, 13500.0]})
        ])
        
        # Create processor with span data
        processor = TradeProcessor(span_df)
        
        # Manually initialize trade directions
        processor.trade_directions = {'span1': {'direction': 'bullish', 'initial_quantity': 200.0, 'abs_initial_quantity': 200.0}}
        
        # Process trades
        result_df = processor.process_trades()
        
        # Verify the result
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.iloc[0]['trade_id'], 'span1')
        
        # Calculate expected duration directly from timestamps
        # This matches exactly what _get_duration_hours does internally
        expected_duration = (timestamps[1] - timestamps[0]).total_seconds() / 3600
        
        # Check the duration hours
        self.assertAlmostEqual(result_df.iloc[0]['duration_hours'], expected_duration, delta=0.01)
        
        # Check that start and end dates are correct
        self.assertEqual(result_df.iloc[0]['start_date'], '2023-06-01')
        self.assertEqual(result_df.iloc[0]['end_date'], '2023-06-05')
        
        # Log test result
        self.log_case_result("Correctly processes trades spanning multiple days", True)

    def test_integration_with_other_methods(self):
        """Test the integration of process_trades with all dependent methods."""
        # Create processor with valid data
        processor = TradeProcessor(self.valid_executions_df)
        
        # We need to let the real preprocess method run to initialize trade directions properly
        # but we still want to verify it was called
        original_preprocess = processor.preprocess
        
        def wrapped_preprocess():
            result = original_preprocess()
            return result
            
        # Set up mocks to verify method calls
        with patch.object(TradeProcessor, 'validate', return_value=True) as mock_validate, \
             patch.object(TradeProcessor, 'preprocess', side_effect=wrapped_preprocess) as mock_preprocess, \
             patch.object(TradeProcessor, '_get_all_aggregations', wraps=processor._get_all_aggregations) as mock_get_aggs, \
             patch.object(TradeProcessor, '_build_trades_dataframe', wraps=processor._build_trades_dataframe) as mock_build_df:
            
            # Process trades
            result_df = processor.process_trades()
            
            # Verify that all methods were called
            mock_validate.assert_called_once()
            mock_preprocess.assert_called_once()
            mock_get_aggs.assert_called_once()
            mock_build_df.assert_called_once()
        
        # Verify the result
        self.assertIsNotNone(result_df)
        
        # Log test result
        self.log_case_result("Correctly integrates with all dependent methods", True)


class TestProcessTradesFunction(BaseTestCase):
    """Test cases for the standalone process_trades wrapper function."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a sample executions DataFrame for testing
        timestamps = [
            pd.Timestamp('2023-01-01 10:00:00'),  # trade1 entry
            pd.Timestamp('2023-01-01 14:30:00'),  # trade1 exit
            pd.Timestamp('2023-02-02 09:15:00'),  # trade2 entry
            pd.Timestamp('2023-02-02 16:45:00'),  # trade2 exit
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        self.valid_executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade2', 'trade2'],
            'execution_id': ['exec1', 'exec2', 'exec3', 'exec4'],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'date': dates,
            'time_of_day': times,
            'is_entry': [1, 0, 1, 0],  # 1 for entries, 0 for exits
            'is_exit': [0, 1, 0, 1],   # 0 for entries, 1 for exits
            'quantity': [100, -100, -200, 200],  # positive for buys, negative for sells
            'execution_timestamp': timestamps,
            'price': [10.0, 11.0, 20.0, 21.0],
            'commission': [1.0, 1.0, 2.0, 2.0]
        })
        
        # Create an invalid DataFrame (missing required columns)
        self.invalid_executions_df = self.valid_executions_df.drop(columns=['is_entry', 'is_exit'])
        
        # Create an empty DataFrame
        self.empty_executions_df = pd.DataFrame(columns=self.valid_executions_df.columns)
        
        # Set up mock for db.get_account_balances() 
        self.account_balances_patcher = patch('utils.db_utils.DatabaseManager.get_account_balances')
        self.mock_get_account_balances = self.account_balances_patcher.start()
        
        # Set up mock account balances return value
        self.mock_get_account_balances.return_value = pd.DataFrame({
            'date': ['2023-01-01', '2023-02-02'],
            'cash_balance': [10000.0, 10500.0]
        })

    def tearDown(self):
        """Clean up after each test"""
        super().tearDown()
        self.account_balances_patcher.stop()

    def test_basic_functionality(self):
        """Test that valid input correctly passes through to the underlying class method."""
        # Use the wrapper function
        from analytics.process_trades import process_trades
        
        # Process trades with valid data
        result_df = process_trades(self.valid_executions_df)
        
        # Verify the result
        self.assertIsNotNone(result_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        
        # Check that expected trades are present
        trade_ids = result_df['trade_id'].tolist()
        self.assertEqual(set(trade_ids), {'trade1', 'trade2'})
        
        # Log test result
        self.log_case_result("Successfully processes valid execution data", True)

    def test_exception_handling(self):
        """Test that exceptions are properly caught and result in None being returned."""
        from analytics.process_trades import process_trades
        
        # Create a mock TradeProcessor that raises an exception
        with patch('analytics.process_trades.TradeProcessor') as mock_processor_class:
            # Make the constructor raise an exception
            mock_processor_class.side_effect = Exception("Test constructor exception")
            
            # Call process_trades with valid data
            with patch('sys.stdout', new=StringIO()) as fake_out:
                result_df = process_trades(self.valid_executions_df)
                
                # Verify error message was printed
                self.assertIn("Error processing trades", fake_out.getvalue())
                
                # Verify the result is None
                self.assertIsNone(result_df)
        
        # Test exception from process_trades method
        with patch('analytics.process_trades.TradeProcessor') as mock_processor_class:
            # Make the process_trades method raise an exception
            mock_processor_instance = MagicMock()
            mock_processor_instance.process_trades.side_effect = Exception("Test method exception")
            mock_processor_class.return_value = mock_processor_instance
            
            # Call process_trades with valid data
            with patch('sys.stdout', new=StringIO()) as fake_out:
                result_df = process_trades(self.valid_executions_df)
                
                # Verify error message was printed
                self.assertIn("Error processing trades", fake_out.getvalue())
                
                # Verify the result is None
                self.assertIsNone(result_df)
        
        # Log test result
        self.log_case_result("Properly handles exceptions and returns None", True)

    def test_input_validation(self):
        """Test that different types of inputs are handled correctly."""
        from analytics.process_trades import process_trades
        
        # Test with empty DataFrame
        result_df = process_trades(self.empty_executions_df)
        self.assertIsNone(result_df)
        
        # Test with invalid DataFrame (missing required columns)
        result_df = process_trades(self.invalid_executions_df)
        self.assertIsNone(result_df)
        
        # Log test result
        self.log_case_result("Correctly handles different input types", True)

    def test_end_to_end(self):
        """Test that the function correctly processes trade data and returns expected results."""
        from analytics.process_trades import process_trades
        
        # Process trades with valid data
        result_df = process_trades(self.valid_executions_df)
        
        # Verify the result contains expected columns and data
        self.assertIsNotNone(result_df)
        
        expected_columns = [
            'trade_id', 'num_executions', 'symbol', 'direction', 'quantity', 
            'entry_price', 'exit_price', 'status'
        ]
        
        for col in expected_columns:
            self.assertIn(col, result_df.columns)
        
        # Check some specific values
        trade1_row = result_df[result_df['trade_id'] == 'trade1'].iloc[0]
        trade2_row = result_df[result_df['trade_id'] == 'trade2'].iloc[0]
        
        # Check trade1 values
        self.assertEqual(trade1_row['symbol'], 'AAPL')
        self.assertEqual(trade1_row['num_executions'], 2)
        self.assertEqual(trade1_row['direction'], 'bullish')
        self.assertEqual(trade1_row['status'], 'closed')
        
        # Check trade2 values
        self.assertEqual(trade2_row['symbol'], 'MSFT')
        self.assertEqual(trade2_row['num_executions'], 2)
        self.assertEqual(trade2_row['direction'], 'bearish')
        self.assertEqual(trade2_row['status'], 'closed')
        
        # Log test result
        self.log_case_result("Correctly processes trade data end-to-end", True)

    def test_nonstandard_inputs(self):
        """Test the function with nonstandard but valid inputs."""
        from analytics.process_trades import process_trades
        
        # Test with a DataFrame that has extra columns
        extra_columns_df = self.valid_executions_df.copy()
        extra_columns_df['extra_column'] = 'test_value'
        
        result_df = process_trades(extra_columns_df)
        self.assertIsNotNone(result_df)
        
        # Test with a DataFrame that has only one row per trade (just entries)
        entries_only_df = self.valid_executions_df[self.valid_executions_df['is_entry'] == 1].copy()
        
        result_df = process_trades(entries_only_df)
        self.assertIsNotNone(result_df)
        
        # Log test result
        self.log_case_result("Handles nonstandard but valid inputs correctly", True)


class TestCalculateRiskRewardRatio(BaseTestCase):
    """Test cases for the _calculate_risk_reward_ratio method of TradeProcessor."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a basic executions DataFrame
        self.executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade2', 'trade3', 'trade4'],
            'execution_id': ['exec1', 'exec2', 'exec3', 'exec4'],
            'quantity': [100, -50, 200, -100],  # positive for bullish, negative for bearish
            'price': [10.0, 20.0, 30.0, 40.0],
            'symbol': ['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
            'is_entry': [1, 1, 1, 1],
            'is_exit': [0, 0, 0, 0],
            'execution_timestamp': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'])
        })
        
        # Initialize processor
        self.processor = TradeProcessor(self.executions_df)
        
        # Set up trade directions manually
        self.processor.trade_directions = {
            'trade1': {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0},
            'trade2': {'direction': 'bearish', 'initial_quantity': -50.0, 'abs_initial_quantity': 50.0},
            'trade3': {'direction': 'bullish', 'initial_quantity': 200.0, 'abs_initial_quantity': 200.0},
            'trade4': {'direction': 'bearish', 'initial_quantity': -100.0, 'abs_initial_quantity': 100.0}
        }

    def test_basic_calculation(self):
        """Test basic risk-reward calculation for bullish and bearish trades."""
        # Set up test data
        entry_prices = pd.Series({
            'trade1': 100.0,  # bullish trade entry
            'trade2': 200.0   # bearish trade entry
        })
        
        exit_prices = pd.Series({
            'trade1': 120.0,  # bullish trade exit (profit)
            'trade2': 180.0   # bearish trade exit (profit)
        })
        
        stop_prices = pd.Series({
            'trade1': 90.0,   # bullish trade stop (below entry)
            'trade2': 210.0   # bearish trade stop (above entry)
        })
        
        # Calculate risk-reward ratios
        result = self.processor._calculate_risk_reward_ratio(
            entry_prices=entry_prices,
            exit_prices=exit_prices,
            stop_prices=stop_prices
        )
        
        # Expected calculations:
        # Bullish trade1: (120 - 100) / (100 - 90) = 20 / 10 = 2.0
        # Bearish trade2: (200 - 180) / (210 - 200) = 20 / 10 = 2.0
        
        # Verify results
        self.assertAlmostEqual(result['trade1'], 2.0)
        self.assertAlmostEqual(result['trade2'], 2.0)
        
        # Log test result
        self.log_case_result("Correctly calculates basic risk-reward ratios", True)

    def test_profit_loss_scenarios(self):
        """Test calculation for both profitable and losing trades."""
        # Set up test data for both profitable and losing trades
        entry_prices = pd.Series({
            'trade1': 100.0,  # bullish trade entry
            'trade2': 200.0,  # bearish trade entry
            'trade3': 300.0,  # bullish trade entry
            'trade4': 400.0   # bearish trade entry
        })
        
        exit_prices = pd.Series({
            'trade1': 120.0,  # bullish trade exit (profit)
            'trade2': 180.0,  # bearish trade exit (profit)
            'trade3': 290.0,  # bullish trade exit (loss)
            'trade4': 410.0   # bearish trade exit (loss)
        })
        
        stop_prices = pd.Series({
            'trade1': 90.0,   # bullish trade stop (below entry)
            'trade2': 210.0,  # bearish trade stop (above entry)
            'trade3': 290.0,  # bullish trade stop (below entry)
            'trade4': 410.0   # bearish trade stop (above entry)
        })
        
        # Calculate risk-reward ratios
        result = self.processor._calculate_risk_reward_ratio(
            entry_prices=entry_prices,
            exit_prices=exit_prices,
            stop_prices=stop_prices
        )
        
        # Expected calculations:
        # Profitable bullish trade1: (120 - 100) / (100 - 90) = 20 / 10 = 2.0
        # Profitable bearish trade2: (200 - 180) / (210 - 200) = 20 / 10 = 2.0
        # Losing bullish trade3: (290 - 300) / (300 - 290) = -10 / 10 = -1.0
        # Losing bearish trade4: (400 - 410) / (410 - 400) = -10 / 10 = -1.0
        
        # Verify results
        self.assertGreater(result['trade1'], 0)  # Profitable bullish trade
        self.assertGreater(result['trade2'], 0)  # Profitable bearish trade
        self.assertLess(result['trade3'], 0)     # Losing bullish trade
        self.assertLess(result['trade4'], 0)     # Losing bearish trade
        
        # Verify exact values
        self.assertAlmostEqual(result['trade1'], 2.0)
        self.assertAlmostEqual(result['trade2'], 2.0)
        self.assertAlmostEqual(result['trade3'], -1.0)
        self.assertAlmostEqual(result['trade4'], -1.0)
        
        # Log test result
        self.log_case_result("Correctly identifies profitable and losing trades", True)

    def test_edge_cases(self):
        """Test edge cases like zero/negative risk and missing values."""
        # Set up test data with edge cases
        entry_prices = pd.Series({
            'trade1': 100.0,    # Normal entry
            'trade2': 200.0,    # Entry equals stop (zero risk)
            'trade3': 300.0,    # Stop above entry for bullish (negative risk)
            'trade4': None      # Missing entry price
        })
        
        exit_prices = pd.Series({
            'trade1': 120.0,    # Normal exit
            'trade2': 220.0,    # Normal exit
            'trade3': None,     # Missing exit price
            'trade4': 420.0     # Normal exit
        })
        
        stop_prices = pd.Series({
            'trade1': 90.0,     # Normal stop
            'trade2': 200.0,    # Stop equals entry (zero risk)
            'trade3': 310.0,    # Stop above entry for bullish (negative risk)
            'trade4': 380.0     # Normal stop
        })
        
        # Calculate risk-reward ratios
        result = self.processor._calculate_risk_reward_ratio(
            entry_prices=entry_prices,
            exit_prices=exit_prices,
            stop_prices=stop_prices
        )
        
        # Expected results:
        # Normal case: Already tested in other methods
        # Zero risk: Should return NaN (risk <= 0)
        # Negative risk: Should return NaN (risk <= 0)
        # Missing values: Should return NaN
        
        # Verify results
        self.assertAlmostEqual(result['trade1'], 2.0)     # Normal case still works
        self.assertTrue(pd.isna(result['trade2']))        # Zero risk returns NaN
        self.assertTrue(pd.isna(result['trade3']))        # Missing exit price returns NaN
        self.assertTrue(pd.isna(result['trade4']))        # Missing entry price returns NaN
        
        # Log test result
        self.log_case_result("Correctly handles edge cases", True)

    def test_direction_specific_calculation(self):
        """Test that calculations differ correctly based on trade direction."""
        # We'll use different values for each direction to ensure proper setup
        entry_prices = pd.Series({
            'trade1': 100.0,  # bullish trade entry
            'trade2': 100.0   # bearish trade entry
        })
        
        exit_prices = pd.Series({
            'trade1': 120.0,  # bullish exit (price went up - profit)
            'trade2': 80.0    # bearish exit (price went down - profit)
        })
        
        stop_prices = pd.Series({
            'trade1': 90.0,   # bullish stop (below entry)
            'trade2': 110.0   # bearish stop (above entry)
        })
        
        # Override trade directions to isolate the direction impact
        original_directions = self.processor.trade_directions.copy()
        self.processor.trade_directions = {
            'trade1': {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0},
            'trade2': {'direction': 'bearish', 'initial_quantity': -100.0, 'abs_initial_quantity': 100.0}
        }
        
        # Calculate risk-reward ratios
        result = self.processor._calculate_risk_reward_ratio(
            entry_prices=entry_prices,
            exit_prices=exit_prices,
            stop_prices=stop_prices
        )
        
        # Expected calculations:
        # Bullish trade1: (120 - 100) / (100 - 90) = 20 / 10 = 2.0
        # Bearish trade2: (100 - 80) / (110 - 100) = 20 / 10 = 2.0
        
        # Verify results - they should be the same value but calculated differently
        self.assertAlmostEqual(result['trade1'], 2.0)
        self.assertAlmostEqual(result['trade2'], 2.0)
        
        # Restore original trade directions
        self.processor.trade_directions = original_directions
        
        # Log test result
        self.log_case_result("Correctly applies direction-specific calculations", True)

    def test_type_verification(self):
        """Ensure the method returns a pandas Series with the correct index."""
        # Set up minimal test data
        entry_prices = pd.Series({'trade1': 100.0})
        exit_prices = pd.Series({'trade1': 120.0})
        stop_prices = pd.Series({'trade1': 90.0})
        
        # Calculate risk-reward ratios
        result = self.processor._calculate_risk_reward_ratio(
            entry_prices=entry_prices,
            exit_prices=exit_prices,
            stop_prices=stop_prices
        )
        
        # Verify result type
        self.assertIsInstance(result, pd.Series)
        
        # Verify index
        self.assertEqual(set(result.index), set(self.processor.trade_directions.keys()))
        
        # Log test result
        self.log_case_result("Returns a pandas Series with the correct index", True)

    def test_multiple_trades(self):
        """Verify it correctly processes multiple trades in one call."""
        # Set up test data for multiple trades
        entry_prices = pd.Series({
            'trade1': 100.0,
            'trade2': 200.0,
            'trade3': 300.0,
            'trade4': 400.0
        })
        
        exit_prices = pd.Series({
            'trade1': 120.0,
            'trade2': 180.0,
            'trade3': 330.0,
            'trade4': 380.0
        })
        
        stop_prices = pd.Series({
            'trade1': 90.0,
            'trade2': 210.0,
            'trade3': 290.0,
            'trade4': 410.0
        })
        
        # Calculate risk-reward ratios
        result = self.processor._calculate_risk_reward_ratio(
            entry_prices=entry_prices,
            exit_prices=exit_prices,
            stop_prices=stop_prices
        )
        
        # Expected calculations:
        # Bullish trade1: (120 - 100) / (100 - 90) = 20 / 10 = 2.0
        # Bearish trade2: (200 - 180) / (210 - 200) = 20 / 10 = 2.0
        # Bullish trade3: (330 - 300) / (300 - 290) = 30 / 10 = 3.0
        # Bearish trade4: (400 - 380) / (410 - 400) = 20 / 10 = 2.0
        
        # Verify all trades are processed
        self.assertEqual(len(result), 4)
        
        # Verify each calculation is correct
        self.assertAlmostEqual(result['trade1'], 2.0)
        self.assertAlmostEqual(result['trade2'], 2.0)
        self.assertAlmostEqual(result['trade3'], 3.0)
        self.assertAlmostEqual(result['trade4'], 2.0)
        
        # Log test result
        self.log_case_result("Correctly processes multiple trades in one call", True)

    def test_integration(self):
        """Test integration with other processor methods."""
        # Create a more realistic processor with real executions
        timestamps = [
            pd.Timestamp('2023-01-01 10:00:00'),  # trade1 entry
            pd.Timestamp('2023-01-01 14:30:00'),  # trade1 exit
            pd.Timestamp('2023-01-02 09:15:00'),  # trade2 entry
            pd.Timestamp('2023-01-02 16:45:00'),  # trade2 exit
        ]
        
        executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade2', 'trade2'],
            'execution_id': ['exec1', 'exec2', 'exec3', 'exec4'],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'date': [ts.strftime('%Y-%m-%d') for ts in timestamps],
            'time_of_day': [ts.strftime('%H:%M:%S') for ts in timestamps],
            'is_entry': [1, 0, 1, 0],
            'is_exit': [0, 1, 0, 1],
            'quantity': [100, -100, -200, 200],  # positive for buys, negative for sells
            'execution_timestamp': timestamps,
            'price': [100.0, 120.0, 200.0, 180.0]
        })
        
        processor = TradeProcessor(executions_df)
        
        # Initialize required state
        processor.preprocess()
        
        # Directly get entry and exit prices from relevant methods
        entry_prices, _, _ = processor._get_quantity_and_entry_price()
        exit_prices = processor._get_exit_price()
        
        # For stop prices, we'll use the _get_stop_prices method with a fixed stop_loss_amount
        stop_prices = processor._get_stop_prices(stop_loss_amount=10.0, entry_prices=entry_prices)
        
        # Calculate risk-reward ratios
        result = processor._calculate_risk_reward_ratio(
            entry_prices=entry_prices,
            exit_prices=exit_prices,
            stop_prices=stop_prices
        )
        
        # Expected calculations:
        # Bullish trade1: (120 - 100) / (100 - 90) = 20 / 10 = 2.0
        # Bearish trade2: (200 - 180) / (210 - 200) = 20 / 10 = 2.0
        
        # Verify results
        self.assertAlmostEqual(result['trade1'], 2.0)
        self.assertAlmostEqual(result['trade2'], 2.0)
        
        # Log test result
        self.log_case_result("Correctly integrates with other processor methods", True)


class TestProcessTradesFunction(BaseTestCase):
    """Test cases for the standalone process_trades wrapper function."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a sample executions DataFrame for testing
        timestamps = [
            pd.Timestamp('2023-01-01 10:00:00'),  # trade1 entry
            pd.Timestamp('2023-01-01 14:30:00'),  # trade1 exit
            pd.Timestamp('2023-02-02 09:15:00'),  # trade2 entry
            pd.Timestamp('2023-02-02 16:45:00'),  # trade2 exit
        ]
        
        # Create dates and times from timestamps
        dates = [ts.strftime('%Y-%m-%d') for ts in timestamps]
        times = [ts.strftime('%H:%M:%S') for ts in timestamps]
        
        self.valid_executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade2', 'trade2'],
            'execution_id': ['exec1', 'exec2', 'exec3', 'exec4'],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'date': dates,
            'time_of_day': times,
            'is_entry': [1, 0, 1, 0],  # 1 for entries, 0 for exits
            'is_exit': [0, 1, 0, 1],   # 0 for entries, 1 for exits
            'quantity': [100, -100, -200, 200],  # positive for buys, negative for sells
            'execution_timestamp': timestamps,
            'price': [10.0, 11.0, 20.0, 21.0],
            'commission': [1.0, 1.0, 2.0, 2.0]
        })
        
        # Create an invalid DataFrame (missing required columns)
        self.invalid_executions_df = self.valid_executions_df.drop(columns=['is_entry', 'is_exit'])
        
        # Create an empty DataFrame
        self.empty_executions_df = pd.DataFrame(columns=self.valid_executions_df.columns)
        
        # Set up mock for db.get_account_balances() 
        self.account_balances_patcher = patch('utils.db_utils.DatabaseManager.get_account_balances')
        self.mock_get_account_balances = self.account_balances_patcher.start()
        
        # Set up mock account balances return value
        self.mock_get_account_balances.return_value = pd.DataFrame({
            'date': ['2023-01-01', '2023-02-02'],
            'cash_balance': [10000.0, 10500.0]
        })

    def tearDown(self):
        """Clean up after each test"""
        super().tearDown()
        self.account_balances_patcher.stop()

    def test_basic_functionality(self):
        """Test that valid input correctly passes through to the underlying class method."""
        # Use the wrapper function
        from analytics.process_trades import process_trades
        
        # Process trades with valid data
        result_df = process_trades(self.valid_executions_df)
        
        # Verify the result
        self.assertIsNotNone(result_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        
        # Check that expected trades are present
        trade_ids = result_df['trade_id'].tolist()
        self.assertEqual(set(trade_ids), {'trade1', 'trade2'})
        
        # Log test result
        self.log_case_result("Successfully processes valid execution data", True)

    def test_exception_handling(self):
        """Test that exceptions are properly caught and result in None being returned."""
        from analytics.process_trades import process_trades
        
        # Create a mock TradeProcessor that raises an exception
        with patch('analytics.process_trades.TradeProcessor') as mock_processor_class:
            # Make the constructor raise an exception
            mock_processor_class.side_effect = Exception("Test constructor exception")
            
            # Call process_trades with valid data
            with patch('sys.stdout', new=StringIO()) as fake_out:
                result_df = process_trades(self.valid_executions_df)
                
                # Verify error message was printed
                self.assertIn("Error processing trades", fake_out.getvalue())
                
                # Verify the result is None
                self.assertIsNone(result_df)
        
        # Test exception from process_trades method
        with patch('analytics.process_trades.TradeProcessor') as mock_processor_class:
            # Make the process_trades method raise an exception
            mock_processor_instance = MagicMock()
            mock_processor_instance.process_trades.side_effect = Exception("Test method exception")
            mock_processor_class.return_value = mock_processor_instance
            
            # Call process_trades with valid data
            with patch('sys.stdout', new=StringIO()) as fake_out:
                result_df = process_trades(self.valid_executions_df)
                
                # Verify error message was printed
                self.assertIn("Error processing trades", fake_out.getvalue())
                
                # Verify the result is None
                self.assertIsNone(result_df)
        
        # Log test result
        self.log_case_result("Properly handles exceptions and returns None", True)

    def test_input_validation(self):
        """Test that different types of inputs are handled correctly."""
        from analytics.process_trades import process_trades
        
        # Test with empty DataFrame
        result_df = process_trades(self.empty_executions_df)
        self.assertIsNone(result_df)
        
        # Test with invalid DataFrame (missing required columns)
        result_df = process_trades(self.invalid_executions_df)
        self.assertIsNone(result_df)
        
        # Log test result
        self.log_case_result("Correctly handles different input types", True)

    def test_end_to_end(self):
        """Test that the function correctly processes trade data and returns expected results."""
        from analytics.process_trades import process_trades
        
        # Process trades with valid data
        result_df = process_trades(self.valid_executions_df)
        
        # Verify the result contains expected columns and data
        self.assertIsNotNone(result_df)
        
        expected_columns = [
            'trade_id', 'num_executions', 'symbol', 'direction', 'quantity', 
            'entry_price', 'exit_price', 'status'
        ]
        
        for col in expected_columns:
            self.assertIn(col, result_df.columns)
        
        # Check some specific values
        trade1_row = result_df[result_df['trade_id'] == 'trade1'].iloc[0]
        trade2_row = result_df[result_df['trade_id'] == 'trade2'].iloc[0]
        
        # Check trade1 values
        self.assertEqual(trade1_row['symbol'], 'AAPL')
        self.assertEqual(trade1_row['num_executions'], 2)
        self.assertEqual(trade1_row['direction'], 'bullish')
        self.assertEqual(trade1_row['status'], 'closed')
        
        # Check trade2 values
        self.assertEqual(trade2_row['symbol'], 'MSFT')
        self.assertEqual(trade2_row['num_executions'], 2)
        self.assertEqual(trade2_row['direction'], 'bearish')
        self.assertEqual(trade2_row['status'], 'closed')
        
        # Log test result
        self.log_case_result("Correctly processes trade data end-to-end", True)

    def test_nonstandard_inputs(self):
        """Test the function with nonstandard but valid inputs."""
        from analytics.process_trades import process_trades
        
        # Test with a DataFrame that has extra columns
        extra_columns_df = self.valid_executions_df.copy()
        extra_columns_df['extra_column'] = 'test_value'
        
        result_df = process_trades(extra_columns_df)
        self.assertIsNotNone(result_df)
        
        # Test with a DataFrame that has only one row per trade (just entries)
        entries_only_df = self.valid_executions_df[self.valid_executions_df['is_entry'] == 1].copy()
        
        result_df = process_trades(entries_only_df)
        self.assertIsNotNone(result_df)
        
        # Log test result
        self.log_case_result("Handles nonstandard but valid inputs correctly", True)


class TestCalculateVWAP(BaseTestCase):
    """Test cases for the _calculate_vwap method of TradeProcessor."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Call parent setUp to set up test tracking attributes
        super().setUp()
        
        # Create a basic TradeProcessor instance 
        executions_df = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade2', 'trade2'],
            'execution_id': ['exec1', 'exec2', 'exec3', 'exec4'],
            'quantity': [100, -100, 200, -200], 
            'price': [10.0, 11.0, 20.0, 21.0],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'is_entry': [1, 0, 1, 0],
            'is_exit': [0, 1, 0, 1],
            'execution_timestamp': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'])
        })
        
        self.processor = TradeProcessor(executions_df)

    def test_basic_calculation(self):
        """Test basic VWAP calculation with positive quantities."""
        # Create test data with positive quantities
        executions = pd.DataFrame({
            'quantity': [100, 200, 300],
            'price': [10.0, 20.0, 30.0]
        })
        
        # Expected calculation: (10*100 + 20*200 + 30*300) / (100 + 200 + 300)
        # = (1000 + 4000 + 9000) / 600 = 14000 / 600 = 23.33333...
        expected_vwap = 23.333333333333332  # Exact floating point value
        
        # Calculate VWAP
        result = self.processor._calculate_vwap(executions)
        
        # Verify result
        self.assertAlmostEqual(result, expected_vwap)
        
        self.log_case_result("Correctly calculates VWAP with positive quantities", True)

    def test_negative_quantities(self):
        """Test VWAP calculation with negative quantities."""
        # Create test data with negative quantities
        executions = pd.DataFrame({
            'quantity': [-100, -200, -300],
            'price': [10.0, 20.0, 30.0]
        })
        
        # Expected calculation: (10*|-100| + 20*|-200| + 30*|-300|) / (|-100| + |-200| + |-300|)
        # = (1000 + 4000 + 9000) / 600 = 14000 / 600 = 23.33333...
        expected_vwap = 23.333333333333332  # Exact floating point value
        
        # Calculate VWAP
        result = self.processor._calculate_vwap(executions)
        
        # Verify result
        self.assertAlmostEqual(result, expected_vwap)
        
        self.log_case_result("Correctly calculates VWAP with negative quantities", True)

    def test_mixed_quantities(self):
        """Test VWAP calculation with both positive and negative quantities."""
        # Create test data with mixed quantities
        executions = pd.DataFrame({
            'quantity': [100, -200, 300],
            'price': [10.0, 20.0, 30.0]
        })
        
        # Expected calculation: (10*|100| + 20*|-200| + 30*|300|) / (|100| + |-200| + |300|)
        # = (1000 + 4000 + 9000) / 600 = 14000 / 600 = 23.33333...
        expected_vwap = 23.333333333333332  # Exact floating point value
        
        # Calculate VWAP
        result = self.processor._calculate_vwap(executions)
        
        # Verify result
        self.assertAlmostEqual(result, expected_vwap)
        
        self.log_case_result("Correctly calculates VWAP with mixed quantities", True)

    def test_empty_dataframe(self):
        """Test that an empty DataFrame returns None."""
        # Create an empty DataFrame
        empty_df = pd.DataFrame(columns=['quantity', 'price'])
        
        # Calculate VWAP
        result = self.processor._calculate_vwap(empty_df)
        
        # Verify result
        self.assertIsNone(result)
        
        self.log_case_result("Correctly returns None for empty DataFrame", True)

    def test_zero_quantities(self):
        """Test that DataFrame with all zero quantities returns None."""
        # Create test data with zero quantities
        executions = pd.DataFrame({
            'quantity': [0, 0, 0],
            'price': [10.0, 20.0, 30.0]
        })
        
        # Calculate VWAP
        result = self.processor._calculate_vwap(executions)
        
        # Verify result
        self.assertIsNone(result)
        
        self.log_case_result("Correctly returns None for zero quantities", True)

    def test_mixed_with_zero_quantities(self):
        """Test VWAP calculation with some zero quantities mixed with non-zero quantities."""
        # Create test data with mixed zero and non-zero quantities
        executions = pd.DataFrame({
            'quantity': [100, 0, 300],
            'price': [10.0, 20.0, 30.0]
        })
        
        # Expected calculation: (10*|100| + 30*|300|) / (|100| + |300|)
        # = (1000 + 9000) / 400 = 10000 / 400 = 25.0
        expected_vwap = 25.0
        
        # Calculate VWAP
        result = self.processor._calculate_vwap(executions)
        
        # Verify result
        self.assertAlmostEqual(result, expected_vwap)
        
        self.log_case_result("Correctly handles mixed zero and non-zero quantities", True)

    def test_precision(self):
        """Test VWAP calculation precision with floating point numbers."""
        # Create test data with fractional prices and quantities
        executions = pd.DataFrame({
            'quantity': [123.45, 678.90, 246.80],
            'price': [10.123, 20.456, 30.789]
        })
        
        # Calculate manually
        numerator = (10.123 * 123.45) + (20.456 * 678.90) + (30.789 * 246.80)
        denominator = 123.45 + 678.90 + 246.80
        expected_vwap = numerator / denominator
        
        # Calculate VWAP
        result = self.processor._calculate_vwap(executions)
        
        # Verify result with high precision
        self.assertAlmostEqual(result, expected_vwap, places=10)
        
        self.log_case_result("Maintains precision with floating point numbers", True)

    def test_large_values(self):
        """Test VWAP calculation with large values to ensure no overflow."""
        # Create test data with large values
        executions = pd.DataFrame({
            'quantity': [1000000, 2000000, 3000000],
            'price': [1000.0, 2000.0, 3000.0]
        })
        
        # Expected calculation: (1000*1000000 + 2000*2000000 + 3000*3000000) / (1000000 + 2000000 + 3000000)
        # = (1000000000 + 4000000000 + 9000000000) / 6000000 = 14000000000 / 6000000 = 2333.33333...
        expected_vwap = 2333.3333333333335  # Exact floating point value
        
        # Calculate VWAP
        result = self.processor._calculate_vwap(executions)
        
        # Verify result
        self.assertAlmostEqual(result, expected_vwap)
        
        self.log_case_result("Correctly handles large values without overflow", True)

    def test_return_type(self):
        """Test that the return type is float when a valid VWAP is calculated."""
        # Create test data
        executions = pd.DataFrame({
            'quantity': [100, 200, 300],
            'price': [10.0, 20.0, 30.0]
        })
        
        # Calculate VWAP
        result = self.processor._calculate_vwap(executions)
        
        # Verify return type
        self.assertIsInstance(result, float)
        
        self.log_case_result("Returns a float type for valid VWAP calculation", True)

    def test_integration(self):
        """Test integration with real execution data in TradeProcessor."""
        # Use the processor's executions_df directly
        result = self.processor._calculate_vwap(self.processor.executions_df)
        
        # Expected calculation for the processor's executions_df:
        # (10*|100| + 11*|-100| + 20*|200| + 21*|-200|) / (|100| + |-100| + |200| + |-200|)
        # = (1000 + 1100 + 4000 + 4200) / 600 = 10300 / 600 = 17.16666...
        expected_vwap = 17.166666666666668
        
        # Verify result
        self.assertAlmostEqual(result, expected_vwap)
        
        self.log_case_result("Correctly calculates VWAP from processor's execution data", True)

class TestGetCommission(BaseTestCase):
    """Test cases for the _get_commission method of TradeProcessor."""

    def setUp(self):
        """Set up test fixtures before each test."""
        super().setUp()
        
        # Create a basic TradeProcessor instance without commission column
        self.executions_df_no_commission = pd.DataFrame({
            'trade_id': ['trade1', 'trade1', 'trade2', 'trade2'],
            'execution_id': ['exec1', 'exec2', 'exec3', 'exec4'],
            'quantity': [100, -100, 200, -200],
            'price': [10.0, 11.0, 20.0, 21.0],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'is_entry': [1, 0, 1, 0],
            'is_exit': [0, 1, 0, 1],
            'execution_timestamp': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'])
        })
        
        # Create a processor without commission data
        self.processor_no_commission = TradeProcessor(self.executions_df_no_commission)
        self.processor_no_commission.trade_directions = {
            'trade1': {'direction': 'bullish', 'initial_quantity': 100},
            'trade2': {'direction': 'bullish', 'initial_quantity': 200}
        }
        
        # Create a DataFrame with commission column
        self.executions_df_with_commission = self.executions_df_no_commission.copy()
        self.executions_df_with_commission['commission'] = [1.5, 2.5, 3.5, 4.5]
        
        # Create a processor with commission data
        self.processor_with_commission = TradeProcessor(self.executions_df_with_commission)
        self.processor_with_commission.trade_directions = {
            'trade1': {'direction': 'bullish', 'initial_quantity': 100},
            'trade2': {'direction': 'bullish', 'initial_quantity': 200}
        }

    def test_with_commission_column(self):
        """Test commission calculation when commission column exists."""
        # Calculate commissions
        result = self.processor_with_commission._get_commission()
        
        # Expected calculations:
        # trade1: 1.5 + 2.5 = 4.0
        # trade2: 3.5 + 4.5 = 8.0
        expected = pd.Series({'trade1': 4.0, 'trade2': 8.0})
        
        # Verify results
        pd.testing.assert_series_equal(result, expected, check_names=False)
        
        self.log_case_result("Correctly sums commissions per trade_id", True)

    def test_without_commission_column(self):
        """Test behavior when commission column is missing."""
        # Calculate commissions with missing column
        result = self.processor_no_commission._get_commission()
        
        # Expected: empty Series with trade_ids as index
        expected = pd.Series(index=['trade1', 'trade2'])
        
        # Verify results
        pd.testing.assert_series_equal(result, expected, check_names=False)
        
        self.log_case_result("Returns empty Series when commission column missing", True)

    def test_mixed_commission_values(self):
        """Test with mixed commission values including zeros and NaNs."""
        # Create DataFrame with mixed commission values
        mixed_df = self.executions_df_no_commission.copy()
        mixed_df['commission'] = [1.5, 0.0, np.nan, 4.5]
        
        processor = TradeProcessor(mixed_df)
        processor.trade_directions = {
            'trade1': {'direction': 'bullish', 'initial_quantity': 100},
            'trade2': {'direction': 'bullish', 'initial_quantity': 200}
        }
        
        # Calculate commissions
        result = processor._get_commission()
        
        # Expected: trade1 = 1.5 + 0.0 = 1.5, trade2 = NaN + 4.5 = NaN
        expected = pd.Series({'trade1': 1.5, 'trade2': 4.5})  # NaN is handled by pandas sum
        
        # Verify results
        pd.testing.assert_series_equal(result, expected, check_dtype=False, check_names=False)
        
        self.log_case_result("Correctly handles mixed commission values", True)

    def test_negative_commission_values(self):
        """Test with negative commission values (e.g., rebates)."""
        # Create DataFrame with negative commission values
        negative_df = self.executions_df_no_commission.copy()
        negative_df['commission'] = [1.5, -0.5, 3.5, -1.0]
        
        processor = TradeProcessor(negative_df)
        processor.trade_directions = {
            'trade1': {'direction': 'bullish', 'initial_quantity': 100},
            'trade2': {'direction': 'bullish', 'initial_quantity': 200}
        }
        
        # Calculate commissions
        result = processor._get_commission()
        
        # Expected: trade1 = 1.5 + (-0.5) = 1.0, trade2 = 3.5 + (-1.0) = 2.5
        expected = pd.Series({'trade1': 1.0, 'trade2': 2.5})
        
        # Verify results
        pd.testing.assert_series_equal(result, expected, check_names=False)
        
        self.log_case_result("Correctly handles negative commission values", True)

    def test_missing_trades(self):
        """Test behavior with trade_ids in trade_directions but not in executions."""
        # Add an extra trade_id to trade_directions that doesn't exist in executions
        processor = TradeProcessor(self.executions_df_with_commission)
        processor.trade_directions = {
            'trade1': {'direction': 'bullish', 'initial_quantity': 100},
            'trade2': {'direction': 'bullish', 'initial_quantity': 200},
            'trade3': {'direction': 'bullish', 'initial_quantity': 300}  # Not in executions
        }
        
        # Calculate commissions
        result = processor._get_commission()
        
        # Expected: trade1 and trade2 have commissions, trade3 is in the index but has no value
        expected = pd.Series({'trade1': 4.0, 'trade2': 8.0})
        
        # Verify only trade1 and trade2 are in the result
        self.assertEqual(set(result.index), {'trade1', 'trade2'})
        pd.testing.assert_series_equal(result, expected, check_names=False)
        
        self.log_case_result("Correctly handles trades not in executions", True)

    def test_empty_executions(self):
        """Test with empty executions DataFrame."""
        # Create an empty DataFrame with the right columns
        empty_df = pd.DataFrame(columns=self.executions_df_with_commission.columns)
        
        processor = TradeProcessor(empty_df)
        processor.trade_directions = {
            'trade1': {'direction': 'bullish', 'initial_quantity': 100},
            'trade2': {'direction': 'bullish', 'initial_quantity': 200}
        }
        
        # Calculate commissions
        result = processor._get_commission()
        
        # Expected: empty Series with trade_ids not in index (since no groupby results)
        expected = pd.Series()
        
        # Verify result is an empty Series
        self.assertTrue(result.empty)
        
        self.log_case_result("Correctly handles empty executions", True)

    def test_type_verification(self):
        """Test that return value is always a pandas Series with proper structure."""
        # Get commissions from both processors
        result_with = self.processor_with_commission._get_commission()
        result_without = self.processor_no_commission._get_commission()
        
        # Verify both return pandas Series
        self.assertIsInstance(result_with, pd.Series)
        self.assertIsInstance(result_without, pd.Series)
        
        # Verify Series with commissions has numeric values
        self.assertTrue(np.issubdtype(result_with.dtype, np.number))
        
        self.log_case_result("Always returns pandas Series with proper structure", True)


class TestGetDirectionExecutions(BaseTestCase):
    """Test cases for the _get_direction_executions method of TradeProcessor."""

    def setUp(self):
        """Set up test fixtures before each test."""
        super().setUp()
        
        # Create a sample executions DataFrame with both bullish and bearish trades
        self.executions_df = pd.DataFrame({
            'trade_id': ['bullish_trade', 'bullish_trade', 'bullish_trade', 
                         'bearish_trade', 'bearish_trade', 'bearish_trade',
                         'mixed_order_trade', 'mixed_order_trade', 'mixed_order_trade',
                         'entry_only_trade', 'multi_entry_exit_trade', 'multi_entry_exit_trade',
                         'multi_entry_exit_trade', 'multi_entry_exit_trade', 'zero_qty_trade'],
            'execution_id': list(range(1, 16)),
            'quantity': [100, 50, -150,              # bullish trade: 2 entries, 1 exit
                         -100, -50, 150,             # bearish trade: 2 entries, 1 exit
                         -75, 50, -25,               # mixed order: entry-exit-entry
                         200,                        # entry only trade: only entry
                         150, 100, -75, -75,         # multi: multiple entries/exits (2 entries, 2 exits)
                         0],                         # zero quantity
            'price': [10.0, 11.0, 12.0, 20.0, 21.0, 19.0, 30.0, 35.0, 25.0, 40.0, 50.0, 51.0, 52.0, 53.0, 60.0],
            'symbol': ['AAPL'] * 15,
            'is_entry': [1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 1, 1, 0, 0, 0],
            'is_exit': [0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 1, 1],
            'execution_timestamp': pd.to_datetime([
                '2023-01-01 10:00:00', '2023-01-01 11:00:00', '2023-01-01 14:00:00',
                '2023-01-02 10:00:00', '2023-01-02 11:00:00', '2023-01-02 14:00:00',
                '2023-01-03 14:00:00', '2023-01-03 10:00:00', '2023-01-03 12:00:00',  # Intentionally out of order
                '2023-01-04 10:00:00',
                '2023-01-05 09:00:00', '2023-01-05 10:00:00', '2023-01-05 11:00:00', '2023-01-05 12:00:00',
                '2023-01-06 10:00:00'
            ])
        })
        
        # Initialize processor
        self.processor = TradeProcessor(self.executions_df)
        
        # Set up trade directions manually
        self.processor.trade_directions = {
            'bullish_trade': {'direction': 'bullish', 'initial_quantity': 100.0, 'abs_initial_quantity': 100.0},
            'bearish_trade': {'direction': 'bearish', 'initial_quantity': -100.0, 'abs_initial_quantity': 100.0},
            'mixed_order_trade': {'direction': 'bearish', 'initial_quantity': -75.0, 'abs_initial_quantity': 75.0},
            'entry_only_trade': {'direction': 'bullish', 'initial_quantity': 200.0, 'abs_initial_quantity': 200.0},
            'multi_entry_exit_trade': {'direction': 'bullish', 'initial_quantity': 150.0, 'abs_initial_quantity': 150.0},
            'zero_qty_trade': {'direction': 'bullish', 'initial_quantity': 0.0, 'abs_initial_quantity': 0.0}
        }

    def test_bullish_trade_direction(self):
        """Test filtering executions for a bullish trade."""
        # Get entry and exit executions for bullish trade
        entry_execs, exit_execs = self.processor._get_direction_executions('bullish_trade')
        
        # Verify entry executions
        self.assertEqual(len(entry_execs), 2)
        self.assertTrue(all(entry_execs['quantity'] > 0))
        self.assertEqual(entry_execs['quantity'].sum(), 150)  # Total entry quantity
        
        # Verify exit executions
        self.assertEqual(len(exit_execs), 1)
        self.assertTrue(all(exit_execs['quantity'] < 0))
        self.assertEqual(exit_execs['quantity'].sum(), -150)  # Total exit quantity
        
        self.log_case_result("Correctly identifies bullish trade entry and exit executions", True)
    
    def test_bearish_trade_direction(self):
        """Test filtering executions for a bearish trade."""
        # Get entry and exit executions for bearish trade
        entry_execs, exit_execs = self.processor._get_direction_executions('bearish_trade')
        
        # Verify entry executions
        self.assertEqual(len(entry_execs), 2)
        self.assertTrue(all(entry_execs['quantity'] < 0))
        self.assertEqual(entry_execs['quantity'].sum(), -150)  # Total entry quantity
        
        # Verify exit executions
        self.assertEqual(len(exit_execs), 1)
        self.assertTrue(all(exit_execs['quantity'] > 0))
        self.assertEqual(exit_execs['quantity'].sum(), 150)  # Total exit quantity
        
        self.log_case_result("Correctly identifies bearish trade entry and exit executions", True)
    
    def test_mixed_execution_order(self):
        """Test that filtering works regardless of execution timestamp order."""
        # Get entry and exit executions for mixed order trade (execution timestamps not in chronological order)
        entry_execs, exit_execs = self.processor._get_direction_executions('mixed_order_trade')
        
        # Verify entry executions (should be 2 entries with negative quantities)
        self.assertEqual(len(entry_execs), 2)
        self.assertTrue(all(entry_execs['quantity'] < 0))
        self.assertEqual(entry_execs['quantity'].sum(), -100)  # Total entry quantity: -75 + -25
        
        # Verify exit executions (should be 1 exit with positive quantity)
        self.assertEqual(len(exit_execs), 1)
        self.assertTrue(all(exit_execs['quantity'] > 0))
        self.assertEqual(exit_execs['quantity'].sum(), 50)  # Total exit quantity
        
        self.log_case_result("Correctly filters executions regardless of timestamp order", True)
    
    def test_multiple_entries_exits(self):
        """Test filtering with multiple entry and exit executions."""
        # Get entry and exit executions for trade with multiple entries and exits
        entry_execs, exit_execs = self.processor._get_direction_executions('multi_entry_exit_trade')
        
        # Verify entry executions
        self.assertEqual(len(entry_execs), 2)
        self.assertTrue(all(entry_execs['quantity'] > 0))
        self.assertEqual(entry_execs['quantity'].sum(), 250)  # Total entry quantity: 150 + 100
        
        # Verify exit executions
        self.assertEqual(len(exit_execs), 2)
        self.assertTrue(all(exit_execs['quantity'] < 0))
        self.assertEqual(exit_execs['quantity'].sum(), -150)  # Total exit quantity: -75 + -75
        
        self.log_case_result("Correctly handles multiple entry and exit executions", True)
    
    def test_only_entry_executions(self):
        """Test filtering for a trade with only entry executions (no exits)."""
        # Get entry and exit executions for trade with only entries
        entry_execs, exit_execs = self.processor._get_direction_executions('entry_only_trade')
        
        # Verify entry executions
        self.assertEqual(len(entry_execs), 1)
        self.assertTrue(all(entry_execs['quantity'] > 0))
        self.assertEqual(entry_execs['quantity'].sum(), 200)  # Total entry quantity
        
        # Verify exit executions (should be empty)
        self.assertEqual(len(exit_execs), 0)
        self.assertTrue(exit_execs.empty)
        
        self.log_case_result("Correctly handles trade with only entry executions", True)
    
    def test_zero_quantity(self):
        """Test handling of executions with zero quantity."""
        # Add a zero quantity execution (should be excluded from both entry and exit)
        trade_execs = self.executions_df[self.executions_df['trade_id'] == 'zero_qty_trade']
        self.assertEqual(len(trade_execs), 1)
        self.assertEqual(trade_execs['quantity'].iloc[0], 0)
        
        # Get entry and exit executions
        entry_execs, exit_execs = self.processor._get_direction_executions('zero_qty_trade')
        
        # Verify both entry and exit executions are empty (zero quantity excluded from both)
        self.assertEqual(len(entry_execs), 0)
        self.assertEqual(len(exit_execs), 0)
        
        self.log_case_result("Correctly excludes zero quantity executions", True)
    
    def test_empty_executions_dataframe(self):
        """Test with an empty executions DataFrame."""
        # Create a processor with empty executions DataFrame
        empty_df = pd.DataFrame(columns=self.executions_df.columns)
        processor = TradeProcessor(empty_df)
        processor.trade_directions = {'test_trade': {'direction': 'bullish'}}
        
        # Get entry and exit executions
        entry_execs, exit_execs = processor._get_direction_executions('test_trade')
        
        # Verify both entry and exit executions are empty
        self.assertEqual(len(entry_execs), 0)
        self.assertEqual(len(exit_execs), 0)
        
        self.log_case_result("Correctly handles empty executions DataFrame", True)
    
    def test_return_type(self):
        """Test that the return value is a tuple of two DataFrames."""
        # Get entry and exit executions
        result = self.processor._get_direction_executions('bullish_trade')
        
        # Verify return type
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], pd.DataFrame)  # entry_execs
        self.assertIsInstance(result[1], pd.DataFrame)  # exit_execs
        
        self.log_case_result("Returns correct tuple of DataFrames", True)
    
    def test_integration(self):
        """Test integration with the TradeProcessor class."""
        # Create a new processor with our test data
        processor = TradeProcessor(self.executions_df)
        
        # Run preprocess to populate entry_execs, exit_execs, and trade_directions
        processor.preprocess()
        
        # Get entry and exit executions for a bullish trade
        entry_execs, exit_execs = processor._get_direction_executions('bullish_trade')
        
        # Verify results match expected behavior from preprocess
        self.assertTrue(all(entry_execs['quantity'] > 0))
        self.assertTrue(all(exit_execs['quantity'] < 0))
        
        # Get entry and exit executions for a bearish trade
        entry_execs, exit_execs = processor._get_direction_executions('bearish_trade')
        
        # Verify results match expected behavior from preprocess
        self.assertTrue(all(entry_execs['quantity'] < 0))
        self.assertTrue(all(exit_execs['quantity'] > 0))
        
        self.log_case_result("Integrates correctly with TradeProcessor class", True)


if __name__ == '__main__':
    unittest.main(exit=False)  # Run tests without exiting
    print_summary()  # Print detailed summary of test results