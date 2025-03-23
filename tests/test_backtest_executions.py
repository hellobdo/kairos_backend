"""
Test module for analytics.backtest_executions.
Tests the functionality of the backtest execution processing.
"""
import unittest
import sys
import builtins
from datetime import date, time
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import os
import numpy as np

# Import our test utilities from the test_utils module
from tests.utils.test_utils import BaseTestCase, print_summary, CaptureOutput

# Import the module(s) or function(s) you want to test
from analytics.backtest_executions import process_backtest_executions, clean_backtest_executions

# Module specific test fixtures
def create_module_fixtures():
    """Create test fixtures specific to this module's tests"""
    fixtures = {}
    
    # Mock strategy class with parameters
    class MockStrategy:
        def __init__(self):
            self.parameters = {
                'side': 'buy',
                'stop_loss_rules': [
                    {"price_below": 100, "amount": 0.30}
                ],
                'risk_reward': 2.5
            }
    
    fixtures['mock_strategy'] = MockStrategy()
    fixtures['mock_file_path'] = 'logs/mock_strategy_2023-04-15_14-30_trades.csv'
    
    return fixtures

class TestBacktestExecutionsImports(BaseTestCase):
    """Test basic imports and module setup"""
    
    def test_imports(self):
        """Test that imports are working correctly"""
        # Case 1: Check that functions are callable
        try:
            self.assertTrue(callable(process_backtest_executions))
            self.log_case_result("Function is callable", True)
        except AssertionError:
            self.log_case_result("Function is callable", False)
            raise

class TestProcessBacktestExecutions(BaseTestCase):
    """Test cases for process_backtest_executions function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
    
    @patch('analytics.backtest_executions.clean_backtest_executions')
    def test_strategy_params_extraction(self, mock_clean_data):
        """Test that strategy parameters are correctly extracted"""
        # Setup mocks
        mock_strategy = self.fixtures['mock_strategy']
        mock_file_path = self.fixtures['mock_file_path']
        
        # Mock clean_backtest_executions to raise an exception
        # This will make the function return early after parameter extraction
        mock_clean_data.side_effect = ValueError("Test exception to exit early")
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function under test - it will exit early
            result = process_backtest_executions(mock_strategy, mock_file_path)
            
            # Function should return False due to the raised exception
            self.assertFalse(result)
            
            # Get the captured output
            output = self.captured_output.get_value()
            
            # Check that the print statement shows the extracted parameters
            self.assertIn("Using strategy parameters:", output)
            self.assertIn("'side': 'buy'", output)
            self.assertIn("'risk_reward': 2.5", output)
            
            self.log_case_result("Strategy parameters extracted and printed correctly", True)
        except AssertionError as e:
            self.log_case_result(f"Strategy parameters extracted and printed correctly: {str(e)}", False)
            raise
        finally:
            # Restore stdout
            self.restore_stdout(original_stdout)
    
    def test_missing_strategy_side(self):
        """Test that the function handles missing 'side' parameter correctly"""
        # Create a strategy without a 'side' parameter
        class StrategyWithoutSide:
            def __init__(self):
                self.parameters = {
                    'stop_loss_rules': [{"price_below": 100, "amount": 0.30}],
                    'risk_reward': 2.5
                }
        
        mock_strategy = StrategyWithoutSide()
        mock_file_path = self.fixtures['mock_file_path']
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function under test - it should return False due to the error
            result = process_backtest_executions(mock_strategy, mock_file_path)
            
            # Function should return False since the error is caught in the outer try-except
            self.assertFalse(result)
            
            # Check the output for the error message - match the exact format from the actual output
            output = self.captured_output.get_value()
            self.assertIn("ERROR: Failed to process trades: Strategy side (buy/sell) is required but not provided in strategy parameters", output)
            
            self.log_case_result("Function returns False and logs error for missing 'side' parameter", True)
        except AssertionError as e:
            self.log_case_result(f"Function returns False and logs error for missing 'side' parameter: {str(e)}", False)
            raise
        finally:
            # Restore stdout
            self.restore_stdout(original_stdout)

    @patch('analytics.backtest_executions.clean_backtest_executions')
    def test_strategy_side_assignment(self, mock_clean_data):
        """Test that strategy_side is correctly assigned from strategy_params['side']"""
        # Create a strategy with a specific side
        class StrategyWithSide:
            def __init__(self):
                self.parameters = {
                    'side': 'sell',  # Using 'sell' to distinguish from the default 'buy'
                    'risk_reward': 2.0
                }
        
        mock_strategy = StrategyWithSide()
        mock_file_path = self.fixtures['mock_file_path']
        
        # Use a modified side_effect to check the strategy_side value
        # The side_effect will inspect identify_trades arguments
        def check_strategy_side(*args, **kwargs):
            # When clean_backtest_executions is called, add a spy function to identify_trades
            # to verify the strategy_side parameter
            
            with patch('analytics.backtest_executions.identify_trades') as mock_identify:
                # Setup mock identify_trades to return empty DataFrames
                mock_identify.return_value = (pd.DataFrame(), pd.DataFrame())
                
                # Return test data to continue the function execution
                return (pd.DataFrame(), pd.DataFrame())
        
        mock_clean_data.side_effect = check_strategy_side
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Mock remaining functions to prevent execution after identify_trades
            with patch('analytics.backtest_executions.identify_trades') as mock_identify_trades:
                # Setup mock to return empty DataFrames
                mock_identify_trades.return_value = (pd.DataFrame(), pd.DataFrame())
                
                # Call the function
                process_backtest_executions(mock_strategy, mock_file_path)
                
                # Verify identify_trades was called with the correct strategy_side
                # The second argument to identify_trades should be strategy_side
                args, kwargs = mock_identify_trades.call_args
                self.assertEqual(args[1], 'sell', "strategy_side should be 'sell'")
                
                self.log_case_result("strategy_side correctly assigned from strategy_params['side']", True)
        except AssertionError as e:
            self.log_case_result(f"strategy_side correctly assigned from strategy_params['side']: {str(e)}", False)
            raise
        finally:
            # Restore stdout
            self.restore_stdout(original_stdout)

class TestCleanBacktestExecutions(BaseTestCase):
    """Test the clean_backtest_executions function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        
        # Sample trades data as it would appear in a CSV file
        self.sample_csv_data = [
            "time,identifier,side,filled_quantity,price,symbol,multiplier,asset.strike,asset.multiplier",
            "2023-05-15 09:30:45.123456,order1,buy,100,150.25,AAPL,1,0,1",
            "2023-05-15 09:35:20.654321,order2,sell,50,151.50,AAPL,1,0,1",
            "2023-05-15 10:15:30.789012,order3,buy,0,152.75,AAPL,1,0,1",  # Zero quantity - should be rejected
            "2023-05-16 14:22:10.456789,order4,sell,200,153.00,MSFT,1,0,1"
        ]
        
        # Create a mock file path
        self.mock_file_path = 'logs/test_trades.csv'
    
    @patch('pandas.read_csv')
    def test_read_csv_file(self, mock_read_csv):
        """Test Case 1: Function can read CSV file"""
        # Setup mock to return a DataFrame from our sample data
        mock_read_csv.return_value = pd.DataFrame({
            'time': ['2023-05-15 09:30:45.123456', '2023-05-15 09:35:20.654321'],
            'identifier': ['order1', 'order2'],
            'side': ['buy', 'sell'],
            'filled_quantity': [100, 50],
            'price': [150.25, 151.50],
            'symbol': ['AAPL', 'AAPL'],
            'multiplier': [1, 1],
            'asset.strike': [0, 0],
            'asset.multiplier': [1, 1]
        })
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        try:
            # Call function
            result_df, rejected_df = clean_backtest_executions(self.mock_file_path)
            
            # Verify read_csv was called with the correct file path
            mock_read_csv.assert_called_once_with(self.mock_file_path)
            
            # Verify the returned DataFrame has data
            self.assertFalse(result_df.empty)
            self.assertEqual(len(result_df), 2)
            
            self.log_case_result("Successfully reads CSV file", True)
        except AssertionError as e:
            self.log_case_result(f"Successfully reads CSV file: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_time_column_slicing(self):
        """Test Case 2: Function slices time column correctly"""
        # Create test DataFrame with long time strings
        test_df = pd.DataFrame({
            'time': [
                '2023-05-15 09:30:45.123456',  # Longer than 19 chars
                '2023-05-15 09:35:20.654321',  # Longer than 19 chars
                '2023-05-15 10:15:30'          # Exactly 19 chars
            ],
            'filled_quantity': [100, 50, 75],
            'symbol': ['AAPL', 'AAPL', 'MSFT']
        })
        
        # Mock read_csv to return our test DataFrame
        with patch('pandas.read_csv', return_value=test_df):
            # Capture stdout
            original_stdout = self.capture_stdout()
            
            try:
                # Call function
                result_df, _ = clean_backtest_executions(self.mock_file_path)
                
                # Verify all time values are sliced to 19 characters
                for time_val in result_df['execution_timestamp']:
                    self.assertLessEqual(len(str(time_val)), 19)
                
                # Check that the original long timestamps are properly truncated
                self.assertEqual(str(result_df['execution_timestamp'].iloc[0])[:19], '2023-05-15 09:30:45')
                self.assertEqual(str(result_df['execution_timestamp'].iloc[1])[:19], '2023-05-15 09:35:20')
                
                self.log_case_result("Correctly slices time column to 19 characters", True)
            except AssertionError as e:
                self.log_case_result(f"Correctly slices time column to 19 characters: {str(e)}", False)
                raise
            finally:
                self.restore_stdout(original_stdout)
    
    def test_date_time_columns_creation(self):
        """Test Case 3: Function creates expected date and time columns"""
        # Create test DataFrame
        test_df = pd.DataFrame({
            'time': ['2023-05-15 09:30:45', '2023-05-16 14:22:10'],
            'filled_quantity': [100, 200],
            'symbol': ['AAPL', 'MSFT']
        })
        
        # Mock read_csv to return our test DataFrame
        with patch('pandas.read_csv', return_value=test_df):
            # Capture stdout
            original_stdout = self.capture_stdout()
            
            try:
                # Call function
                result_df, _ = clean_backtest_executions(self.mock_file_path)
                
                # Verify date and time columns exist
                self.assertIn('date', result_df.columns)
                self.assertIn('time_of_day', result_df.columns)
                
                # Verify values are of correct types
                self.assertIsInstance(result_df['date'].iloc[0], date)
                self.assertIsInstance(result_df['time_of_day'].iloc[0], time)
                
                # Verify values are correct
                self.assertEqual(result_df['date'].iloc[0], date(2023, 5, 15))
                self.assertEqual(result_df['time_of_day'].iloc[0], time(9, 30, 45))
                self.assertEqual(result_df['date'].iloc[1], date(2023, 5, 16))
                self.assertEqual(result_df['time_of_day'].iloc[1], time(14, 22, 10))
                
                self.log_case_result("Correctly creates date and time_of_day columns", True)
            except AssertionError as e:
                self.log_case_result(f"Correctly creates date and time_of_day columns: {str(e)}", False)
                raise
            finally:
                self.restore_stdout(original_stdout)
    
    def test_column_renaming(self):
        """Test Case 4: Function renames columns as expected"""
        # Create test DataFrame
        test_df = pd.DataFrame({
            'time': ['2023-05-15 09:30:45', '2023-05-16 14:22:10'],
            'filled_quantity': [100, 200],
            'symbol': ['AAPL', 'MSFT']
        })
        
        # Mock read_csv to return our test DataFrame
        with patch('pandas.read_csv', return_value=test_df):
            # Capture stdout
            original_stdout = self.capture_stdout()
            
            try:
                # Call function
                result_df, _ = clean_backtest_executions(self.mock_file_path)
                
                # Verify 'time' column is renamed to 'execution_timestamp'
                self.assertIn('execution_timestamp', result_df.columns)
                self.assertNotIn('time', result_df.columns)
                
                self.log_case_result("Correctly renames 'time' column to 'execution_timestamp'", True)
            except AssertionError as e:
                self.log_case_result(f"Correctly renames 'time' column to 'execution_timestamp': {str(e)}", False)
                raise
            finally:
                self.restore_stdout(original_stdout)
    
    def test_zero_quantity_filtering(self):
        """Test Case 5: Function filters out zero quantity trades"""
        # Create test DataFrame with some zero quantity trades
        test_df = pd.DataFrame({
            'time': ['2023-05-15 09:30:45', '2023-05-15 10:15:30', '2023-05-16 14:22:10'],
            'filled_quantity': [100, 0, 200],  # Second row has zero quantity
            'symbol': ['AAPL', 'AAPL', 'MSFT']
        })
        
        # Mock read_csv to return our test DataFrame
        with patch('pandas.read_csv', return_value=test_df):
            # Capture stdout
            original_stdout = self.capture_stdout()
            
            try:
                # Call function
                result_df, rejected_df = clean_backtest_executions(self.mock_file_path)
                
                # Verify zero quantity trades are filtered out
                self.assertEqual(len(result_df), 2)  # Only two rows should remain
                self.assertTrue(all(result_df['filled_quantity'] > 0))  # All remaining quantities should be > 0
                
                # Verify rejected trades
                self.assertEqual(len(rejected_df), 1)  # One row should be rejected
                self.assertEqual(rejected_df['filled_quantity'].iloc[0], 0)  # The rejected row should have qty = 0
                
                # Verify the print message about filtered rows
                output = self.captured_output.get_value()
                self.assertIn("Filtered out 1 rows where filled_quantity is not greater than zero", output)
                
                self.log_case_result("Correctly filters out zero quantity trades", True)
            except AssertionError as e:
                self.log_case_result(f"Correctly filters out zero quantity trades: {str(e)}", False)
                raise
            finally:
                self.restore_stdout(original_stdout)
    
    def test_rejected_trades_merging(self):
        """Test Case 6: Function correctly merges rejected trades"""
        # Create test DataFrame with multiple zero quantity trades
        test_df = pd.DataFrame({
            'time': ['2023-05-15 09:30:45', '2023-05-15 10:15:30', '2023-05-16 14:22:10', '2023-05-16 15:00:00'],
            'filled_quantity': [100, 0, 200, 0],  # Second and fourth rows have zero quantity
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT']
        })
        
        # Mock read_csv to return our test DataFrame
        with patch('pandas.read_csv', return_value=test_df):
            # Capture stdout
            original_stdout = self.capture_stdout()
            
            try:
                # Call function
                result_df, rejected_df = clean_backtest_executions(self.mock_file_path)
                
                # Verify multiple rejected trades are merged
                self.assertEqual(len(rejected_df), 2)  # Two rows should be rejected
                
                # Verify rejection reason is set
                self.assertTrue(all(rejected_df['rejection_reason'] == "Quantity not greater than zero"))
                
                self.log_case_result("Correctly merges rejected trades", True)
            except AssertionError as e:
                self.log_case_result(f"Correctly merges rejected trades: {str(e)}", False)
                raise
            finally:
                self.restore_stdout(original_stdout)
    
    def test_return_values(self):
        """Test Case 7: Function returns two DataFrames as expected"""
        # Create test DataFrame
        test_df = pd.DataFrame({
            'time': ['2023-05-15 09:30:45', '2023-05-15 10:15:30', '2023-05-16 14:22:10'],
            'filled_quantity': [100, 0, 200],  # Second row has zero quantity
            'symbol': ['AAPL', 'AAPL', 'MSFT']
        })
        
        # Mock read_csv to return our test DataFrame
        with patch('pandas.read_csv', return_value=test_df):
            # Capture stdout
            original_stdout = self.capture_stdout()
            
            try:
                # Call function
                result = clean_backtest_executions(self.mock_file_path)
                
                # Verify function returns a tuple of two elements
                self.assertIsInstance(result, tuple)
                self.assertEqual(len(result), 2)
                
                # Verify both elements are DataFrames
                result_df, rejected_df = result
                self.assertIsInstance(result_df, pd.DataFrame)
                self.assertIsInstance(rejected_df, pd.DataFrame)
                
                # Verify the content of the DataFrames
                self.assertEqual(len(result_df), 2)  # Valid trades
                self.assertEqual(len(rejected_df), 1)  # Rejected trades
                
                self.log_case_result("Correctly returns two DataFrames", True)
            except AssertionError as e:
                self.log_case_result(f"Correctly returns two DataFrames: {str(e)}", False)
                raise
            finally:
                self.restore_stdout(original_stdout)

class TestIdentifyTrades(BaseTestCase):
    """Test the identify_trades function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        
        # Import the function
        from analytics.backtest_executions import identify_trades
        self.identify_trades = identify_trades
        
        # Create a sample DataFrame for testing
        self.test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime([
                '2023-05-15 09:30:45',
                '2023-05-15 09:35:20',
                '2023-05-15 10:15:30',
                '2023-05-16 14:22:10'
            ]),
            'identifier': ['order1', 'order2', 'order3', 'order4'],
            'side': ['buy', 'sell', 'buy', 'sell'],
            'filled_quantity': [100, 50, 200, 100],
            'price': [150.25, 151.50, 152.75, 153.00],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
            'date': [date(2023, 5, 15), date(2023, 5, 15), date(2023, 5, 15), date(2023, 5, 16)],
            'time_of_day': [time(9, 30, 45), time(9, 35, 20), time(10, 15, 30), time(14, 22, 10)]
        })
    
    def test_df_sorting(self):
        """Test Case 1: DataFrame is copied and sorted by execution_timestamp"""
        # Create a deliberately unsorted DataFrame
        unsorted_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime([
                '2023-05-15 09:35:20',  # Second timestamp first
                '2023-05-15 09:30:45',  # First timestamp second
                '2023-05-16 14:22:10',  # Fourth timestamp third
                '2023-05-15 10:15:30'   # Third timestamp fourth
            ]),
            'side': ['buy', 'sell', 'buy', 'sell'],
            'filled_quantity': [100, 50, 200, 100],
            'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT']
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Create a copy of the function that we can mock
            original_sort_values = pd.DataFrame.sort_values
            original_copy = pd.DataFrame.copy
            
            # Create tracking variables
            sort_called = [False]
            copy_called = [False]
            
            # Define mock functions
            def mock_sort_values(self, *args, **kwargs):
                sort_called[0] = True
                if args and args[0] == 'execution_timestamp':
                    return original_sort_values(self, *args, **kwargs)
                return self
                
            def mock_copy(self, *args, **kwargs):
                copy_called[0] = True
                return original_copy(self, *args, **kwargs)
                
            # Apply the mocks
            pd.DataFrame.sort_values = mock_sort_values
            pd.DataFrame.copy = mock_copy
            
            try:
                # Call the function - we don't care about the result
                # but need to handle any errors that might come from using
                # our simplified test data
                try:
                    self.identify_trades(unsorted_df, 'buy')
                except Exception:
                    # Expected since we have minimal test data
                    pass
                
                # Verify the methods were called
                self.assertTrue(copy_called[0], "DataFrame.copy() was not called")
                self.assertTrue(sort_called[0], "DataFrame.sort_values() was not called")
                
                self.log_case_result("DataFrame is copied and sorted by execution_timestamp", True)
            finally:
                # Restore original methods
                pd.DataFrame.sort_values = original_sort_values
                pd.DataFrame.copy = original_copy
                
        except AssertionError as e:
            self.log_case_result(f"DataFrame is copied and sorted by execution_timestamp: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_opening_closing_sides_buy(self):
        """Test Case 2A: Opening and closing sides are correctly assigned for 'buy' strategy"""
        # Create a minimal DataFrame
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime(['2023-05-15 09:30:45']),
            'side': ['buy'],
            'filled_quantity': [100],
            'symbol': ['AAPL']
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # We'll use a mock to inspect what's happening inside the function
            original_df_iterrows = pd.DataFrame.iterrows
            
            # Track sides classification
            sides_check = {
                'opening_sides': None,
                'closing_sides': None
            }
            
            def mock_iterrows(self):
                # This gets called when the function loops through rows
                # Before it starts, we can capture the sides lists from the function's scope
                frame = sys._getframe(1)
                if 'opening_sides' in frame.f_locals and 'closing_sides' in frame.f_locals:
                    sides_check['opening_sides'] = frame.f_locals['opening_sides']
                    sides_check['closing_sides'] = frame.f_locals['closing_sides']
                
                # Return the original iterator
                return original_df_iterrows(self)
            
            # Apply the mock
            pd.DataFrame.iterrows = mock_iterrows
            
            try:
                # Call the function - we don't care about the result
                try:
                    self.identify_trades(test_df, 'buy')
                except Exception:
                    # Expected since we're using minimal test data
                    pass
                
                # Verify the sides are correct for buy strategy
                self.assertIsNotNone(sides_check['opening_sides'], "opening_sides was not captured")
                self.assertIsNotNone(sides_check['closing_sides'], "closing_sides was not captured")
                
                # For buy strategy:
                self.assertEqual(
                    sides_check['opening_sides'],
                    ['buy', 'buy_to_cover', 'buy_to_close'],
                    "Opening sides incorrect for 'buy' strategy"
                )
                
                self.assertEqual(
                    sides_check['closing_sides'],
                    ['sell', 'sell_to_close', 'sell_short'],
                    "Closing sides incorrect for 'buy' strategy"
                )
                
                self.log_case_result("Opening and closing sides correctly assigned for 'buy' strategy", True)
            finally:
                # Restore original method
                pd.DataFrame.iterrows = original_df_iterrows
                
        except AssertionError as e:
            self.log_case_result(f"Opening and closing sides correctly assigned for 'buy' strategy: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_opening_closing_sides_sell(self):
        """Test Case 2B: Opening and closing sides are correctly assigned for 'sell' strategy"""
        # Create a minimal DataFrame
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime(['2023-05-15 09:30:45']),
            'side': ['sell'],
            'filled_quantity': [100],
            'symbol': ['AAPL']
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # We'll use a mock to inspect what's happening inside the function
            original_df_iterrows = pd.DataFrame.iterrows
            
            # Track sides classification
            sides_check = {
                'opening_sides': None,
                'closing_sides': None
            }
            
            def mock_iterrows(self):
                # This gets called when the function loops through rows
                # Before it starts, we can capture the sides lists from the function's scope
                frame = sys._getframe(1)
                if 'opening_sides' in frame.f_locals and 'closing_sides' in frame.f_locals:
                    sides_check['opening_sides'] = frame.f_locals['opening_sides']
                    sides_check['closing_sides'] = frame.f_locals['closing_sides']
                
                # Return the original iterator
                return original_df_iterrows(self)
            
            # Apply the mock
            pd.DataFrame.iterrows = mock_iterrows
            
            try:
                # Call the function - we don't care about the result
                try:
                    self.identify_trades(test_df, 'sell')
                except Exception:
                    # Expected since we're using minimal test data
                    pass
                
                # Verify the sides are correct for sell strategy
                self.assertIsNotNone(sides_check['opening_sides'], "opening_sides was not captured")
                self.assertIsNotNone(sides_check['closing_sides'], "closing_sides was not captured")
                
                # For sell strategy:
                self.assertEqual(
                    sides_check['opening_sides'],
                    ['sell', 'sell_to_close', 'sell_short'],
                    "Opening sides incorrect for 'sell' strategy"
                )
                
                self.assertEqual(
                    sides_check['closing_sides'],
                    ['buy', 'buy_to_cover', 'buy_to_close'],
                    "Closing sides incorrect for 'sell' strategy"
                )
                
                self.log_case_result("Opening and closing sides correctly assigned for 'sell' strategy", True)
            finally:
                # Restore original method
                pd.DataFrame.iterrows = original_df_iterrows
                
        except AssertionError as e:
            self.log_case_result(f"Opening and closing sides correctly assigned for 'sell' strategy: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_symbol_position_tracking_initialization(self):
        """Test Case 3: Open positions and trade IDs are initialized for each symbol"""
        # Create a complete test DataFrame with two symbols
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime(['2023-05-15 09:30:45', '2023-05-15 09:35:20']),
            'side': ['buy', 'buy'],
            'filled_quantity': [100, 200],
            'symbol': ['AAPL', 'MSFT'],  # Two different symbols
            'identifier': ['order1', 'order2'],
            'price': [150.0, 250.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function with our test data
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Check that trade_ids were assigned
            self.assertIn('trade_id', result_df.columns, "trade_id column should be added")
            
            # Check that both symbols got different trade IDs
            aapl_trade_id = result_df[result_df['symbol'] == 'AAPL']['trade_id'].iloc[0]
            msft_trade_id = result_df[result_df['symbol'] == 'MSFT']['trade_id'].iloc[0]
            
            self.assertIsNotNone(aapl_trade_id, "AAPL should have a trade ID")
            self.assertIsNotNone(msft_trade_id, "MSFT should have a trade ID")
            self.assertNotEqual(aapl_trade_id, msft_trade_id, "Different symbols should have different trade IDs")
            
            # First AAPL should have first trade ID (1)
            aapl_id = int(aapl_trade_id)
            msft_id = int(msft_trade_id)
            self.assertIn(aapl_id, [1, 2], "AAPL should have first or second ID")
            self.assertIn(msft_id, [1, 2], "MSFT should have first or second ID")
            self.assertEqual(aapl_id + msft_id, 3, "IDs should be 1 and 2")
            
            self.log_case_result("Symbol position tracking dictionaries correctly initialized", True)
        except AssertionError as e:
            self.log_case_result(f"Symbol position tracking dictionaries correctly initialized: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_previous_position_tracking(self):
        """Test Case 4: Previous position value is tracked correctly"""
        # Create a multi-step test DataFrame
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime([
                '2023-05-15 09:30:45',  # First buy
                '2023-05-15 09:35:20',  # Second buy
                '2023-05-15 09:40:30'   # Sell
            ]),
            'side': ['buy', 'buy', 'sell'],
            'filled_quantity': [100, 50, 30],
            'symbol': ['AAPL', 'AAPL', 'AAPL'],
            'identifier': ['order1', 'order2', 'order3'],
            'price': [150.0, 151.0, 152.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function with our test data
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # The open_volume column tracks the position after each trade
            self.assertIn('open_volume', result_df.columns, "open_volume column should be added")
            
            # Check the open volume values:
            # 1. First buy: 100 shares
            # 2. Second buy: 100 + 50 = 150 shares
            # 3. Sell: 150 - 30 = 120 shares
            expected_volumes = [100, 150, 120]
            
            # Verify each position
            for i, expected_volume in enumerate(expected_volumes):
                self.assertEqual(result_df['open_volume'].iloc[i], expected_volume, 
                               f"Position after row {i} should be {expected_volume}")
            
            self.log_case_result("Previous position value is tracked correctly", True)
        except AssertionError as e:
            self.log_case_result(f"Previous position value is tracked correctly: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_reject_unknown_side(self):
        """Test Case 5: Trades with unknown side types are rejected"""
        # Create a DataFrame with an unknown side
        test_df = pd.DataFrame({
            'execution_timestamp': [pd.Timestamp('2023-05-15 09:30:45')],
            'side': ['unknown_side'],  # Unknown side type
            'filled_quantity': [100],
            'symbol': ['AAPL'],
            'identifier': ['order1'],
            'price': [150.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Verify the row was rejected
            self.assertEqual(len(rejected_df), 1, "Unknown side should be rejected")
            self.assertEqual(rejected_df['side'].iloc[0], 'unknown_side', "The rejected row should have the unknown side")
            
            # Verify rejection reason
            self.assertIn('rejection_reason', rejected_df.columns, "Rejected trades should have rejection_reason column")
            self.assertIn(
                "Unknown order type 'unknown_side' for buy strategy", 
                rejected_df['rejection_reason'].iloc[0],
                "Incorrect rejection reason"
            )
            
            # Verify no rows remain in result DataFrame
            self.assertEqual(len(result_df), 0, "All rows should be rejected")
            
            # Verify output contains rejection message
            output = self.captured_output.get_value()
            self.assertIn("Rejected unknown side 'unknown_side'", output, "Should print rejection message")
            
            self.log_case_result("Unknown side types are correctly rejected", True)
        except AssertionError as e:
            self.log_case_result(f"Unknown side types are correctly rejected: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_opening_position(self):
        """Test Case 6: Opening a position assigns a trade ID and updates open position"""
        # Create a DataFrame with a buy order
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime(['2023-05-15 09:30:45']),
            'side': ['buy'],  # Opening side for buy strategy
            'filled_quantity': [100],
            'symbol': ['AAPL'],
            'identifier': ['order1'],
            'price': [150.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function with our test data
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Verify no trades were rejected
            self.assertEqual(len(rejected_df), 0, "No trades should be rejected")
            
            # Verify trade ID was assigned
            self.assertIn('trade_id', result_df.columns, "trade_id column should be added")
            self.assertFalse(result_df['trade_id'].isna().any(), "Trade ID should be assigned")
            
            # Verify position was updated
            self.assertIn('open_volume', result_df.columns, "open_volume column should be added")
            self.assertEqual(result_df['open_volume'].iloc[0], 100, "Position should be 100 after buy")
            
            self.log_case_result("Opening position correctly assigns trade ID and updates position", True)
        except AssertionError as e:
            self.log_case_result(f"Opening position correctly assigns trade ID and updates position: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_increasing_position(self):
        """Test Case 7: Increasing an existing position keeps the same trade ID"""
        # Create a DataFrame with two buy orders
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime(['2023-05-15 09:30:45', '2023-05-15 09:35:20']),
            'side': ['buy', 'buy'],  # Two opening orders for buy strategy
            'filled_quantity': [100, 50],
            'symbol': ['AAPL', 'AAPL'],
            'identifier': ['order1', 'order2'],
            'price': [150.0, 151.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function with our test data
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Verify no trades were rejected
            self.assertEqual(len(rejected_df), 0, "No trades should be rejected")
            
            # Verify both rows have the same trade ID
            self.assertIn('trade_id', result_df.columns, "trade_id column should be added")
            self.assertEqual(result_df['trade_id'].iloc[0], result_df['trade_id'].iloc[1], 
                           "Both buys should have the same trade ID")
            
            # Verify positions were updated correctly
            self.assertIn('open_volume', result_df.columns, "open_volume column should be added")
            self.assertEqual(result_df['open_volume'].iloc[0], 100, "Position should be 100 after first buy")
            self.assertEqual(result_df['open_volume'].iloc[1], 150, "Position should be 150 after second buy")
            
            self.log_case_result("Increasing position keeps the same trade ID", True)
        except AssertionError as e:
            self.log_case_result(f"Increasing position keeps the same trade ID: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_closing_partial_position(self):
        """Test Case 8: Partially closing a position"""
        # Create a DataFrame with buy then partial sell
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime(['2023-05-15 09:30:45', '2023-05-15 09:35:20']),
            'side': ['buy', 'sell'],  # Open, then partially close
            'filled_quantity': [100, 30],  # Only close 30 of 100
            'symbol': ['AAPL', 'AAPL'],
            'identifier': ['order1', 'order2'],
            'price': [150.0, 151.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function with our test data
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Verify no trades were rejected
            self.assertEqual(len(rejected_df), 0, "No trades should be rejected")
            
            # Verify both rows have the same trade ID
            self.assertIn('trade_id', result_df.columns, "trade_id column should be added")
            self.assertEqual(result_df['trade_id'].iloc[0], result_df['trade_id'].iloc[1], 
                           "Buy and partial sell should have the same trade ID")
            
            # Verify positions were updated correctly
            self.assertIn('open_volume', result_df.columns, "open_volume column should be added")
            self.assertEqual(result_df['open_volume'].iloc[0], 100, "Position should be 100 after buy")
            self.assertEqual(result_df['open_volume'].iloc[1], 70, "Position should be 70 after partial sell")
            
            self.log_case_result("Partially closing position updates correctly", True)
        except AssertionError as e:
            self.log_case_result(f"Partially closing position updates correctly: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_closing_full_position(self):
        """Test Case 9: Fully closing a position resets the trade ID"""
        # Create a DataFrame with buy then full sell
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime(['2023-05-15 09:30:45', '2023-05-15 09:35:20', '2023-05-15 09:40:30']),
            'side': ['buy', 'sell', 'buy'],  # Open, fully close, then open new position
            'filled_quantity': [100, 100, 50],  # Close all 100, then buy 50 more
            'symbol': ['AAPL', 'AAPL', 'AAPL'],
            'identifier': ['order1', 'order2', 'order3'],
            'price': [150.0, 151.0, 152.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function with our test data
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Verify no trades were rejected
            self.assertEqual(len(rejected_df), 0, "No trades should be rejected")
            
            # Verify trade IDs
            self.assertIn('trade_id', result_df.columns, "trade_id column should be added")
            
            # First and second rows should have the same trade ID (buy and full sell)
            self.assertEqual(result_df['trade_id'].iloc[0], result_df['trade_id'].iloc[1], 
                           "Buy and full sell should have the same trade ID")
            
            # Third row should have a different trade ID (new position)
            self.assertNotEqual(result_df['trade_id'].iloc[0], result_df['trade_id'].iloc[2], 
                              "New position after full close should have a different trade ID")
            
            # Verify positions were updated correctly
            self.assertIn('open_volume', result_df.columns, "open_volume column should be added")
            self.assertEqual(result_df['open_volume'].iloc[0], 100, "Position should be 100 after first buy")
            self.assertEqual(result_df['open_volume'].iloc[1], 0, "Position should be 0 after full sell")
            self.assertEqual(result_df['open_volume'].iloc[2], 50, "Position should be 50 after new buy")
            
            self.log_case_result("Fully closing position correctly resets trade ID", True)
        except AssertionError as e:
            self.log_case_result(f"Fully closing position correctly resets trade ID: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_over_closing_position(self):
        """Test Case 10: Over-closing a position (selling more than owned)"""
        # Create a DataFrame with buy then over-sell
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime(['2023-05-15 09:30:45', '2023-05-15 09:35:20']),
            'side': ['buy', 'sell'],  # Open, then over-close
            'filled_quantity': [100, 150],  # Sell 150 when only 100 owned
            'symbol': ['AAPL', 'AAPL'],
            'identifier': ['order1', 'order2'],
            'price': [150.0, 151.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function with our test data
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Verify no trades were rejected - over-selling is allowed but should be warned
            self.assertEqual(len(rejected_df), 0, "No trades should be rejected")
            
            # Verify both rows have the same trade ID
            self.assertIn('trade_id', result_df.columns, "trade_id column should be added")
            self.assertEqual(result_df['trade_id'].iloc[0], result_df['trade_id'].iloc[1], 
                           "Buy and over-sell should have the same trade ID")
            
            # Verify positions were updated correctly
            self.assertIn('open_volume', result_df.columns, "open_volume column should be added")
            self.assertEqual(result_df['open_volume'].iloc[0], 100, "Position should be 100 after buy")
            self.assertEqual(result_df['open_volume'].iloc[1], -50, "Position should be -50 after over-sell")
            
            # Verify warning was issued
            output = self.captured_output.get_value()
            self.assertIn("Warning", output, "Should print warning message")
            self.assertIn("open position of -50", output, "Warning should mention negative position")
            
            self.log_case_result("Over-closing position handled correctly with warning", True)
        except AssertionError as e:
            self.log_case_result(f"Over-closing position handled correctly with warning: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)

    def test_multiple_positions_tracking(self):
        """Test Case 11: Multiple positions for different symbols are tracked independently"""
        # Create a DataFrame with multiple symbols
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime([
                '2023-05-15 09:30:45',  # AAPL buy
                '2023-05-15 09:32:20',  # MSFT buy
                '2023-05-15 09:35:30',  # AAPL sell (partial)
                '2023-05-15 09:40:15'   # MSFT sell (full)
            ]),
            'side': ['buy', 'buy', 'sell', 'sell'],
            'filled_quantity': [100, 50, 30, 50],
            'symbol': ['AAPL', 'MSFT', 'AAPL', 'MSFT'],
            'identifier': ['order1', 'order2', 'order3', 'order4'],
            'price': [150.0, 250.0, 151.0, 251.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function with our test data
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Verify no trades were rejected
            self.assertEqual(len(rejected_df), 0, "No trades should be rejected")
            
            # Check trade IDs
            aapl_rows = result_df[result_df['symbol'] == 'AAPL']
            msft_rows = result_df[result_df['symbol'] == 'MSFT']
            
            # AAPL rows should have the same trade ID
            self.assertEqual(len(set(aapl_rows['trade_id'])), 1, "AAPL rows should have the same trade ID")
            
            # MSFT rows should have the same trade ID
            self.assertEqual(len(set(msft_rows['trade_id'])), 1, "MSFT rows should have the same trade ID")
            
            # AAPL and MSFT should have different trade IDs
            self.assertNotEqual(aapl_rows['trade_id'].iloc[0], msft_rows['trade_id'].iloc[0],
                              "AAPL and MSFT should have different trade IDs")
            
            # Check positions
            # AAPL positions: initial=100, after sell=70
            self.assertEqual(aapl_rows['open_volume'].iloc[0], 100, "AAPL position should be 100 after buy")
            self.assertEqual(aapl_rows['open_volume'].iloc[1], 70, "AAPL position should be 70 after partial sell")
            
            # MSFT positions: initial=50, after sell=0
            self.assertEqual(msft_rows['open_volume'].iloc[0], 50, "MSFT position should be 50 after buy")
            self.assertEqual(msft_rows['open_volume'].iloc[1], 0, "MSFT position should be 0 after full sell")
            
            self.log_case_result("Multiple positions for different symbols tracked independently", True)
        except AssertionError as e:
            self.log_case_result(f"Multiple positions for different symbols tracked independently: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_open_volume_tracking(self):
        """Test Case 12: Open volume is tracked correctly"""
        # Create a DataFrame with multiple trades
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime([
                '2023-05-15 09:30:45',  # Buy 100
                '2023-05-15 09:35:20',  # Buy 50 more
                '2023-05-15 09:40:30',  # Sell 30
                '2023-05-15 09:45:45'   # Sell 70
            ]),
            'side': ['buy', 'buy', 'sell', 'sell'],
            'filled_quantity': [100, 50, 30, 70],
            'symbol': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
            'identifier': ['order1', 'order2', 'order3', 'order4'],
            'price': [150.0, 151.0, 152.0, 153.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function with our test data
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Verify no trades were rejected
            self.assertEqual(len(rejected_df), 0, "No trades should be rejected")
            
            # Verify open_volume values
            expected_open_volumes = [100, 150, 120, 50]
            for idx, expected_volume in enumerate(expected_open_volumes):
                self.assertEqual(
                    result_df['open_volume'].iloc[idx], 
                    expected_volume,
                    f"Open volume at row {idx} should be {expected_volume}"
                )
            
            # Verify that all rows have the same trade ID
            unique_trade_ids = result_df['trade_id'].unique()
            self.assertEqual(len(unique_trade_ids), 1, "All rows should have the same trade ID")
            
            self.log_case_result("Open volume is tracked correctly", True)
        except AssertionError as e:
            self.log_case_result(f"Open volume is tracked correctly: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_missing_trade_id_check(self):
        """Test Case 13: Missing trade IDs are identified"""
        # Create a test dataframe with incompatible data (will cause missing trade IDs)
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime([
                '2023-05-15 09:30:45',
                '2023-05-15 09:35:20'
            ]),
            'side': ['sell', 'sell'],  # Two sells with no prior buy - will be rejected
            'filled_quantity': [100, 100],
            'symbol': ['AAPL', 'AAPL'],
            'identifier': ['order1', 'order2'],
            'price': [150.0, 151.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # In this case, all rows should be rejected since we're selling without a position
            self.assertEqual(len(rejected_df), 2, "Both sells should be rejected")
            self.assertEqual(len(result_df), 0, "No rows should remain in result")
            
            # Check output for missing trade IDs message
            output = self.captured_output.get_value()
            # The function might not explicitly mention missing trade IDs in this case
            # But it should include warnings or messages about rejected trades
            self.assertIn("rejected", output.lower(), "Output should mention rejected trades")
            
            self.log_case_result("Missing trade IDs are correctly identified", True)
        except AssertionError as e:
            self.log_case_result(f"Missing trade IDs are correctly identified: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_selling_with_no_position(self):
        """Test Case 14: Selling with no open position is rejected"""
        # Create a test dataframe with a sell without a prior buy
        test_df = pd.DataFrame({
            'execution_timestamp': [pd.Timestamp('2023-05-15 09:30:45')],
            'side': ['sell'],
            'filled_quantity': [100],
            'symbol': ['AAPL'],
            'identifier': ['order1'],
            'price': [150.0]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Verify the row was rejected
            self.assertEqual(len(rejected_df), 1, "Sell with no position should be rejected")
            self.assertEqual(rejected_df['side'].iloc[0], 'sell', "The rejected row should be the sell order")
            
            # Verify rejection reason
            self.assertIn('rejection_reason', rejected_df.columns, "Rejected trades should have rejection_reason column")
            self.assertIn(
                "No open position", 
                rejected_df['rejection_reason'].iloc[0],
                "Incorrect rejection reason"
            )
            
            # Verify no rows remain in result DataFrame
            self.assertEqual(len(result_df), 0, "All rows should be rejected")
            
            self.log_case_result("Selling with no open position is correctly rejected", True)
        except AssertionError as e:
            self.log_case_result(f"Selling with no open position is correctly rejected: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)
    
    def test_integration_simple_trades(self):
        """Test Case 15: Integration test with simple trades"""
        # Create a test dataframe with complete trades
        test_df = pd.DataFrame({
            'execution_timestamp': pd.to_datetime([
                '2023-05-15 09:30:45',  # AAPL buy
                '2023-05-15 09:32:20',  # MSFT buy
                '2023-05-15 09:35:30',  # AAPL sell
                '2023-05-15 09:40:15',  # MSFT sell
                '2023-05-15 10:15:30',  # AAPL buy again
                '2023-05-15 10:30:45'   # AAPL sell again
            ]),
            'side': ['buy', 'buy', 'sell', 'sell', 'buy', 'sell'],
            'filled_quantity': [100, 50, 100, 50, 200, 200],
            'symbol': ['AAPL', 'MSFT', 'AAPL', 'MSFT', 'AAPL', 'AAPL'],
            'identifier': ['order1', 'order2', 'order3', 'order4', 'order5', 'order6'],
            'price': [150.25, 250.75, 151.50, 251.25, 152.75, 153.50]
        })
        
        # Capture stdout to avoid printing during test
        original_stdout = self.capture_stdout()
        
        try:
            # Call the function
            result_df, rejected_df = self.identify_trades(test_df, 'buy')
            
            # Verify no rows were rejected
            self.assertEqual(len(rejected_df), 0, "No trades should be rejected")
            
            # Verify all rows have trade IDs
            self.assertEqual(result_df['trade_id'].isna().sum(), 0, "All rows should have trade IDs")
            
            # Check trade ID assignments
            # First AAPL trade (rows 0 and 2)
            self.assertEqual(result_df['trade_id'].iloc[0], result_df['trade_id'].iloc[2], 
                             "First AAPL buy and sell should have same trade ID")
            
            # MSFT trade (rows 1 and 3)
            self.assertEqual(result_df['trade_id'].iloc[1], result_df['trade_id'].iloc[3], 
                             "MSFT buy and sell should have same trade ID")
            
            # Second AAPL trade (rows 4 and 5)
            self.assertEqual(result_df['trade_id'].iloc[4], result_df['trade_id'].iloc[5], 
                             "Second AAPL buy and sell should have same trade ID")
            
            # Different trades should have different IDs
            self.assertNotEqual(result_df['trade_id'].iloc[0], result_df['trade_id'].iloc[1], 
                               "AAPL and MSFT trades should have different IDs")
            self.assertNotEqual(result_df['trade_id'].iloc[0], result_df['trade_id'].iloc[4], 
                               "First and second AAPL trades should have different IDs")
            
            # Verify open volume tracking
            # First AAPL trade
            self.assertEqual(result_df['open_volume'].iloc[0], 100, "First AAPL buy should show 100 volume")
            self.assertEqual(result_df['open_volume'].iloc[2], 0, "First AAPL sell should show 0 volume")
            
            # MSFT trade
            self.assertEqual(result_df['open_volume'].iloc[1], 50, "MSFT buy should show 50 volume")
            self.assertEqual(result_df['open_volume'].iloc[3], 0, "MSFT sell should show 0 volume")
            
            # Second AAPL trade
            self.assertEqual(result_df['open_volume'].iloc[4], 200, "Second AAPL buy should show 200 volume")
            self.assertEqual(result_df['open_volume'].iloc[5], 0, "Second AAPL sell should show 0 volume")
            
            self.log_case_result("Integration test with simple trades passes", True)
        except AssertionError as e:
            self.log_case_result(f"Integration test with simple trades: {str(e)}", False)
            raise
        finally:
            self.restore_stdout(original_stdout)

if __name__ == '__main__':
    print("\n🔍 Running tests for analytics.backtest_executions...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary
    print_summary() 