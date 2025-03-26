"""
Test cases for analytics/process_trades.py module.
Focusing on the process_trades function.
"""
import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

# Import our test utilities
from tests import BaseTestCase, print_summary, MockDatabaseConnection

# Import the function to test
from analytics.process_trades import process_trades, TradeProcessor

# Module specific test fixtures
def create_test_executions_df():
    """Create a sample executions DataFrame for testing"""
    return pd.DataFrame({
        'trade_id': [1, 1, 2, 2, 3],
        'symbol': ['AAPL', 'AAPL', 'TSLA', 'TSLA', 'MSFT'],
        'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'],
        'time_of_day': ['09:30:00', '10:30:00', '11:30:00', '12:30:00', '13:30:00'],
        'is_entry': [1, 0, 1, 0, 1],
        'is_exit': [0, 1, 0, 1, 0],
        'quantity': [100, -100, -50, 50, 200],
        'execution_timestamp': pd.to_datetime(['2023-01-01 09:30:00', '2023-01-02 10:30:00', 
                                              '2023-01-03 11:30:00', '2023-01-04 12:30:00',
                                              '2023-01-05 13:30:00']),
        'price': [150.0, 155.0, 200.0, 190.0, 300.0]
    })

def create_module_fixtures():
    """Create test fixtures specific to process_trades tests"""
    fixtures = {}
    
    # Valid executions DataFrame
    fixtures['valid_executions_df'] = create_test_executions_df()
    
    # Empty DataFrame
    fixtures['empty_df'] = pd.DataFrame()
    
    # DataFrame with missing columns
    missing_cols_df = create_test_executions_df().drop(columns=['is_entry', 'is_exit'])
    fixtures['missing_cols_df'] = missing_cols_df
    
    return fixtures

class TestProcessTradesImports(BaseTestCase):
    """Test basic imports and module setup"""
    
    def test_imports(self):
        """Test that imports are working correctly"""
        # Case 1: Check that functions are callable
        try:
            self.assertTrue(callable(process_trades))
            self.log_case_result("process_trades function is callable", True)
        except AssertionError:
            self.log_case_result("process_trades function is callable", False)
            raise

class TestProcessTrades(BaseTestCase):
    """Test cases for process_trades function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
    
    @patch('analytics.process_trades.TradeProcessor.process_trades')
    def test_process_trades_calls_trade_processor(self, mock_process_trades):
        """Test that process_trades calls TradeProcessor.process_trades"""
        # Setup the mock
        mock_process_trades.return_value = pd.DataFrame({'result': [1, 2, 3]})
        
        # Call the function
        result = process_trades(self.fixtures['valid_executions_df'])
        
        # Verify TradeProcessor was initialized with our DataFrame
        mock_process_trades.assert_called_once()
        
        # Verify the result is what was returned by the TradeProcessor
        self.assertIsNotNone(result)
        pd.testing.assert_frame_equal(result, pd.DataFrame({'result': [1, 2, 3]}))
        
        self.log_case_result("process_trades correctly uses TradeProcessor", True)
    
    def test_process_trades_with_empty_df(self):
        """Test process_trades with an empty DataFrame"""
        # Call function with empty DataFrame
        result = process_trades(self.fixtures['empty_df'])
        
        # Verify result is None for empty DataFrame
        self.assertIsNone(result)
        
        self.log_case_result("process_trades returns None for empty DataFrame", True)
    
    def test_process_trades_with_missing_columns(self):
        """Test process_trades with DataFrame missing required columns"""
        # Call function with DataFrame missing required columns
        result = process_trades(self.fixtures['missing_cols_df'])
        
        # Verify result is None when required columns are missing
        self.assertIsNone(result)
        
        self.log_case_result("process_trades returns None when required columns missing", True)
    
    @patch('analytics.process_trades.TradeProcessor.process_trades')
    def test_process_trades_error_handling(self, mock_process_trades):
        """Test error handling in process_trades"""
        # Setup mock to raise an exception
        mock_process_trades.side_effect = Exception("Test exception")
        
        # Call the function
        result = process_trades(self.fixtures['valid_executions_df'])
        
        # Verify result is None when an exception occurs
        self.assertIsNone(result)
        
        self.log_case_result("process_trades handles errors correctly", True)

class TestTakeProfitPrice(BaseTestCase):
    """Test cases for TradeProcessor._get_take_profit_price method"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        
        # Create a sample executions DataFrame with both bullish and bearish trades
        executions_df = pd.DataFrame({
            'trade_id': [1, 1, 2, 2, 3],
            'symbol': ['AAPL', 'AAPL', 'TSLA', 'TSLA', 'MSFT'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'],
            'time_of_day': ['09:30:00', '10:30:00', '11:30:00', '12:30:00', '13:30:00'],
            'is_entry': [1, 0, 1, 0, 1],
            'is_exit': [0, 1, 0, 1, 0],
            'quantity': [100, -100, -50, 50, 200],  # Trade 1: bullish, Trade 2: bearish, Trade 3: bullish
            'execution_timestamp': pd.to_datetime(['2023-01-01 09:30:00', '2023-01-02 10:30:00', 
                                                  '2023-01-03 11:30:00', '2023-01-04 12:30:00',
                                                  '2023-01-05 13:30:00']),
            'price': [150.0, 155.0, 200.0, 190.0, 300.0]
        })
        
        # Initialize TradeProcessor
        self.processor = TradeProcessor(executions_df)
        
        # Setup our processor for testing
        self.processor.validate()
        self.processor.preprocess()
        
        # For mocking the dependencies
        self.entry_prices = {1: 150.0, 2: 200.0, 3: 300.0}
        self.risk_amounts = {1: 3.0, 2: 4.0, 3: 6.0}  # $3, $4, $6 risk per share
    
    def test_take_profit_with_none_risk_reward_goal(self):
        """Test take profit price calculation when risk_reward_goal is None"""
        # When risk_reward_goal is None, all take profit prices should be None
        result = self.processor._get_take_profit_price(risk_reward_goal=None)
        
        # Verify results
        self.assertIsInstance(result, pd.Series)
        for trade_id in [1, 2, 3]:
            self.assertIsNone(result.get(trade_id))
        
        self.log_case_result("Returns None for all trades when risk_reward_goal is None", True)
    
    @patch('analytics.process_trades.TradeProcessor._get_quantity_and_entry_price')
    @patch('analytics.process_trades.TradeProcessor._get_risk_amount_per_share')
    def test_take_profit_for_bullish_trade(self, mock_risk_amount, mock_entry_price):
        """Test take profit price calculation for bullish trades"""
        # Setup mocks
        mock_entry_price.return_value = (None, pd.Series(self.entry_prices), None)
        mock_risk_amount.return_value = pd.Series(self.risk_amounts)
        
        # Calculate take profit prices with risk-reward ratio of 2.0
        result = self.processor._get_take_profit_price(risk_reward_goal=2.0)
        
        # Verify results for bullish trades (trade_id 1 and 3)
        # Bullish TP = entry_price + (risk_amount * risk_reward_goal)
        expected_tp_1 = 150.0 + (3.0 * 2.0)  # 156.0
        expected_tp_3 = 300.0 + (6.0 * 2.0)  # 312.0
        
        self.assertAlmostEqual(result.get(1), expected_tp_1)
        self.assertAlmostEqual(result.get(3), expected_tp_3)
        
        self.log_case_result("Correctly calculates take profit for bullish trades", True)
    
    @patch('analytics.process_trades.TradeProcessor._get_quantity_and_entry_price')
    @patch('analytics.process_trades.TradeProcessor._get_risk_amount_per_share')
    def test_take_profit_for_bearish_trade(self, mock_risk_amount, mock_entry_price):
        """Test take profit price calculation for bearish trades"""
        # Setup mocks
        mock_entry_price.return_value = (None, pd.Series(self.entry_prices), None)
        mock_risk_amount.return_value = pd.Series(self.risk_amounts)
        
        # Calculate take profit prices with risk-reward ratio of 2.0
        result = self.processor._get_take_profit_price(risk_reward_goal=2.0)
        
        # Verify results for bearish trade (trade_id 2)
        # Bearish TP = entry_price - (risk_amount * risk_reward_goal)
        expected_tp_2 = 200.0 - (4.0 * 2.0)  # 192.0
        
        self.assertAlmostEqual(result.get(2), expected_tp_2)
        
        self.log_case_result("Correctly calculates take profit for bearish trades", True)
    
    @patch('analytics.process_trades.TradeProcessor._get_quantity_and_entry_price')
    @patch('analytics.process_trades.TradeProcessor._get_risk_amount_per_share')
    def test_take_profit_with_missing_entry_price(self, mock_risk_amount, mock_entry_price):
        """Test take profit calculation with missing entry price"""
        # Setup mocks with trade_id 2 missing entry price
        entry_prices_with_missing = {1: 150.0, 2: None, 3: 300.0}
        mock_entry_price.return_value = (None, pd.Series(entry_prices_with_missing), None)
        mock_risk_amount.return_value = pd.Series(self.risk_amounts)
        
        # Calculate take profit prices
        result = self.processor._get_take_profit_price(risk_reward_goal=2.0)
        
        # Verify results - missing entry price should result in NaN
        self.assertTrue(pd.isna(result.get(2)))
        
        self.log_case_result("Correctly handles missing entry prices", True)
    
    @patch('analytics.process_trades.TradeProcessor._get_quantity_and_entry_price')
    @patch('analytics.process_trades.TradeProcessor._get_risk_amount_per_share')
    def test_take_profit_with_missing_risk_amount(self, mock_risk_amount, mock_entry_price):
        """Test take profit calculation with missing risk amount per share"""
        # Setup mocks with trade_id 3 missing risk amount
        risk_amounts_with_missing = {1: 3.0, 2: 4.0, 3: None}
        mock_entry_price.return_value = (None, pd.Series(self.entry_prices), None)
        mock_risk_amount.return_value = pd.Series(risk_amounts_with_missing)
        
        # Calculate take profit prices
        result = self.processor._get_take_profit_price(risk_reward_goal=2.0)
        
        # Verify results - missing risk amount should result in NaN
        self.assertTrue(pd.isna(result.get(3)))
        
        self.log_case_result("Correctly handles missing risk amounts", True)
    
    @patch('analytics.process_trades.TradeProcessor._get_quantity_and_entry_price')
    @patch('analytics.process_trades.TradeProcessor._get_risk_amount_per_share')
    def test_take_profit_with_extreme_values(self, mock_risk_amount, mock_entry_price):
        """Test take profit calculation with extreme risk-reward values"""
        # Setup mocks
        mock_entry_price.return_value = (None, pd.Series(self.entry_prices), None)
        mock_risk_amount.return_value = pd.Series(self.risk_amounts)
        
        # Calculate take profit prices with very large risk-reward ratio
        large_rr = 10.0
        result = self.processor._get_take_profit_price(risk_reward_goal=large_rr)
        
        # Expected take profit for bullish trade 1: entry + (risk * large_rr)
        expected_tp_1 = 150.0 + (3.0 * large_rr)  # 180.0
        
        # Expected take profit for bearish trade 2: entry - (risk * large_rr)
        expected_tp_2 = 200.0 - (4.0 * large_rr)  # 160.0
        
        self.assertAlmostEqual(result.get(1), expected_tp_1)
        self.assertAlmostEqual(result.get(2), expected_tp_2)
        
        self.log_case_result("Correctly handles extreme risk-reward values", True)

if __name__ == '__main__':
    print("\n🔍 Running tests for process_trades function...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary
    print_summary() 