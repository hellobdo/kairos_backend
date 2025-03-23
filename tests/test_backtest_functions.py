"""
Tests for helper methods in BaseStrategy class from backtests/utils/backtest_functions.py
"""
import unittest
import sys
import os
from datetime import datetime
import pandas as pd
from unittest.mock import patch, MagicMock

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import our test utilities from the tests package
from tests.utils.test_utils import BaseTestCase, print_summary

# Import the class to test
from backtests.utils.backtest_functions import BaseStrategy
from lumibot.entities import Asset

# Module specific test fixtures
def create_module_fixtures():
    """Create test fixtures specific to this module's tests"""
    fixtures = {}
    
    # Test data for indicators
    fixtures['test_df'] = pd.DataFrame({
        'open': [100, 101, 102, 103, 104] * 5,
        'high': [105, 106, 107, 108, 109] * 5,
        'low': [95, 96, 97, 98, 99] * 5,
        'close': [102, 103, 104, 105, 106] * 5,
        'volume': [1000, 1100, 1200, 1300, 1400] * 5
    })
    
    # Test indicators and load function
    fixtures['indicators'] = ['test_indicator1', 'test_indicator2']
    fixtures['load_function'] = MagicMock()
    fixtures['load_function'].side_effect = lambda name: lambda df: df.assign(is_indicator=True)
    
    # Test time objects
    fixtures['time_0'] = datetime(2023, 1, 1, 10, 0, 0)
    fixtures['time_30'] = datetime(2023, 1, 1, 10, 30, 0)
    fixtures['time_other'] = datetime(2023, 1, 1, 10, 15, 0)
    
    # Test stop loss rules
    fixtures['stop_loss_rules'] = [
        {"price_below": 150, "amount": 0.30},
        {"price_above": 150, "amount": 1.00}
    ]
    
    return fixtures

# Mock class for broker
class MockBroker:
    def __init__(self):
        self.name = "mock_broker"
        
    def get_datetime(self):
        return datetime.now()
        
    def is_backtesting(self):
        return True
        
    def get_last_price(self, *args, **kwargs):
        return 100.0

# Base class for all BaseStrategy tests
class BaseStrategyTestCase(BaseTestCase):
    """Base test case with common setup for BaseStrategy tests"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
        
        # Create a patch for the Strategy.__init__ to avoid broker validation
        self.init_patcher = patch('lumibot.strategies.strategy.Strategy.__init__', return_value=None)
        self.mock_init = self.init_patcher.start()
        
        # Create a BaseStrategy instance
        self.base_strategy = BaseStrategy()
        
        # Mock necessary properties and methods
        # Set broker and other attributes via object.__setattr__ to bypass property restrictions
        object.__setattr__(self.base_strategy, 'broker', MockBroker())
        object.__setattr__(self.base_strategy, 'parameters', {'max_loss_positions': 2, 'risk_per_trade': 0.01})
        
        # Create a vars object with realistic properties
        vars_obj = type('obj', (object,), {
            'daily_loss_count': 0,
            'trade_log': []
        })
        object.__setattr__(self.base_strategy, 'vars', vars_obj)
        
        # Mock methods
        self.base_strategy.get_positions = MagicMock(return_value=[])
        self.base_strategy.get_datetime = MagicMock(return_value=datetime.now())
        self.base_strategy.get_last_price = MagicMock(return_value=100.0)
    
    def tearDown(self):
        """Clean up patchers"""
        super().tearDown()
        self.init_patcher.stop()

class TestIndicatorLoading(BaseStrategyTestCase):
    """Test indicator loading functionality"""
    
    def test_load_indicators_success(self):
        """Test successful loading of indicators"""
        indicators = self.fixtures['indicators']
        load_function = self.fixtures['load_function']
        
        # Call the method
        result = self.base_strategy._load_indicators(indicators, load_function)
        
        # Verify the result is a dictionary
        self.assertTrue(isinstance(result, dict))
        self.log_case_result("_load_indicators returns a dictionary", True)
        
        # Verify all indicators are loaded
        self.assertEqual(len(result), len(indicators))
        self.log_case_result("_load_indicators loads all indicators", True)
        
        # Verify the load function was called for each indicator
        self.assertEqual(load_function.call_count, len(indicators))
        self.log_case_result("load_function is called for each indicator", True)
    
    def test_load_indicators_error(self):
        """Test error handling in indicator loading"""
        indicators = self.fixtures['indicators']
        
        # Create a load function that raises an exception
        def failing_load_function(name):
            if name == 'test_indicator2':
                raise ValueError("Test error")
            return lambda df: df
        
        # Call the function and check it handles the error properly
        result = self.base_strategy._load_indicators(indicators, failing_load_function)
        
        # It should still return a dictionary with the first indicator
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(len(result), 1)  # Only the first indicator loaded
        self.assertTrue('test_indicator1' in result)
        self.assertFalse('test_indicator2' in result)
        self.log_case_result("_load_indicators handles errors gracefully", True)

class TestPositionLimits(BaseStrategyTestCase):
    """Test position limit checking functionality"""
    
    def test_max_positions_reached(self):
        """Test when max positions limit is reached"""
        # Create mock positions
        mock_position1 = MagicMock()
        mock_position1.asset.symbol = 'AAPL'
        mock_position1.asset.asset_type = 'stock'
        
        mock_position2 = MagicMock()
        mock_position2.asset.symbol = 'MSFT'
        mock_position2.asset.asset_type = 'stock'
        
        # Set up the mock to return 2 positions
        self.base_strategy.get_positions.return_value = [mock_position1, mock_position2]
        
        # Test the method
        result = self.base_strategy._check_position_limits()
        
        # Verify it returns True (limit reached)
        self.assertTrue(result)
        self.log_case_result("_check_position_limits returns True when max positions reached", True)
    
    def test_daily_loss_limit_reached(self):
        """Test when daily loss limit is reached"""
        # Set up daily loss count equal to max positions
        self.base_strategy.vars.daily_loss_count = 2
        
        # Set up the mock to return 0 positions
        self.base_strategy.get_positions.return_value = []
        
        # Test the method
        result = self.base_strategy._check_position_limits()
        
        # Verify it returns True (limit reached)
        self.assertTrue(result)
        self.log_case_result("_check_position_limits returns True when daily loss limit reached", True)
    
    def test_limits_not_reached(self):
        """Test when no limits are reached"""
        # Set up daily loss count below limit
        self.base_strategy.vars.daily_loss_count = 1
        
        # Set up the mock to return 1 position
        mock_position = MagicMock()
        mock_position.asset.symbol = 'AAPL'
        mock_position.asset.asset_type = 'stock'
        self.base_strategy.get_positions.return_value = [mock_position]
        
        # Test the method
        result = self.base_strategy._check_position_limits()
        
        # Verify it returns False (limits not reached)
        self.assertFalse(result)
        self.log_case_result("_check_position_limits returns False when limits not reached", True)

class TestTimeConditions(BaseStrategyTestCase):
    """Test time condition checking functionality"""
    
    def test_minute_0(self):
        """Test for minute 0"""
        time = self.fixtures['time_0']
        result = self.base_strategy._check_time_conditions(time)
        self.assertTrue(result)
        self.log_case_result("_check_time_conditions returns True for minute 0", True)
    
    def test_minute_30(self):
        """Test for minute 30"""
        time = self.fixtures['time_30']
        result = self.base_strategy._check_time_conditions(time)
        self.assertTrue(result)
        self.log_case_result("_check_time_conditions returns True for minute 30", True)
    
    def test_other_minute(self):
        """Test for other minutes"""
        time = self.fixtures['time_other']
        result = self.base_strategy._check_time_conditions(time)
        self.assertFalse(result)
        self.log_case_result("_check_time_conditions returns False for other minutes", True)

class TestIndicatorApplication(BaseStrategyTestCase):
    """Test indicator application functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.test_df = self.fixtures['test_df'].copy()
    
    def test_all_indicators_positive(self):
        """Test when all indicators return True"""
        # Create indicator functions that always return True
        calculate_indicators = {
            'ind1': lambda df: df.assign(is_indicator=True),
            'ind2': lambda df: df.assign(is_indicator=True)
        }
        
        # Apply indicators
        result, df = self.base_strategy._apply_indicators(self.test_df, calculate_indicators)
        
        # Verify result is True and is_indicator column is True
        self.assertTrue(result)
        self.assertTrue(df['is_indicator'].iloc[-1])
        self.log_case_result("_apply_indicators returns True when all indicators are positive", True)
    
    def test_one_indicator_negative(self):
        """Test when one indicator returns False"""
        # Create indicator functions where the second returns False
        calculate_indicators = {
            'ind1': lambda df: df.assign(is_indicator=True),
            'ind2': lambda df: df.assign(is_indicator=False)
        }
        
        # Apply indicators
        result, df = self.base_strategy._apply_indicators(self.test_df, calculate_indicators)
        
        # Verify result is False and is_indicator column is False
        self.assertFalse(result)
        self.assertFalse(df['is_indicator'].iloc[-1])
        self.log_case_result("_apply_indicators returns False when any indicator is negative", True)
    
    def test_short_circuit(self):
        """Test that processing stops at first False indicator"""
        # Create a mock for tracking function calls
        mock_first = MagicMock(return_value=self.test_df.assign(is_indicator=False))
        mock_second = MagicMock(return_value=self.test_df.assign(is_indicator=True))
        
        calculate_indicators = {
            'ind1': mock_first,
            'ind2': mock_second
        }
        
        # Apply indicators
        result, df = self.base_strategy._apply_indicators(self.test_df, calculate_indicators)
        
        # Verify first function was called
        mock_first.assert_called_once()
        # Verify second function was not called (short-circuit)
        mock_second.assert_not_called()
        
        self.log_case_result("_apply_indicators short-circuits on first False indicator", True)

class TestQuantityCalculation(BaseStrategyTestCase):
    """Test quantity calculation functionality"""
    
    def test_standard_calculation(self):
        """Test standard quantity calculation"""
        stop_loss_amount = 0.5
        risk_per_trade = 0.01  # 1%
        
        # Calculate expected result: 30000 * 0.01 // 0.5 = 600
        expected = 600
        
        # Call the method
        result = self.base_strategy._calculate_qty(stop_loss_amount, risk_per_trade)
        
        # Verify the result
        self.assertEqual(result, expected)
        self.log_case_result("_calculate_qty returns correct integer value", True)
    
    def test_different_risk_values(self):
        """Test with different risk values"""
        stop_loss_amount = 1.0
        
        # Test with 0.5% risk
        risk_per_trade = 0.005
        expected = 150  # 30000 * 0.005 // 1.0 = 150
        result = self.base_strategy._calculate_qty(stop_loss_amount, risk_per_trade)
        self.assertEqual(result, expected)
        
        # Test with 2% risk
        risk_per_trade = 0.02
        expected = 600  # 30000 * 0.02 // 1.0 = 600
        result = self.base_strategy._calculate_qty(stop_loss_amount, risk_per_trade)
        self.assertEqual(result, expected)
        
        self.log_case_result("_calculate_qty handles different risk_per_trade values", True)

class TestStopLossRules(BaseStrategyTestCase):
    """Test stop loss rule application"""
    
    def test_price_below_rule(self):
        """Test price below rule"""
        price = 140  # Below 150
        expected_amount = 0.30
        
        result = self.base_strategy._determine_stop_loss(price, self.fixtures['stop_loss_rules'])
        
        self.assertEqual(result, expected_amount)
        self.log_case_result("_determine_stop_loss returns correct amount for price below rule", True)
    
    def test_price_above_rule(self):
        """Test price above rule"""
        price = 160  # Above 150
        expected_amount = 1.00
        
        result = self.base_strategy._determine_stop_loss(price, self.fixtures['stop_loss_rules'])
        
        self.assertEqual(result, expected_amount)
        self.log_case_result("_determine_stop_loss returns correct amount for price above rule", True)
    
    def test_no_matching_rule(self):
        """Test when no rules match"""
        # Create rules that won't match
        rules = [
            {"price_below": 100, "amount": 0.30},
            {"price_above": 200, "amount": 1.00}
        ]
        
        price = 150  # Between rules
        
        result = self.base_strategy._determine_stop_loss(price, rules)
        
        self.assertIsNone(result)
        self.log_case_result("_determine_stop_loss returns None when no rules match", True)

class TestPriceLevels(BaseStrategyTestCase):
    """Test price level calculation"""
    
    def test_buy_side(self):
        """Test price levels for buy side"""
        entry_price = 100.0
        stop_loss_amount = 1.0
        side = 'buy'
        risk_reward = 3
        
        expected_stop_loss = 99.0  # entry_price - stop_loss_amount
        expected_take_profit = 103.0  # entry_price + (stop_loss_amount * risk_reward)
        
        stop_loss_price, take_profit_price = self.base_strategy._calculate_price_levels(
            entry_price, stop_loss_amount, side, risk_reward
        )
        
        self.assertEqual(stop_loss_price, expected_stop_loss)
        self.assertEqual(take_profit_price, expected_take_profit)
        self.log_case_result("_calculate_price_levels correct for 'buy' side", True)
    
    def test_sell_side(self):
        """Test price levels for sell side"""
        entry_price = 100.0
        stop_loss_amount = 1.0
        side = 'sell'
        risk_reward = 3
        
        expected_stop_loss = 101.0  # entry_price + stop_loss_amount
        expected_take_profit = 97.0  # entry_price - (stop_loss_amount * risk_reward)
        
        stop_loss_price, take_profit_price = self.base_strategy._calculate_price_levels(
            entry_price, stop_loss_amount, side, risk_reward
        )
        
        self.assertEqual(stop_loss_price, expected_stop_loss)
        self.assertEqual(take_profit_price, expected_take_profit)
        self.log_case_result("_calculate_price_levels correct for 'sell' side", True)

if __name__ == '__main__':
    print("\n🔍 Running tests for BaseStrategy helper methods...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary
    print_summary() 