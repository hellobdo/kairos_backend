import unittest
import pandas as pd
import numpy as np
from tests._utils.test_utils import BaseTestCase, print_summary
from analytics.trade_results import calculate_returns_based_on_close_and_open

class TestCalculateReturnsBasedOnCloseAndOpen(BaseTestCase):
    """Test cases for calculate_returns_based_on_close_and_open function"""
    
    def test_daily_returns(self):
        """Test calculation of daily returns"""
        # Capture stdout to suppress function output during testing
        original_stdout = self.capture_stdout()
        
        # Create test data for daily returns
        df = pd.DataFrame({
            'ticker': ['SPY', 'SPY', 'QQQ', 'QQQ'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-02'],
            'open': [100.0, 102.0, 300.0, 305.0],
            'close': [101.0, 104.0, 305.0, 310.0],
            'period': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-02']
        })
        
        # Calculate returns
        result = calculate_returns_based_on_close_and_open(df, 'day')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Check that the result is a DataFrame
        self.assertIsInstance(result, pd.DataFrame)
        
        # Check that perc_return is calculated correctly for each row
        expected_returns = [(101.0 - 100.0) / 100.0, (104.0 - 102.0) / 102.0, 
                            (305.0 - 300.0) / 300.0, (310.0 - 305.0) / 305.0]
        for i, expected in enumerate(expected_returns):
            self.assertAlmostEqual(result.iloc[i]['perc_return'], expected)
            
        # Check that a Total row was added
        self.assertIn('Total', result['period'].values)
        
        # Validate Total calculation (first open to last close for each ticker)
        total_row_spy = result[(result['period'] == 'Total')].iloc[0]
        self.log_case_result('Daily returns calculation', True)
    
    def test_weekly_returns(self):
        """Test calculation of weekly returns"""
        # Capture stdout to suppress function output during testing
        original_stdout = self.capture_stdout()
        
        # Create test data for weekly returns
        df = pd.DataFrame({
            'ticker': ['SPY', 'SPY', 'SPY', 'QQQ', 'QQQ', 'QQQ'],
            'date': ['2023-01-02', '2023-01-03', '2023-01-04', '2023-01-02', '2023-01-03', '2023-01-04'],
            'open': [100.0, 101.0, 102.0, 300.0, 305.0, 310.0],
            'close': [101.0, 102.0, 104.0, 305.0, 310.0, 315.0],
            'period': ['2023-W01', '2023-W01', '2023-W01', '2023-W01', '2023-W01', '2023-W01']
        })
        
        # Calculate returns
        result = calculate_returns_based_on_close_and_open(df, 'week')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Check that the result aggregates by ticker and period
        self.assertEqual(len(result[result['period'] != 'Total']), 2)  # One row per ticker
        
        # Check that first open and last close are used
        spy_row = result[(result['ticker'] == 'SPY') & (result['period'] == '2023-W01')].iloc[0]
        self.assertEqual(spy_row['open'], 100.0)  # First open of the week
        self.assertEqual(spy_row['close'], 104.0)  # Last close of the week
        
        # Check perc_return calculation for the period
        expected_spy_return = (104.0 - 100.0) / 100.0
        self.assertAlmostEqual(spy_row['perc_return'], expected_spy_return)
        
        # Check QQQ row
        qqq_row = result[(result['ticker'] == 'QQQ') & (result['period'] == '2023-W01')].iloc[0]
        expected_qqq_return = (315.0 - 300.0) / 300.0
        self.assertAlmostEqual(qqq_row['perc_return'], expected_qqq_return)
        
        # Check Total rows
        self.assertIn('Total', result['period'].values)
        self.log_case_result('Weekly returns calculation', True)
    
    def test_monthly_returns(self):
        """Test calculation of monthly returns"""
        # Capture stdout to suppress function output during testing
        original_stdout = self.capture_stdout()
        
        # Create test data for monthly returns
        df = pd.DataFrame({
            'ticker': ['SPY', 'SPY', 'SPY', 'QQQ', 'QQQ', 'QQQ'],
            'date': ['2023-01-02', '2023-01-15', '2023-01-30', '2023-01-02', '2023-01-15', '2023-01-30'],
            'open': [100.0, 105.0, 110.0, 300.0, 310.0, 320.0],
            'close': [105.0, 110.0, 115.0, 310.0, 320.0, 330.0],
            'period': ['2023-01', '2023-01', '2023-01', '2023-01', '2023-01', '2023-01']
        })
        
        # Calculate returns
        result = calculate_returns_based_on_close_and_open(df, 'month')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Check that the result aggregates by ticker and period
        self.assertEqual(len(result[result['period'] != 'Total']), 2)  # One row per ticker per month
        
        # Check that first open and last close are used
        spy_row = result[(result['ticker'] == 'SPY') & (result['period'] == '2023-01')].iloc[0]
        self.assertEqual(spy_row['open'], 100.0)  # First open of the month
        self.assertEqual(spy_row['close'], 115.0)  # Last close of the month
        
        # Check perc_return calculation for the period
        expected_spy_return = (115.0 - 100.0) / 100.0
        self.assertAlmostEqual(spy_row['perc_return'], expected_spy_return)
        
        self.log_case_result('Monthly returns calculation', True)
    
    def test_yearly_returns(self):
        """Test calculation of yearly returns"""
        # Capture stdout to suppress function output during testing
        original_stdout = self.capture_stdout()
        
        # Create test data for yearly returns
        df = pd.DataFrame({
            'ticker': ['SPY', 'SPY', 'SPY', 'QQQ', 'QQQ', 'QQQ'],
            'date': ['2023-01-02', '2023-06-15', '2023-12-30', '2023-01-02', '2023-06-15', '2023-12-30'],
            'open': [100.0, 110.0, 120.0, 300.0, 330.0, 360.0],
            'close': [110.0, 120.0, 130.0, 330.0, 360.0, 390.0],
            'period': ['2023', '2023', '2023', '2023', '2023', '2023']
        })
        
        # Calculate returns
        result = calculate_returns_based_on_close_and_open(df, 'year')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Check that the result aggregates by ticker and period
        self.assertEqual(len(result[result['period'] != 'Total']), 2)  # One row per ticker per year
        
        # Check that first open and last close are used
        spy_row = result[(result['ticker'] == 'SPY') & (result['period'] == '2023')].iloc[0]
        self.assertEqual(spy_row['open'], 100.0)  # First open of the year
        self.assertEqual(spy_row['close'], 130.0)  # Last close of the year
        
        # Check perc_return calculation for the period
        expected_spy_return = (130.0 - 100.0) / 100.0
        self.assertAlmostEqual(spy_row['perc_return'], expected_spy_return)
        
        self.log_case_result('Yearly returns calculation', True)
    
    def test_empty_dataframe(self):
        """Test handling of empty dataframe"""
        # Capture stdout to suppress function output during testing
        original_stdout = self.capture_stdout()
        
        # Create empty dataframe with required columns
        df = pd.DataFrame(columns=['ticker', 'date', 'open', 'close', 'period'])
        
        # Calculate returns
        result = calculate_returns_based_on_close_and_open(df, 'day')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Check that the result is a DataFrame
        self.assertIsInstance(result, pd.DataFrame)
        
        # Check that the result is empty
        self.assertEqual(len(result), 0)
        
        self.log_case_result('Empty dataframe handling', True)
    
    def test_missing_columns(self):
        """Test error handling when required columns are missing"""
        # Capture stdout to suppress function output during testing
        original_stdout = self.capture_stdout()
        
        # Create dataframe with missing columns
        df = pd.DataFrame({
            'ticker': ['SPY', 'QQQ'],
            'date': ['2023-01-01', '2023-01-01']
            # Missing 'open' and 'close'
        })
        
        # Check that function raises an error
        with self.assertRaises(Exception):
            calculate_returns_based_on_close_and_open(df, 'day')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        self.log_case_result('Missing columns error handling', True)
    
    def test_total_row_calculation(self):
        """Test that the Total row calculation is correct"""
        # Capture stdout to suppress function output during testing
        original_stdout = self.capture_stdout()
        
        # Create test data with specific values to test Total row
        df = pd.DataFrame({
            'ticker': ['SPY', 'SPY', 'SPY'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'open': [100.0, 105.0, 110.0],
            'close': [105.0, 110.0, 120.0],
            'period': ['2023-01-01', '2023-01-02', '2023-01-03']
        })
        
        # Calculate returns
        result = calculate_returns_based_on_close_and_open(df, 'day')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Check that a Total row exists
        self.assertIn('Total', result['period'].values)
        
        # Check that the Total row calculation uses first open and last close
        total_row = result[result['period'] == 'Total'].iloc[0]
        expected_total_return = (120.0 - 100.0) / 100.0  # (last close - first open) / first open
        self.assertAlmostEqual(total_row['perc_return'], expected_total_return)
        
        self.log_case_result('Total row calculation', True)

def run_tests():
    """Run all test cases and print summary"""
    # Create test suite with all test cases
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCalculateReturnsBasedOnCloseAndOpen)
    
    # Run the tests
    unittest.TextTestRunner(verbosity=0).run(suite)
    
    # Print summary of test results
    print_summary()

if __name__ == '__main__':
    run_tests()
