import unittest
import pandas as pd
import numpy as np
from tests._utils.test_utils import BaseTestCase, print_summary
from analytics.trade_results import get_backtest_timeframe


class TestGetBacktestTimeframe(BaseTestCase):
    """Tests for the get_backtest_timeframe function"""
    
    @classmethod
    def tearDownClass(cls):
        """Print summary after all tests have completed"""
        print_summary()
    
    def test_basic_functionality(self):
        """Test that function returns correct start and end dates for the backtest timeframe"""
        # Create sample data
        data = {
            'start_date': ['2023-01-05', '2023-01-10'],
            'end_date': ['2023-01-20', '2023-01-15']
        }
        df = pd.DataFrame(data)
        
        # Capture stdout to check logging statements
        original_stdout = self.capture_stdout()
        
        try:
            # Call function
            result = get_backtest_timeframe(df)
            
            # Verify results
            self.assertIsInstance(result, dict)
            self.assertTrue('start_date' in result)
            self.assertTrue('end_date' in result)
            
            # The expected start date should be the business day before min(start_date)
            # Finding the expected start date manually
            expected_start_date = pd.bdate_range(end=pd.Timestamp('2023-01-05'), periods=2)[0].strftime('%Y-%m-%d')
            expected_end_date = pd.Timestamp('2023-01-20').strftime('%Y-%m-%d')
            
            self.assertEqual(result['start_date'], expected_start_date)
            self.assertEqual(result['end_date'], expected_end_date)
            
            # Check output logging
            output = self.captured_output.get_value()
            self.assertIn("Date range:", output)
            
            self.log_case_result("Date range test", True)
        except Exception as e:
            self.log_case_result("Date range test", False)
            raise e
        finally:
            self.restore_stdout(original_stdout)
    
    def test_empty_dataframe(self):
        """Test behavior with empty DataFrame"""
        df = pd.DataFrame(columns=['start_date', 'end_date'])
        
        try:
            # This should raise an exception since there are no dates
            with self.assertRaises(Exception):
                get_backtest_timeframe(df)
            
            self.log_case_result("Empty DataFrame test", True)
        except Exception as e:
            self.log_case_result("Empty DataFrame test", False)
            raise e
    
    def test_missing_end_dates(self):
        """Test when all end dates are NaN"""
        data = {
            'start_date': ['2023-01-05', '2023-01-10'],
            'end_date': [None, None]
        }
        df = pd.DataFrame(data)
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        try:
            result = get_backtest_timeframe(df)
            
            # Verify max date is max of start dates
            expected_start_date = pd.bdate_range(end=pd.Timestamp('2023-01-05'), periods=2)[0].strftime('%Y-%m-%d')
            expected_end_date = pd.Timestamp('2023-01-10').strftime('%Y-%m-%d')
            
            self.assertEqual(result['start_date'], expected_start_date)
            self.assertEqual(result['end_date'], expected_end_date)
            
            # Verify logging message about end dates
            output = self.captured_output.get_value()
            self.log_case_result("Missing end dates test", True)
        except Exception as e:
            self.log_case_result("Missing end dates test", False)
            raise e
        finally:
            self.restore_stdout(original_stdout)
    
    def test_single_date(self):
        """Test with only one date in the input"""
        data = {
            'start_date': ['2023-01-05'],
            'end_date': ['2023-01-05']
        }
        df = pd.DataFrame(data)
        
        original_stdout = self.capture_stdout()
        
        try:
            result = get_backtest_timeframe(df)
            
            # The timeframe should include the previous business day and the date itself
            expected_start_date = pd.bdate_range(end=pd.Timestamp('2023-01-05'), periods=2)[0].strftime('%Y-%m-%d')
            expected_end_date = pd.Timestamp('2023-01-05').strftime('%Y-%m-%d')
            
            self.assertEqual(result['start_date'], expected_start_date)
            self.assertEqual(result['end_date'], expected_end_date)
            
            self.log_case_result("Single date test", True)
        except Exception as e:
            self.log_case_result("Single date test", False)
            raise e
        finally:
            self.restore_stdout(original_stdout)
    
    def test_start_equals_end(self):
        """Test when earliest date equals max date"""
        data = {
            'start_date': ['2023-01-05', '2023-01-05'],
            'end_date': ['2023-01-05', '2023-01-05']
        }
        df = pd.DataFrame(data)
        
        original_stdout = self.capture_stdout()
        
        try:
            result = get_backtest_timeframe(df)
            
            # Should still include previous business day
            expected_start_date = pd.bdate_range(end=pd.Timestamp('2023-01-05'), periods=2)[0].strftime('%Y-%m-%d')
            expected_end_date = pd.Timestamp('2023-01-05').strftime('%Y-%m-%d')
            
            self.assertEqual(result['start_date'], expected_start_date)
            self.assertEqual(result['end_date'], expected_end_date)
            
            self.log_case_result("Start equals end test", True)
        except Exception as e:
            self.log_case_result("Start equals end test", False)
            raise e
        finally:
            self.restore_stdout(original_stdout)
    
    def test_date_format(self):
        """Test that output dates are string objects in the correct format"""
        data = {
            'start_date': ['2023-01-05', '2023-01-10'],
            'end_date': ['2023-01-20', '2023-01-15']
        }
        df = pd.DataFrame(data)
        
        original_stdout = self.capture_stdout()
        
        try:
            result = get_backtest_timeframe(df)
            
            # Check that dates are strings in YYYY-MM-DD format
            self.assertIsInstance(result['start_date'], str)
            self.assertIsInstance(result['end_date'], str)
            
            # Check the date format
            date_format = r'\d{4}-\d{2}-\d{2}'
            self.assertRegex(result['start_date'], date_format)
            self.assertRegex(result['end_date'], date_format)
            
            self.log_case_result("Date format test", True)
        except Exception as e:
            self.log_case_result("Date format test", False)
            raise e
        finally:
            self.restore_stdout(original_stdout)
    
    def test_large_date_range(self):
        """Test with dates spanning several years"""
        data = {
            'start_date': ['2020-01-01'],
            'end_date': ['2023-01-01']
        }
        df = pd.DataFrame(data)
        
        original_stdout = self.capture_stdout()
        
        try:
            result = get_backtest_timeframe(df)
            
            # Check first and last dates
            expected_start_date = pd.bdate_range(end=pd.Timestamp('2020-01-01'), periods=2)[0].strftime('%Y-%m-%d')
            expected_end_date = pd.Timestamp('2023-01-01').strftime('%Y-%m-%d')
            
            self.assertEqual(result['start_date'], expected_start_date)
            self.assertEqual(result['end_date'], expected_end_date)
            
            self.log_case_result("Large date range test", True)
        except Exception as e:
            self.log_case_result("Large date range test", False)
            raise e
        finally:
            self.restore_stdout(original_stdout)


if __name__ == '__main__':
    unittest.main()
    print_summary()

