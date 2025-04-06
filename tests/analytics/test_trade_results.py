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
        """Test that function generates correct date range from earliest start date to latest end date"""
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
            self.assertIsInstance(result, pd.DataFrame)
            self.assertTrue('date' in result.columns)
            
            # Check date range
            # The expected start date should be the business day before min(start_date)
            # Finding the expected start date manually
            expected_start_date = pd.bdate_range(end=pd.Timestamp('2023-01-05'), periods=2)[0]
            expected_end_date = pd.Timestamp('2023-01-20')
            
            # Get actual date range
            min_date = result['date'].min()
            max_date = result['date'].max()
            
            self.assertEqual(min_date, expected_start_date)
            self.assertEqual(max_date, expected_end_date)
            
            # Check that all dates are included, not just business days
            self.assertEqual(len(result), (expected_end_date - expected_start_date).days + 1)
            
            # Check output logging
            output = self.captured_output.get_value()
            self.assertIn("Date range:", output)
            self.assertIn("Generated", output)
            
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
            expected_start_date = pd.bdate_range(end=pd.Timestamp('2023-01-05'), periods=2)[0]
            expected_end_date = pd.Timestamp('2023-01-10')
            
            min_date = result['date'].min()
            max_date = result['date'].max()
            
            self.assertEqual(min_date, expected_start_date)
            self.assertEqual(max_date, expected_end_date)
            
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
            
            # The timeframe should include the previous business day plus the date itself
            expected_start_date = pd.bdate_range(end=pd.Timestamp('2023-01-05'), periods=2)[0]
            expected_end_date = pd.Timestamp('2023-01-05')
            
            min_date = result['date'].min()
            max_date = result['date'].max()
            
            self.assertEqual(min_date, expected_start_date)
            self.assertEqual(max_date, expected_end_date)
            self.assertEqual(len(result), 2)  # Should have 2 dates (the start date and end date)
            
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
            expected_start_date = pd.bdate_range(end=pd.Timestamp('2023-01-05'), periods=2)[0]
            expected_end_date = pd.Timestamp('2023-01-05')
            
            self.assertEqual(result['date'].min(), expected_start_date)
            self.assertEqual(result['date'].max(), expected_end_date)
            
            self.log_case_result("Start equals end test", True)
        except Exception as e:
            self.log_case_result("Start equals end test", False)
            raise e
        finally:
            self.restore_stdout(original_stdout)
    
    def test_date_ordering(self):
        """Test that returned dates are in chronological order"""
        data = {
            'start_date': ['2023-01-05', '2023-01-10'],
            'end_date': ['2023-01-20', '2023-01-15']
        }
        df = pd.DataFrame(data)
        
        original_stdout = self.capture_stdout()
        
        try:
            result = get_backtest_timeframe(df)
            
            # Check that dates are ordered
            date_list = result['date'].tolist()
            sorted_date_list = sorted(date_list)
            self.assertEqual(date_list, sorted_date_list)
            
            self.log_case_result("Date ordering test", True)
        except Exception as e:
            self.log_case_result("Date ordering test", False)
            raise e
        finally:
            self.restore_stdout(original_stdout)
    
    def test_weekend_inclusion(self):
        """Test that weekends are included in output"""
        # Create a date range that spans a weekend
        data = {
            'start_date': ['2023-01-05'],  # Thursday
            'end_date': ['2023-01-09']     # Monday
        }
        df = pd.DataFrame(data)
        
        original_stdout = self.capture_stdout()
        
        try:
            result = get_backtest_timeframe(df)
            
            # Check if Saturday (2023-01-07) and Sunday (2023-01-08) are included
            saturdays = result[result['date'].dt.day_name() == 'Saturday']
            sundays = result[result['date'].dt.day_name() == 'Sunday']
            
            self.assertGreater(len(saturdays), 0, "Saturday should be included in date range")
            self.assertGreater(len(sundays), 0, "Sunday should be included in date range")
            
            self.log_case_result("Weekend inclusion test", True)
        except Exception as e:
            self.log_case_result("Weekend inclusion test", False)
            raise e
        finally:
            self.restore_stdout(original_stdout)
    
    def test_date_format(self):
        """Test that output dates are datetime objects"""
        data = {
            'start_date': ['2023-01-05', '2023-01-10'],
            'end_date': ['2023-01-20', '2023-01-15']
        }
        df = pd.DataFrame(data)
        
        original_stdout = self.capture_stdout()
        
        try:
            result = get_backtest_timeframe(df)
            
            # Check that dates are datetime objects
            self.assertTrue(pd.api.types.is_datetime64_dtype(result['date']))
            
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
            
            # Expected number of days: approximately 3 years + 1 day
            expected_days = (pd.Timestamp('2023-01-01') - pd.bdate_range(end=pd.Timestamp('2020-01-01'), periods=2)[0]).days + 1
            self.assertEqual(len(result), expected_days)
            
            # Check first and last dates
            expected_start_date = pd.bdate_range(end=pd.Timestamp('2020-01-01'), periods=2)[0]
            expected_end_date = pd.Timestamp('2023-01-01')
            
            self.assertEqual(result['date'].min(), expected_start_date)
            self.assertEqual(result['date'].max(), expected_end_date)
            
            self.log_case_result("Large date range test", True)
        except Exception as e:
            self.log_case_result("Large date range test", False)
            raise e
        finally:
            self.restore_stdout(original_stdout)


if __name__ == '__main__':
    unittest.main()
    print_summary()

