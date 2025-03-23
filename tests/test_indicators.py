import unittest
import os
import pandas as pd
import numpy as np
from pathlib import Path
import importlib.util
from tests.utils.test_utils import BaseTestCase, print_summary

class TestIndicators(BaseTestCase):
    """Test suite for checking indicator files"""
    
    def setUp(self):
        """Set up test environment"""
        super().setUp()
        self.indicators_dir = Path(__file__).parent.parent.parent / 'indicators'
        self.indicator_files = []
        self.compliant_indicators = []
        self.non_compliant_indicators = []
        
        # Get all .py files in the indicators directory (excluding subdirectories)
        for item in self.indicators_dir.glob('*.py'):
            if item.is_file() and item.name != '__init__.py':
                self.indicator_files.append(item)
    
    def test_indicator_has_is_indicator_column(self):
        """Test that all indicator files return a DataFrame with an is_indicator column containing boolean values"""
        # Create test data
        test_df = pd.DataFrame({
            'open': [100, 101, 102, 103, 104] * 5,  # 25 rows to ensure enough data for indicators
            'high': [105, 106, 107, 108, 109] * 5,
            'low': [95, 96, 97, 98, 99] * 5,
            'close': [102, 103, 104, 105, 106] * 5,
            'volume': [1000, 1100, 1200, 1300, 1400] * 5
        })
        
        for indicator_file in self.indicator_files:
            indicator_name = indicator_file.stem
            
            # Log which file we're testing
            print(f"Testing indicator: {indicator_name}")
            
            # Import the indicator module
            spec = importlib.util.spec_from_file_location(indicator_name, indicator_file)
            indicator = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(indicator)
            
            # Check if the module has a calculate_indicator function
            has_calculate_function = hasattr(indicator, 'calculate_indicator')
            self.assertTrue(has_calculate_function, 
                           f"Indicator {indicator_name} should have a calculate_indicator function")
            
            if has_calculate_function:
                # Calculate the indicator
                try:
                    result_df = indicator.calculate_indicator(test_df)
                    
                    # Check if result is a DataFrame
                    is_dataframe = isinstance(result_df, pd.DataFrame)
                    self.assertTrue(is_dataframe, 
                                  f"Indicator {indicator_name} should return a DataFrame")
                    
                    if is_dataframe:
                        # Check if result has is_indicator column
                        has_indicator_column = 'is_indicator' in result_df.columns
                        self.assertTrue(has_indicator_column, 
                                      f"Indicator {indicator_name} should have an is_indicator column")
                        
                        if has_indicator_column:
                            # Check if is_indicator column contains boolean values
                            is_boolean = result_df['is_indicator'].dtype == bool or all(
                                isinstance(x, bool) for x in result_df['is_indicator'].dropna())
                            
                            self.assertTrue(is_boolean, 
                                          f"Indicator {indicator_name} should have boolean values in is_indicator column")
                            
                            if is_boolean:
                                self.compliant_indicators.append(indicator_name)
                                self.log_case_result(f"{indicator_name} has a boolean is_indicator column", True)
                            else:
                                self.non_compliant_indicators.append(f"{indicator_name} (non-boolean is_indicator)")
                                self.log_case_result(f"{indicator_name} has a boolean is_indicator column", False)
                        else:
                            self.non_compliant_indicators.append(f"{indicator_name} (missing is_indicator)")
                            self.log_case_result(f"{indicator_name} has an is_indicator column", False)
                    else:
                        self.non_compliant_indicators.append(f"{indicator_name} (not returning DataFrame)")
                        self.log_case_result(f"{indicator_name} returns a DataFrame", False)
                except Exception as e:
                    self.non_compliant_indicators.append(f"{indicator_name} (exception: {str(e)})")
                    self.log_case_result(f"{indicator_name} calculates without error", False)
            else:
                self.non_compliant_indicators.append(f"{indicator_name} (missing calculate_indicator function)")
                self.log_case_result(f"{indicator_name} has calculate_indicator function", False)
    
    def tearDown(self):
        """Print summary of compliant and non-compliant indicators"""
        super().tearDown()
        
        print("\nIndicators with proper is_indicator boolean column:")
        for indicator in self.compliant_indicators:
            print(f"✓ {indicator}")
        
        if self.non_compliant_indicators:
            print("\nIndicators that do not comply:")
            for indicator in self.non_compliant_indicators:
                print(f"✗ {indicator}")

if __name__ == '__main__':
    unittest.main(exit=False)
    print_summary() 