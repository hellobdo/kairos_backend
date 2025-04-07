import unittest
import os
import pandas as pd
import numpy as np
from pathlib import Path
import importlib.util
from tests import BaseTestCase, print_summary
import inspect

class TestIndicators(BaseTestCase):
    """Test suite for checking indicator files"""
    
    def setUp(self):
        """Set up test environment"""
        super().setUp()
        self.indicators_dir = Path(__file__).parent.parent.parent / 'indicators'
        self.entry_indicators_dir = self.indicators_dir / 'entry'
        self.indicator_files = []
        self.compliant_indicators = []
        self.non_compliant_indicators = []
        
        # Get all .py files in the indicators/entry directory
        if self.entry_indicators_dir.exists():
            for item in self.entry_indicators_dir.glob('*.py'):
                if item.is_file() and item.name != '__init__.py':
                    self.indicator_files.append(item)
        else:
            print(f"Warning: Directory {self.entry_indicators_dir} does not exist")
    
    def test_indicator_has_calculate_function(self):
        """Test that all entry indicator files have a calculate_indicator function that returns a DataFrame"""
        # Create test data
        test_df = pd.DataFrame({
            'open': [100, 101, 102, 103, 104] * 5,
            'high': [105, 106, 107, 108, 109] * 5,
            'low': [95, 96, 97, 98, 99] * 5,
            'close': [102, 103, 104, 105, 106] * 5,
            'volume': [1000, 1100, 1200, 1300, 1400] * 5
        })
        
        for indicator_file in self.indicator_files:
            indicator_name = indicator_file.stem
            
            # Log which file we're testing
            print(f"Testing calculate_indicator function for: {indicator_name}")
            
            # Import the indicator module
            spec = importlib.util.spec_from_file_location(indicator_name, indicator_file)
            indicator = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(indicator)
            
            # Check if the module has a calculate_indicator function
            has_calculate_function = hasattr(indicator, 'calculate_indicator')
            self.assertTrue(has_calculate_function, 
                           f"Entry indicator {indicator_name} should have a calculate_indicator function")
            
            if has_calculate_function:
                # Check if the function returns a DataFrame
                try:
                    result = indicator.calculate_indicator(test_df.copy())
                    is_dataframe = isinstance(result, pd.DataFrame)
                    self.assertTrue(is_dataframe, 
                                  f"calculate_indicator in entry indicator {indicator_name} should return a DataFrame")
                    
                    if is_dataframe:
                        self.log_case_result(f"Entry indicator {indicator_name} has a calculate_indicator function that returns a DataFrame", True)
                    else:
                        self.log_case_result(f"Entry indicator {indicator_name} has a calculate_indicator function that returns a DataFrame", False)
                except Exception as e:
                    self.log_case_result(f"Entry indicator {indicator_name} calculate_indicator function runs without errors", False)
                    print(f"Error testing {indicator_name}: {str(e)}")
            else:
                self.log_case_result(f"Entry indicator {indicator_name} has a calculate_indicator function", False)
    
    def test_indicator_has_is_indicator_column(self):
        """Test that all entry indicator files return a DataFrame with an is_indicator column containing boolean values"""
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
            print(f"Testing entry indicator: {indicator_name}")
            
            # Import the indicator module
            spec = importlib.util.spec_from_file_location(indicator_name, indicator_file)
            indicator = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(indicator)
            
            # Check if the module has a calculate_indicator function
            has_calculate_function = hasattr(indicator, 'calculate_indicator')
            self.assertTrue(has_calculate_function, 
                           f"Entry indicator {indicator_name} should have a calculate_indicator function")
            
            if has_calculate_function:
                # Calculate the indicator
                try:
                    result_df = indicator.calculate_indicator(test_df)
                    
                    # Check if result is a DataFrame
                    is_dataframe = isinstance(result_df, pd.DataFrame)
                    self.assertTrue(is_dataframe, 
                                  f"Entry indicator {indicator_name} should return a DataFrame")
                    
                    if is_dataframe:
                        # Check if result has is_indicator column
                        has_indicator_column = 'is_indicator' in result_df.columns
                        self.assertTrue(has_indicator_column, 
                                      f"Entry indicator {indicator_name} should have an is_indicator column")
                        
                        if has_indicator_column:
                            # Check if is_indicator column contains boolean values
                            is_boolean = result_df['is_indicator'].dtype == bool or all(
                                isinstance(x, bool) for x in result_df['is_indicator'].dropna())
                            
                            self.assertTrue(is_boolean, 
                                          f"Entry indicator {indicator_name} should have boolean values in is_indicator column")
                            
                            if is_boolean:
                                self.compliant_indicators.append(indicator_name)
                                self.log_case_result(f"Entry indicator {indicator_name} has a boolean is_indicator column", True)
                            else:
                                self.non_compliant_indicators.append(f"{indicator_name} (non-boolean is_indicator)")
                                self.log_case_result(f"Entry indicator {indicator_name} has a boolean is_indicator column", False)
                        else:
                            self.non_compliant_indicators.append(f"{indicator_name} (missing is_indicator)")
                            self.log_case_result(f"Entry indicator {indicator_name} has an is_indicator column", False)
                    else:
                        self.non_compliant_indicators.append(f"{indicator_name} (not returning DataFrame)")
                        self.log_case_result(f"Entry indicator {indicator_name} returns a DataFrame", False)
                except Exception as e:
                    self.non_compliant_indicators.append(f"{indicator_name} (exception: {str(e)})")
                    self.log_case_result(f"Entry indicator {indicator_name} calculates without error", False)
            else:
                self.non_compliant_indicators.append(f"{indicator_name} (missing calculate_indicator function)")
                self.log_case_result(f"Entry indicator {indicator_name} has calculate_indicator function", False)
    
    def test_normalize_columns_call(self):
        """Test that all entry indicator files call normalize_columns at the beginning of calculate_indicator"""
        # Track indicators that comply and don't comply with this requirement
        normalize_compliant = []
        normalize_non_compliant = []
        
        for indicator_file in self.indicator_files:
            indicator_name = indicator_file.stem
            
            # Log which file we're testing
            print(f"Testing normalize_columns usage in entry indicator: {indicator_name}")
            
            # Import the indicator module
            spec = importlib.util.spec_from_file_location(indicator_name, indicator_file)
            indicator = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(indicator)
            
            # Check if the module has a calculate_indicator function
            if hasattr(indicator, 'calculate_indicator'):
                # Get the source code of the calculate_indicator function
                source_code = inspect.getsource(indicator.calculate_indicator)
                
                # Check if normalize_columns is imported
                module_source = inspect.getsource(indicator)
                normalize_imported = 'normalize_columns' in module_source
                
                # Check if normalize_columns is called at the beginning
                function_lines = source_code.strip().split('\n')
                
                # Skip function definition and docstring
                code_start_index = 0
                for i, line in enumerate(function_lines):
                    if i == 0:
                        continue  # Skip function definition
                    if line.strip().startswith('"""') or line.strip().startswith("'''"):
                        # Skip until end of docstring
                        for j in range(i + 1, len(function_lines)):
                            if ('"""' in function_lines[j] and function_lines[i].strip().startswith('"""')) or \
                               ("'''" in function_lines[j] and function_lines[i].strip().startswith("'''")):
                                code_start_index = j + 1
                                break
                        break
                    else:
                        code_start_index = i
                        break
                
                if code_start_index < len(function_lines):
                    first_code_lines = [line.strip() for line in function_lines[code_start_index:code_start_index+5]]
                    normalize_called_first = any('normalize_columns' in line and '=' in line for line in first_code_lines)
                    
                    if normalize_imported and normalize_called_first:
                        normalize_compliant.append(indicator_name)
                        self.log_case_result(f"Entry indicator {indicator_name} calls normalize_columns at the beginning", True)
                    else:
                        reason = "normalize_columns not imported" if not normalize_imported else "normalize_columns not called at beginning"
                        normalize_non_compliant.append(f"{indicator_name} ({reason})")
                        self.log_case_result(f"Entry indicator {indicator_name} calls normalize_columns at the beginning", False)
                else:
                    normalize_non_compliant.append(f"{indicator_name} (empty function body)")
                    self.log_case_result(f"Entry indicator {indicator_name} calls normalize_columns at the beginning", False)
            else:
                normalize_non_compliant.append(f"{indicator_name} (missing calculate_indicator function)")
                self.log_case_result(f"Entry indicator {indicator_name} calls normalize_columns at the beginning", False)
        
        # Print summary of normalize_columns compliance
        print("\nEntry indicators that properly call normalize_columns first:")
        for indicator in normalize_compliant:
            print(f"✓ {indicator}")
        
        if normalize_non_compliant:
            print("\nEntry indicators that don't properly call normalize_columns first:")
            for indicator in normalize_non_compliant:
                print(f"✗ {indicator}")
    
    def tearDown(self):
        """Print summary of compliant and non-compliant entry indicators"""
        super().tearDown()
        
        print("\nEntry indicators with proper is_indicator boolean column:")
        for indicator in self.compliant_indicators:
            print(f"✓ {indicator}")
        
        if self.non_compliant_indicators:
            print("\nEntry indicators that do not comply:")
            for indicator in self.non_compliant_indicators:
                print(f"✗ {indicator}")

if __name__ == '__main__':
    unittest.main(exit=False)
    print_summary() 