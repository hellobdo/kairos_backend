import unittest
import pandas as pd
from tests.utils.test_utils import BaseTestCase, print_summary
from indicators.helpers.column_utils import normalize_ohlc_columns

class TestColumnUtils(BaseTestCase):
    """Test cases for the column_utils module"""
    
    def test_normalize_ohlc_columns(self):
        """Test that normalize_ohlc_columns converts all column names to lowercase"""
        
        # Test case 1: DataFrame with uppercase columns
        df_upper = pd.DataFrame({
            'Open': [100, 101, 102],
            'High': [105, 106, 107],
            'Low': [95, 96, 97],
            'Close': [102, 103, 104],
            'Volume': [1000, 1100, 1200]
        })
        
        result_upper = normalize_ohlc_columns(df_upper)
        
        # Check that all columns are lowercase
        for col in result_upper.columns:
            self.assertEqual(col, col.lower(), f"Column '{col}' should be lowercase")
        
        self.log_case_result("uppercase columns converted to lowercase", 
                            all(col == col.lower() for col in result_upper.columns))
        
        # Test case 2: DataFrame with mixed case columns
        df_mixed = pd.DataFrame({
            'Open': [100, 101, 102],
            'high': [105, 106, 107],
            'Low': [95, 96, 97],
            'close': [102, 103, 104],
            'VOLUME': [1000, 1100, 1200]
        })
        
        result_mixed = normalize_ohlc_columns(df_mixed)
        
        # Check that all columns are lowercase
        for col in result_mixed.columns:
            self.assertEqual(col, col.lower(), f"Column '{col}' should be lowercase")
        
        self.log_case_result("mixed case columns converted to lowercase", 
                            all(col == col.lower() for col in result_mixed.columns))
        
        # Test case 3: DataFrame with already lowercase columns
        df_lower = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [105, 106, 107],
            'low': [95, 96, 97],
            'close': [102, 103, 104],
            'volume': [1000, 1100, 1200]
        })
        
        result_lower = normalize_ohlc_columns(df_lower)
        
        # Check that all columns are lowercase (should be unchanged)
        for col in result_lower.columns:
            self.assertEqual(col, col.lower(), f"Column '{col}' should be lowercase")
        
        self.log_case_result("lowercase columns remain lowercase", 
                            all(col == col.lower() for col in result_lower.columns))
        
        # Test case 4: DataFrame with arbitrary column names
        df_arbitrary = pd.DataFrame({
            'Price_OPEN': [100, 101, 102],
            'DAILY_High': [105, 106, 107],
            'min_LOW': [95, 96, 97],
            'LastPrice': [102, 103, 104],
            'VOL': [1000, 1100, 1200]
        })
        
        result_arbitrary = normalize_ohlc_columns(df_arbitrary)
        
        # Check that all columns are lowercase
        for col in result_arbitrary.columns:
            self.assertEqual(col, col.lower(), f"Column '{col}' should be lowercase")
        
        self.log_case_result("arbitrary column names converted to lowercase", 
                            all(col == col.lower() for col in result_arbitrary.columns))

if __name__ == '__main__':
    unittest.main(exit=False)
    print_summary() 