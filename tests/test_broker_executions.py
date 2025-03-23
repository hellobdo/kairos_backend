import unittest
import sys
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime

# Import our test utilities from the tests package
from tests import BaseTestCase, print_summary, MockDatabaseConnection

# Import the functions we want to test
from analytics.broker_executions import (
    process_ibkr_data,
    identify_trade_ids,
    insert_executions_to_db,
    process_account_data
)

def create_executions_fixtures():
    """Create test fixtures specific to executions module tests"""
    fixtures = {}
    
    # Sample raw trade data
    fixtures['raw_trades_df'] = pd.DataFrame({
        'ClientAccountID': ['U1234567', 'U1234567', 'U1234567'],
        'TradeID': ['T1', 'T2', 'T3'],
        'trade_external_ID': ['T1', 'T2', 'T3'],
        'OrderID': ['O1', 'O2', 'O3'],
        'Symbol': ['AAPL', 'AAPL', 'AAPL'],
        'Quantity': ['100', '-50', '-50'],
        'Price': ['150.50', '160.25', '165.75'],
        'NetCashWithBillable': ['-15050.00', '8012.50', '8287.50'],
        'Commission': ['1.50', '1.50', '1.50'],
        'Date/Time': ['2024-03-20;10:30:00', '2024-03-21;14:15:00', '2024-03-22;15:45:00']
    })
    
    # Sample existing trades in database
    fixtures['existing_trades'] = ['T1']
    
    # Sample existing positions
    fixtures['existing_positions'] = [
        ('MSFT', 100, 1),  # (symbol, volume, trade_id)
        ('GOOGL', -50, 2)
    ]
    
    return fixtures

class TestExecutionsImports(BaseTestCase):
    """Test basic imports and module setup"""
    
    def test_imports(self):
        """Test that imports are working correctly"""
        # Case 1: Check that functions are callable
        try:
            self.assertTrue(callable(process_ibkr_data))
            self.assertTrue(callable(identify_trade_ids))
            self.assertTrue(callable(insert_executions_to_db))
            self.assertTrue(callable(process_account_data))
            self.log_case_result("Functions are callable", True)
        except AssertionError:
            self.log_case_result("Functions are callable", False)
            raise

class TestProcessIBKRData(BaseTestCase):
    """Test cases for process_ibkr_data function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_executions_fixtures()
    
    @patch('analytics.broker_executions.db')
    def test_filtering_existing_trades(self, mock_db):
        """Test filtering out existing trades"""
        # Setup mock
        mock_db.get_existing_trade_external_ids.return_value = self.fixtures['existing_trades']
        
        # Process data
        result = process_ibkr_data(self.fixtures['raw_trades_df'])
        
        # Verify
        self.assertEqual(len(result), 2)  # Should have filtered out one trade
        self.assertNotIn('T1', result['TradeID'].values)
        self.log_case_result("Successfully filters existing trades", True)
    
    @patch('analytics.broker_executions.db')
    def test_numeric_conversion(self, mock_db):
        """Test numeric field conversion"""
        # Setup mock
        mock_db.get_existing_trade_ids.return_value = []
        
        # Process data
        result = process_ibkr_data(self.fixtures['raw_trades_df'])
        
        # Verify numeric fields
        numeric_fields = ['Quantity', 'Price', 'NetCashWithBillable', 'Commission']
        for field in numeric_fields:
            self.assertTrue(pd.api.types.is_numeric_dtype(result[field]))
        
        self.log_case_result("Successfully converts numeric fields", True)
    
    @patch('analytics.broker_executions.db')
    def test_datetime_processing(self, mock_db):
        """Test date/time field processing"""
        # Setup mock
        mock_db.get_existing_trade_ids.return_value = []
        
        # Process data
        result = process_ibkr_data(self.fixtures['raw_trades_df'])
        
        # Verify date/time processing
        self.assertTrue('date' in result.columns)
        self.assertTrue('time_of_day' in result.columns)
        self.assertEqual(result['date'].iloc[0], '2024-03-20')
        self.assertEqual(result['time_of_day'].iloc[0], '10:30:00')
        
        self.log_case_result("Successfully processes date/time fields", True)
    
    @patch('analytics.broker_executions.db')
    def test_side_determination(self, mock_db):
        """Test trade side determination"""
        # Setup mock
        mock_db.get_existing_trade_ids.return_value = []
        
        # Process data
        result = process_ibkr_data(self.fixtures['raw_trades_df'])
        
        # Verify side determination
        self.assertEqual(result['side'].iloc[0], 'BUY')  # Quantity = 100
        self.assertEqual(result['side'].iloc[1], 'SELL')  # Quantity = -50
        
        self.log_case_result("Successfully determines trade sides", True)

class TestIdentifyTradeIds(BaseTestCase):
    """Test cases for identify_trade_ids function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_executions_fixtures()
    
    @patch('analytics.broker_executions.db')
    def test_new_position_opening(self, mock_db):
        """Test opening a new position"""
        # Setup mock
        mock_db.get_max_trade_id.return_value = 2
        mock_db.get_open_positions.return_value = []
        
        # Create test data
        test_df = pd.DataFrame({
            'Symbol': ['AAPL'],
            'Quantity': [100]
        })
        
        # Process data
        result = identify_trade_ids(test_df)
        
        # Verify
        self.assertTrue(result['is_entry'].iloc[0])
        self.assertFalse(result['is_exit'].iloc[0])
        self.assertEqual(result['trade_id'].iloc[0], 3)
        
        self.log_case_result("Successfully identifies new position entry", True)
    
    @patch('analytics.broker_executions.db')
    def test_position_closing(self, mock_db):
        """Test closing an existing position"""
        # Setup mock
        mock_db.get_max_trade_id.return_value = 1
        mock_db.get_open_positions.return_value = [('AAPL', 100, 1)]
        
        # Create test data for closing trade
        test_df = pd.DataFrame({
            'Symbol': ['AAPL'],
            'Quantity': [-100]
        })
        
        # Process data
        result = identify_trade_ids(test_df)
        
        # Verify
        self.assertTrue(result['is_exit'].iloc[0])
        self.assertEqual(result['trade_id'].iloc[0], 1)
        self.assertEqual(result['open_volume'].iloc[0], 0)
        
        self.log_case_result("Successfully identifies position exit", True)
    
    @patch('analytics.broker_executions.db')
    def test_multiple_symbols(self, mock_db):
        """Test handling multiple symbols simultaneously"""
        # Setup mock
        mock_db.get_max_trade_id.return_value = 2
        mock_db.get_open_positions.return_value = [('AAPL', 100, 1)]
        
        # Create test data with multiple symbols
        test_df = pd.DataFrame({
            'Symbol': ['AAPL', 'MSFT'],
            'Quantity': [-100, 50]
        })
        
        # Process data
        result = identify_trade_ids(test_df)
        
        # Verify
        self.assertTrue(result['is_exit'].iloc[0])  # AAPL position closes
        self.assertTrue(result['is_entry'].iloc[1])  # MSFT position opens
        self.assertEqual(result['trade_id'].iloc[0], 1)  # AAPL keeps trade_id 1
        self.assertEqual(result['trade_id'].iloc[1], 3)  # MSFT gets new trade_id
        
        self.log_case_result("Successfully handles multiple symbols", True)

class TestInsertExecutionsToDB(BaseTestCase):
    """Test cases for insert_executions_to_db function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_executions_fixtures()
    
    @patch('analytics.broker_executions.db')
    def test_successful_insertion(self, mock_db):
        """Test successful insertion of records"""
        # Create test data
        test_df = pd.DataFrame({
            'ClientAccountID': ['U1234567'],
            'TradeID': ['T1'],
            'OrderID': ['O1'],
            'Symbol': ['AAPL'],
            'Quantity': [100],
            'Price': [150.50],
            'NetCashWithBillable': [-15050.00],
            'Commission': [1.50],
            'execution_timestamp': ['2024-03-20;10:30:00'],
            'date': ['2024-03-20'],
            'time_of_day': ['10:30:00'],
            'side': ['BUY'],
            'trade_id': [1],
            'is_entry': [True],
            'is_exit': [False]
        })
        
        # Process data
        result = insert_executions_to_db(test_df)
        
        # Verify
        self.assertEqual(result, 1)
        mock_db.insert_execution.assert_called_once()
        
        self.log_case_result("Successfully inserts records", True)
    
    @patch('analytics.broker_executions.db')
    def test_database_error(self, mock_db):
        """Test database error handling"""
        # Setup mock to raise exception
        mock_db.insert_execution.side_effect = Exception("Database error")
        
        # Create test data
        test_df = pd.DataFrame({
            'ClientAccountID': ['U1234567'],
            'TradeID': ['T1']
        })
        
        # Verify exception is propagated
        with self.assertRaises(Exception):
            insert_executions_to_db(test_df)
        
        self.log_case_result("Properly handles database errors", True)

class TestProcessAccountData(BaseTestCase):
    """Test cases for process_account_data function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_executions_fixtures()
    
    @patch('analytics.broker_executions.get_ibkr_report')
    @patch('analytics.broker_executions.process_ibkr_data')
    @patch('analytics.broker_executions.identify_trade_ids')
    @patch('analytics.broker_executions.insert_executions_to_db')
    def test_successful_processing(self, mock_insert, mock_identify, mock_process, mock_get_report):
        """Test successful end-to-end processing"""
        # Setup mocks
        mock_get_report.return_value = self.fixtures['raw_trades_df']
        mock_process.return_value = self.fixtures['raw_trades_df']
        mock_identify.return_value = self.fixtures['raw_trades_df']
        mock_insert.return_value = 3
        
        # Process data
        result = process_account_data("token", "query_id", "test")
        
        # Verify
        self.assertTrue(result)
        mock_get_report.assert_called_once_with("token", "query_id", "trade_confirmations")
        mock_process.assert_called_once()
        mock_identify.assert_called_once()
        mock_insert.assert_called_once()
        
        self.log_case_result("Successfully processes account data", True)
    
    @patch('analytics.broker_executions.get_ibkr_report')
    def test_api_failure(self, mock_get_report):
        """Test handling of API failure"""
        # Setup mock to simulate API failure
        mock_get_report.return_value = False
        
        # Process data
        result = process_account_data("token", "query_id", "test")
        
        # Verify
        self.assertFalse(result)
        
        self.log_case_result("Properly handles API failure", True)

if __name__ == '__main__':
    print("\n🔍 Running tests for broker_executions.py...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary
    print_summary() 