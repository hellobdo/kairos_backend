"""
Tests for the IBKR API module functionality
"""
import unittest
import sys
from unittest.mock import patch, MagicMock
import pandas as pd
from io import StringIO

# Import our test utilities from the tests package
from tests import BaseTestCase, print_summary, MockDatabaseConnection

# Import the module to test
from api.ibkr import get_ibkr_flex_data, get_ibkr_report

# Module specific test fixtures
def create_module_fixtures():
    """Create test fixtures specific to this module's tests"""
    fixtures = {}
    
    # Test data
    fixtures['token'] = 'test_token'
    fixtures['query_id'] = 'test_query_id'
    fixtures['ref_code'] = 'TEST_REF_123'
    
    # Sample CSV for successful responses
    fixtures['valid_csv'] = """Date,Symbol,Quantity,Price,Value
2023-01-01,AAPL,100,150.00,15000.00
2023-01-02,MSFT,50,250.00,12500.00
2023-01-03,GOOG,25,2000.00,50000.00"""

    # Sample CSV with different data types and formatting
    fixtures['complex_csv'] = """Date,Symbol,Quantity,Price,Value,Notes
2023-01-01,AAPL,100,150.00,15000.00,"Regular purchase"
2023-01-02,MSFT,50.5,250.75,12662.88,"Fractional shares"
2023-01-03,GOOG,25,2000.00,50000.00,"Limit order"
2023-01-04,AMZN,-10,120.50,-1205.00,"Short sale"
"""

    # Sample CSV with only a single row (like cash report)
    fixtures['single_row_csv'] = """ClientAccountID,StartingCash,StartingCashSecurities,EndingCash,EndingCashSecurities,ChangeInCashBalance,ChangeInCashBalanceSecurities,CurrencyPrimary,FromDate,ToDate
DUE384799,29283.455653,29283.455653,29283.455653,29283.455653,0,0,BASE_SUMMARY,2025-03-20,2025-03-20"""
    
    # Properly formatted XML response
    fixtures['valid_xml'] = """<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse>
    <Status>Success</Status>
    <ReferenceCode>TEST_REF_123</ReferenceCode>
    <Url>https://ndcdyn.interactivebrokers.com/AccountManagement/FlexStatements</Url>
</FlexStatementResponse>"""

    # XML with missing reference code
    fixtures['xml_missing_ref'] = """<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse>
    <Status>Success</Status>
    <Url>https://ndcdyn.interactivebrokers.com/AccountManagement/FlexStatements</Url>
</FlexStatementResponse>"""

    # XML with failure status
    fixtures['xml_failure'] = """<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse>
    <Status>Fail</Status>
    <ErrorCode>1234</ErrorCode>
    <ErrorMessage>Invalid query ID</ErrorMessage>
</FlexStatementResponse>"""
    
    return fixtures

class TestIBKRApiImports(BaseTestCase):
    """Test basic imports and module setup"""
    
    def test_imports(self):
        """Test that imports are working correctly"""
        try:
            self.assertTrue(callable(get_ibkr_flex_data))
            self.assertTrue(callable(get_ibkr_report))
            self.log_case_result("IBKR API functions are callable", True)
        except AssertionError:
            self.log_case_result("IBKR API functions are callable", False)
            raise

class TestGetIBKRFlexData(BaseTestCase):
    """Test cases for get_ibkr_flex_data function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
    
    @patch('api.ibkr.requests.get')
    @patch('api.ibkr.time.sleep')
    def test_successful_report_retrieval(self, mock_sleep, mock_get):
        """Test successful execution of report retrieval"""
        # Setup mock responses
        mock_send_request = MagicMock()
        mock_send_request.text = f"<ReferenceCode>{self.fixtures['ref_code']}</ReferenceCode><Success>Success</Success>"
        
        mock_get_statement = MagicMock()
        mock_get_statement.text = self.fixtures['valid_csv']
        mock_get_statement.status_code = 200
        
        # Configure mock to return different responses for each call
        mock_get.side_effect = [mock_send_request, mock_get_statement]
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (3, 5))  # 3 rows, 5 columns
        self.assertEqual(list(result.columns), ['Date', 'Symbol', 'Quantity', 'Price', 'Value'])
        self.assertEqual(result['Symbol'].tolist(), ['AAPL', 'MSFT', 'GOOG'])
        
        # Verify mock calls
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)
        
        self.log_case_result("Successfully retrieves and processes report data", True)

    @patch('api.ibkr.requests.get')
    @patch('api.ibkr.time.sleep')
    def test_single_row_csv(self, mock_sleep, mock_get):
        """Test processing of a CSV with only a single row (cash report case)"""
        # Setup mock responses
        mock_send_request = MagicMock()
        mock_send_request.text = f"<ReferenceCode>{self.fixtures['ref_code']}</ReferenceCode><Success>Success</Success>"
        
        mock_get_statement = MagicMock()
        mock_get_statement.text = self.fixtures['single_row_csv']
        mock_get_statement.status_code = 200
        
        # Configure mock to return different responses for each call
        mock_get.side_effect = [mock_send_request, mock_get_statement]
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (1, 10))  # 1 row, 10 columns
        self.assertEqual(result['ClientAccountID'].iloc[0], 'DUE384799')
        self.assertEqual(result['StartingCash'].iloc[0], 29283.455653)
        
        # Verify mock calls
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)
        
        self.log_case_result("Successfully processes single-row CSV (cash report case)", True)

    @patch('api.ibkr.requests.get')
    @patch('api.ibkr.time.sleep')
    def test_proper_xml_parsing(self, mock_sleep, mock_get):
        """Test handling of properly formatted XML responses"""
        # Setup mock responses
        mock_send_request = MagicMock()
        mock_send_request.text = self.fixtures['valid_xml']
        
        mock_get_statement = MagicMock()
        mock_get_statement.text = self.fixtures['valid_csv']
        mock_get_statement.status_code = 200
        
        # Configure mock to return different responses for each call
        mock_get.side_effect = [mock_send_request, mock_get_statement]
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (3, 5))  # 3 rows, 5 columns
        
        # Verify mock calls
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)
        
        self.log_case_result("Successfully parses proper XML format", True)
        
    @patch('api.ibkr.requests.get')
    def test_xml_with_missing_reference(self, mock_get):
        """Test handling of XML with missing reference code"""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.text = self.fixtures['xml_missing_ref']
        mock_get.return_value = mock_response
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertFalse(result)
        self.assertEqual(mock_get.call_count, 1)  # Only the first request should be made
        
        self.log_case_result("Properly handles XML with missing reference code", True)
        
    @patch('api.ibkr.requests.get')
    def test_xml_with_failure_status(self, mock_get):
        """Test handling of XML with failure status"""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.text = self.fixtures['xml_failure']
        mock_get.return_value = mock_response
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertFalse(result)
        self.assertEqual(mock_get.call_count, 1)  # Only the first request should be made
        
        self.log_case_result("Properly handles XML with failure status", True)
        
    @patch('api.ibkr.requests.get')
    @patch('api.ibkr.time.sleep')
    def test_complex_csv_processing(self, mock_sleep, mock_get):
        """Test handling of more complex CSV data with various formats"""
        # Setup mock responses
        mock_send_request = MagicMock()
        mock_send_request.text = f"<ReferenceCode>{self.fixtures['ref_code']}</ReferenceCode><Success>Success</Success>"
        
        mock_get_statement = MagicMock()
        mock_get_statement.text = self.fixtures['complex_csv']
        mock_get_statement.status_code = 200
        
        # Configure mock to return different responses for each call
        mock_get.side_effect = [mock_send_request, mock_get_statement]
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (4, 6))  # 4 rows, 6 columns
        self.assertEqual(list(result.columns), ['Date', 'Symbol', 'Quantity', 'Price', 'Value', 'Notes'])
        
        # Check data types were properly handled
        self.assertEqual(result['Quantity'].dtype.kind, 'f')  # Should be float due to 50.5
        self.assertEqual(result['Value'][1], 12662.88)  # Decimal handling
        
        # Check negative values (short positions)
        self.assertEqual(result['Quantity'][3], -10)
        self.assertEqual(result['Value'][3], -1205.00)
        
        # Check quoted string handling
        self.assertEqual(result['Notes'][0], "Regular purchase")
        
        self.log_case_result("Successfully processes complex CSV with various data formats", True)
        
    @patch('api.ibkr.requests.get')
    def test_failed_report_generation(self, mock_get):
        """Test when the initial report generation fails"""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.text = "<ErrorMessage>Invalid token</ErrorMessage>"
        mock_get.return_value = mock_response
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertFalse(result)
        self.assertEqual(mock_get.call_count, 1)  # Only the first request should be made
        
        self.log_case_result("Properly handles failed report generation", True)
        
    @patch('api.ibkr.requests.get')
    def test_empty_reference_code(self, mock_get):
        """Test when no reference code is returned"""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.text = "<Success>Success</Success>"  # Missing reference code
        mock_get.return_value = mock_response
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertFalse(result)
        self.assertEqual(mock_get.call_count, 1)  # Only the first request should be made
        
        self.log_case_result("Properly handles missing reference code", True)
        
    @patch('api.ibkr.requests.get')
    @patch('api.ibkr.time.sleep')
    def test_invalid_csv_response(self, mock_sleep, mock_get):
        """Test when the report response is not valid CSV"""
        # Setup mock responses
        mock_send_request = MagicMock()
        mock_send_request.text = f"<ReferenceCode>{self.fixtures['ref_code']}</ReferenceCode><Success>Success</Success>"
        
        mock_get_statement = MagicMock()
        mock_get_statement.text = "This is not a CSV"  # Invalid CSV
        mock_get_statement.status_code = 200
        
        # Configure mock to return different responses for each call
        mock_get.side_effect = [mock_send_request, mock_get_statement]
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertFalse(result)
        
        self.log_case_result("Properly handles invalid CSV response", True)
        
    @patch('api.ibkr.requests.get')
    @patch('api.ibkr.time.sleep')
    def test_empty_dataframe(self, mock_sleep, mock_get):
        """Test when the CSV produces an empty DataFrame"""
        # Setup mock responses
        mock_send_request = MagicMock()
        mock_send_request.text = f"<ReferenceCode>{self.fixtures['ref_code']}</ReferenceCode><Success>Success</Success>"
        
        mock_get_statement = MagicMock()
        mock_get_statement.text = "Date,Symbol,Quantity\n"  # Only header, no data
        mock_get_statement.status_code = 200
        
        # Configure mock to return different responses for each call
        mock_get.side_effect = [mock_send_request, mock_get_statement]
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertFalse(result)
        
        self.log_case_result("Properly handles empty DataFrame", True)
        
    @patch('api.ibkr.requests.get')
    def test_exception_handling(self, mock_get):
        """Test exception handling in the function"""
        # Setup mock to raise an exception
        mock_get.side_effect = Exception("Connection error")
        
        # Call function under test
        result = get_ibkr_flex_data(self.fixtures['token'], self.fixtures['query_id'])
        
        # Assertions
        self.assertFalse(result)
        
        self.log_case_result("Properly handles exceptions", True)

class TestGetIBKRReport(BaseTestCase):
    """Test cases for the get_ibkr_report function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
    
    @patch('api.ibkr.get_ibkr_flex_data')
    def test_successful_report(self, mock_get_ibkr_flex_data):
        """Test successful retrieval of a report"""
        # Create a mock DataFrame
        mock_df = pd.DataFrame({
            'Symbol': ['AAPL', 'MSFT'],
            'Price': [150.0, 250.0]
        })
        mock_get_ibkr_flex_data.return_value = mock_df
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call function under test
        result = get_ibkr_report(self.fixtures['token'], self.fixtures['query_id'], 'cash')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Assertions
        self.assertIsInstance(result, pd.DataFrame)
        mock_get_ibkr_flex_data.assert_called_once_with(self.fixtures['token'], self.fixtures['query_id'])
        
        # Check output messages
        output = self.captured_output.get_value()
        self.assertIn("Fetching cash report from IBKR", output)
        self.assertIn("Successfully retrieved cash report", output)
        
        self.log_case_result("Successfully retrieves report with proper logging", True)
    
    @patch('api.ibkr.get_ibkr_flex_data')
    def test_column_normalization(self, mock_get_ibkr_flex_data):
        """Test that column names are normalized to lowercase"""
        # Setup mock data with mixed case columns
        mock_df = pd.DataFrame({
            'Symbol': ['AAPL', 'MSFT', 'GOOG'],
            'QUANTITY': [100, 50, 25],
            'Price': [150.0, 250.0, 2000.0],
            'TradeID': ['T1', 'T2', 'T3'],
            'ClientAccountID': ['U1', 'U1', 'U1']
        })
        
        # Configure mock
        mock_get_ibkr_flex_data.return_value = mock_df
        
        # Capture stdout to verify print statements
        original_stdout = self.capture_stdout()
        
        # Call function under test
        result = get_ibkr_report(self.fixtures['token'], self.fixtures['query_id'])
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Verify all column names are lowercase
        for column in result.columns:
            self.assertEqual(column, column.lower())
        
        # Check specific examples
        self.assertIn('symbol', result.columns)
        self.assertIn('quantity', result.columns)
        self.assertIn('tradeid', result.columns)
        self.assertIn('clientaccountid', result.columns)
        
        # Verify original data values are preserved
        self.assertEqual(result['symbol'].tolist(), ['AAPL', 'MSFT', 'GOOG'])
        self.assertEqual(result['quantity'].tolist(), [100, 50, 25])
        
        # Verify output message
        output = self.captured_output.get_value()
        self.assertIn("Column names normalized to lowercase", output)
        
        self.log_case_result("Successfully normalizes column names to lowercase", True)
        
    @patch('api.ibkr.get_ibkr_flex_data')
    def test_failed_report(self, mock_get_ibkr_flex_data):
        """Test handling when the report retrieval fails"""
        # Setup mock to return False
        mock_get_ibkr_flex_data.return_value = False
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call function under test
        result = get_ibkr_report(self.fixtures['token'], self.fixtures['query_id'], 'cash')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Assertions
        self.assertFalse(result)
        mock_get_ibkr_flex_data.assert_called_once_with(self.fixtures['token'], self.fixtures['query_id'])
        
        # Check output messages
        output = self.captured_output.get_value()
        self.assertIn("Fetching cash report from IBKR", output)
        # Should not contain success message
        self.assertNotIn("Successfully retrieved", output)
        
        self.log_case_result("Properly handles failed report retrieval", True)
    
    @patch('api.ibkr.get_ibkr_flex_data')
    def test_exception_handling(self, mock_get_ibkr_flex_data):
        """Test exception handling in the get_ibkr_report function"""
        # Setup mock to raise an exception
        mock_get_ibkr_flex_data.side_effect = Exception("Test exception")
        
        # Capture stdout
        original_stdout = self.capture_stdout()
        
        # Call function under test
        result = get_ibkr_report(self.fixtures['token'], self.fixtures['query_id'], 'cash')
        
        # Restore stdout
        self.restore_stdout(original_stdout)
        
        # Assertions
        self.assertFalse(result)
        mock_get_ibkr_flex_data.assert_called_once_with(self.fixtures['token'], self.fixtures['query_id'])
        
        # Check output messages
        output = self.captured_output.get_value()
        self.assertIn("Error fetching cash data: Test exception", output)
        
        self.log_case_result("Properly handles exceptions", True)
    
    @patch('api.ibkr.get_ibkr_flex_data')
    def test_report_type_logging(self, mock_get_ibkr_flex_data):
        """Test that report_type is used correctly in logging"""
        # Create a mock DataFrame
        mock_df = pd.DataFrame({'A': [1, 2, 3]})
        mock_get_ibkr_flex_data.return_value = mock_df
        
        # Test with different report types
        report_types = ['cash', 'trade_confirmations', 'positions', 'custom_type']
        
        for report_type in report_types:
            # Capture stdout
            original_stdout = self.capture_stdout()
            
            # Call function under test
            get_ibkr_report(self.fixtures['token'], self.fixtures['query_id'], report_type)
            
            # Restore stdout
            self.restore_stdout(original_stdout)
            
            # Check output messages
            output = self.captured_output.get_value()
            self.assertIn(f"Fetching {report_type} report from IBKR", output)
            self.assertIn(f"Successfully retrieved {report_type} report", output)
        
        self.log_case_result("Properly includes report type in logging", True)

if __name__ == '__main__':
    print("\n🔍 Running tests for IBKR API module...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary
    print_summary() 