"""
Tests for the get_latest_trade_report utility module.
"""
import unittest
import sys
from unittest.mock import patch, MagicMock, call
from pathlib import Path
import os
from datetime import datetime
import pytest

# Import our test utilities from the tests package
from tests import BaseTestCase, print_summary, MockDatabaseConnection

# Import the module to test
from utils.get_latest_trade_report import get_latest_trade_report

# Module specific test fixtures
def create_module_fixtures():
    """Create test fixtures specific to this module's tests"""
    fixtures = {}
    
    # Timestamps for mock files (newest to oldest)
    now = datetime.now().timestamp()
    fixtures['timestamps'] = {
        'html_newest': now,
        'html_older': now - 3600,  # 1 hour older
        'csv_newest': now - 1800,  # 30 minutes older than newest html
        'csv_older': now - 7200,   # 2 hours older than newest html
    }
    
    # Mock file paths
    fixtures['html_files'] = [
        Path('logs/trade_report_20230601.html'),  # newest
        Path('logs/trade_report_20230531.html')   # older
    ]
    
    fixtures['csv_files'] = [
        Path('logs/trades_20230601.csv'),  # newest
        Path('logs/trades_20230530.csv')   # older
    ]
    
    return fixtures


class TestGetLatestTradeReport(BaseTestCase):
    """Test cases for get_latest_trade_report function"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.fixtures = create_module_fixtures()
    
    def test_imports(self):
        """Test that imports are working correctly"""
        try:
            self.assertTrue(callable(get_latest_trade_report))
            self.log_case_result("get_latest_trade_report function is importable", True)
        except AssertionError:
            self.log_case_result("get_latest_trade_report function is importable", False)
            raise
    
    def test_invalid_type(self):
        """Test function with invalid report type"""
        with self.assertRaises(ValueError) as context:
            get_latest_trade_report("pdf")
        
        self.assertEqual(
            str(context.exception),
            "Invalid report type. Must be 'html' or 'csv'."
        )
        self.log_case_result("Function rejects invalid report type", True)
    
    def test_no_type(self):
        """Test function with no type provided (should raise TypeError)"""
        with self.assertRaises(TypeError):
            get_latest_trade_report()
        
        self.log_case_result("Function requires type parameter", True)
    
    @patch('pathlib.Path.exists')
    @patch('pathlib.Path.glob')
    @patch('os.path.getctime')
    def test_html_type(self, mock_getctime, mock_glob, mock_exists):
        """Test function with HTML type"""
        # Setup mocks
        mock_exists.return_value = True
        mock_glob.return_value = self.fixtures['html_files']
        
        # Configure getctime to return different timestamps for different files
        def mock_getctime_fn(path):
            if str(path).endswith('20230601.html'):
                return self.fixtures['timestamps']['html_newest']
            else:
                return self.fixtures['timestamps']['html_older']
        
        mock_getctime.side_effect = mock_getctime_fn
        
        # Capture stdout to verify output messages
        stdout_original = self.capture_stdout()
        
        # Call the function
        result = get_latest_trade_report("html")
        
        # Restore stdout
        self.restore_stdout(stdout_original)
        
        # Verify correct glob pattern was used
        mock_glob.assert_called_with("*trade_report*.html")
        
        # Verify the newest file was returned
        self.assertEqual(result, self.fixtures['html_files'][0])
        
        # Verify output message contains the file path
        self.assertIn(str(self.fixtures['html_files'][0]), self.captured_output.value)
        
        self.log_case_result("Function correctly handles HTML type", True)
    
    @patch('pathlib.Path.exists')
    @patch('pathlib.Path.glob')
    @patch('os.path.getctime')
    def test_csv_type(self, mock_getctime, mock_glob, mock_exists):
        """Test function with CSV type"""
        # Setup mocks
        mock_exists.return_value = True
        mock_glob.return_value = self.fixtures['csv_files']
        
        # Configure getctime to return different timestamps for different files
        def mock_getctime_fn(path):
            if str(path).endswith('20230601.csv'):
                return self.fixtures['timestamps']['csv_newest']
            else:
                return self.fixtures['timestamps']['csv_older']
        
        mock_getctime.side_effect = mock_getctime_fn
        
        # Capture stdout
        stdout_original = self.capture_stdout()
        
        # Call the function
        result = get_latest_trade_report("csv")
        
        # Restore stdout
        self.restore_stdout(stdout_original)
        
        # Verify correct glob pattern was used
        mock_glob.assert_called_with("*trades*.csv")
        
        # Verify the newest file was returned
        self.assertEqual(result, self.fixtures['csv_files'][0])
        
        # Verify output message contains the file path
        self.assertIn(str(self.fixtures['csv_files'][0]), self.captured_output.value)
        
        self.log_case_result("Function correctly handles CSV type", True)
    
    @patch('pathlib.Path.exists')
    def test_logs_directory_not_found(self, mock_exists):
        """Test behavior when logs directory doesn't exist"""
        # Setup mocks
        mock_exists.return_value = False
        
        # Test function raises FileNotFoundError
        with self.assertRaises(FileNotFoundError) as context:
            get_latest_trade_report("html")
        
        # Verify the error message contains the logs directory path
        self.assertIn("logs", str(context.exception))
        
        self.log_case_result("Function correctly handles missing logs directory", True)
    
    @patch('pathlib.Path.exists')
    @patch('pathlib.Path.glob')
    def test_no_files_found(self, mock_glob, mock_exists):
        """Test behavior when no files are found"""
        # Setup mocks
        mock_exists.return_value = True
        mock_glob.return_value = []
        
        # Test function raises FileNotFoundError
        with self.assertRaises(FileNotFoundError) as context:
            get_latest_trade_report("html")
        
        # Verify the error message mentions HTML files
        self.assertIn("html", str(context.exception))
        
        # Test with CSV type too
        with self.assertRaises(FileNotFoundError) as context:
            get_latest_trade_report("csv")
        
        # Verify the error message mentions CSV files
        self.assertIn("csv", str(context.exception))
        
        self.log_case_result("Function correctly handles no files found", True)
    
    @patch('pathlib.Path.exists')
    @patch('pathlib.Path.glob')
    @patch('os.path.getctime')
    def test_returns_newest_file(self, mock_getctime, mock_glob, mock_exists):
        """Test function returns the newest file based on timestamp"""
        # Setup mocks for multiple files with different timestamps
        mock_exists.return_value = True
        
        # For HTML files
        html_files = [
            Path('logs/trade_report_oldest.html'),
            Path('logs/trade_report_middle.html'),
            Path('logs/trade_report_newest.html'),
        ]
        mock_glob.return_value = html_files
        
        # Create timestamps: newest, middle, oldest
        now = datetime.now().timestamp()
        timestamps = {
            str(html_files[0]): now - 86400,  # 1 day old
            str(html_files[1]): now - 43200,  # 12 hours old
            str(html_files[2]): now,          # newest
        }
        
        # Configure getctime to return different timestamps
        def mock_getctime_fn(path):
            return timestamps[str(path)]
        
        mock_getctime.side_effect = mock_getctime_fn
        
        # Call the function
        result = get_latest_trade_report("html")
        
        # Verify the newest file was returned
        self.assertEqual(result, html_files[2])
        
        # Reset mocks for CSV test
        mock_glob.reset_mock()
        mock_getctime.reset_mock()
        
        # For CSV files - reverse the order to ensure it's using timestamps, not list order
        csv_files = [
            Path('logs/trades_newest.csv'),
            Path('logs/trades_middle.csv'),
            Path('logs/trades_oldest.csv'),
        ]
        mock_glob.return_value = csv_files
        
        # Create timestamps but with a different order than the list
        csv_timestamps = {
            str(csv_files[0]): now,          # newest
            str(csv_files[1]): now - 43200,  # 12 hours old
            str(csv_files[2]): now - 86400,  # 1 day old
        }
        
        # Update mock_getctime to use the CSV timestamps
        def mock_getctime_csv_fn(path):
            return csv_timestamps[str(path)]
        
        mock_getctime.side_effect = mock_getctime_csv_fn
        
        # Call the function for CSV
        result = get_latest_trade_report("csv")
        
        # Verify the newest file was returned
        self.assertEqual(result, csv_files[0])
        
        self.log_case_result("Function correctly returns the newest file based on timestamp", True)


if __name__ == '__main__':
    print("\n🔍 Running tests for get_latest_trade_report.py...")
    
    # Run the tests with default verbosity
    unittest.main(exit=False, verbosity=0)
    
    # Print summary
    print_summary() 