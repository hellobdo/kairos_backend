import unittest
import sys
import pandas as pd
from unittest.mock import patch, MagicMock
import io

# Dictionary to track test results for summary
test_results = {}
# Global flag to indicate if any test has failed or had an error
has_test_failures = False

class CaptureOutput:
    """A class to capture stdout output safely in tests"""
    def __init__(self):
        self.value = ""
    
    def write(self, txt):
        self.value += txt
    
    def flush(self):
        pass
    
    def get_value(self):
        return self.value

class BaseTestCase(unittest.TestCase):
    """Base class for all test cases with common setup/teardown and utilities"""
    
    def setUp(self):
        """Set up environment for tests"""
        # Get the test name
        self.test_name = self.id().split('.')[-1]
        # Print the test name
        print(f"\n[✓] TEST: {self.test_name}")
        
        # Initialize test results for this test
        test_results[self.test_name] = {'cases': [], 'passed': True}
        
        # Create stdout capture for function output
        self.captured_output = CaptureOutput()
    
    def log_case_result(self, case_name, passed):
        """Log the result of a test case"""
        result = "✓" if passed else "✗"
        # Print the case result
        print(f"[{result}] CASE: {case_name}")
        
        # Store result for summary
        test_results[self.test_name]['cases'].append({
            'name': case_name,
            'passed': passed
        })
        
        # If any case fails, mark the test as failed
        if not passed:
            test_results[self.test_name]['passed'] = False
    
    def tearDown(self):
        """Clean up after each test"""
        # Restore sys.stdout if it was changed
        if hasattr(sys, '_stdout_bak'):
            sys.stdout = sys._stdout_bak
            
        # Stop all patches
        patch.stopall()

    def capture_stdout(self):
        """Helper to capture stdout and return the original stdout"""
        original_stdout = sys.stdout
        sys.stdout = self.captured_output
        return original_stdout

    def restore_stdout(self, original_stdout):
        """Helper to restore stdout after capture"""
        sys.stdout = original_stdout

    def run(self, result=None):
        """Override run to track if tests fail or have errors"""
        global has_test_failures
        # Run the test normally
        super().run(result)
        
        # Check if this test failed or had an error
        if result and (result.failures or result.errors):
            has_test_failures = True

def print_summary():
    """Print a summary of all test results"""
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    
    # Track if any tests have failed according to our tracking
    all_passed = True
    
    # Print the summary of our tracked tests
    for test_name, result in test_results.items():
        test_symbol = "✓" if result['passed'] else "✗"
        print(f"[{test_symbol}] {test_name}")
        
        if not result['passed']:
            all_passed = False
            # Print failed cases
            for case in result['cases']:
                if not case['passed']:
                    print(f"    [✗] {case['name']}")
    
    print("\n" + "="*50)
    # Use both our local tracking and the global flag
    global has_test_failures
    if all_passed and not has_test_failures:
        print("All tests passed successfully!")
    else:
        print("Some tests failed. See failures and errors above.")
    print("="*50)

class MockDatabaseConnection:
    """A utility class for mocking database connections in tests"""
    
    @staticmethod
    def create_mock_db():
        """Create a mock database connection and cursor"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn, mock_cursor 