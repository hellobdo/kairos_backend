import unittest
import sys
import pandas as pd
from unittest.mock import patch, MagicMock
import io
from collections import OrderedDict

# Dictionary to track test results for summary
# Structure: {class_name: {"methods": {method_name: {"cases": [], "passed": bool}}, "passed": bool}}
test_results = OrderedDict()
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
        # Get the test class and method name
        self.class_name = self.__class__.__name__
        self.test_name = self.id().split('.')[-1]
        
        # Suppress print output during setup
        # Initialize test results for this class if not exists
        if self.class_name not in test_results:
            test_results[self.class_name] = {
                "methods": {},
                "passed": True
            }
        
        # Initialize test results for this method
        if self.test_name not in test_results[self.class_name]["methods"]:
            test_results[self.class_name]["methods"][self.test_name] = {
                "cases": [],
                "passed": True
            }
        
        # Create stdout capture for function output
        self.captured_output = CaptureOutput()
    
    def log_case_result(self, case_name, passed):
        """Log the result of a test case"""
        # Store result for summary without printing during execution
        test_results[self.class_name]["methods"][self.test_name]["cases"].append({
            'name': case_name,
            'passed': passed
        })
        
        # If any case fails, mark the method as failed
        if not passed:
            test_results[self.class_name]["methods"][self.test_name]["passed"] = False
            # Also mark the class as failed
            test_results[self.class_name]["passed"] = False
    
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
            # Check if this test specifically failed
            for failure in result.failures:
                if failure[0] == self:
                    # Update our tracking to mark the test as failed
                    test_results[self.class_name]["methods"][self.test_name]["passed"] = False
                    test_results[self.class_name]["passed"] = False
                    has_test_failures = True
                    break
            
            for error in result.errors:
                if error[0] == self:
                    # Update our tracking to mark the test as failed
                    test_results[self.class_name]["methods"][self.test_name]["passed"] = False
                    test_results[self.class_name]["passed"] = False
                    has_test_failures = True
                    break

def print_summary():
    """Print a summary of all test results"""
    # Print detailed test results by test class and method
    for class_name, class_result in test_results.items():
        class_symbol = "✓" if class_result["passed"] else "✗"
        print(f"[{class_symbol}] {class_name}")
        
        # Print all methods under this class
        for method_name, method_result in class_result["methods"].items():
            method_symbol = "✓" if method_result["passed"] else "✗"
            print(f"  [{method_symbol}] {method_name}")
            
            # Print all cases for this method
            for case in method_result["cases"]:
                case_symbol = "✓" if case["passed"] else "✗"
                print(f"    [{case_symbol}] {case['name']}")
    
    # Print summary header
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    
    # Count classes, methods, cases
    class_count = len(test_results)
    class_pass_count = sum(1 for result in test_results.values() if result["passed"])
    
    method_count = 0
    method_pass_count = 0
    case_count = 0
    case_pass_count = 0
    
    for class_result in test_results.values():
        for method_name, method_result in class_result["methods"].items():
            method_count += 1
            if method_result["passed"]:
                method_pass_count += 1
            
            for case in method_result["cases"]:
                case_count += 1
                if case["passed"]:
                    case_pass_count += 1
    
    # Print class-level summary only
    for class_name, class_result in test_results.items():
        class_symbol = "✓" if class_result["passed"] else "✗"
        print(f"[{class_symbol}] {class_name}")
    
    # Print statistics
    print(f"\nTEST CLASSES: {class_pass_count}/{class_count} passed")
    print(f"TEST METHODS: {method_pass_count}/{method_count} passed")
    print(f"TEST CASES: {case_pass_count}/{case_count} passed")
    
    # Overall result
    if class_pass_count == class_count:
        print("✅ All tests passed successfully!")
    else:
        print("❌ Some tests failed. See details above.")
    print("="*50)

class MockDatabaseConnection:
    """A utility class for mocking database connections in tests"""
    
    def __init__(self):
        """Initialize the mock database connection"""
        self.insert_dataframe = MagicMock()
        self.insert_dataframe.return_value = 0
    
    @staticmethod
    def create_mock_db():
        """Create a mock database connection and cursor"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn, mock_cursor 