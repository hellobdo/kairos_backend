# Testing Framework for Kairos Backend

This directory contains tests for the Kairos backend application. The testing framework is designed to be modular, standardized, and easy to extend.

## Testing Architecture

The testing framework is built with modularity in mind:

1. `test_utils.py` - Contains shared utilities for all tests:
   - `BaseTestCase` class that extends `unittest.TestCase` with common functionality
   - Output capture and result tracking
   - Summary reporting
   - Generic mocking utilities like `MockDatabaseConnection`

2. `__init__.py` - Makes the test directory a proper package and exposes common utilities:
   - Exposes `BaseTestCase` and other utilities for easy importing
   - Eliminates the need for messy relative imports

3. Module-specific test files (e.g., `test_cash.py`):
   - Each file focuses on testing a specific module
   - Contains module-specific fixtures
   - Organized into test classes by functionality

## Test Documentation

### `test_cash.py`

This file tests the functionality in `analytics/cash.py`, which handles cash data processing from Interactive Brokers (IBKR) and updates the database with cash balances.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestCashImports`**: Tests basic imports and environment setup
2. **`TestProcessIBKRAccount`**: Tests the IBKR account data processing function
3. **`TestUpdateAccountsBalances`**: Tests the database update functionality
4. **`TestMainBlockExecution`**: Tests the main execution block error handling
5. **`TestUtilities`**: Tests utility functions like date validation

#### Tested Functions

1. **`process_ibkr_account(token, query_id)`**: Retrieves cash data from IBKR using the flex query service.
2. **`update_accounts_balances(df)`**: Updates the database with cash report data, ensuring only one entry per account_ID + date combination.

#### Test Cases

The test suite includes the following categories of tests:

1. **Basic Import Tests** (`TestCashImports.test_imports`):
   - Verifies that functions are callable
   - Checks that required modules are accessible
   - Confirms DataFrame creation works

2. **Environment Variable Tests** (`TestCashImports.test_environment_variables`):
   - Verifies that IBKR paper trading credentials are set
   - Verifies that IBKR live trading credentials are set

3. **Function Tests for `process_ibkr_account`**:
   - Successful data retrieval (`TestProcessIBKRAccount.test_successful_data_retrieval`)
   - API failure handling (`TestProcessIBKRAccount.test_api_failure`)
   - Empty DataFrame handling (`TestProcessIBKRAccount.test_empty_dataframe`)

4. **Main Block Testing** (`TestMainBlockExecution.test_main_block_error_handling`):
   - Tests handling when `process_ibkr_account` returns `False`

5. **Function Tests for `update_accounts_balances`**:
   - Successful insertion of valid data (`TestUpdateAccountsBalances.test_successful_insert`)
   - Empty DataFrame handling (`TestUpdateAccountsBalances.test_empty_dataframe`)
   - Missing required columns (`TestUpdateAccountsBalances.test_missing_columns`)
   - Duplicate record detection and skipping (`TestUpdateAccountsBalances.test_duplicate_records`)
   - SQL error handling (`TestUpdateAccountsBalances.test_sql_error_handling`)

6. **Date Format Regex Testing** (`TestUtilities.test_date_format_regex`):
   - Tests valid date formats (YYYY-MM-DD)
   - Tests invalid date formats

#### Running the Tests

To run the tests:

```bash
python -m tests.test_cash
```

The test execution will provide a summary of all test cases and their status (passed/failed).

## Test Framework

The tests use Python's `unittest` framework with mocking provided by the `unittest.mock` module. The test results are tracked and displayed using a custom reporting mechanism that shows a summary of all test cases. 