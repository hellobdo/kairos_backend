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
2. **`TestUpdateAccountsBalances`**: Tests the database update functionality
3. **`TestProcessAccountData`**: Tests the account data processing function
4. **`TestUtilities`**: Tests utility functions like date validation

#### Tested Functions

1. **`process_account_data(token, query_id, account_type)`**: Retrieves and processes cash data from IBKR using the centralized report function.
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

3. **Function Tests for `update_accounts_balances`**:
   - Successful insertion of valid data (`TestUpdateAccountsBalances.test_successful_insert`)
   - Empty DataFrame handling (`TestUpdateAccountsBalances.test_empty_dataframe`)
   - Missing required columns (`TestUpdateAccountsBalances.test_missing_columns`)
   - Duplicate record detection and skipping (`TestUpdateAccountsBalances.test_duplicate_records`)
   - SQL error handling (`TestUpdateAccountsBalances.test_sql_error_handling`)

4. **Function Tests for `process_account_data`**:
   - Successful processing (`TestProcessAccountData.test_successful_processing`)
   - API failure handling (`TestProcessAccountData.test_api_failure`)
   - No new data handling (`TestProcessAccountData.test_no_new_data`)
   - Exception handling (`TestProcessAccountData.test_exception_handling`)

5. **Date Format Regex Testing** (`TestUtilities.test_date_format_regex`):
   - Tests valid date formats (YYYY-MM-DD)
   - Tests invalid date formats

### `test_ibkr_api.py`

This file tests the functionality in `analytics/ibkr_api.py`, which handles communication with the Interactive Brokers Flex Web Service API and processes the returned data.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestIBKRAPIImports`**: Tests basic imports and module setup
2. **`TestGetIBKRReport`**: Tests the centralized report retrieval function
3. **`TestCSVProcessing`**: Tests CSV data processing functionality
4. **`TestErrorHandling`**: Tests error handling and edge cases

#### Tested Functions

1. **`get_ibkr_report(token, query_id, report_type)`**: Centralized function that handles report generation, retrieval, and data processing for different report types.

#### Test Cases

The test suite includes the following categories of tests:

1. **Basic Import Tests** (`TestIBKRAPIImports.test_imports`):
   - Verifies that functions are callable

2. **Report Retrieval Tests**:
   - Successful report retrieval (`TestGetIBKRReport.test_successful_report`)
   - Failed report retrieval (`TestGetIBKRReport.test_failed_report`)
   - Exception handling (`TestGetIBKRReport.test_exception_handling`)
   - Report type logging (`TestGetIBKRReport.test_report_type_logging`)

3. **CSV Processing Tests**:
   - Complex CSV processing (`TestCSVProcessing.test_complex_csv_processing`)
   - Single row CSV handling (`TestCSVProcessing.test_single_row_csv`)
   - Invalid CSV response handling (`TestCSVProcessing.test_invalid_csv_response`)
   - Empty DataFrame handling (`TestCSVProcessing.test_empty_dataframe`)

### `test_executions.py`

This file tests the functionality in `analytics/executions.py`, which handles trade execution data processing from IBKR and manages trade identification and position tracking.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestExecutionsImports`**: Tests basic imports and module setup
2. **`TestProcessIBKRData`**: Tests the IBKR data processing function
3. **`TestIdentifyTradeIds`**: Tests trade identification and position tracking
4. **`TestInsertExecutionsToDB`**: Tests database insertion functionality
5. **`TestProcessAccountData`**: Tests the end-to-end processing workflow

#### Tested Functions

1. **`process_ibkr_data(df)`**: Processes raw IBKR data and adds derived fields
2. **`identify_trade_ids(df)`**: Assigns trade IDs based on position tracking
3. **`insert_executions_to_db(df)`**: Inserts processed executions into the database
4. **`process_account_data(token, query_id, account_type)`**: Handles end-to-end workflow

#### Test Cases

1. **Process IBKR Data Tests**:
   - Filtering existing trades (`TestProcessIBKRData.test_filtering_existing_trades`)
   - Numeric field conversion (`TestProcessIBKRData.test_numeric_conversion`)
   - Date/time processing (`TestProcessIBKRData.test_datetime_processing`)
   - Trade side determination (`TestProcessIBKRData.test_side_determination`)

2. **Trade ID Tests**:
   - New position opening (`TestIdentifyTradeIds.test_new_position_opening`)
   - Position closing (`TestIdentifyTradeIds.test_position_closing`)
   - Multiple symbols handling (`TestIdentifyTradeIds.test_multiple_symbols`)

3. **Database Operation Tests**:
   - Successful insertion (`TestInsertExecutionsToDB.test_successful_insertion`)
   - Database error handling (`TestInsertExecutionsToDB.test_database_error`)

4. **End-to-End Processing Tests**:
   - Successful processing (`TestProcessAccountData.test_successful_processing`)
   - API failure handling (`TestProcessAccountData.test_api_failure`)

### `test_db_utils.py`

This file tests the functionality in `data/db_utils.py`, which provides a DatabaseManager class to handle all database operations across the analytics modules.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestDatabaseManagerImports`**: Tests basic imports and module setup
2. **`TestDatabaseManagerBasics`**: Tests basic functionality like connections
3. **`TestAccountBalanceOperations`**: Tests account balance related database operations
4. **`TestExecutionOperations`**: Tests execution related database operations

#### Tested Functions

The test suite tests the following DatabaseManager methods:

1. **Connection Management**:
   - Context manager for database connections
   - Exception handling in connections

2. **General Database Operations**:
   - `execute_query`: Execute a single query
   - `execute_many`: Execute a query with multiple parameter sets
   - `fetch_df`: Fetch query results as a pandas DataFrame
   - `record_exists`: Check if a record exists based on conditions

3. **Account Balance Operations**:
   - `get_account_map`: Get mapping from account_external_ID to ID
   - `check_balance_exists`: Check if a balance record exists
   - `insert_account_balances`: Insert account balance records

4. **Execution Operations**:
   - `get_existing_trade_ids`: Get set of existing trade IDs
   - `get_max_trade_id`: Get the maximum trade ID
   - `get_open_positions`: Get current open positions
   - `insert_execution`: Insert a single execution record

#### Test Cases

The test suite includes the following categories of tests:

1. **Basic Import Tests** (`TestDatabaseManagerImports.test_imports`):
   - Verifies that the DatabaseManager class is importable

2. **Connection Management Tests**:
   - Connection context manager (`TestDatabaseManagerBasics.test_connection_context_manager`)
   - Exception handling in connections (`TestDatabaseManagerBasics.test_connection_exception_handling`)

3. **Account Balance Operations Tests**:
   - Getting account mapping (`TestAccountBalanceOperations.test_get_account_map`)
   - Checking if balance exists (`TestAccountBalanceOperations.test_check_balance_exists`)
   - Inserting account balances (`TestAccountBalanceOperations.test_insert_account_balances`)

4. **Execution Operations Tests**:
   - Getting existing trade IDs (`TestExecutionOperations.test_get_existing_trade_ids`)
   - Getting maximum trade ID (`TestExecutionOperations.test_get_max_trade_id`)
   - Getting open positions (`TestExecutionOperations.test_get_open_positions`)
   - Inserting execution (`TestExecutionOperations.test_insert_execution`)

## Test Framework

The tests use Python's `unittest` framework with mocking provided by the `unittest.mock` module. The test results are tracked and displayed using a custom reporting mechanism that shows a summary of all test cases.