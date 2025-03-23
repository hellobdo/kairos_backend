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

### `test_broker_cash.py`

This file tests the functionality in `analytics/cash.py`, which handles cash data processing from Interactive Brokers (IBKR) and updates the database with cash balances.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestCashImports`**: Tests basic imports and environment setup
2. **`TestUpdateAccountsBalances`**: Tests the database update functionality
3. **`TestProcessAccountData`**: Tests the account data processing function
4. **`TestUtilities`**: Tests utility functions like date validation

#### Tested Functions

##### `process_account_data(token, query_id, account_type)`
- **Case: Successful processing**
  - Tests the complete flow of retrieving and processing cash data with valid credentials
  - Verifies correct database updates

- **Case: API failure handling**
  - Tests behavior when the IBKR API call fails
  - Verifies appropriate error messages and status

- **Case: No new data handling**
  - Tests behavior when no new cash data is available
  - Verifies function returns gracefully

- **Case: Exception handling**
  - Tests behavior when unexpected exceptions occur
  - Verifies proper error logging and exception propagation

##### `update_accounts_balances(df)`
- **Case: Successful insertion**
  - Tests inserting valid cash balance data into the database
  - Verifies correct record counts and data integrity

- **Case: Empty DataFrame handling**
  - Tests behavior when an empty DataFrame is provided
  - Verifies function returns early without errors

- **Case: Missing required columns**
  - Tests behavior when DataFrame is missing essential columns
  - Verifies appropriate error messages

- **Case: Duplicate records**
  - Tests handling of duplicate account/date combinations
  - Verifies duplicates are properly skipped

- **Case: SQL error handling**
  - Tests behavior when database operations fail
  - Verifies proper error capture and reporting

##### Date Format Validation
- **Case: Valid date formats**
  - Tests that valid date formats (YYYY-MM-DD) are accepted
  - Verifies regex pattern matching works correctly

- **Case: Invalid date formats**
  - Tests that invalid date formats are rejected
  - Verifies error handling for improperly formatted dates

### `test_ibkr_api.py`

This file tests the functionality in `analytics/ibkr_api.py`, which handles communication with the Interactive Brokers Flex Web Service API and processes the returned data.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestIBKRAPIImports`**: Tests basic imports and module setup
2. **`TestGetIBKRReport`**: Tests the centralized report retrieval function
3. **`TestCSVProcessing`**: Tests CSV data processing functionality
4. **`TestErrorHandling`**: Tests error handling and edge cases

#### Tested Functions

##### `get_ibkr_report(token, query_id, report_type)`
- **Case: Successful report retrieval**
  - Tests retrieving reports with valid credentials and query IDs
  - Verifies correct parsing and data structure

- **Case: Failed report retrieval**
  - Tests behavior when report generation fails
  - Verifies appropriate error handling

- **Case: Exception handling**
  - Tests behavior when unexpected exceptions occur
  - Verifies proper error logging and exception propagation

- **Case: Report type logging**
  - Tests that report type is correctly logged
  - Verifies proper identification of cash vs. executions reports

##### CSV Processing
- **Case: Complex CSV processing**
  - Tests handling of multi-row, multi-column CSV data
  - Verifies correct parsing and structure conversion

- **Case: Single row CSV**
  - Tests processing of CSV with only one row
  - Verifies correct DataFrame creation

- **Case: Invalid CSV response**
  - Tests behavior with malformed CSV data
  - Verifies appropriate error handling

- **Case: Empty DataFrame**
  - Tests handling of cases that result in empty DataFrames
  - Verifies graceful handling without errors

### `test_broker_executions.py`

This file tests the functionality in `analytics/executions.py`, which handles trade execution data processing from IBKR and manages trade identification and position tracking.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestExecutionsImports`**: Tests basic imports and module setup
2. **`TestProcessIBKRData`**: Tests the IBKR data processing function
3. **`TestIdentifyTradeIds`**: Tests trade identification and position tracking
4. **`TestInsertExecutionsToDB`**: Tests database insertion functionality
5. **`TestProcessAccountData`**: Tests the end-to-end processing workflow

#### Tested Functions

##### `process_ibkr_data(df)`
- **Case: Filtering existing trades**
  - Tests that already processed executions are filtered out
  - Verifies only new executions proceed to further processing

- **Case: Numeric field conversion**
  - Tests conversion of string values to appropriate numeric types
  - Verifies proper data typing for calculations

- **Case: Date/time processing**
  - Tests conversion of date/time string representations to proper datetime objects
  - Verifies timezone handling and format consistency

- **Case: Trade side determination**
  - Tests correct identification of BUY/SELL based on quantity sign
  - Verifies proper side assignment for position tracking

##### `identify_trade_ids(df)`
- **Case: New position opening**
  - Tests assignment of new trade IDs for initial positions
  - Verifies trade IDs are unique and properly incremented

- **Case: Position closing**
  - Tests matching of closing executions with existing positions
  - Verifies correct trade ID assignment for exit executions

- **Case: Multiple symbols handling**
  - Tests position tracking across different symbols simultaneously
  - Verifies independent tracking per symbol

##### `insert_executions_to_db(df)`
- **Case: Successful insertion**
  - Tests inserting processed executions into the database
  - Verifies correct record counts and data integrity

- **Case: Database error handling**
  - Tests behavior when database operations fail
  - Verifies proper error capture and reporting

##### `process_account_data(token, query_id, account_type)`
- **Case: Successful processing**
  - Tests the complete flow of retrieving, processing, and storing execution data
  - Verifies all steps execute correctly

- **Case: API failure handling**
  - Tests behavior when the IBKR API call fails
  - Verifies appropriate error messages and status

### `test_db_utils.py`

This file tests the functionality in `data/db_utils.py`, which provides a DatabaseManager class to handle all database operations across the analytics modules.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestDatabaseManagerImports`**: Tests basic imports and module setup
2. **`TestDatabaseManagerBasics`**: Tests basic functionality like connections
3. **`TestAccountBalanceOperations`**: Tests account balance related database operations
4. **`TestExecutionOperations`**: Tests execution related database operations
5. **`TestTradeAnalysisOperations`**: Tests trade analysis related database operations

#### Tested Functions

##### Connection Management
- **Case: Connection context manager**
  - Tests that context manager correctly opens and closes connections
  - Verifies resource cleanup

- **Case: Exception handling**
  - Tests behavior when connection or query errors occur
  - Verifies proper exception propagation and resource cleanup

##### `execute_query`
- **Case: Simple query execution**
  - Tests execution of basic SQL queries
  - Verifies correct results returned

- **Case: Parametrized queries**
  - Tests query execution with parameters
  - Verifies SQL injection protection

- **Case: Error handling**
  - Tests behavior when query fails
  - Verifies proper error reporting

##### `execute_many`
- **Case: Batch execution**
  - Tests execution of multiple parameter sets in a single query
  - Verifies performance optimization for bulk operations

- **Case: Empty parameter list**
  - Tests behavior with empty list of parameters
  - Verifies graceful handling

##### `fetch_df`
- **Case: Query to DataFrame conversion**
  - Tests conversion of query results to pandas DataFrame
  - Verifies column names and data types

- **Case: Empty result set**
  - Tests behavior when query returns no rows
  - Verifies empty DataFrame creation

##### Account Balance Operations
- **Case: get_account_map**
  - Tests mapping between external and internal account IDs
  - Verifies correct ID translation

- **Case: check_balance_exists**
  - Tests checking for existing balance records
  - Verifies detection of duplicates

- **Case: insert_account_balances**
  - Tests insertion of new balance records
  - Verifies data integrity constraints

##### Execution Operations
- **Case: get_max_trade_id**
  - Tests finding the maximum trade ID in the database
  - Verifies correct value for new ID allocation

- **Case: get_open_positions**
  - Tests retrieval of current open positions
  - Verifies accurate position accounting

- **Case: insert_execution**
  - Tests insertion of execution records
  - Verifies all fields stored correctly

##### Backtest Operations
- **Case: get_backtest_runs**
  - Tests retrieving backtest runs with various filtering options
  - Verifies correct SQL queries are generated for different filter combinations
  - Tests filtering by run_id, symbol, and direction
  - Tests filtering by is_valid boolean flag for backtest run validation status
  - Verifies both DataFrame and dictionary list return formats
  - Tests proper SQL parameters are passed to prevent injection

- **Case: save_to_backtest_runs**
  - Tests saving new backtest run data to the database
  - Verifies correct SQL INSERT query is generated
  - Tests that provided data is properly passed to the database
  - Verifies proper handling of the is_valid field for backtest validation status
  - Verifies proper transaction commit is performed
  - Confirms the function returns the correct run_id from the database

### `test_backtest_executions.py`

This file tests the functionality in `analytics/backtest_executions.py`, which processes trades from backtest executions, identifies complete trades, and generates performance reports.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestBacktestExecutionsImports`**: Tests basic imports and module setup
2. **`TestProcessBacktestExecutions`**: Tests the main processing function with different scenarios
3. **`TestCleanBacktestExecutions`**: Tests the data cleaning and preparation function

#### Tested Functions

##### `process_backtest_executions(strategy, file_path)`
- **Case: import availability**
  - Tests that the function is importable and callable
  - Verifies basic module setup

- **Case: strategy parameter extraction**
  - Tests that parameters are correctly extracted from the strategy object
  - Verifies parameters like 'side' and 'risk_reward' are properly accessed
  - Ensures the extracted parameters are correctly printed to the output

- **Case: missing strategy side parameter**
  - Tests behavior when the 'side' parameter is missing from strategy parameters
  - Verifies the function returns False and prints an appropriate error message
  - Confirms the exact error message format matches expectations

- **Case: strategy side assignment**
  - Tests that the strategy_side variable is correctly assigned from strategy_params['side']
  - Verifies the assigned value is correctly passed to downstream functions
  - Uses a custom strategy with 'sell' side to distinguish from default test cases

##### `clean_backtest_executions(file_path)`
- **Case: CSV file reading**
  - Tests that the function can read from a CSV file
  - Verifies pandas.read_csv is called with the correct file path
  - Confirms returned DataFrame contains the expected data

- **Case: time column slicing**
  - Tests that timestamps are correctly truncated to 19 characters
  - Verifies the function handles both long timestamps and standard-length ones
  - Confirms the sliced timestamps have the correct format

- **Case: date and time column creation**
  - Tests that 'date' and 'time_of_day' columns are correctly created from timestamps
  - Verifies these columns have the correct data types
  - Confirms the values match the original timestamp data

- **Case: column renaming**
  - Tests that the 'time' column is correctly renamed to 'execution_timestamp'
  - Verifies the original column no longer exists and the new one is present

- **Case: zero quantity filtering**
  - Tests that trades with zero quantity are properly filtered out
  - Verifies rejected trades are stored separately with appropriate rejection reasons
  - Confirms the filtered DataFrame only contains valid quantity trades
  - Checks that the correct information is printed to stdout

- **Case: rejected trades merging**
  - Tests that multiple rejected trades are correctly combined into a single DataFrame
  - Verifies rejection reasons are properly assigned
  - Confirms all rejected trades are included in the result

- **Case: return value structure**
  - Tests that the function returns a tuple containing exactly two DataFrames
  - Verifies both DataFrames have the expected structure and content
  - Confirms valid trades and rejected trades are correctly separated

### `test_get_latest_trade_report.py`

This file tests the functionality in `backtests/helpers/utils/get_latest_trade_report.py`, which provides a utility function to find the most recent trade report file in the logs directory.

#### Test Classes and Organization

The tests are organized into the `TestGetLatestTradeReport` class with several test cases that verify different aspects of the function:

1. **Import Tests**: Test that the module imports correctly
2. **Type Validation Tests**: Test that the function validates input types correctly
3. **Logs Directory Handling**: Test how the function behaves when the logs directory doesn't exist
4. **File Finding Tests**: Test the function's ability to find and return the most recent file

#### Tested Functions

##### `get_latest_trade_report(type)`
- **Case: import availability**
  - Tests that the function is importable and callable
  - Verifies basic module setup

- **Case: invalid report type**
  - Tests behavior with invalid report type ("pdf" instead of "html" or "csv")
  - Verifies appropriate ValueError is raised with correct message

- **Case: missing type parameter**
  - Tests behavior when no type parameter is provided
  - Verifies appropriate TypeError is raised

- **Case: HTML type**
  - Tests finding HTML report files using the correct glob pattern
  - Verifies the function returns the newest file based on timestamp
  - Checks that appropriate information is printed to stdout

- **Case: CSV type**
  - Tests finding CSV report files using the correct glob pattern
  - Verifies the function returns the newest file based on timestamp
  - Checks that appropriate information is printed to stdout

- **Case: logs directory not found**
  - Tests behavior when the logs directory doesn't exist
  - Verifies appropriate FileNotFoundError is raised
  - Verifies error message contains reference to the logs directory

- **Case: no files found**
  - Tests behavior when no files match the specified pattern
  - Verifies appropriate FileNotFoundError is raised
  - Verifies error message mentions the file type that wasn't found

- **Case: newest file selection**
  - Tests function's ability to select the newest file based on timestamp
  - Verifies the function correctly identifies the newest file regardless of the order in which files are discovered
  - Tests both HTML and CSV file types


## Test Framework

The tests use Python's `unittest` framework with mocking provided by the `unittest.mock` module. The test results are tracked and displayed using a custom reporting mechanism that shows a summary of all test cases.