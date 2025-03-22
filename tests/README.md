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
  - Verifies both DataFrame and dictionary list return formats
  - Tests proper SQL parameters are passed to prevent injection

- **Case: save_to_backtest_runs**
  - Tests saving new backtest run data to the database
  - Verifies correct SQL INSERT query is generated
  - Tests that provided data is properly passed to the database
  - Verifies proper transaction commit is performed
  - Confirms the function returns the correct run_id from the database

### `test_trade_analysis_utils.py`

This file tests the functionality in `analytics/trade_analysis_utils.py`, which provides a TradeAnalysis class for analyzing trade executions and generating trade metrics.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestTradeAnalysisImports`**: Tests basic imports and class setup
2. **`TestTradeAnalysisInit`**: Tests initialization and cash balance setting
3. **`TestTradeAnalysisDataPreparation`**: Tests data preparation methods
4. **`TestTradeAnalysisCashBalance`**: Tests cash balance retrieval methods
5. **`TestTradeAnalysisTradeExecution`**: Tests trade execution processing
6. **`TestTradeAnalysisRiskCalculations`**: Tests risk calculation methods
7. **`TestTradeAnalysisTradeMetrics`**: Tests trade metrics calculation
8. **`TestTradeAnalysisTradeFiltering`**: Tests filtering of trades based on status

#### Tested Functions

##### Initialization
- **Case: Class initialization**
  - Tests that the class initializes with cash_balances_df set to None

##### `set_cash_balances`
- **Case: Valid cash balance data**
  - Tests setting valid cash balance DataFrame
  - Verifies data is correctly stored

- **Case: Empty DataFrame**
  - Tests setting empty cash balance DataFrame
  - Verifies appropriate warning message

##### `get_account_cash_balance`
- **Case: Exact date match**
  - Tests retrieving cash balance with exact date match
  - Verifies correct balance returned

- **Case: No exact date match**
  - Tests behavior when no exact date match exists
  - Verifies appropriate error raised

- **Case: Future date**
  - Tests requesting balance for future date
  - Verifies appropriate error raised

- **Case: Past date with data**
  - Tests retrieving balance for past date with data
  - Verifies correct historical balance returned

- **Case: String date input**
  - Tests using string date format as input
  - Verifies correct date parsing and balance retrieval

- **Case: Multiple entries for same date**
  - Tests behavior when multiple entries exist for same date
  - Verifies first entry used with warning

- **Case: Invalid account ID**
  - Tests requesting balance for non-existent account
  - Verifies appropriate error raised

- **Case: Cash balances not set**
  - Tests behavior when cash_balances_df is None
  - Verifies appropriate error raised

- **Case: Only account_id provided**
  - Tests providing only account_id without date
  - Verifies appropriate error raised

- **Case: Only date provided**
  - Tests providing only date without account_id
  - Verifies appropriate error raised

##### `prepare_executions_data`
- **Case: Empty DataFrame**
  - Tests behavior with empty executions DataFrame
  - Verifies empty DataFrame returned

- **Case: Valid data merging**
  - Tests merging execution data with account info
  - Verifies correct join operation

- **Case: Missing cash balances**
  - Tests behavior when cash balances not set
  - Verifies appropriate error handling

##### `process_trade_entry`
- **Case: Valid entry data**
  - Tests extraction of entry data from executions
  - Verifies all fields correctly extracted

- **Case: Missing entry**
  - Tests behavior when no entry execution found
  - Verifies appropriate error reporting

##### `process_trade_exit`
- **Case: Valid exit data**
  - Tests extraction of exit data from executions
  - Verifies all fields correctly extracted

- **Case: Missing exit**
  - Tests behavior when no exit execution found
  - Verifies appropriate handling for open trades

##### `calculate_stop_price_based_on_risk_perc`
- **Case: BUY side calculation**
  - Tests stop price calculation for BUY trades
  - Verifies correct risk-based stop price

- **Case: SELL side calculation**
  - Tests stop price calculation for SELL trades
  - Verifies correct risk-based stop price

- **Case: Missing account_id**
  - Tests behavior with missing account_id
  - Verifies appropriate error raised

- **Case: Missing execution date**
  - Tests behavior with missing execution date
  - Verifies appropriate error raised

- **Case: Account not in cash balances**
  - Tests behavior with invalid account ID
  - Verifies appropriate error raised

- **Case: Cash balances not set**
  - Tests behavior when cash_balances_df is None
  - Verifies appropriate error raised

##### `calculate_risk_metrics`
- **Case: Open trade**
  - Tests metrics calculation for open trades
  - Verifies appropriate null values for unrealized metrics

- **Case: Closed winning BUY trade**
  - Tests metrics for profitable BUY trades
  - Verifies correct risk/reward and percentage return

- **Case: Closed losing SELL trade**
  - Tests metrics for unprofitable SELL trades
  - Verifies correct negative metrics

##### `calculate_trade_metrics`
- **Case: Complete trade**
  - Tests metrics calculation for trades with entry and exit
  - Verifies all metrics correctly calculated

- **Case: Missing entry**
  - Tests behavior when entry execution missing
  - Verifies appropriate error handling

- **Case: Open trade**
  - Tests metrics for trades with entry but no exit
  - Verifies appropriate status and blank exit fields

##### `create_trades_summary`
- **Case: Multiple trades**
  - Tests creation of summary DataFrame for multiple trades
  - Verifies correct aggregation and metrics

- **Case: Empty executions**
  - Tests behavior with empty executions DataFrame
  - Verifies empty summary returned

- **Case: Error handling during processing**
  - Tests behavior when errors occur during individual trade processing
  - Verifies other trades still processed

##### `filter_out_closed_trade_executions`
- **Case: Mixed closed and open trades**
  - Tests filtering executions to remove closed trades
  - Verifies only open trade executions remain

- **Case: Empty DataFrames**
  - Tests behavior with empty input DataFrames
  - Verifies graceful handling

- **Case: No closed trades**
  - Tests behavior when no trades are closed
  - Verifies all executions returned

## Test Framework

The tests use Python's `unittest` framework with mocking provided by the `unittest.mock` module. The test results are tracked and displayed using a custom reporting mechanism that shows a summary of all test cases.