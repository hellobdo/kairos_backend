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

This file tests the functionality in `analytics/broker_executions.py`, which handles trade execution data processing from IBKR and manages trade execution processing and database insertion.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestExecutionsImports`**: Tests basic imports and module setup
2. **`TestProcessIBKRData`**: Tests the IBKR data processing function
3. **`TestInsertExecutionsToDB`**: Tests database insertion functionality
4. **`TestProcessAccountData`**: Tests the end-to-end processing workflow

#### Tested Functions

##### `process_ibkr_data(df)`
- **Case: Filtering existing trades**
  - Tests that already processed executions are filtered out
  - Verifies only new executions proceed to further processing

- **Case: Process datetime fields call**
  - Tests that the process_datetime_fields utility function is called correctly
  - Verifies proper parameter passing and integration

- **Case: Trade side determination**
  - Tests correct identification of BUY/SELL based on quantity sign
  - Verifies proper side assignment for position tracking

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

### `test_column_utils.py`

This file tests the functionality in `indicators/helpers/column_utils.py`, which provides utilities for standardizing and normalizing DataFrame column names for indicator calculations.

#### Test Classes and Organization

The tests are organized into the following class:

1. **`TestColumnUtils`**: Tests column name standardization functionality

#### Tested Functions

##### `normalize_ohlc_columns(df)`
- **Case: Uppercase column names**
  - Tests conversion of uppercase column names to lowercase
  - Verifies all column names like 'Open', 'High', 'Low', 'Close', 'Volume' are properly normalized

- **Case: Mixed case column names**
  - Tests handling of DataFrames with a mix of uppercase and lowercase column names
  - Verifies consistent lowercase conversion regardless of original case

- **Case: Already lowercase column names**
  - Tests that already lowercase column names remain unchanged
  - Verifies no unintended modifications to correctly formatted names

- **Case: Arbitrary column names**
  - Tests handling of non-standard column names
  - Verifies all column names are converted to lowercase regardless of format

### `test_indicators.py`

This file tests the indicator modules in the `indicators` directory, ensuring they all follow a consistent interface and behavior pattern.

#### Test Classes and Organization

The tests are organized into the following class:

1. **`TestIndicators`**: Tests all indicator modules for compliance with standard interface

#### Tested Functionality

##### Indicator Module Compliance
- **Case: calculate_indicator function exists**
  - Tests that each indicator module has a calculate_indicator function
  - Verifies the function signature is consistent across all indicators

- **Case: Returns DataFrame**
  - Tests that each indicator's calculate_indicator function returns a pandas DataFrame
  - Verifies the returned object type for consistent downstream processing

- **Case: is_indicator column exists**
  - Tests that each indicator adds an 'is_indicator' column to the DataFrame
  - Verifies standardized column naming for cross-indicator compatibility

- **Case: Boolean values in is_indicator**
  - Tests that the 'is_indicator' column contains boolean values (True/False)
  - Verifies consistent data type for signal representation across indicators
  - Ensures compatibility with downstream systems expecting boolean signals

##### Compliance Summary
- Produces a list of compliant and non-compliant indicators
- Identifies specific issues with non-compliant indicators (missing functions, wrong return types, etc.)
- Helps maintain consistency across the indicator ecosystem

### `test_backtest_functions.py`

This file tests the `BaseStrategy` class in `backtests/utils/helper_functions.py`, which provides common helper methods for trading strategies.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestBaseStrategyImports`**: Tests basic imports and class hierarchy
2. **`TestIndicatorLoading`**: Tests indicator loading functionality
3. **`TestPositionLimits`**: Tests position limit checking functionality
4. **`TestTimeConditions`**: Tests time condition verification
5. **`TestIndicatorApplication`**: Tests sequential indicator application functionality
6. **`TestQuantityCalculation`**: Tests quantity calculation based on risk parameters
7. **`TestStopLossRules`**: Tests stop loss rule application
8. **`TestPriceLevels`**: Tests price level calculation for different trade sides

#### Tested Functions

##### `_load_indicators(indicators, load_function)`
- **Case: Successful loading**
  - Tests loading indicators with a valid load function
  - Verifies all indicators are correctly loaded and accessible
  - Confirms the load function is called for each indicator

- **Case: Error handling**
  - Tests behavior when the load function raises an exception
  - Verifies graceful handling of errors during indicator loading

##### `_check_position_limits()`
- **Case: Max positions reached**
  - Tests behavior when maximum number of positions is reached
  - Verifies function returns True to prevent opening new positions

- **Case: Daily loss limit reached**
  - Tests behavior when daily loss count equals max limit
  - Verifies function returns True to prevent opening new positions

- **Case: Limits not reached**
  - Tests behavior when neither position count nor daily loss limit is reached
  - Verifies function returns False to allow opening new positions

##### `_check_time_conditions(time)`
- **Case: Minute 0**
  - Tests behavior at the top of the hour (minute 0)
  - Verifies function returns True to allow trading

- **Case: Minute 30**
  - Tests behavior at half-hour mark (minute 30)
  - Verifies function returns True to allow trading

- **Case: Other minutes**
  - Tests behavior at other times
  - Verifies function returns False to prevent trading

##### `_apply_indicators(df, calculate_indicators)`
- **Case: All indicators positive**
  - Tests behavior when all indicators return True
  - Verifies function returns True overall with updated DataFrame

- **Case: One indicator negative**
  - Tests behavior when at least one indicator returns False
  - Verifies function returns False overall with updated DataFrame

- **Case: Short-circuit processing**
  - Tests that processing stops at the first False indicator
  - Verifies subsequent indicator functions are not called

##### `_calculate_qty(stop_loss_amount, risk_per_trade)`
- **Case: Standard calculation**
  - Tests standard quantity calculation based on risk and stop loss
  - Verifies correct integer calculation

- **Case: Different risk values**
  - Tests calculation with different risk percentages
  - Verifies correct handling of various risk parameters

##### `_determine_stop_loss(price, rules)`
- **Case: Price below rule**
  - Tests determining stop loss amount when price is below threshold
  - Verifies correct rule is applied and amount returned

- **Case: Price above rule**
  - Tests determining stop loss amount when price is above threshold
  - Verifies correct rule is applied and amount returned

- **Case: No matching rule**
  - Tests behavior when no rule matches the current price
  - Verifies function returns None when no rule applies

##### `_calculate_price_levels(entry_price, stop_loss_amount, side, risk_reward)`
- **Case: Buy side**
  - Tests calculation of stop loss and take profit levels for buy trades
  - Verifies correct placement of stop below and target above entry price

- **Case: Sell side**
  - Tests calculation of stop loss and take profit levels for sell trades
  - Verifies correct placement of stop above and target below entry price

### `test_process_executions.py`

This file tests the functionality in `backtests/utils/process_executions.py`, which provides utilities for processing execution data, particularly CSV file handling for backtests.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestProcessExecutionsImports`**: Tests basic imports and module setup
2. **`TestDropColumns`**: Tests the column dropping functionality 

#### Tested Functions

##### `drop_columns(df)`
- **Case: Dropping existing columns**
  - Tests removal of specific predefined columns from a DataFrame
  - Verifies columns like 'strategy', 'status', 'multiplier', etc. are successfully dropped
  - Ensures other columns remain intact
  - Confirms proper diagnostic messages are output

- **Case: Handling non-existent columns**
  - Tests behavior when specified columns don't exist in the DataFrame
  - Verifies the DataFrame remains unchanged
  - Ensures proper diagnostic messages are output

##### `process_csv(csv_file)`
- **Case: Successful processing**
  - Tests the full processing pipeline that reads a CSV file with columns to drop, verifies that unwanted columns are removed, and confirms the result is a properly formatted DataFrame.
  - **Case: Processing without columns to drop**: Tests processing a CSV file that doesn't contain any of the predefined columns to drop, ensuring the function handles this case correctly.
  - **Case: CSV loading failure**: Tests that when CSV loading fails (e.g., file not found), the function returns None and properly logs the error message.

### `test_process_executions_utils.py`

This file tests the functionality in `utils/process_executions_utils.py`, which provides utility functions for processing execution data, particularly date/time field handling and trade identification.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestProcessExecutionsUtilsImports`**: Tests basic imports and module setup
2. **`TestProcessDatetimeFields`**: Tests the date/time processing functionality
3. **`TestIdentifyTradeIds`**: Tests trade identification and position tracking functionality

#### Tested Functions

##### `process_datetime_fields(df, datetime_column='Date/Time')`
- **Case: Successful processing**
  - Tests successful splitting of date/time values in the specified format (YYYY-MM-DD;HH:MM:SS)
  - Verifies correct extraction of date and time components into separate columns
  - Confirms creation of the execution_timestamp field as a copy of the input column
  - Ensures the original DataFrame is not modified

- **Case: Invalid format handling**
  - Tests behavior when date/time values are not in the expected format
  - Verifies graceful fallback where the date field gets the full date/time string
  - Confirms the time_of_day field is set to an empty string when parsing fails
  - Ensures proper error messages are logged

- **Case: Missing column handling**
  - Tests behavior when the specified date/time column doesn't exist in the DataFrame
  - Verifies the function returns the original DataFrame unchanged
  - Confirms appropriate warning messages are logged

- **Case: Custom column name**
  - Tests using a different column name than the default 'Date/Time'
  - Verifies the function works correctly with any specified column
  - Confirms the correct column is used for extracting date/time components

##### `identify_trade_ids(df, db_validation=True)`
- **Case: DB validation mode**
  - Tests behavior when using database validation (db_validation=True)
  - Verifies correct handling of current trade IDs and open positions from database
  - Tests proper assignment of trade IDs and position tracking
  
- **Case: Backtest mode**
  - Tests behavior when skipping database validation (db_validation=False)
  - Verifies trade IDs are assigned sequentially starting from 1
  - Tests correct position tracking without database dependency

- **Case: Multiple trades handling**
  - Tests handling multiple sequential trades for the same symbol
  - Verifies proper assignment of new trade IDs for each new position
  - Tests entry and exit identification for consecutive trades

- **Case: Database None value handling**
  - Tests graceful handling when database returns None values
  - Verifies appropriate default values are used
  - Ensures function continues to work correctly with missing database data

### `test_pandas_utils.py`

This file tests the functionality in `utils/pandas_utils.py`, which provides utility functions for pandas DataFrame operations.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestPandasUtilsImports`**: Tests basic imports and module setup
2. **`TestConvertToNumeric`**: Tests the numeric conversion functionality
3. **`TestCsvToDataFrame`**: Tests CSV file conversion to pandas DataFrame

#### Tested Functions

##### `convert_to_numeric(df, numeric_fields)`
- **Case: Basic conversion**
  - Tests conversion of string columns to numeric types
  - Verifies proper handling of integer, float, and mixed columns
  - Confirms non-numeric columns remain unchanged
  - Ensures numeric type conversion is correctly applied

- **Case: Missing columns**
  - Tests behavior when specified columns don't exist in the DataFrame
  - Verifies function gracefully skips non-existent columns
  - Ensures existing columns are still properly converted

- **Case: Empty DataFrame**
  - Tests handling of empty DataFrames
  - Verifies function returns empty DataFrame without errors
  - Confirms column structure is preserved

- **Case: No fields to convert**
  - Tests behavior when no fields are specified for conversion
  - Verifies original DataFrame is returned unchanged

- **Case: All invalid values**
  - Tests conversion of columns with all non-numeric values
  - Verifies proper NaN conversion when all values are invalid
  - Confirms column type is still changed to numeric

- **Case: Already numeric columns**
  - Tests behavior with columns that are already numeric types
  - Verifies no changes are made to already-numeric columns
  - Ensures original values are preserved

##### `csv_to_dataframe(csv_path)`
- **Case: Valid CSV file**
  - Tests reading a valid CSV file into a pandas DataFrame
  - Verifies correct conversion of CSV structure to DataFrame
  - Ensures column names and data values are properly preserved
  - Confirms row count and column count match expected values

- **Case: Error handling**
  - Tests behavior when file doesn't exist or is inaccessible
  - Verifies function returns None when errors occur
  - Confirms appropriate error messages are output for debugging

### `test_analytics/test_process_trades.py`

This file tests the functionality in `analytics/process_trades.py`, which provides utilities for generating trade data from executions and calculating various trade attributes.

#### Test Classes and Organization

The tests are organized into the following classes:

1. **`TestModuleImports`**: Tests basic imports and module setup
2. **`TestGetNumExecutions`**: Tests the execution count calculation functionality
3. **`TestGetStartDate`**: Tests the trade start date extraction functionality
4. **`TestGetStartTime`**: Tests the trade start time extraction functionality
5. **`TestGetSymbols`**: Tests the symbol extraction functionality
6. **`TestProcessTrades`**: Tests the end-to-end trade processing workflow

#### Tested Functions

##### `get_num_executions(executions_df)`
- **Case: Standard executions count**
  - Tests counting executions grouped by trade_id
  - Verifies correct counts for each unique trade_id
  - Ensures the resulting DataFrame has the expected structure

- **Case: Empty DataFrame**
  - Tests behavior with an empty executions DataFrame
  - Verifies function returns an empty DataFrame without errors

##### `get_start_date(executions_df)`
- **Case: Entry execution dates**
  - Tests extracting the first date for each trade_id where is_entry=1
  - Verifies correct date extraction for multiple trades
  - Ensures dates are associated with the correct trade_ids

- **Case: Empty DataFrame**
  - Tests behavior with an empty executions DataFrame
  - Verifies function returns None when no entries are available

##### `get_start_time(executions_df)`
- **Case: Entry execution times**
  - Tests extracting the first time_of_day for each trade_id where is_entry=1
  - Verifies correct time extraction for multiple trades
  - Ensures times are associated with the correct trade_ids

- **Case: Empty DataFrame**
  - Tests behavior with an empty executions DataFrame
  - Verifies function returns None when no entries are available

##### `get_symbols(executions_df)`
- **Case: Symbol extraction**
  - Tests extracting the first symbol for each trade_id
  - Verifies correct symbol extraction for multiple trades
  - Ensures symbols are associated with the correct trade_ids

- **Case: Empty DataFrame**
  - Tests behavior with an empty executions DataFrame
  - Verifies function returns an empty Series without errors

##### `process_trades(executions_df)`
- **Case: Complete trade processing flow**
  - Tests the integration of all component functions to generate a complete trades DataFrame
  - Verifies all required columns are present (trade_id, num_executions, symbol, start_date, start_time)
  - Ensures each trade has the correct values from the constituent functions
  - Verifies proper column structure and data integrity

- **Case: Empty DataFrame**
  - Tests behavior with an empty executions DataFrame
  - Verifies function returns None without errors

## Test Framework

The tests use Python's `unittest` framework with mocking provided by the `unittest.mock` module. The test results are tracked and displayed using a custom reporting mechanism that shows a summary of all test cases.