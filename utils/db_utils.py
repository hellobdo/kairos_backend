import sqlite3
import pandas as pd
from contextlib import contextmanager
import os
from pathlib import Path

class DatabaseManager:
    """
    A utility class to manage database operations across the analytics modules.
    Provides a consistent interface for database interactions.
    """
    
    def __init__(self, db_path='data/kairos.db'):
        """Initialize with the database path"""
        self.db_path = db_path
        self._ensure_db_exists()
    
    def _ensure_db_exists(self):
        """Make sure the database directory exists"""
        # Skip directory creation for in-memory database
        if self.db_path == ':memory:':
            return
            
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
    
    @contextmanager
    def connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query, params=None):
        """Execute a query and return the cursor"""
        with self.connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
    
    def execute_many(self, query, params_list):
        """Execute a query with multiple parameter sets"""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            return cursor.rowcount
    
    def fetch_df(self, query, params=None):
        """Fetch query results as a pandas DataFrame"""
        with self.connection() as conn:
            return pd.read_sql(query, conn, params=params)
    
    def record_exists(self, table, conditions):
        """Check if a record exists based on conditions"""
        query = f"SELECT 1 FROM {table} WHERE "
        clauses = []
        params = []
        
        for column, value in conditions.items():
            clauses.append(f"{column} = ?")
            params.append(value)
            
        query += " AND ".join(clauses)
        result = self.fetch_df(query, params)
        return not result.empty
    
    def get_account_map(self):
        """Get mapping from account_external_id to id"""
        return self.fetch_df("SELECT id, account_external_id FROM accounts")
    
    def check_balance_exists(self, account_id, date):
        """Check if a balance record exists for account and date"""
        return self.record_exists('accounts_balances', {
            'account_id': account_id,
            'date': date
        })
    
    def get_existing_trade_external_ids(self):
        """Get set of existing trade_external_ids"""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT execution_external_id FROM executions")
        return {row[0] for row in cursor.fetchall()}
    
    def get_max_trade_id(self):
        """Get the maximum trade_id from the executions table"""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(trade_id) FROM executions")
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0
    
    def get_open_positions(self):
        """Get current open positions from the executions table based on sum of quantities"""
        query = """
            SELECT symbol, SUM(quantity) as quantity, trade_id 
            FROM executions 
            GROUP BY symbol, trade_id
            HAVING SUM(quantity) != 0
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
        return cursor.fetchall()
    
    def save_to_backtest_runs(self, data):
        """
        Save mapped data to the backtest_runs table.
        
        Args:
            data (dict): Dictionary containing the mapped data
            
        Returns:
            int: The ID of the inserted run
        """
        insert_sql = """
        INSERT INTO backtest_runs (
            timestamp, indicators, symbols_traded, direction,
            stop_loss, risk_reward, risk_per_trade,
            backtest_start_date, backtest_end_date, source_file, is_valid
        ) VALUES (
            :timestamp, :indicators, :symbols_traded, :direction,
            :stop_loss, :risk_reward, :risk_per_trade,
            :backtest_start_date, :backtest_end_date, :source_file, :is_valid
        )
        """
        
        with self.connection() as conn:
            cursor = conn.cursor()
            
            # Insert new run
            cursor.execute(insert_sql, data)
            run_id = cursor.lastrowid
            conn.commit()
            return run_id
    
    def get_backtest_runs(self, run_id=None, symbol=None, direction=None, is_valid=None, as_df=True):
        """
        Retrieve backtest runs from the database with optional filtering.
        
        Args:
            run_id (int, optional): Filter by specific run ID
            symbol (str, optional): Filter by symbol traded 
            direction (str, optional): Filter by direction (long/short)
            is_valid (bool, optional): Filter by is_valid (True/False)
            as_df (bool, optional): Return results as pandas DataFrame if True, list of dicts if False
            
        Returns:
            If as_df=True: pandas DataFrame containing the backtest runs
            If as_df=False: List of dictionaries containing the backtest runs
        """
        query = "SELECT * FROM backtest_runs"
        params = []
        conditions = []
        
        # Add filters
        if run_id:
            conditions.append("run_id = ?")
            params.append(run_id)
        
        if symbol:
            conditions.append("symbols_traded LIKE ?")
            params.append(f"%{symbol}%")
        
        if direction:
            conditions.append("direction = ?")
            params.append(direction)
        
        if is_valid is not None:
            conditions.append("is_valid = ?")
            params.append(is_valid)
        
        # Add WHERE clause if needed
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Add sorting and limit
        query += " ORDER BY timestamp DESC"
        
        # Return as DataFrame if requested
        if as_df:
            return self.fetch_df(query, params)
        
        # Otherwise return as list of dictionaries
        with self.connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
            
    def insert_dataframe(self, df, table_name, if_exists='append', index=False, **kwargs):
        """
        Insert a pandas DataFrame into a database table.
        
        Args:
            df (pandas.DataFrame): DataFrame to insert
            table_name (str): Name of the target database table
            if_exists (str): How to behave if the table already exists:
                             'fail', 'replace', or 'append' (default: 'append')
            index (bool): Whether to include the DataFrame's index (default: False)
            **kwargs: Additional arguments to pass to pandas.to_sql
            
        Returns:
            int: Number of records inserted
        """
        try:
            with self.connection() as conn:
                df.to_sql(
                    table_name, 
                    conn, 
                    if_exists=if_exists,
                    index=index,
                    method='multi',
                    **kwargs
                )
                return len(df)
        except Exception as e:
            print(f"Error inserting DataFrame into {table_name}: {e}")
            raise

    def get_backtest_executions(self):
        """
        Retrieve all records from the backtest_executions table.
        
        Returns:
            pandas.DataFrame: DataFrame containing all backtest executions
        """
        query = "SELECT * FROM backtest_executions ORDER BY execution_timestamp"
        try:
            return self.fetch_df(query)
        except Exception as e:
            print(f"Error retrieving backtest executions: {e}")
            return pd.DataFrame()
            
    def get_executions(self):
        """
        Retrieve all records from the executions table.
        
        Returns:
            pandas.DataFrame: DataFrame containing all broker executions
        """
        query = "SELECT * FROM executions ORDER BY execution_timestamp"
        try:
            return self.fetch_df(query)
        except Exception as e:
            print(f"Error retrieving executions: {e}")
            return pd.DataFrame()
            
    def get_trades(self):
        """
        Retrieve all records from the trades table.
        
        Returns:
            pandas.DataFrame: DataFrame containing all broker trades
        """
        query = "SELECT * FROM trades ORDER BY entry_timestamp"
        try:
            return self.fetch_df(query)
        except Exception as e:
            print(f"Error retrieving trades: {e}")
            return pd.DataFrame()
            
    def get_backtest_trades(self):
        """
        Retrieve all records from the backtest_trades table.
        
        Returns:
            pandas.DataFrame: DataFrame containing all backtest trades
        """
        query = "SELECT * FROM backtest_trades ORDER BY entry_timestamp"
        try:
            return self.fetch_df(query)
        except Exception as e:
            print(f"Error retrieving backtest trades: {e}")
            return pd.DataFrame()
            
    def get_account_balances(self):
        """
        Retrieve all records from the accounts_balances table.
        
        Returns:
            pandas.DataFrame: DataFrame containing all account balance records
        """
        query = "SELECT * FROM accounts_balances ORDER BY date"
        try:
            return self.fetch_df(query)
        except Exception as e:
            print(f"Error retrieving account balances: {e}")
            return pd.DataFrame()