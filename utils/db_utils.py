import sqlite3
import pandas as pd
from contextlib import contextmanager
import os

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
    
    def fetch_df(self, query, params=None):
        """Fetch query results as a pandas DataFrame"""
        with self.connection() as conn:
            return pd.read_sql(query, conn, params=params)
        
    def select_distinct(self, table, column):
        """Select distinct values from a table"""
        query = f"SELECT DISTINCT {column} FROM {table}"
        with self.connection() as conn:
            return pd.read_sql(query, conn)
    
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
    
    def get_max_id(self,table,column):
        """Get the maximum id from a table"""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT MAX({column}) FROM {table}")
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
            
    def insert_dataframe(self, df, table_name, if_exists='append', index=False, update_existing=False, id_field=None, **kwargs):
        """
        Insert a pandas DataFrame into a database table.
        
        Args:
            df (pandas.DataFrame): DataFrame to insert
            table_name (str): Name of the target database table
            if_exists (str): How to behave if the table already exists:
                             'fail', 'replace', or 'append' (default: 'append')
            index (bool): Whether to include the DataFrame's index (default: False)
            update_existing (bool): Whether to update existing records (default: False)
            id_field (str): The column to use as unique identifier for updates (required if update_existing=True)
            **kwargs: Additional arguments to pass to pandas.to_sql
            
        Returns:
            int: Number of records inserted or updated
        """
        if update_existing and id_field is None:
            raise ValueError("id_field must be specified when update_existing is True")
        
        try:
            # If we're not updating existing records, just insert normally
            if not update_existing:
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
            
            # For updates, we need to handle each record individually
            with self.connection() as conn:
                cursor = conn.cursor()
                
                # Get list of all column names except the ID field
                columns = [col for col in df.columns if col != id_field]
                
                # Build SQL for UPDATE statement
                set_clause = ", ".join([f"{col} = ?" for col in columns])
                
                # Process each row for update or insert
                records_modified = 0
                
                for _, row in df.iterrows():
                    # Check if record exists
                    id_value = row[id_field]
                    exists_query = f"SELECT 1 FROM {table_name} WHERE {id_field} = ?"
                    cursor.execute(exists_query, (id_value,))
                    
                    if cursor.fetchone():
                        # Update existing record
                        update_query = f"UPDATE {table_name} SET {set_clause} WHERE {id_field} = ?"
                        values = [row[col] for col in columns] + [id_value]
                        cursor.execute(update_query, values)
                    else:
                        # Insert new record
                        cols = ", ".join(df.columns)
                        placeholders = ", ".join(["?"] * len(df.columns))
                        insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
                        values = [row[col] for col in df.columns]
                        cursor.execute(insert_query, values)
                    
                    records_modified += 1
                
                return records_modified
                
        except Exception as e:
            print(f"Error inserting DataFrame into {table_name}: {e}")
            raise
            
    def get_table_data(self, table, order_by='timestamp'):
        """
        Retrieve all records from a specified table ordered by timestamp.
        
        Args:
            table (str): Name of the database table to query
        
        Returns:
            pandas.DataFrame: DataFrame containing all records from the specified table
        """
        query = f"SELECT * FROM {table} ORDER BY {order_by}"
        
        try:
            return self.fetch_df(query)
        except Exception as e:
            print(f"Error retrieving data from {table}: {e}")
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