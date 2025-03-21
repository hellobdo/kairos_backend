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
        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
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
    
    # Cash module specific methods
    def get_account_map(self):
        """Get mapping from account_external_ID to ID"""
        return self.fetch_df("SELECT ID, account_external_ID FROM accounts")
    
    def check_balance_exists(self, account_id, date):
        """Check if a balance record exists for account and date"""
        return self.record_exists('accounts_balances', {
            'account_ID': account_id,
            'date': date
        })
    
    def insert_account_balances(self, balances_data):
        """Insert account balance records"""
        return self.execute_many("""
            INSERT INTO accounts_balances 
            (account_ID, date, cash_balance, record_date) 
            VALUES (?, ?, ?, ?)
        """, balances_data)
    
    # Executions module specific methods
    def get_existing_trade_ids(self):
        """Get set of existing trade_external_IDs"""
        cursor = self.execute_query("SELECT trade_external_ID FROM executions")
        return {row[0] for row in cursor.fetchall()}
    
    def get_max_trade_id(self):
        """Get the maximum trade_id from the executions table"""
        cursor = self.execute_query("SELECT MAX(trade_id) FROM executions")
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0
    
    def get_open_positions(self):
        """Get current open positions from the database"""
        query = """
            WITH position_sums AS (
                SELECT 
                    symbol,
                    SUM(quantity) as total_quantity,
                    MAX(trade_id) as latest_trade_id
                FROM executions
                GROUP BY symbol
                HAVING SUM(quantity) != 0
            )
            SELECT symbol, total_quantity, latest_trade_id
            FROM position_sums
        """
        cursor = self.execute_query(query)
        return cursor.fetchall()
    
    def insert_execution(self, execution_data):
        """Insert a single execution record"""
        query = """
            INSERT INTO executions (
                accountId, trade_external_ID, orderID, symbol, quantity, 
                price, netCashWithBillable, execution_timestamp, commission,
                date, time_of_day, side, trade_id, is_entry, is_exit
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        return self.execute_query(query, execution_data).rowcount 