import sqlite3
import os

def create_indexes():
    # Get the path to the database file
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'kairos.db')
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # List of index creation statements
    index_statements = [
        # trades table indexes
        """CREATE INDEX IF NOT EXISTS idx_trades_symbol 
           ON trades(symbol)""",
        """CREATE INDEX IF NOT EXISTS idx_trades_strategy 
           ON trades(strategy)""",
        """CREATE INDEX IF NOT EXISTS idx_trades_entry_date 
           ON trades(entry_date)""",
        """CREATE INDEX IF NOT EXISTS idx_trades_symbol_entry_date 
           ON trades(symbol, entry_date)""",
        """CREATE INDEX IF NOT EXISTS idx_trades_winning 
           ON trades(winning_trade)""",
           
        # improved_trades table indexes
        """CREATE INDEX IF NOT EXISTS idx_improved_trades_symbol 
           ON improved_trades(symbol)""",
        """CREATE INDEX IF NOT EXISTS idx_improved_trades_strategy 
           ON improved_trades(strategy)""",
        """CREATE INDEX IF NOT EXISTS idx_improved_trades_entry_date 
           ON improved_trades(entry_date)""",
        """CREATE INDEX IF NOT EXISTS idx_improved_trades_symbol_entry_date 
           ON improved_trades(symbol, entry_date)""",
        """CREATE INDEX IF NOT EXISTS idx_improved_trades_winning 
           ON improved_trades(winning_trade)""",
           
        # executions table indexes
        """CREATE INDEX IF NOT EXISTS idx_executions_trade_id 
           ON executions(trade_id)""",
        """CREATE INDEX IF NOT EXISTS idx_executions_account_id 
           ON executions(account_id)""",
        """CREATE INDEX IF NOT EXISTS idx_executions_symbol 
           ON executions(symbol)""",
        """CREATE INDEX IF NOT EXISTS idx_executions_date 
           ON executions(date)""",
           
        # historical_data_30mins table indexes
        """CREATE INDEX IF NOT EXISTS idx_historical_symbol_timestamp 
           ON historical_data_30mins(symbol, timestamp)""",
        """CREATE INDEX IF NOT EXISTS idx_historical_timestamp 
           ON historical_data_30mins(timestamp)""",
           
        # account_size table indexes
        """CREATE INDEX IF NOT EXISTS idx_account_size_date 
           ON account_size(date)""",
        """CREATE INDEX IF NOT EXISTS idx_account_size_account_id 
           ON account_size(account_id)""",
           
        # accounts table indexes
        """CREATE INDEX IF NOT EXISTS idx_accounts_account_id 
           ON accounts(account_id)""",
           
        # strategies table indexes
        """CREATE INDEX IF NOT EXISTS idx_strategies_name 
           ON strategies(name)""",
           
        # entry_type table indexes
        """CREATE INDEX IF NOT EXISTS idx_entry_type_name 
           ON entry_type(name)"""
    ]
    
    # Create each index
    for statement in index_statements:
        try:
            cursor.execute(statement)
            index_name = statement.split('CREATE INDEX IF NOT EXISTS')[1].split('ON')[0].strip()
            print(f"Successfully created index: {index_name}")
        except sqlite3.OperationalError as e:
            print(f"Error creating index: {e}")
    
    # Commit the changes
    conn.commit()
    
    # Analyze the database to optimize the indexes
    cursor.execute("ANALYZE")
    
    # Get index statistics by querying each table
    tables = [
        'trades', 'improved_trades', 'executions', 'historical_data_30mins',
        'account_size', 'accounts', 'strategies', 'entry_type'
    ]
    
    print("\nDatabase Index Summary:")
    total_indexes = 0
    
    print("\nIndexes by table:")
    for table in tables:
        cursor.execute(f"""
            SELECT name FROM sqlite_master 
            WHERE type='index' AND tbl_name=? 
            AND name NOT LIKE 'sqlite_autoindex%'
        """, (table,))
        
        indexes = cursor.fetchall()
        if indexes:
            print(f"\n{table}:")
            for idx in indexes:
                print(f"  - {idx[0]}")
                total_indexes += 1
    
    print(f"\nTotal number of indexes: {total_indexes}")
    
    # Close the connection
    conn.close()
    print("\nIndex creation completed.")

if __name__ == "__main__":
    create_indexes() 