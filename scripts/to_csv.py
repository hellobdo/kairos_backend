import sqlite3
import pandas as pd
import os
import sys

def export_symbols_to_csv(symbols, db_path='data/historical_data.db'):
    # Create output directory if it doesn't exist
    os.makedirs('data/csv', exist_ok=True)
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    
    for symbol in symbols:
        try:
            # Query the table directly using the symbol as table name
            query = f"SELECT * FROM {symbol}"
            df = pd.read_sql_query(query, conn)
            
            if len(df) == 0:
                print(f"No data found for symbol: {symbol}")
                continue
                
            # Save to CSV
            output_file = f'data/csv/{symbol}.csv'
            df.to_csv(output_file, index=False)
            
            # Print summary for this symbol
            print(f"\nData extracted successfully for {symbol}")
            print(f"Output file: {output_file}")
            print(f"Total rows: {len(df)}")
            
        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
            continue
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    # Example usage: python to_csv.py AAPL MSFT GOOGL
    if len(sys.argv) < 2:
        print("Usage: python to_csv.py SYMBOL1 SYMBOL2 ...")
        sys.exit(1)
        
    symbols = sys.argv[1:]
    export_symbols_to_csv(symbols)