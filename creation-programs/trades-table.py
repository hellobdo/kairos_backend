import sqlite3

# Connect to SQLite (creates a file if it doesn’t exist)
conn = sqlite3.connect("../kairos.db")
cursor = conn.cursor() # creates the cursor, the intermediary between python and the DB

# creates executions table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        trade_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        strategy TEXT NOT NULL,
        entry_type TEXT NOT NULL,
        direction TEXT NOT NULL,
        entry_date TEXT NOT NULL,
        instrument_type TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        entry_price REAL NOT NULL,
        stop_price REAL NOT NULL,
        exit_price REAL NOT NULL,
        exit_date TEXT NOT NULL,
        capital_required REAL NOT NULL,
        trade_duration REAL NOT NULL,
        winning_trade INTEGER NOT NULL,
        risk_reward REAL NOT NULL,
        risk_per_trade REAL NOT NULL,
        perc_return REAL NOT NULL
    )
""")


# Insert the schema details for the trades table
cursor.execute("""
    INSERT INTO schema (table_name, column_name, data_type, constraints, description)
    VALUES
    ('trades', 'trade_id', 'INTEGER', 'NOT NULL', 'Unique identifier for each trade'),
    ('trades', 'symbol', 'TEXT', 'NOT NULL', 'The symbol of the asset being traded'),
    ('trades', 'strategy', 'TEXT', 'NOT NULL', 'The strategy used for the trade'),
    ('trades', 'entry_type', 'TEXT', 'NOT NULL', 'The entry type, either confirmation or anticipation. Confirmation waits for the candle to close and enters usually with market orders. Anticipation enters with conditional orders and may or might not wait for the candle to close'),
    ('trades', 'direction', 'TEXT', 'NOT NULL', 'The direction of the trade bullish or bearish'),
    ('trades', 'entry_date', 'TEXT', 'NOT NULL', 'Date and time when the trade was opened'),
    ('trades', 'instrument_type', 'TEXT', 'NOT NULL', 'The type of instrument being traded (e.g., stock, option)'),
    ('trades', 'quantity', 'INTEGER', 'NOT NULL', 'The number of shares/contracts traded'),
    ('trades', 'entry_price', 'REAL', 'NOT NULL', 'Price at which the trade was entered'),
    ('trades', 'stop_price', 'REAL', 'NOT NULL', 'The stop loss price for the trade'),
    ('trades', 'exit_price', 'REAL', 'NOT NULL', 'The price at which the trade was exited'),
    ('trades', 'exit_date', 'TEXT', 'NOT NULL', 'Date and time when the trade was exited'),
    ('trades', 'capital_required', 'REAL', 'NOT NULL', 'The capital required to execute the trade'),
    ('trades', 'trade_duration', 'REAL', 'NOT NULL', 'The duration of the trade in hours'),
    ('trades', 'winning_trade', 'INTEGER', 'NOT NULL', 'Flag indicating whether the trade was a winning trade (1 for win, 0 for loss)'),
    ('trades', 'risk_reward', 'REAL', 'NOT NULL', 'The risk/reward ratio for the trade'),
    ('trades', 'risk_per_trade', 'REAL', 'NOT NULL', 'The risk amount per trade as a percentage of the account size'),
    ('trades', 'perc_return', 'REAL', 'NOT NULL', 'The percentage return on the trade')
""")


conn.commit()
conn.close()