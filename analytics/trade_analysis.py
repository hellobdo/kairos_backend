import pandas as pd
import sqlite3
from datetime import datetime

def get_all_executions():
    """
    Get all executions from the database with proper type conversions.
    
    Returns:
        pandas.DataFrame: All executions with proper data types
    """
    conn = sqlite3.connect('data/kairos.db')
    
    try:
        # Simple query to get all executions
        query = "SELECT * FROM executions ORDER BY execution_timestamp"
        
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("No executions found in database")
            return pd.DataFrame()
        
        # Convert numeric fields
        numeric_fields = ['quantity', 'price', 'netCashWithBillable', 'commission', 'open_volume']
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # Convert boolean fields
        boolean_fields = ['is_entry', 'is_exit']
        for field in boolean_fields:
            if field in df.columns:
                df[field] = df[field].astype(bool)
        
        # Convert timestamp fields
        if 'execution_timestamp' in df.columns:
            df['execution_timestamp'] = pd.to_datetime(df['execution_timestamp'])
        
        print(f"Retrieved {len(df)} executions from database")
        return df
        
    finally:
        conn.close()

def mark_closed_trades_as_processed():
    """
    Get all closed trades from the trades table, then mark their executions
    as processed by clearing their trade_ids. This prevents reprocessing of
    executions that are already part of completed trades.
    
    Returns:
        int: Number of executions marked as processed
    """
    conn = sqlite3.connect('data/kairos.db')
    cursor = conn.cursor()
    
    try:
        # Get all closed trade ids (trades with status='closed')
        cursor.execute("""
            SELECT trade_id FROM trades 
            WHERE status = 'closed'
        """)
        
        closed_trade_ids = [row[0] for row in cursor.fetchall()]
        
        if not closed_trade_ids:
            print("No closed trades found in the database")
            return 0
            
        print(f"Found {len(closed_trade_ids)} closed trades")
        
        # Create a placeholder string for the SQL IN clause
        placeholders = ','.join(['?'] * len(closed_trade_ids))
        
        # Count how many executions will be marked as processed
        cursor.execute(f"""
            SELECT COUNT(*) FROM executions 
            WHERE trade_id IN ({placeholders})
        """, closed_trade_ids)
        
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("No executions found for closed trades")
            return 0
            
        print(f"Marking {count} executions as processed...")
        
        # Update the executions to clear their trade_id
        cursor.execute(f"""
            UPDATE executions 
            SET trade_id = NULL, is_entry = 0, is_exit = 0
            WHERE trade_id IN ({placeholders})
        """, closed_trade_ids)
        
        conn.commit()
        
        print(f"Successfully marked {count} executions as processed")
        return count
        
    except Exception as e:
        conn.rollback()
        print(f"Error marking closed trades as processed: {e}")
        return 0
        
    finally:
        conn.close()

def create_trades_summary(risk_per_trade=0.005, mark_processed=True):
    """
    Create a summary DataFrame of trades from raw executions data.
    Groups executions by trade_id and calculates various metrics.
    
    Args:
        risk_per_trade (float): Risk per trade for percentage return calculation. Defaults to 0.005 (0.5%)
        mark_processed (bool): Whether to mark executions from closed trades as processed. Defaults to True.
    
    Returns:
        pandas.DataFrame: Summary of trades with calculated metrics
    """
    # Optionally mark executions from closed trades as processed
    if mark_processed:
        mark_closed_trades_as_processed()
    
    # Get raw executions
    executions = get_all_executions()
    if executions.empty:
        return pd.DataFrame()
        
    # Get account IDs mapping
    conn = sqlite3.connect('data/kairos.db')
    accounts_df = pd.read_sql_query("SELECT ID, account_external_ID FROM accounts", conn)
    conn.close()
    
    # Merge with accounts to get internal account ID
    executions = executions.merge(
        accounts_df,
        left_on='accountId',
        right_on='account_external_ID',
        how='left'
    )
    
    # Get the most recent cash balance for each account
    conn = sqlite3.connect('data/kairos.db')
    try:
        # Query to get most recent cash balance for each account
        cash_balances_query = """
            SELECT ab.account_ID, ab.date, ab.cash_balance 
            FROM accounts_balances ab
            INNER JOIN (
                SELECT account_ID, MAX(date) as max_date
                FROM accounts_balances
                GROUP BY account_ID
            ) latest ON ab.account_ID = latest.account_ID AND ab.date = latest.max_date
        """
        
        # Load cash balances into DataFrame
        cash_balances_df = pd.read_sql_query(cash_balances_query, conn)
        
        if not cash_balances_df.empty:
            print(f"Found cash balances for {len(cash_balances_df)} accounts")
            for _, row in cash_balances_df.iterrows():
                print(f"Account ID: {row['account_ID']}, Date: {row['date']}, Cash Balance: ${row['cash_balance']:,.2f}")
        else:
            print("ERROR: No cash balances found in the database")
            print("Please add cash balances before running trade analysis")
            return pd.DataFrame()  # Return empty DataFrame to indicate error
            
        # Merge with executions to add cash balance (optional)
        if 'ID' in executions.columns and not cash_balances_df.empty:
            executions = executions.merge(
                cash_balances_df,
                left_on='ID',
                right_on='account_ID',
                how='left',
                suffixes=('', '_cash')
            )
            
            # Fill missing cash balances with 0 or a default value
            if 'cash_balance' in executions.columns:
                executions['cash_balance'] = executions['cash_balance'].fillna(0)
                
        # Use a default value for stop_loss_amount if no cash balance
        if cash_balances_df.empty:
            default_cash = 10000  # Default cash balance
            default_stop_loss = risk_per_trade * default_cash
            print(f"Using default stop loss amount of ${default_stop_loss:.2f} (based on ${default_cash:.2f} default cash)")
    except Exception as e:
        print(f"Error retrieving cash balances: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error
    finally:
        conn.close()
    
    # Initialize list to store trade summaries
    trades_data = []
    
    # Process each trade
    for trade_id in executions['trade_id'].unique():
        if pd.isna(trade_id):
            continue
            
        # Get all executions for this trade
        trade_execs = executions[executions['trade_id'] == trade_id]
        
        # Get entry execution (must have is_entry=True)
        entry_execs = trade_execs[trade_execs['is_entry']]
        if len(entry_execs) == 0:
            print(f"Error: Trade {trade_id} has no entry execution (is_entry=True)")
            continue
        entry_exec = entry_execs.iloc[0]
        
        # Get exit execution (must have is_exit=True)
        exit_execs = trade_execs[trade_execs['is_exit']]
        has_exit = len(exit_execs) > 0
        
        # Extract entry date components
        entry_date = pd.to_datetime(entry_exec['execution_timestamp'])
        
        # Calculate trade metrics
        trade_data = {
            'trade_id': int(trade_id),
            'num_executions': len(trade_execs),
            'symbol': entry_exec['symbol'],
            'start_date': entry_exec['date'],
            'start_time': entry_exec['time_of_day'],
            'end_date': None,
            'end_time': None,
            'duration_hours': None,
            'quantity': abs(float(entry_exec['quantity'])),  # Use absolute value for quantity
            'risk_per_trade': risk_per_trade,
            'entry_price': float(entry_exec['price']),
            'exit_price': None,
            'capital_required': abs(float(entry_exec['quantity'])) * float(entry_exec['price']),
            'exit_type': None,  # Placeholder for now
            'account_id': entry_exec['ID'],  # Internal account ID from the join
            'status': 'open'  # Default to open
        }
        
        # If we have an exit execution, update the trade data
        if has_exit:
            exit_exec = exit_execs.iloc[0]
            duration_hours = (exit_exec['execution_timestamp'] - entry_exec['execution_timestamp']).total_seconds() / 3600
            
            trade_data.update({
                'end_date': exit_exec['date'],
                'end_time': exit_exec['time_of_day'],
                'duration_hours': round(duration_hours, 2),
                'exit_price': float(exit_exec['price']),
                'status': 'closed'
            })
        
        # Calculate stop price based on entry side
        entry_side = entry_exec['side'].upper()
        
        # Get account-specific stop loss amount if available
        trade_account_id = trade_data.get('account_id')
        if trade_account_id is not None:
            account_cash = cash_balances_df[cash_balances_df['account_ID'] == trade_account_id]['cash_balance'].values
            if len(account_cash) > 0:
                stop_loss_amount_for_trade = risk_per_trade * float(account_cash[0])
            else:
                # Account exists but no cash balance found for this specific account
                print(f"WARNING: No cash balance found for account ID {trade_account_id}")
                return pd.DataFrame()  # Return empty DataFrame to indicate error
        else:
            # No valid account ID
            print(f"ERROR: No valid account ID for trade {trade_id}")
            return pd.DataFrame()  # Return empty DataFrame to indicate error
        
        # Calculate per-share stop loss
        per_share_stop_loss = stop_loss_amount_for_trade / trade_data['quantity']
        print(f"Trade {trade_id}: Stop loss amount ${stop_loss_amount_for_trade:.2f}, Per share ${per_share_stop_loss:.2f}")
        
        if entry_side == 'BUY':
            # For buy trades: stop_price is below entry_price
            trade_data['stop_price'] = trade_data['entry_price'] - per_share_stop_loss
        else:  # SELL
            # For sell trades: stop_price is above entry_price
            trade_data['stop_price'] = trade_data['entry_price'] + per_share_stop_loss
            
        # Initialize risk/reward metrics as None for open trades
        trade_data.update({
            'risk_reward': None,
            'winning_trade': None,
            'perc_return': None
        })
        
        # Calculate risk/reward metrics only for closed trades
        if has_exit:
            # Calculate risk_reward based on entry side
            if entry_side == 'BUY':
                # For buy trades: (exit_price - entry_price) / (entry_price - stop_price)
                risk_reward_value = (trade_data['exit_price'] - trade_data['entry_price']) / (trade_data['entry_price'] - trade_data['stop_price'])
            else:  # SELL
                # For sell trades: (entry_price - exit_price) / (stop_price - entry_price)
                risk_reward_value = (trade_data['entry_price'] - trade_data['exit_price']) / (trade_data['stop_price'] - trade_data['entry_price'])
            
            # Update risk/reward metrics for closed trades
            trade_data.update({
                'risk_reward': risk_reward_value,
                'winning_trade': 1 if risk_reward_value > 0 else 0,
                'perc_return': round(risk_reward_value * risk_per_trade * 100, 2)
            })
        
        # Extract time periods
        trade_data['week'] = str(entry_date.isocalendar()[1])  # Week number (1-52)
        trade_data['month'] = str(entry_date.month)  # Month number (1-12)
        trade_data['year'] = entry_date.year
        
        trades_data.append(trade_data)
    
    # Convert to DataFrame
    trades_df = pd.DataFrame(trades_data)
    
    # Sort by trade_id
    trades_df = trades_df.sort_values('trade_id')
    
    print(f"Created summary for {len(trades_df)} trades")
    return trades_df

def save_trades_to_db(trades_df):
    """
    Save the trades DataFrame to the database.
    
    Args:
        trades_df (pandas.DataFrame): DataFrame containing trade summary data.
        
    Returns:
        int: Number of trades inserted into the database.
    """
    if trades_df.empty:
        print("No trades to save to database")
        return 0
    
    # Connect to the database
    conn = sqlite3.connect('data/kairos.db')
    cursor = conn.cursor()
    
    # Track how many trades were inserted
    trades_inserted = 0
    
    try:
        # First, get existing trade_ids to avoid duplicates
        cursor.execute("SELECT trade_id FROM trades")
        existing_trade_ids = {row[0] for row in cursor.fetchall()}
        
        # Process each trade
        for _, trade in trades_df.iterrows():
            # Skip if this trade_id already exists in the database
            if trade['trade_id'] in existing_trade_ids:
                print(f"Skipping trade_id {trade['trade_id']} - already exists in database")
                continue
            
            # Convert boolean to integer (SQLite doesn't have a native boolean type)
            winning_trade = 1 if trade['winning_trade'] == 1 else 0
            
            # Insert the trade into the database
            cursor.execute("""
                INSERT INTO trades (
                    trade_id, num_executions, symbol, start_date, start_time, 
                    end_date, end_time, duration_hours, quantity, entry_price, 
                    stop_price, exit_price, capital_required, exit_type, risk_reward, 
                    winning_trade, perc_return, week, month, year, account_id, risk_per_trade,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade['trade_id'], 
                trade['num_executions'],
                trade['symbol'],
                trade['start_date'],
                trade['start_time'],
                trade['end_date'],
                trade['end_time'],
                trade['duration_hours'],
                trade['quantity'],
                trade['entry_price'],
                trade['stop_price'],
                trade['exit_price'],
                trade['capital_required'],
                trade['exit_type'] or "market",  # Default to "market" if None
                trade['risk_reward'],
                winning_trade if trade['winning_trade'] is not None else None,
                trade['perc_return'],
                trade['week'],
                trade['month'],
                trade['year'],
                trade['account_id'],
                trade['risk_per_trade'],
                trade['status']
            ))
            trades_inserted += 1
        
        # Commit the changes
        conn.commit()
        print(f"Successfully inserted {trades_inserted} trades into the database")
        
    except Exception as e:
        conn.rollback()
        print(f"Error inserting trades into database: {e}")
        
    finally:
        conn.close()
        
    return trades_inserted

if __name__ == "__main__":
    # When run directly, print all executions and trade summary
    print("Step 1: Mark executions from closed trades as processed...")
    processed_count = mark_closed_trades_as_processed()
    
    print("\nStep 2: Get executions for analysis...")
    executions = get_all_executions()
    if not executions.empty:
        print("\nSample of executions:")
        print(executions.head())
        print(f"\nTotal executions: {len(executions)}")
        
        print("\nStep 3: Generating trade summary...")
        # Set risk_per_trade to 0.5%, but skip additional marking since we did it above
        trades = create_trades_summary(risk_per_trade=0.005, mark_processed=False)
        if not trades.empty:
            print("\nSample of trades summary (using risk_per_trade=0.5%):")
            print(trades.head())
            print(f"\nTotal trades: {len(trades)}")
            
            print("\nStep 4: Saving trades to database...")
            num_saved = save_trades_to_db(trades)
            print(f"Saved {num_saved} new trades to database")
        else:
            print("No trades generated - nothing to save to database")
    else:
        print("No executions found in database")
