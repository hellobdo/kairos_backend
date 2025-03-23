import pandas as pd
import numpy as np
from datetime import time

def create_trades_summary(df):
    """
    Create a summary of trades based on the identified trades.
    
    Args:
        df: DataFrame containing trade executions with trade_id and flags
        
    Returns:
        pd.DataFrame: Summary DataFrame with metrics for each complete trade
    """
    # Create a summary of trades with improved metrics
    trades_summary = []
    
    # Process each trade
    for trade_id in df['trade_id'].unique():
        trade_df = df[df['trade_id'] == trade_id]
        
        # Extract basic information
        symbol = trade_df['symbol'].iloc[0]
        
        # Get entry and exit timestamps
        entry_row = trade_df[trade_df['is_entry']].iloc[0] if any(trade_df['is_entry']) else trade_df.iloc[0]
        exit_row = trade_df[trade_df['is_exit']].iloc[0] if any(trade_df['is_exit']) else trade_df.iloc[-1]
        
        # Extract date and time
        start_date = entry_row['date']
        start_time = entry_row['time_of_day']
        end_date = exit_row['date']
        end_time = exit_row['time_of_day']
        
        # Calculate duration in hours
        start_timestamp = entry_row['execution_timestamp']
        end_timestamp = exit_row['execution_timestamp']
        duration_hours = (end_timestamp - start_timestamp).total_seconds() / 3600
        
        # Get entry and exit prices
        entry_price = entry_row['price']
        exit_price = exit_row['price']
        
        # Get quantity (use the entry quantity)
        quantity = entry_row['filled_quantity']
        
        # Number of executions
        num_executions = len(trade_df)
        
        # Create trade summary record
        trade_summary = {
            'trade_id': trade_id,
            'num_executions': num_executions,
            'symbol': symbol,
            'start_date': start_date,
            'start_time': start_time,
            'end_date': end_date,
            'end_time': end_time,
            'duration_hours': duration_hours,
            'quantity': quantity,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'exit_type': ''  # Add empty exit_type column
        }
        
        trades_summary.append(trade_summary)
    
    # Convert to DataFrame
    trades_summary_df = pd.DataFrame(trades_summary)
    
    print(f"Identified {len(trades_summary_df)} complete trades from {len(df)} executions")
    
    return trades_summary_df

def calculate_trade_metrics(trades_summary, trades_df, strategy_params=None):
    """
    Calculate all trade metrics and prepare data for HTML report generation.
    
    Args:
        trades_summary (pd.DataFrame): DataFrame containing trade summary data
        trades_df (pd.DataFrame): DataFrame containing detailed trade data
        strategy_params (dict): Dictionary containing strategy parameters
        
    Returns:
        dict: Dictionary containing all calculated metrics and formatted data
    """
    # Initialize results dictionary
    results = {
        'trades_summary_display': None,
        'trades_df_display': None,
        'weekly_metrics_df': pd.DataFrame(),
        'monthly_metrics_df': pd.DataFrame(),
        'yearly_metrics_df': pd.DataFrame(),
        'strategy_metrics': {
            'side': None,
            'stop_loss': None,
            'risk_reward': None,
            'risk_per_trade': None
        }
    }
    
    # Format trades_summary for better display
    trades_summary_display = trades_summary.copy()
    
    # Strategy parameters details for display
    strategy_side = None
    strategy_stop_loss = None
    strategy_risk_reward = None
    strategy_risk_per_trade = None
    
    # Add stop price calculation if stop_loss parameter is available
    if strategy_params and 'side' in strategy_params:
        side = strategy_params['side']
        
        # Save for display in header
        strategy_side = side
        results['strategy_metrics']['side'] = side
        
        # Calculate stop loss based on entry price using stop_loss_module
        # Use the first trade's entry price to determine stop loss amount
        entry_price = trades_summary_display['entry_price'].iloc[0]
        
        # Get stop loss amount based on entry price and stop_loss_rules parameter
        stop_loss = None
        if 'stop_loss_rules' in strategy_params:
            stop_loss_rules = strategy_params['stop_loss_rules']
            # Determine stop loss amount based on price and rules
            for rule in stop_loss_rules:
                if "price_below" in rule and entry_price < rule["price_below"]:
                    stop_loss = rule["amount"]
                    break
                elif "price_above" in rule and entry_price >= rule["price_above"]:
                    stop_loss = rule["amount"]
                    break

        else:
            raise ValueError("No stop loss rules provided in strategy parameters")
        
        # Convert stop_loss to string for display
        results['strategy_metrics']['stop_loss'] = str(stop_loss)
        
        # Get risk_reward if available
        if 'risk_reward' in strategy_params:
            strategy_risk_reward = strategy_params['risk_reward']
            results['strategy_metrics']['risk_reward'] = strategy_risk_reward
            
        # Get risk_per_trade if available
        if 'risk_per_trade' in strategy_params:
            strategy_risk_per_trade = strategy_params['risk_per_trade']
            risk_per_trade = strategy_params['risk_per_trade']
            results['strategy_metrics']['risk_per_trade'] = risk_per_trade
        else:
            # perc_return calculation requires risk_per_trade
            print("Warning: risk_per_trade not provided - percentage return calculation will be skipped")
            risk_per_trade = None
        
        # Calculate stop price based on side and entry price
        if side == 'buy':
            trades_summary_display['stop_price'] = trades_summary_display['entry_price'] - stop_loss
        else:  # sell
            trades_summary_display['stop_price'] = trades_summary_display['entry_price'] + stop_loss
            
        # Round stop price to 2 decimal places
        trades_summary_display['stop_price'] = trades_summary_display['stop_price'].round(2)
        
        # Calculate take profit price if risk_reward is available
        if strategy_risk_reward:
            risk_reward = strategy_params['risk_reward']
            if side == 'buy':
                # For buy trades: entry_price + (entry_price - stop_price) * risk_reward
                trades_summary_display['take_profit_price'] = trades_summary_display['entry_price'] + \
                    (trades_summary_display['entry_price'] - trades_summary_display['stop_price']) * risk_reward
            else:  # sell
                # For sell trades: entry_price - (stop_price - entry_price) * risk_reward
                trades_summary_display['take_profit_price'] = trades_summary_display['entry_price'] - \
                    (trades_summary_display['stop_price'] - trades_summary_display['entry_price']) * risk_reward
            
            # Round take profit price to 2 decimal places
            trades_summary_display['take_profit_price'] = trades_summary_display['take_profit_price'].round(2)
            
            print(f"Added take profit price calculation (risk_reward: {risk_reward})")
            
        # Calculate capital required and place it after stop_price
        # We'll do this by creating a new DataFrame with the columns in the desired order
        trades_summary_display['capital_required'] = (trades_summary_display['entry_price'] * trades_summary_display['quantity']).round(2)
        
        # Determine exit type for each trade
        # Use a small tolerance for price comparisons (1% of entry price)
        price_tolerance = 0.01
        
        # First, ensure end_time is a proper time object for all trades
        try:
            # Check if at least the first non-null value is a time object
            is_time_already = isinstance(trades_summary_display['end_time'].dropna().iloc[0], time)
        except (IndexError, AttributeError):
            is_time_already = False
            
        if not is_time_already:
            # Convert end_time to time objects if they aren't already
            trades_summary_display['end_time'] = pd.to_datetime(trades_summary_display['end_time'], format='%H:%M:%S', errors='coerce').dt.time
        
        # Define market close window time range
        market_close_start = time(15, 50, 0)  # 15:50:00
        market_close_end = time(16, 0, 0)  # 16:00:00
        
        # Create mask for trades that ended during market close window
        end_of_day_mask = trades_summary_display['end_time'].apply(
            lambda x: market_close_start <= x <= market_close_end if pd.notna(x) else False
        )
        
        # Initialize all exit types based on end of day check
        trades_summary_display['exit_type'] = "end of day"
        trades_summary_display.loc[~end_of_day_mask, 'exit_type'] = "unclassified"
        
        # Check for stop losses for all trades
        if side == 'buy':
            # For buy trades: exit_price <= stop_price + tolerance = stop loss
            stop_mask = trades_summary_display['exit_price'] <= (trades_summary_display['stop_price'] + price_tolerance)
            trades_summary_display.loc[stop_mask, 'exit_type'] = "stop"
            
            # For buy trades: If risk_reward exists, check for take profits
            if strategy_risk_reward:
                # exit_price >= take_profit_price - tolerance = take profit
                tp_mask = trades_summary_display['exit_price'] >= (trades_summary_display['take_profit_price'] - price_tolerance)
                trades_summary_display.loc[tp_mask, 'exit_type'] = "take profit"
        else:  # sell
            # For sell trades: exit_price >= stop_price - tolerance = stop loss
            stop_mask = trades_summary_display['exit_price'] >= (trades_summary_display['stop_price'] - price_tolerance)
            trades_summary_display.loc[stop_mask, 'exit_type'] = "stop"
            
            # For sell trades: If risk_reward exists, check for take profits
            if strategy_risk_reward:
                # exit_price <= take_profit_price + tolerance = take profit
                tp_mask = trades_summary_display['exit_price'] <= (trades_summary_display['take_profit_price'] + price_tolerance)
                trades_summary_display.loc[tp_mask, 'exit_type'] = "take profit"
        
        # Convert any remaining unclassified trades to end of day
        trades_summary_display.loc[trades_summary_display['exit_type'] == "unclassified", 'exit_type'] = "end of day"
        
        print("Classified trade exits as: stop, take profit, or end of day")
        
        # Adjust exit prices for stop losses to match the exact stop price
        stop_exits_mask = (trades_summary_display['exit_type'] == "stop") & (trades_summary_display['exit_price'] != trades_summary_display['stop_price'])
        trades_summary_display.loc[stop_exits_mask, 'exit_price'] = trades_summary_display.loc[stop_exits_mask, 'stop_price']
        
        # Adjust exit prices for take profit exits to match the exact take profit price
        if 'take_profit_price' in trades_summary_display.columns:
            tp_exits_mask = (trades_summary_display['exit_type'] == "take profit") & (trades_summary_display['exit_price'] != trades_summary_display['take_profit_price'])
            trades_summary_display.loc[tp_exits_mask, 'exit_price'] = trades_summary_display.loc[tp_exits_mask, 'take_profit_price']
        
        # Add risk_per_trade to trades_summary_display
        if risk_per_trade is not None:
            trades_summary_display['risk_per_trade'] = risk_per_trade * 100
            trades_summary_display['risk_per_trade'] = trades_summary_display['risk_per_trade'].apply(lambda x: f"{x:.2f}%")

        # Recalculate risk/reward ratio with the adjusted exit prices
        if side == 'buy':
            # For buy trades: (exit_price - entry_price) / (entry_price - stop_price)
            trades_summary_display['risk_reward'] = (
                (trades_summary_display['exit_price'] - trades_summary_display['entry_price']) / 
                (trades_summary_display['entry_price'] - trades_summary_display['stop_price'])
            )
        else:  # sell
            # For sell trades: (entry_price - exit_price) / (stop_price - entry_price)
            trades_summary_display['risk_reward'] = (
                (trades_summary_display['entry_price'] - trades_summary_display['exit_price']) / 
                (trades_summary_display['stop_price'] - trades_summary_display['entry_price'])
            )
        
        # Handle possible infinity or NaN values in risk_reward
        trades_summary_display['risk_reward'] = trades_summary_display['risk_reward'].replace([np.inf, -np.inf], np.nan)
    
        
        print("Recalculated risk/reward ratios with adjusted exit prices")
        
        # Add winning trade column based on adjusted exit price comparison
        # For floating point comparison, use a small tolerance
        tolerance = 0.01  # 1 cent tolerance for price comparison
        
        if side == 'buy':
            # For buy trades: 
            # - Winning trade (1): exit_price > entry_price (sold higher than bought)
            # - Losing trade (0): exit_price <= entry_price (sold lower than or same as bought)
            trades_summary_display['winning_trade'] = (
                trades_summary_display['exit_price'] > (trades_summary_display['entry_price'] + tolerance)
            ).astype(int)
        else:  # sell
            # For sell trades:
            # - Winning trade (1): exit_price < entry_price (bought back lower than sold)
            # - Losing trade (0): exit_price >= entry_price (bought back higher than or same as sold)
            trades_summary_display['winning_trade'] = (
                trades_summary_display['exit_price'] < (trades_summary_display['entry_price'] - tolerance)
            ).astype(int)
            
            # Calculate percentage return only if risk_per_trade is available
        if risk_per_trade is not None:
            # Calculate percentage return using risk_per_trade * actual_risk_reward for ALL trades
            trades_summary_display['perc_return'] = risk_per_trade * trades_summary_display['risk_reward']
            
            # Convert to percentage format and round
            trades_summary_display['perc_return'] = (trades_summary_display['perc_return'] * 100).round(2)
            
            # Add week, month, and year columns for time-based grouping
            trades_summary_calc = trades_summary_display.copy()
            
            # Convert start_date to datetime if it's not already
            if not pd.api.types.is_datetime64_any_dtype(trades_summary_calc['start_date']):
                trades_summary_calc['start_date'] = pd.to_datetime(trades_summary_calc['start_date'])
            
            # Extract week, month, and year
            trades_summary_calc['week'] = trades_summary_calc['start_date'].dt.isocalendar().week.astype(str)  # Week number (1-52)
            trades_summary_calc['month'] = trades_summary_calc['start_date'].dt.month.astype(str)  # Month number (1-12)
            trades_summary_calc['year'] = trades_summary_calc['start_date'].dt.year
            
            # Add the time period columns to the display DataFrame
            trades_summary_display['week'] = trades_summary_calc['week']
            trades_summary_display['month'] = trades_summary_calc['month']
            trades_summary_display['year'] = trades_summary_calc['year']
            
            # Create weekly metrics
            weekly_metrics = []
            for week, week_df in trades_summary_calc.groupby(['year', 'week']):
                year, week_num = week  # Now unpacking (year, week) tuple
                
                total_trades = len(week_df)
                winning_trades = week_df['winning_trade'].sum()
                accuracy = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                
                # Get average risk reward for winning and losing trades
                winning_mask = week_df['winning_trade'] == 1
                losing_mask = week_df['winning_trade'] == 0
                avg_win = week_df.loc[winning_mask, 'risk_reward'].mean() if winning_mask.any() else 0
                avg_loss = week_df.loc[losing_mask, 'risk_reward'].mean() if losing_mask.any() else 0
                avg_risk_reward = week_df['risk_reward'].mean()
                
                # Calculate total return
                total_return = week_df['perc_return'].sum()
                
                weekly_metrics.append({
                    'Period': f"Week {week_num}, {year}",
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{week_df['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                })
            
            # Create monthly metrics
            monthly_metrics = []
            for month_group, month_df in trades_summary_calc.groupby(['year', 'month']):
                year, month_num = month_group  # Now unpacking (year, month) tuple
                
                total_trades = len(month_df)
                winning_trades = month_df['winning_trade'].sum()
                accuracy = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                
                # Get average risk reward for winning and losing trades
                winning_mask = month_df['winning_trade'] == 1
                losing_mask = month_df['winning_trade'] == 0
                avg_win = month_df.loc[winning_mask, 'risk_reward'].mean() if winning_mask.any() else 0
                avg_loss = month_df.loc[losing_mask, 'risk_reward'].mean() if losing_mask.any() else 0
                avg_risk_reward = month_df['risk_reward'].mean()
                
                # Calculate total return
                total_return = month_df['perc_return'].sum()
                
                # Convert month number to month name
                month_name = calendar.month_name[int(month_num)]
                
                monthly_metrics.append({
                    'Period': f"{month_name} {year}",
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{month_df['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                })
            
            # Create yearly metrics
            yearly_metrics = []
            for year, year_df in trades_summary_calc.groupby('year'):
                total_trades = len(year_df)
                winning_trades = year_df['winning_trade'].sum()
                accuracy = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                
                # Get average risk reward for winning and losing trades
                winning_mask = year_df['winning_trade'] == 1
                losing_mask = year_df['winning_trade'] == 0
                avg_win = year_df.loc[winning_mask, 'risk_reward'].mean() if winning_mask.any() else 0
                avg_loss = year_df.loc[losing_mask, 'risk_reward'].mean() if losing_mask.any() else 0
                avg_risk_reward = year_df['risk_reward'].mean()
                
                # Calculate total return
                total_return = year_df['perc_return'].sum()
                
                yearly_metrics.append({
                    'Period': str(year),
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{year_df['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                })
            
            # Convert to DataFrames
            weekly_metrics_df = pd.DataFrame(weekly_metrics)
            monthly_metrics_df = pd.DataFrame(monthly_metrics)
            yearly_metrics_df = pd.DataFrame(yearly_metrics)
            
            # Add totals row to weekly metrics
            if not weekly_metrics_df.empty:
                total_trades = len(trades_summary_calc)
                winning_trades = trades_summary_calc['winning_trade'].sum()
                accuracy = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                
                # Get average risk reward for winning and losing trades for all weeks
                winning_mask = trades_summary_calc['winning_trade'] == 1
                losing_mask = trades_summary_calc['winning_trade'] == 0
                avg_win = trades_summary_calc.loc[winning_mask, 'risk_reward'].mean() if winning_mask.any() else 0
                avg_loss = trades_summary_calc.loc[losing_mask, 'risk_reward'].mean() if losing_mask.any() else 0
                avg_risk_reward = trades_summary_calc['risk_reward'].mean()
                
                # Calculate total return for all weeks
                total_return = trades_summary_calc['perc_return'].sum()
                
                # Create a total row
                total_row = pd.DataFrame([{
                    'Period': 'TOTAL',
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{trades_summary_calc['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                }])
                
                # Append the total row to the weekly metrics
                weekly_metrics_df = pd.concat([weekly_metrics_df, total_row], ignore_index=True)
            
            # Add totals row to monthly metrics
            if not monthly_metrics_df.empty:
                # Reuse the same total values calculated above
                total_row = pd.DataFrame([{
                    'Period': 'TOTAL',
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{trades_summary_calc['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                }])
                
                # Append the total row to the monthly metrics
                monthly_metrics_df = pd.concat([monthly_metrics_df, total_row], ignore_index=True)
            
            # Add totals row to yearly metrics
            if not yearly_metrics_df.empty:
                # Reuse the same total values calculated above
                total_row = pd.DataFrame([{
                    'Period': 'TOTAL',
                    'Trades': total_trades,
                    'Accuracy': f"{accuracy:.2f}%",
                    'Risk Per Trade': f"{risk_per_trade*100:.2f}%",
                    'Avg Win': f"{avg_win:.2f}",
                    'Avg Loss': f"{avg_loss:.2f}",
                    'Avg Return': f"{trades_summary_calc['perc_return'].mean():.2f}%",
                    'Total Return': f"{total_return:+.2f}%"
                }])
                
                # Append the total row to the yearly metrics
                yearly_metrics_df = pd.concat([yearly_metrics_df, total_row], ignore_index=True)
            
            results['weekly_metrics_df'] = weekly_metrics_df
            results['monthly_metrics_df'] = monthly_metrics_df
            results['yearly_metrics_df'] = yearly_metrics_df
            
            print(f"Added stop price calculation (side: {side}, stop_loss: {stop_loss})")
            print(f"Added winning trade column based on entry vs exit price comparison")
            print(f"Added capital required and actual risk/reward columns")
            print(f"Added percentage return column (risk_per_trade × actual_risk_reward)")
            print(f"Added weekly, monthly, and yearly performance metrics")
        else:
            # If no strategy parameters, still calculate capital required
            trades_summary_display['capital_required'] = (trades_summary_display['entry_price'] * trades_summary_display['quantity']).round(2)
            
            results['weekly_metrics_df'] = pd.DataFrame()
            results['monthly_metrics_df'] = pd.DataFrame()
            results['yearly_metrics_df'] = pd.DataFrame()
    else:
        # If no strategy parameters, still calculate capital required
        trades_summary_display['capital_required'] = (trades_summary_display['entry_price'] * trades_summary_display['quantity']).round(2)
        
        results['weekly_metrics_df'] = pd.DataFrame()
        results['monthly_metrics_df'] = pd.DataFrame()
        results['yearly_metrics_df'] = pd.DataFrame()
    
    # Format numeric columns
    if 'duration_hours' in trades_summary_display.columns:
        trades_summary_display['duration_hours'] = trades_summary_display['duration_hours'].round(2)
    if 'entry_price' in trades_summary_display.columns:
        trades_summary_display['entry_price'] = trades_summary_display['entry_price'].round(2)
    if 'exit_price' in trades_summary_display.columns:
        trades_summary_display['exit_price'] = trades_summary_display['exit_price'].round(2)
    
    # Ensure trade_id is displayed as integer without decimals
    trades_summary_display['trade_id'] = trades_summary_display['trade_id'].astype(int)
    
    # Reorder columns to put capital_required after stop_price
    if 'stop_price' in trades_summary_display.columns:
        # Get all column names
        all_cols = trades_summary_display.columns.tolist()
        
        # Remove columns we want to reorder
        if 'capital_required' in all_cols:
            all_cols.remove('capital_required')
        if 'stop_price' in all_cols:
            all_cols.remove('stop_price')
        if 'entry_price' in all_cols:
            all_cols.remove('entry_price')
        if 'exit_price' in all_cols:
            all_cols.remove('exit_price')
            
        # Find the position of entry_price (where it would be)
        try:
            # If there's a quantity column, insert entry_price after it
            quantity_pos = all_cols.index('quantity')
            
            # Insert columns in the desired order: entry_price, stop_price, exit_price, capital_required
            all_cols.insert(quantity_pos + 1, 'entry_price')
            all_cols.insert(quantity_pos + 2, 'stop_price')
            all_cols.insert(quantity_pos + 3, 'exit_price')
            all_cols.insert(quantity_pos + 4, 'capital_required')
        except ValueError:
            # If no quantity column, append them at the end in the right order
            all_cols.extend(['entry_price', 'stop_price', 'exit_price', 'capital_required'])
        
        # Reorder the DataFrame
        trades_summary_display = trades_summary_display[all_cols]
    
    # Format capital_required with commas
    if 'capital_required' in trades_summary_display.columns:
        trades_summary_display['capital_required'] = trades_summary_display['capital_required'].apply(lambda x: f"{x:,.2f}")
        
    # Format perc_return as percentage with % symbol
    if 'perc_return' in trades_summary_display.columns:
        trades_summary_display['perc_return'] = trades_summary_display['perc_return'].apply(lambda x: f"{x:+.2f}%" if not pd.isna(x) else "")
    
    # Create a copy of trades_df for display and convert trade_id to int
    trades_df_display = trades_df.copy()
    trades_df_display['trade_id'] = trades_df_display['trade_id'].astype(int)
    
    # Store formatted DataFrames in results
    results['trades_summary_display'] = trades_summary_display
    results['trades_df_display'] = trades_df_display
    
    return results