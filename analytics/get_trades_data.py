#!/usr/bin/env python3
import sqlite3
import pandas as pd
import argparse
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_trades_data(db_path='data/algos.db', run_id=None):
    """
    Retrieve trades data from the database.
    
    Args:
        db_path (str): Path to the SQLite database
        run_id (int, optional): Specific run_id to filter by. If None, all trades are returned.
        
    Returns:
        pd.DataFrame: DataFrame containing trades data
    """
    # Check if database exists
    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        return pd.DataFrame()
    
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        
        # Build the query
        query = "SELECT * FROM trades"
        params = []
        
        if run_id is not None:
            query += " WHERE run_id = ?"
            params.append(run_id)
        
        # Execute the query
        df = pd.read_sql_query(query, conn, params=params)
        
        # Close the connection
        conn.close()
        
        return df
    
    except Exception as e:
        logger.error(f"Error retrieving trades data: {e}")
        return pd.DataFrame()

def calculate_metrics(group_by_run_id=True, db_path='data/algos.db', run_id=None):
    """
    Calculate metrics on trades data grouped by day, week, and month, and optionally by run_id.
    
    Args:
        group_by_run_id (bool): Whether to group by run_id as well
        db_path (str): Path to the SQLite database
        run_id (int, optional): Specific run_id to filter by
        
    Returns:
        dict: Dictionary containing DataFrames with calculated metrics for each time period
              (keys: 'daily', 'weekly', 'monthly')
    """
    # Get trades data
    trades_df = get_trades_data(db_path, run_id)
    
    if trades_df.empty:
        return {'daily': pd.DataFrame(), 'weekly': pd.DataFrame(), 'monthly': pd.DataFrame()}
    
    # Check if we have the necessary date columns
    if 'entry_date' not in trades_df.columns:
        logger.error("'entry_date' column not found in trades data")
        return {'daily': pd.DataFrame(), 'weekly': pd.DataFrame(), 'monthly': pd.DataFrame()}
    
    # Convert entry_date to datetime for proper date handling
    trades_df['entry_date'] = pd.to_datetime(trades_df['entry_date'])
    
    # Extract date components for grouping
    trades_df['date'] = trades_df['entry_date'].dt.date
    trades_df['week'] = trades_df['entry_date'].dt.isocalendar().week
    trades_df['month'] = trades_df['entry_date'].dt.month
    trades_df['year'] = trades_df['entry_date'].dt.year
    
    # Create combined period columns
    trades_df['year_week'] = trades_df['year'].astype(str) + '-' + trades_df['week'].astype(str)
    trades_df['year_month'] = trades_df['year'].astype(str) + '-' + trades_df['month'].astype(str)
    
    # Dictionary to store results
    results = {}
    
    # Define grouping columns
    groupby_columns = {
        'daily': 'date',
        'weekly': 'year_week',
        'monthly': 'year_month'
    }
    
    # Add run_id to grouping if requested
    if group_by_run_id and 'run_id' in trades_df.columns:
        for period in groupby_columns:
            groupby_columns[period] = ['run_id', groupby_columns[period]]
    
    # Calculate metrics for each time period
    for period, group_col in groupby_columns.items():
        # Group by the appropriate time period
        grouped = trades_df.groupby(group_col)
        
        # Calculate metrics
        metrics = pd.DataFrame({
            'nr_of_trades': grouped.size(),
            'accuracy': grouped['winning_trade'].mean() if 'winning_trade' in trades_df.columns else None,
            'risk_per_trade': grouped['risk_per_trade'].mean() if 'risk_per_trade' in trades_df.columns else None,
            'avg_win': grouped[['winning_trade', 'risk_reward']].apply(
                lambda x: x[x['winning_trade'] == 1]['risk_reward'].mean() 
                if 'winning_trade' in x.columns and 'risk_reward' in x.columns else None
            ) if all(col in trades_df.columns for col in ['winning_trade', 'risk_reward']) else None,
            'avg_loss': grouped[['winning_trade', 'risk_reward']].apply(
                lambda x: x[x['winning_trade'] == 0]['risk_reward'].mean() 
                if 'winning_trade' in x.columns and 'risk_reward' in x.columns else None
            ) if all(col in trades_df.columns for col in ['winning_trade', 'risk_reward']) else None,
            'avg_return': grouped['risk_reward'].mean() if 'risk_reward' in trades_df.columns else None,
            'total_return': grouped['perc_return'].sum() if 'perc_return' in trades_df.columns else None
        })
        
        # Reset index for better display
        metrics = metrics.reset_index()
        
        # Store in results dictionary
        results[period] = metrics
    
    return results

def generate_html_report(metrics_dict, output_file='analytics/reports/metrics_report.html'):
    """
    Generate a simple HTML report with the metrics tables.
    
    Args:
        metrics_dict (dict): Dictionary containing DataFrames with metrics
        output_file (str): Path to save the HTML file
        
    Returns:
        str: Path to the saved HTML file
    """
    if not metrics_dict:
        logger.warning("No metrics to display")
        return None
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Start building HTML content
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Trading Metrics Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #333; }
            h2 { color: #666; margin-top: 30px; }
            table { border-collapse: collapse; width: 100%; margin-bottom: 30px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: right; }
            th { background-color: #f2f2f2; text-align: center; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            tr:hover { background-color: #f1f1f1; }
        </style>
    </head>
    <body>
        <h1>Trading Metrics Report</h1>
        <p>Generated on: """ + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
    """
    
    # Add each period's metrics to the HTML
    for period, df in metrics_dict.items():
        if df.empty:
            continue
        
        # Format numeric columns to 2 decimal places
        formatted_df = df.copy()
        for col in formatted_df.columns:
            if col in ['date', 'run_id', 'year_week', 'year_month', 'nr_of_trades']:
                continue
            if col == 'accuracy' and pd.api.types.is_numeric_dtype(formatted_df[col]):
                # Format accuracy as percentage with 2 decimal places
                formatted_df[col] = formatted_df[col].map(lambda x: f"{x*100:.2f}%" if pd.notnull(x) else "")
            elif pd.api.types.is_numeric_dtype(formatted_df[col]):
                formatted_df[col] = formatted_df[col].map(lambda x: f"{x:.2f}" if pd.notnull(x) else "")
        
        # Add section for this period
        html_content += f"""
        <h2>{period.capitalize()} Metrics</h2>
        {formatted_df.to_html(index=False, classes='metrics-table', na_rep='')}
        """
    
    # Close HTML tags
    html_content += """
    </body>
    </html>
    """
    
    # Write to file
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    logger.info(f"HTML report saved to {output_file}")
    return output_file

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Calculate and display trading metrics')
    parser.add_argument('--db-path', default='data/algos.db', help='Path to the SQLite database')
    parser.add_argument('--run-id', type=int, help='Specific run_id to analyze')
    parser.add_argument('--output', default='analytics/reports/metrics_report.html', help='Output HTML file path')
    args = parser.parse_args()
    
    # Calculate metrics
    metrics = calculate_metrics(db_path=args.db_path, run_id=args.run_id)
    
    # Generate HTML report
    html_file = generate_html_report(metrics, args.output)
    
    if html_file:
        print(f"HTML report saved to: {os.path.abspath(html_file)}")
        print(f"Open this file in a web browser to view the report.")
    else:
        print("No report was generated")