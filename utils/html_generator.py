import os
from datetime import datetime
import webbrowser
import pandas as pd

def generate_html_report(trades_df, trades_summary, metrics, output_file='trade_report.html', auto_open=True, original_file=None, strategy_params=None, rejected_trades=None):
    """
    Generate an HTML report from the trade execution data and summary
    
    Args:
        trades_df: DataFrame with detailed trade executions
        trades_summary: DataFrame with trade summaries
        output_file: Path to save the HTML report
        auto_open: Whether to automatically open the report in a browser
        original_file: Path to the original trades CSV file
        strategy_params: Dictionary of strategy parameters
        rejected_trades: DataFrame containing trades that were rejected during processing
        
    Returns:
        str: Path to the generated HTML file
    """
    # Create output directory if needed
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    
    
    # Extract data from metrics dictionary
    trades_summary_display = metrics['trades_summary_display']
    trades_df_display = metrics['trades_df_display']
    weekly_metrics_df = metrics['weekly_metrics_df']
    monthly_metrics_df = metrics['monthly_metrics_df']
    yearly_metrics_df = metrics['yearly_metrics_df']
    strategy_metrics = metrics['strategy_metrics']
    
    # Strategy parameters display
    strategy_side = strategy_metrics['side']
    strategy_stop_loss = strategy_metrics['stop_loss']
    strategy_risk_reward = strategy_metrics['risk_reward']
    strategy_risk_per_trade = strategy_metrics['risk_per_trade']
    
    # Load original CSV data if provided
    original_data_html = ""
    if original_file and os.path.exists(original_file):
        try:
            # Read the original CSV file
            original_df = pd.read_csv(original_file)
            # Add the original data section
            original_data_html = f"""
            <h2>Original CSV Data</h2>
            <p>This is the raw data from the CSV file before processing.</p>
            {original_df.head(30).to_html(index=False)}
            <p><em>Note: Showing first 30 rows only. Total rows: {len(original_df)}</em></p>
            """
        except Exception as e:
            original_data_html = f"""
            <h2>Original CSV Data</h2>
            <p>Error loading original CSV file: {str(e)}</p>
            """
    
    # Generate Rejected Trades HTML
    rejected_trades_html = ""
    if rejected_trades is not None and not rejected_trades.empty:
        rejected_trades_html = f"""
        <div class="section">
            <h2>Rejected Trades</h2>
            <p>These trades were filtered out during processing because they didn't meet the criteria for this strategy.</p>
            {rejected_trades.to_html(index=False)}
            <p><em>Total rejected trades: {len(rejected_trades)}</em></p>
        </div>
        """
    
    # Generate Weekly Metrics HTML
    weekly_metrics_html = ""
    if not weekly_metrics_df.empty:
        # Add CSS class to the total row
        weekly_metrics_html_table = weekly_metrics_df.to_html(index=False)
        weekly_metrics_html_table = weekly_metrics_html_table.replace('<tr>', '<tr class="row">')
        weekly_metrics_html_table = weekly_metrics_html_table.replace('<tr class="row">\n      <td>TOTAL</td>', '<tr class="total-row">\n      <td>TOTAL</td>')
        
        weekly_metrics_html = f"""
        <div class="section">
            <h2>Weekly Performance Metrics</h2>
            {weekly_metrics_html_table}
        </div>
        """
    
    # Generate Monthly Metrics HTML
    monthly_metrics_html = ""
    if not monthly_metrics_df.empty:
        # Add CSS class to the total row
        monthly_metrics_html_table = monthly_metrics_df.to_html(index=False)
        monthly_metrics_html_table = monthly_metrics_html_table.replace('<tr>', '<tr class="row">')
        monthly_metrics_html_table = monthly_metrics_html_table.replace('<tr class="row">\n      <td>TOTAL</td>', '<tr class="total-row">\n      <td>TOTAL</td>')
        
        monthly_metrics_html = f"""
        <div class="section">
            <h2>Monthly Performance Metrics</h2>
            {monthly_metrics_html_table}
        </div>
        """
    
    # Generate Yearly Metrics HTML
    yearly_metrics_html = ""
    if not yearly_metrics_df.empty:
        # Add CSS class to the total row
        yearly_metrics_html_table = yearly_metrics_df.to_html(index=False)
        yearly_metrics_html_table = yearly_metrics_html_table.replace('<tr>', '<tr class="row">')
        yearly_metrics_html_table = yearly_metrics_html_table.replace('<tr class="row">\n      <td>TOTAL</td>', '<tr class="total-row">\n      <td>TOTAL</td>')
        
        yearly_metrics_html = f"""
        <div class="section">
            <h2>Yearly Performance Metrics</h2>
            {yearly_metrics_html_table}
        </div>
        """
    
    # Strategy parameters HTML
    strategy_params_html = ""
    source_file_html = f"""<p><span class="highlight">Source File:</span> {os.path.basename(original_file) if original_file else "Unknown"}</p>"""
    
    if strategy_params:
        # Display all strategy parameters in the header
        for key, value in strategy_params.items():
            # Format the key for display (convert snake_case to title case)
            display_key = ' '.join(word.capitalize() for word in key.split('_'))
            
            # Format the value appropriately based on type
            if isinstance(value, float):
                if key == 'risk_per_trade':
                    # Format risk_per_trade as percentage
                    formatted_value = f"{value * 100:.2f}%"
                elif key == 'tight_threshold' or value < 0.01:
                    # Use scientific notation for very small numbers
                    formatted_value = f"{value:.6f}"
                else:
                    # Format other floats with 2 decimal places
                    formatted_value = f"{value:.2f}"
            elif isinstance(value, bool):
                formatted_value = str(value)
            else:
                formatted_value = str(value)
                
            # Add parameter to HTML
            param_html = f"""<p><span class="highlight">{display_key}:</span> {formatted_value}</p>"""
            strategy_params_html += param_html
            
        # Add source file after all strategy parameters
        strategy_params_html += source_file_html
    else:
        # If no strategy parameters, just show source file
        strategy_params_html = source_file_html
    
    # HTML template
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Trade Analysis Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1, h2 {{ color: #2c3e50; }}
            table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f2f2f2; }}
            tr:hover {{ background-color: #f5f5f5; }}
            .summary {{ background-color: #e8f4f8; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .highlight {{ font-weight: bold; }}
            .section {{ margin-bottom: 30px; border-bottom: 1px solid #eee; padding-bottom: 20px; }}
            .total-row {{ background-color: #f8f9fa; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h1>Trade Analysis Report</h1>
        <div class="summary">
            <p><span class="highlight">Report Generated:</span> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><span class="highlight">Total Trades:</span> {len(trades_summary)}</p>
            <p><span class="highlight">Total Executions:</span> {len(trades_df)}</p>
            <p><span class="highlight">Symbols Traded:</span> {', '.join(trades_df['symbol'].unique())}</p>
            {strategy_params_html}
        </div>
        
        {yearly_metrics_html}
        
        {monthly_metrics_html}
        
        {weekly_metrics_html}
        
        <div class="section">
            <h2>Trade Summary</h2>
            {trades_summary_display.to_html(index=False)}
        </div>
        
        <div class="section">
            <h2>Processed Executions</h2>
            {trades_df_display[['execution_timestamp', 'date', 'time_of_day', 'identifier', 'symbol', 'side', 'filled_quantity', 'price', 'trade_id', 'open_volume', 'is_entry', 'is_exit']].to_html(index=False)}
            <p><em>Total rows: {len(trades_df)}</em></p>
        </div>
        
        {rejected_trades_html}
        
        <div class="section">
            {original_data_html}
        </div>
    </body>
    </html>
    """
    
    # Write the HTML file
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"HTML report generated: {output_file}")
    
    # Automatically open the HTML file in the default browser if requested
    if auto_open:
        try:
            # Convert to absolute path for browser
            abs_path = os.path.abspath(output_file)
            print(f"Opening report in browser: {abs_path}")
            # Use file:// protocol for local files
            webbrowser.open(f"file://{abs_path}")
        except Exception as e:
            print(f"Warning: Could not open browser automatically: {str(e)}")
    
    return output_file