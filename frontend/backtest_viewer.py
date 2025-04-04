import streamlit as st
import pandas as pd
import sys
import os
import asyncio
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from backtests.backtest_runner import get_latest_backtest_files, process_data, generate_reports, run_backtest, get_backtest_files


def view_backtest_results():
    
    st.title("Backtest Results Viewer")
    
    # Create a row with button and dropdown side by side
    col1, col2 = st.columns([1, 3])
    
    # Add dropdown to select backtest in second column
    with col2:
        backtest_files = get_backtest_files()
        if backtest_files:
            selected_file = st.selectbox(
                "Select backtest file",
                options=list(backtest_files.keys()),
                key="backtest_selector"
            )
            selected_path = backtest_files[selected_file]
        else:
            st.warning("No backtest files found")
            return
    
    # Add button to run new backtest in first column
    with col1:
        if st.button("Run New Backtest"):
            with st.spinner('Running backtest...'):
                try:# Run the backtest asynchronously
                    executions_df, trades_df, reports = run_backtest(selected_path, insert_to_db=False)
                    
                    if executions_df is not None:
                        st.success("Backtest completed successfully!")
                    else:
                        st.error("Failed to run backtest")
                        return
                except Exception as e:
                    st.error(f"Error running backtest: {str(e)}")
                    return
    
    # If we haven't just run a backtest, load latest data
    if 'executions_df' not in locals():
        settings_file, trades_file = get_latest_backtest_files()
        if settings_file and trades_file:
            executions_df, trades_df = process_data(trades_file)
            reports = generate_reports(trades_df)
        else:
            executions_df, trades_df = None, None
    
    if executions_df is None or trades_df is None:
        st.warning("No backtest data found. Run a backtest first!")
        return
    
      # reports
    reports = generate_reports(trades_df)
    if reports:
        # Column display names
        column_display_names = {
            'nr_trades': 'nr trades',
            'accuracy': 'accuracy',
            'avg_risk_per_trade': 'risk per trade',
            'avg_risk_reward_wins': 'avg win',
            'avg_risk_reward_losses': 'avg loss',
            'avg_return_per_trade': 'avg return',
            'total_return': 'total return'
        }

        # Style formatting for percentage columns
        reports_format = {
            'accuracy': '{:.2%}',
            'risk per trade': '{:.2%}',
            'avg win': '{:.2%}',
            'avg loss': '{:.2%}',
            'avg return': '{:.2%}',
            'total return': '{:.2%}'
        }

        # Style formatting for number columns
        base_dfs_format = {
            'capital_required': '{:,.2f}', # trades
            'entry_price': '{:,.2f}', # trades
            'exit_price': '{:,.2f}', # trades
            'stop_price': '{:,.2f}', # trades
            'take_profit_price': '{:,.2f}', # trades
            'price': '{:,.2f}', # executions
            'trade_cost': '{:,.2f}', # executions
            'quantity': '{:,.0f}', # executions and trades
            'risk_reward': '{:,.2f}', # trades
            'risk_amount_per_share': '{:,.2f}', # trades
            'risk_per_trade': '{:.2%}', # trades
            'perc_return': '{:.2%}', # trades
            'duration_hours': '{:,.4f}', # trades
        }

        st.subheader("Yearly Report")
        st.dataframe(
            reports['year']
            .rename(columns=column_display_names)
            .style.format(reports_format),
            hide_index=True
        )

        st.subheader("Monthly Report")
        st.dataframe(
            reports['month']
            .rename(columns=column_display_names)
            .style.format(reports_format),
            hide_index=True
        )

        st.subheader("Weekly Report")
        st.dataframe(
            reports['week']
            .rename(columns=column_display_names)
            .style.format(reports_format),
            hide_index=True
        )

    # trades
    st.subheader("Trades")
    st.dataframe(
        trades_df
        .style.format(base_dfs_format),
        hide_index=True)

    # executions
    st.subheader("Executions")
    st.dataframe(executions_df
        .style.format(base_dfs_format),
         hide_index=True)

if __name__ == "__main__":
    view_backtest_results() 