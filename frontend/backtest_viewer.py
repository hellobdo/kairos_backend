import streamlit as st
import pandas as pd
import sys
import os
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from backtests.backtest_runner import get_latest_backtest_files, process_data, generate_reports

def view_backtest_results():
    st.title("Backtest Results Viewer")
    
    # Load latest data
    settings_file, trades_file = get_latest_backtest_files()
    if settings_file and trades_file:
        executions_df, trades_df = process_data(trades_file)
    else:
        executions_df, trades_df = None, None
    
    if executions_df is None or trades_df is None:
        st.warning("No backtest data found. Run a backtest first!")
        return
    
      # reports
    reports = generate_reports(trades_df)
    if reports:
        st.subheader("Yearly Report")
        st.dataframe(reports['year'])

        st.subheader("Monthly Report")
        st.dataframe(reports['month'])

        st.subheader("Weekly Report")
        st.dataframe(reports['week'])

    # trades
    st.subheader("Trades")
    st.dataframe(trades_df)

    # executions
    st.subheader("Executions")
    st.dataframe(executions_df)

if __name__ == "__main__":
    view_backtest_results() 