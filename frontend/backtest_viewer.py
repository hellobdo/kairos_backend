import streamlit as st
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from backtests.backtest_runner import run_backtest, get_backtest_files_for_display
from backtests.utils.backtest_data_to_db import insert_to_db

def view_backtest_results():
    
    st.title("Backtest Results Viewer")
    
    # Get list of backtest files
    backtest_files = get_backtest_files_for_display()
    
    # Add file selector and run button in the same row
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        selected_path = st.selectbox(
            "Select backtest file",
            options=list(backtest_files.keys()),
            format_func=lambda x: x
        )
    
    with col2:
        if st.button("Run Backtest"):
            if selected_path:
                try:
                    full_path = backtest_files[selected_path]
                    executions_df, trades_df, reports = run_backtest(full_path)
                    if executions_df is None:
                        st.error("Backtest failed!")
                    else:
                        # Store in session state
                        st.session_state['executions_df'] = executions_df
                        st.session_state['trades_df'] = trades_df
                        st.session_state['reports'] = reports
                        st.success("Backtest completed successfully!")
                except Exception as e:
                    st.error(f"Error running backtest: {str(e)}")
            else:
                st.warning("Please select a backtest file first")

    with col3:
        if st.button("Insert to DB"):
            if 'executions_df' in st.session_state and 'trades_df' in st.session_state:
                st.write("Ready to insert data from previous backtest run")
                try:
                    success = insert_to_db(st.session_state['executions_df'], st.session_state['trades_df'])
                    if success:
                        st.success("Data successfully inserted into database!")
                    else:
                        st.error("Failed to insert data into database. Check the logs for details.")
                except Exception as e:
                    st.error(f"Error inserting to database: {str(e)}")
            else:
                st.warning("No backtest data available. Run a backtest first!")

    # Column display names
    column_display_names = {
        'nr_trades': 'nr trades',
        'accuracy': 'accuracy',
        'avg_risk_per_trade_perc': 'risk per trade',
        'avg_risk_reward_wins': 'avg win',
        'avg_risk_reward_losses': 'avg loss',
        'avg_return_per_trade': 'avg return',
        'total_return': 'total return',
        'risk_per_trade_perc': 'risk per trade',
    }

    # Style formatting for percentage columns
    styling_format = {
        'accuracy': '{:.2%}', # reports
        'avg_duration_hours': '{:.4f}', # reports
        'risk per trade': '{:.2%}', # reports
        'avg win': '{:.2}', # reports
        'avg loss': '{:.2}', # reports
        'avg return': '{:.2%}', # reports
        'total return': '{:.2%}', # reports
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
        'risk_per_trade_amount': '{:,.2f}' # trades
    }

    # Display reports if they exist
    if 'reports' in st.session_state:
        st.subheader("Yearly Report")
        st.dataframe(
            st.session_state['reports']['year']
            .rename(columns=column_display_names)
            .style.format(styling_format),
            hide_index=True
        )

        st.subheader("Monthly Report")
        st.dataframe(
            st.session_state['reports']['month']
            .rename(columns=column_display_names)
            .style.format(styling_format),
            hide_index=True
        )

        st.subheader("Weekly Report")
        st.dataframe(
            st.session_state['reports']['week']
            .rename(columns=column_display_names)
            .style.format(styling_format),
            hide_index=True
        )

    # Display trades if they exist
    if 'trades_df' in st.session_state:
        st.subheader("Trades")
        st.dataframe(
            st.session_state['trades_df']
            .rename(columns=column_display_names)
            .style.format(styling_format),
            hide_index=True)

    # Display executions if they exist
    if 'executions_df' in st.session_state:
        st.subheader("Executions")
        st.dataframe(
            st.session_state['executions_df']
            .style.format(styling_format),
            hide_index=True)

if __name__ == "__main__":
    view_backtest_results() 