import streamlit as st
import sys
import os
import glob
import pandas as pd

def load_optimization_results():
    """Load optimization results from the optimization_results directory"""
    # Find all CSV files in the optimization_results directory
    optimization_dir = os.path.join(os.getcwd(), "optimization_results")
    if not os.path.exists(optimization_dir):
        return None
        
    csv_files = glob.glob(os.path.join(optimization_dir, "*.csv"))
    
    # Group files by run ID
    result_groups = {}
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        # Extract run ID from filename (e.g., "run_001_" or "baseline_")
        if file_name.startswith("run_"):
            run_id = file_name.split("_", 2)[0] + "_" + file_name.split("_", 2)[1]
        elif file_name.startswith("baseline_"):
            run_id = "baseline"
        else:
            continue
            
        # Determine file type from name
        file_type = None
        if "_year" in file_name:
            file_type = "year"
        elif "_month" in file_name:
            file_type = "month"
        elif "_week" in file_name:
            file_type = "week"
        elif "_trades" in file_name:
            file_type = "trades"
        elif "_executions" in file_name:
            file_type = "executions"
        
        if not file_type:
            continue
            
        if run_id not in result_groups:
            result_groups[run_id] = {}
            
        # Load file content into DataFrame
        try:
            result_groups[run_id][file_type] = pd.read_csv(file_path)
        except Exception as e:
            print(f"Error loading {file_name}: {str(e)}")
    
    return result_groups

def main_page():
    st.title("Kairos")
    
    # Initialize session state for comparison view
    if 'show_comparison' not in st.session_state:
        st.session_state.show_comparison = False
    if 'selected_run' not in st.session_state:
        st.session_state.selected_run = None
    if 'selected_report_type' not in st.session_state:
        st.session_state.selected_report_type = 'year'
    if 'selected_data_type' not in st.session_state:
        st.session_state.selected_data_type = 'trades'
    
    # Add a section for optimization results comparison
    st.header("Optimization Results Comparison")
    
    # Load optimization results
    results = load_optimization_results()
    
    if not results:
        st.warning("No optimization results found. Run optimization first.")
    else:
        # Get available run IDs (excluding baseline)
        run_ids = list(results.keys())
        if "baseline" in run_ids:
            run_ids.remove("baseline")
        
        if not run_ids:
            st.warning("No optimization runs found to compare with baseline.")
        else:
            # Create a selectbox to choose which run to compare with baseline
            selected_run = st.selectbox(
                "Select optimization run to compare with baseline:",
                run_ids,
                index=0,
                key="run_selection"
            )
            
            # Check if baseline exists
            if "baseline" not in results:
                st.warning("Baseline results not found. Only showing optimization results.")
                baseline_results = None
            else:
                baseline_results = results["baseline"]
            
            # Display side-by-side comparison
            def on_compare_click():
                st.session_state.show_comparison = True
                st.session_state.selected_run = selected_run
            
            st.button("Compare with Baseline", on_click=on_compare_click)
            
            # Show comparison if button was clicked
            if st.session_state.show_comparison and st.session_state.selected_run:
                show_comparison(baseline_results, results.get(st.session_state.selected_run), st.session_state.selected_run)
    
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
        'spy_perc_return': 'SPY',
        'qqq_perc_return': 'QQQ',
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
        'risk_per_trade_amount': '{:,.2f}', # trades
        'SPY': '{:.2%}', # trades
        'QQQ': '{:.2%}', # trades
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

def show_comparison(baseline_results, comparison_results, comparison_name):
    """Show side-by-side comparison of baseline and selected optimization results"""
    # Extract parameter information from file name if available
    param_info = ""
    if comparison_name.startswith("run_"):
        # Check if "params" column exists in any of the DataFrames
        for data_type in comparison_results:
            if isinstance(comparison_results[data_type], pd.DataFrame) and 'params' in comparison_results[data_type].columns:
                params_str = str(comparison_results[data_type]['params'].iloc[0])
                
                # Try to parse the params string into a cleaner format
                try:
                    # Remove the curly braces
                    params_str = params_str.strip('{}')
                    
                    # Split by comma and clean up each key-value pair
                    param_parts = []
                    for part in params_str.split(','):
                        if ':' in part:
                            key, value = part.split(':', 1)
                            key = key.strip().strip("'\"")
                            value = value.strip().strip("'\"")
                            param_parts.append(f"{key}: {value}")
                    
                    # Join the cleaned parts
                    if param_parts:
                        param_info = ", ".join(param_parts)
                except Exception:
                    # If parsing fails, just use the original string
                    param_info = params_str
                
                break
    
    st.header(f"Comparing Baseline vs {comparison_name}")
    
    if not baseline_results and not comparison_results:
        st.warning("No results available for comparison")
        return
    
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
        'spy_perc_return': 'SPY',
        'qqq_perc_return': 'QQQ',
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
        'risk_per_trade_amount': '{:,.2f}', # trades
        'SPY': '{:.2%}', # trades
        'QQQ': '{:.2%}', # trades
    }
    
    # Create tabs for different report categories
    tab_selection = st.radio("Select category:", ["Reports", "Trades & Executions"], horizontal=True)
    
    if tab_selection == "Reports":
        # Reports section (year, month, week)
        report_types = ['year', 'month', 'week']
        
        # Update session state when selection changes
        def on_report_type_change():
            st.session_state.selected_report_type = st.session_state.report_type_select
            
        selected_report = st.selectbox(
            "Select report type:", 
            report_types,
            index=report_types.index(st.session_state.selected_report_type) if st.session_state.selected_report_type in report_types else 0,
            key="report_type_select",
            on_change=on_report_type_change
        )
        
        st.subheader(f"{st.session_state.selected_report_type.capitalize()} Report")
        
        # Display baseline
        st.markdown("**Baseline (Original Parameters)**")
        if baseline_results and st.session_state.selected_report_type in baseline_results:
            st.dataframe(
                baseline_results[st.session_state.selected_report_type]
                .rename(columns=column_display_names)
                .style.format(styling_format),
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info(f"No baseline {st.session_state.selected_report_type} report available")
        
        # Display comparison run
        st.markdown(f"**{comparison_name} | {param_info}**")
        if comparison_results and st.session_state.selected_report_type in comparison_results:
            st.dataframe(
                comparison_results[st.session_state.selected_report_type]
                .rename(columns=column_display_names)
                .style.format(styling_format),
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info(f"No {st.session_state.selected_report_type} report available for this run")
    else:
        # Trades & Executions section
        data_types = ['trades', 'executions']
        
        # Update session state when selection changes
        def on_data_type_change():
            st.session_state.selected_data_type = st.session_state.data_type_select
            
        selected_data = st.selectbox(
            "Select data type:", 
            data_types,
            index=data_types.index(st.session_state.selected_data_type) if st.session_state.selected_data_type in data_types else 0,
            key="data_type_select",
            on_change=on_data_type_change
        )
        
        st.subheader(st.session_state.selected_data_type.capitalize())
        
        # Display baseline
        st.markdown("**Baseline (Original Parameters)**")
        if baseline_results and st.session_state.selected_data_type in baseline_results:
            df_display = baseline_results[st.session_state.selected_data_type]
            if st.session_state.selected_data_type == 'trades':
                df_display = df_display.rename(columns=column_display_names)
            st.dataframe(
                df_display.style.format(styling_format),
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info(f"No baseline {st.session_state.selected_data_type} available")
        
        # Display comparison run
        st.markdown(f"**{comparison_name} | {param_info}**")
        if comparison_results and st.session_state.selected_data_type in comparison_results:
            df_display = comparison_results[st.session_state.selected_data_type]
            if st.session_state.selected_data_type == 'trades':
                df_display = df_display.rename(columns=column_display_names)
            st.dataframe(
                df_display.style.format(styling_format),
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info(f"No {st.session_state.selected_data_type} available for this run")

if __name__ == "__main__":
    main_page() 