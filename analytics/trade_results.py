import pandas as pd

def check_columns(df: pd.DataFrame) -> bool:

    required_columns = [
        "num_executions",
        "symbol",
        "direction",
        "quantity",
        "entry_price",
        "capital_required",
        "exit_price",
        "stop_price",
        "take_profit_price",
        "risk_reward",
        "risk_amount_per_share",
        "is_winner",
        "risk_per_trade",
        "perc_return",
        "status",
        "exit_type",
        "end_date",
        "end_time",
        "duration_hours",
        "commission",
        "start_date",
        "start_time",
        "week",
        "month",
        "year"
    ]

    return True if all(col in df.columns for col in required_columns) else False

def process_trade_results(df: pd.DataFrame) -> pd.DataFrame:
    
    # Create a copy to avoid modifying the original DataFrame
    result_df = df.copy()

    if not check_columns(result_df):
        raise ValueError("Missing required columns")
    
    
    
    return result_df
