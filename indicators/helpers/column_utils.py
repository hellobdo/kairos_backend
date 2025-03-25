import pandas as pd

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names by converting them to lowercase.
    
    Args:
        df: DataFrame with any column names
        
    Returns:
        pd.DataFrame: DataFrame with all column names converted to lowercase
    """
    # Create a copy of the DataFrame
    df_normalized = df.copy()
    
    # Convert all column names to lowercase
    df_normalized.columns = df_normalized.columns.str.lower()
    
    return df_normalized 