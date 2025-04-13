import pandas as pd

def convert_to_numeric(df, numeric_fields):
    """
    Convert specified columns in a DataFrame to numeric types.
    
    Args:
        df (pandas.DataFrame): The DataFrame containing columns to convert
        numeric_fields (list): List of column names to convert to numeric
        
    Returns:
        pandas.DataFrame: DataFrame with converted columns
    """
    processed_df = df.copy()
    for field in numeric_fields:
        if field in processed_df.columns:
            processed_df[field] = pd.to_numeric(processed_df[field], errors='coerce')
    
    return processed_df

def csv_to_dataframe(csv_path):
    """
    Read a CSV file and convert it to a pandas DataFrame.
    
    Args:
        csv_path (str): Path to the CSV file
        
    Returns:
        pandas.DataFrame: DataFrame containing the CSV data
    """
    try:
        df = pd.read_csv(csv_path)
        return df
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None