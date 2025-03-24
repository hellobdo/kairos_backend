import pandas as pd

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
