import requests
import pandas as pd
import time
from io import StringIO

def get_ibkr_flex_data(token, query_id, flex_version=3):
    """
    Fetch IBKR Flex report data and return it as a DataFrame.
    
    Args:
        token (str): IBKR Flex Web Service token
        query_id (str): IBKR Flex query ID
        flex_version (int, optional): Flex API version. Defaults to 3.
    
    Returns:
        pandas.DataFrame: DataFrame containing the CSV data.
        If there's an error, returns False
    """
    request_base = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
    
    # Step 1: Request report generation
    send_slug = "/SendRequest"
    send_params = {
        "t": token, 
        "q": query_id, 
        "v": flex_version
    }
    
    print(f"Requesting report generation with query ID: {query_id}...")
    flex_req = requests.get(url=request_base+send_slug, params=send_params)
    
    try:
        # Parse the response to get reference code
        response_data = flex_req.text
        if "Success" not in response_data:
            print(f"Failed to generate Flex statement: {response_data}")
            return False
            
        # Extract reference code - assuming format contains <ReferenceCode>CODE</ReferenceCode>
        start_idx = response_data.find("<ReferenceCode>") + len("<ReferenceCode>")
        end_idx = response_data.find("</ReferenceCode>")
        ref_code = response_data[start_idx:end_idx]
        
        if not ref_code:
            print("No reference code found in response.")
            return False
        
        print(f"Report generation request successful. Reference code: {ref_code}")
        print("Waiting for report to be generated (20 seconds)...")
        time.sleep(20)
        
        # Step 2: Retrieve the report
        receive_slug = "/GetStatement"
        receive_params = {
            "t": token, 
            "q": ref_code, 
            "v": flex_version
        }
        
        print("Retrieving report...")
        receive_req = requests.get(url=request_base+receive_slug, params=receive_params, allow_redirects=True)
        
        print(f"Response status code: {receive_req.status_code}")
        
        # Check if response is empty or malformed
        if not receive_req.text.strip():
            print("Response is empty")
            return False
            
        # Check if response might be an empty report or error message
        if "," not in receive_req.text or len(receive_req.text.split("\n")) < 3:
            print("Response does not appear to be a valid CSV (not enough lines or no commas found)")
            print("First 100 characters of response:")
            print(receive_req.text[:100])
            return False
            
        try:
            # Parse CSV data - assume first row contains headers
            df = pd.read_csv(
                StringIO(receive_req.text), 
                skipinitialspace=True
            )
            
            # Check if DataFrame is empty after parsing
            if df.empty:
                print("Parsed DataFrame is empty")
                return False
                
            # Check if DataFrame has only a header row or empty values
            if len(df) <= 1 or df.iloc[0].isnull().all():
                print("DataFrame contains only headers or empty rows")
                return False
                
            print("Successfully parsed the report data")
            print(f"DataFrame shape: {df.shape}")
            return df
        except Exception as parsing_error:
            print(f"Error parsing CSV data: {parsing_error}")
            print("Response content (first 500 chars):")
            print(receive_req.text[:500])
            return False
        
    except Exception as e:
        print(f"Error processing report: {e}")
        print("Response content:")
        print(receive_req.text[:500])  # Print first 500 chars of response for debugging
        return False

if __name__ == "__main__":
    print("This module provides the get_ibkr_flex_data function for fetching IBKR Flex report data.")
    print("Import and use it in your own scripts with:")
    print("from ibkr_connection import get_ibkr_flex_data") 