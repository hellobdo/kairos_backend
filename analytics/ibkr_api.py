import requests
import pandas as pd
import time
from io import StringIO
import xml.etree.ElementTree as ET

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
    base_url = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
    
    try:
        # Step 1: Request report generation
        print(f"Requesting report generation with query ID: {query_id}...")
        response = requests.get(
            url=f"{base_url}/SendRequest",
            params={"t": token, "q": query_id, "v": flex_version}
        )
        
        # Parse XML response properly
        try:
            xml_root = ET.fromstring(response.text)
            
            # Check for successful report request
            # Look for both "Status" (in test fixtures) and "Success" (in real API)
            success_elem = xml_root.find(".//Status")
            if success_elem is not None:
                if success_elem.text != "Success":
                    print(f"Failed to generate Flex statement: {response.text}")
                    return False
            elif "Success" not in response.text:
                print(f"Failed to generate Flex statement: {response.text}")
                return False
            
            # Extract reference code
            ref_code_elem = xml_root.find(".//ReferenceCode")
            if ref_code_elem is not None and ref_code_elem.text:
                ref_code = ref_code_elem.text
            else:
                print("No reference code found in response.")
                return False
            
        except ET.ParseError:
            # Fallback to old method if XML parsing fails
            if "Success" not in response.text:
                print(f"Failed to generate Flex statement: {response.text}")
                return False
                
            # Extract reference code with more robust method
            start_tag = "<ReferenceCode>"
            end_tag = "</ReferenceCode>"
            
            if start_tag not in response.text or end_tag not in response.text:
                print("No reference code found in response.")
                return False
                
            start_idx = response.text.find(start_tag) + len(start_tag)
            end_idx = response.text.find(end_tag)
            ref_code = response.text[start_idx:end_idx]
            
            if not ref_code:
                print("Empty reference code found in response.")
                return False
        
        print(f"Report generation request successful. Reference code: {ref_code}")
        print("Waiting for report to be generated (20 seconds)...")
        time.sleep(20)
        
        # Step 2: Retrieve the report
        print("Retrieving report...")
        report_response = requests.get(
            url=f"{base_url}/GetStatement",
            params={"t": token, "q": ref_code, "v": flex_version},
            allow_redirects=True
        )
        
        print(f"Response status code: {report_response.status_code}")
        
        # Validate response content
        content = report_response.text.strip()
        if not content or "," not in content or len(content.split("\n")) < 2:
            print("Response is empty or does not appear to be valid CSV")
            return False
        
        # Parse CSV data into DataFrame
        df = pd.read_csv(StringIO(content), skipinitialspace=True)
        
        # Validate DataFrame - modified to accept single row DataFrames
        if df.empty or df.iloc[0].isnull().all():
            print("DataFrame is empty or contains only headers/empty rows")
            return False
        
        print(f"Successfully parsed the report data. DataFrame shape: {df.shape}")
        return df
        
    except Exception as e:
        print(f"Error processing report: {e}")
        return False 