#!/usr/bin/env python
"""
Debug script for IBKR API and cash module integration.
This script tests the IBKR connection and prints detailed information about responses.
"""
import os
import sys
import time
from dotenv import load_dotenv
from analytics.ibkr_api import get_ibkr_flex_data
from analytics.cash import process_account_data

# Load environment variables
load_dotenv()

def debug_ibkr_connection():
    """Test the connection to IBKR Flex Query system and print detailed diagnostics"""
    
    # Get credentials from environment
    token = os.getenv("IBKR_TOKEN_PAPER")
    query_id = os.getenv("IBKR_QUERY_ID_CASH_PAPER")
    
    if not token or not query_id:
        print("ERROR: Missing IBKR credentials in environment variables")
        print("Please set IBKR_TOKEN_PAPER and IBKR_QUERY_ID_CASH_PAPER in your .env file")
        return False
    
    print(f"\n{'='*50}")
    print("IBKR API DEBUG")
    print(f"{'='*50}")
    print(f"IBKR_QUERY_ID_CASH_PAPER: {query_id}")
    print(f"IBKR_TOKEN_PAPER: {token[:5]}..." if token else "None")
    
    # First, attempt to get data using our existing function
    print("\nAttempting standard data retrieval...")
    result = get_ibkr_flex_data(token, query_id)
    
    if isinstance(result, bool) and not result:
        print("\n❌ Standard retrieval failed")
    else:
        print(f"\n✅ Standard retrieval succeeded! DataFrame shape: {result.shape}")
        print("\nFirst 5 rows of data:")
        print(result.head())
        
        # Verify cash module process works
        print("\nTesting cash.process_account_data...")
        try:
            process_account_data(token, query_id, "paper")
            print("\n✅ process_account_data completed successfully")
        except Exception as e:
            print(f"\n❌ process_account_data failed: {str(e)}")
        
        return True
    
    return False
        
if __name__ == "__main__":
    print("Starting IBKR API debug...")
    success = debug_ibkr_connection()
    
    if success:
        print("\n✅ Debug completed successfully - data was retrieved!")
    else:
        print("\n❌ Debug failed - couldn't retrieve data") 