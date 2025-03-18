#!/usr/bin/env python3
"""
Clean Logs Utility

This script deletes all files in the logs directory to free up disk space.
It calculates the total size of log files before deletion and requires 
confirmation unless explicitly bypassed.

Usage:
    python clean_logs.py              # Interactive mode with confirmation
    
In code:
    from clean_logs import clean_logs
    clean_logs(logs_dir="logs", confirm=False)  # No confirmation needed
"""
import os
import glob
import sys
import shutil

def clean_logs(logs_dir="logs", confirm=True):
    """
    Delete all files in the logs directory
    
    Args:
        logs_dir: Path to the logs directory
        confirm: Whether to ask for confirmation
        
    Returns:
        bool: Success status
    """
    # Check if logs directory exists
    if not os.path.exists(logs_dir):
        print(f"Logs directory '{logs_dir}' does not exist. Nothing to clean.")
        return True
    
    # Get all files in the logs directory
    log_files = glob.glob(os.path.join(logs_dir, "*"))
    
    if not log_files:
        print(f"No files found in '{logs_dir}'. Nothing to clean.")
        return True
    
    # Count total files and their size
    total_size_mb = sum(os.path.getsize(f) for f in log_files if os.path.isfile(f)) / (1024 * 1024)
    file_count = len([f for f in log_files if os.path.isfile(f)])
    
    # Print what will happen
    print(f"This will delete all {file_count} files in '{logs_dir}' (total size: {total_size_mb:.2f} MB)")
    
    # Ask for confirmation if needed
    if confirm:
        confirmation = input("Do you want to proceed? (y/N): ")
        if confirmation.lower() != 'y':
            print("Operation cancelled.")
            return False
    
    # Delete all files
    deleted_count = 0
    error_count = 0
    
    for log_file in log_files:
        try:
            if os.path.isfile(log_file):
                os.remove(log_file)
                deleted_count += 1
        except Exception as e:
            print(f"Error deleting {log_file}: {e}")
            error_count += 1
    
    # Print summary
    print(f"Deleted {deleted_count} files from '{logs_dir}'")
    if error_count > 0:
        print(f"Encountered {error_count} errors while deleting")
    
    return True

if __name__ == "__main__":
    # Simple version - just clean the logs folder
    clean_logs() 