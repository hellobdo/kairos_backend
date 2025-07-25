#!/usr/bin/env python3
"""
Clean Logs Utility

This script provides functions to clean log files in the logs directory.

Usage:
    python clean_logs.py              # Automatically cleans logs without confirmation
    
In code:
    from clean_logs import clean_logs
    clean_logs(confirm=True)  # With confirmation
"""
import os
import glob

def clean_directory(directory, confirm=True, dir_description="directory"):
    """
    Delete all files in the specified directory
    
    Args:
        directory: Path to the directory to clean
        confirm: Whether to ask for confirmation
        dir_description: Description of the directory for messages
        
    Returns:
        bool: Success status
    """
    # Check if directory exists
    if not os.path.exists(directory):
        print(f"{dir_description} '{directory}' does not exist. Nothing to clean.")
        return True
    
    # Get all files in the directory
    files = glob.glob(os.path.join(directory, "*"))
    
    if not files:
        print(f"No files found in '{directory}'. Nothing to clean.")
        return True
    
    # Count total files and their size
    total_size_mb = sum(os.path.getsize(f) for f in files if os.path.isfile(f)) / (1024 * 1024)
    file_count = len([f for f in files if os.path.isfile(f)])
    
    # Print what will happen
    print(f"This will delete all {file_count} files in '{directory}' (total size: {total_size_mb:.2f} MB)")
    
    # Ask for confirmation if needed
    if confirm:
        confirmation = input("Do you want to proceed? (y/N): ")
        if confirmation.lower() != 'y':
            print("Operation cancelled.")
            return False
    
    # Delete all files
    deleted_count = 0
    error_count = 0
    
    for file in files:
        try:
            if os.path.isfile(file):
                os.remove(file)
                deleted_count += 1
        except Exception as e:
            print(f"Error deleting {file}: {e}")
            error_count += 1
    
    # Print summary
    print(f"Deleted {deleted_count} files from '{directory}'")
    if error_count > 0:
        print(f"Encountered {error_count} errors while deleting")
    
    return True

def clean_logs(logs_dir="logs", confirm=True):
    """
    Delete all files in the logs directory
    
    Args:
        logs_dir: Path to the logs directory
        confirm: Whether to ask for confirmation
        
    Returns:
        bool: Success status
    """
    return clean_directory(logs_dir, confirm, "Logs directory")

if __name__ == "__main__":
    # Run clean_logs without confirmation when run directly
    clean_logs(confirm=False) 