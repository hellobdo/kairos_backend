#!/usr/bin/env python3
"""
Clean Examples Utility

This script provides functions to clean example files in the indicators/examples directory.

Usage:
    python clean_examples.py         # Automatically cleans examples without confirmation
    
In code:
    from clean_examples import clean_examples
    clean_examples(confirm=True)  # With confirmation
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

def clean_examples(examples_dir="indicators/examples", confirm=True):
    """
    Delete all files in the indicators/examples directory
    
    Args:
        examples_dir: Path to the examples directory
        confirm: Whether to ask for confirmation
        
    Returns:
        bool: Success status
    """
    return clean_directory(examples_dir, confirm, "Examples directory")

if __name__ == "__main__":
    # Run clean_examples without confirmation when run directly
    clean_examples(confirm=False) 