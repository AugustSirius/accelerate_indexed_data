#!/usr/bin/env python3
"""
Script to compare CSV files with the same names across three folders.
Performs pairwise comparisons and reports whether files are identical.
"""

import os
import hashlib
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Set
import sys

def get_file_hash(filepath: Path) -> str:
    """Calculate MD5 hash of a file for quick comparison."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def compare_files_content(file1: Path, file2: Path) -> Tuple[bool, str]:
    """
    Compare two files for exact content match.
    Returns (is_identical, message)
    """
    # First, quick check with file size
    size1 = file1.stat().st_size
    size2 = file2.stat().st_size
    
    if size1 != size2:
        return False, f"Different file sizes: {size1} vs {size2} bytes"
    
    # Then check with hash
    hash1 = get_file_hash(file1)
    hash2 = get_file_hash(file2)
    
    if hash1 != hash2:
        # Try to load as CSV for more detailed comparison
        try:
            df1 = pd.read_csv(file1)
            df2 = pd.read_csv(file2)
            
            # Check dimensions
            if df1.shape != df2.shape:
                return False, f"Different dimensions: {df1.shape} vs {df2.shape}"
            
            # Check if dataframes are equal
            if not df1.equals(df2):
                # Try to find where they differ
                diff_info = []
                
                # Check column names
                if list(df1.columns) != list(df2.columns):
                    diff_info.append("Column names differ")
                
                # Check for value differences
                try:
                    comparison = df1.compare(df2)
                    if not comparison.empty:
                        diff_info.append(f"Values differ in {len(comparison)} rows")
                except:
                    diff_info.append("Values differ (detailed comparison failed)")
                
                return False, f"Content differs: {', '.join(diff_info) if diff_info else 'Unknown differences'}"
            
        except Exception as e:
            return False, f"Hash mismatch (couldn't parse as CSV: {str(e)})"
    
    return True, "Files are identical"

def get_csv_files(folder: Path) -> Dict[str, Path]:
    """Get all CSV files in a folder."""
    csv_files = {}
    if not folder.exists():
        print(f"Warning: Folder does not exist: {folder}")
        return csv_files
    
    for file in folder.glob("*.csv"):
        csv_files[file.name] = file
    
    return csv_files

def compare_folders(folder1: Path, folder2: Path, name1: str, name2: str) -> None:
    """Compare CSV files between two folders."""
    print(f"\n{'='*80}")
    print(f"Comparing: {name1} vs {name2}")
    print(f"{'='*80}")
    
    files1 = get_csv_files(folder1)
    files2 = get_csv_files(folder2)
    
    if not files1:
        print(f"No CSV files found in {name1}")
        return
    if not files2:
        print(f"No CSV files found in {name2}")
        return
    
    # Find common files
    common_files = set(files1.keys()) & set(files2.keys())
    only_in_1 = set(files1.keys()) - set(files2.keys())
    only_in_2 = set(files2.keys()) - set(files1.keys())
    
    print(f"\nTotal files in {name1}: {len(files1)}")
    print(f"Total files in {name2}: {len(files2)}")
    print(f"Common files: {len(common_files)}")
    
    if only_in_1:
        print(f"\nFiles only in {name1} ({len(only_in_1)}):")
        for f in sorted(only_in_1)[:10]:  # Show first 10
            print(f"  - {f}")
        if len(only_in_1) > 10:
            print(f"  ... and {len(only_in_1) - 10} more")
    
    if only_in_2:
        print(f"\nFiles only in {name2} ({len(only_in_2)}):")
        for f in sorted(only_in_2)[:10]:  # Show first 10
            print(f"  - {f}")
        if len(only_in_2) > 10:
            print(f"  ... and {len(only_in_2) - 10} more")
    
    if not common_files:
        print("\nNo common files to compare!")
        return
    
    # Compare common files
    print(f"\nComparing {len(common_files)} common files...")
    identical_count = 0
    different_count = 0
    different_files = []
    
    for filename in sorted(common_files):
        file1 = files1[filename]
        file2 = files2[filename]
        
        is_identical, message = compare_files_content(file1, file2)
        
        if is_identical:
            identical_count += 1
        else:
            different_count += 1
            different_files.append((filename, message))
    
    # Summary
    print(f"\n{'Summary':^40}")
    print(f"{'-'*40}")
    print(f"Identical files: {identical_count}/{len(common_files)}")
    print(f"Different files: {different_count}/{len(common_files)}")
    
    if different_files:
        print(f"\nFiles with differences:")
        for filename, message in different_files[:20]:  # Show first 20
            print(f"  - {filename}: {message}")
        if len(different_files) > 20:
            print(f"  ... and {len(different_files) - 20} more different files")

def main():
    # Define the three folders
    base_path = Path("/Users/augustsirius/Desktop/DIA_peak_group_extraction/accelerate_indexed_data")
    
    folders = {
        "timstof": base_path / "timstof" / "output_precursors",
        "timstof-opt_build_indexed": base_path / "timstof-opt_build_indexed" / "output_precursors",
        "timstof-opt_build_indexed-opt_read": base_path / "timstof-opt_build_indexed-opt_read" / "output_precursors"
    }
    
    # Check if folders exist
    print("Checking folders...")
    for name, folder in folders.items():
        if folder.exists():
            print(f"✓ {name}: {folder}")
        else:
            print(f"✗ {name}: {folder} (NOT FOUND)")
    
    # Perform pairwise comparisons
    folder_pairs = [
        ("timstof", "timstof-opt_build_indexed"),
        ("timstof", "timstof-opt_build_indexed-opt_read"),
        ("timstof-opt_build_indexed", "timstof-opt_build_indexed-opt_read")
    ]
    
    for name1, name2 in folder_pairs:
        compare_folders(folders[name1], folders[name2], name1, name2)
    
    print(f"\n{'='*80}")
    print("Comparison complete!")
    print(f"{'='*80}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)