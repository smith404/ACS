# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from tabulate import tabulate
import argparse
from duckdb_wrapper import DuckDBWrapper  # Import DuckDBWrapper from the new file
from config import database_path

def main():
    """
    Main method to demonstrate the usage of DuckDBWrapper class.
    """
    parser = argparse.ArgumentParser(description="DuckDB Wrapper Script Executor")
    parser.add_argument("--script", type=str, help="The name of the script to be run (optional)")
    parser.add_argument("--table", type=str, help="The name of the table to load data into (required with --data)")
    parser.add_argument("--data", type=str, help="The path to the data file to load (required with --table)")
    parser.add_argument("--toparquet", type=str, help="The path to the CSV file to convert to Parquet")
    parser.add_argument("--tocsv", type=str, help="The path to the Parquet file to convert to CSV")
    parser.add_argument("--quiet", action="store_true", help="If provided, do not enter interactive query mode")
    parser.add_argument("--extract", type=str, help="The name of the table to extract as CSV")
    parser.add_argument("--delimiter", type=str, default=";", help="The delimiter to use for CSV files (default is ';')")
    parser.add_argument("--dump", type=str, help="The schema name to dump tables from")  # Add new argument
    parser.add_argument("--load", type=str, help="The schema name to load CSV files into")  # Add new argument
    parser.add_argument("--describe", type=str, help="The path to the Parquet file to describe")  # Add new argument
    args = parser.parse_args()

    if (args.table and not args.data) or (args.data and not args.table):
        parser.error("--table and --data must be provided together")

    db_wrapper = DuckDBWrapper(database_path)
    db_wrapper.connect()
    
    if args.script:
        db_wrapper.execute_script_from_file(args.script)  # Use the script name from the argument
    
    if args.table and args.data:
        db_wrapper.load_parquet_to_table(args.data, args.table)  # Load data into the specified table
    
    if args.toparquet:
        parquet_file_path = args.toparquet.replace('.csv', '.parquet')
        db_wrapper.convert_csv_to_parquet(args.toparquet, parquet_file_path)
        print(f"Converted {args.toparquet} to {parquet_file_path}")
    
    if args.tocsv:
        csv_file_path = args.tocsv.replace('.parquet', '.csv')
        db_wrapper.convert_parquet_to_csv(args.tocsv, csv_file_path)
        print(f"Converted {args.tocsv} to {csv_file_path}")
    
    if args.extract:
        csv_file_path = f"{args.extract}"
        csv_file_path = csv_file_path.replace('.', '_')
        csv_file_path = f"{csv_file_path}.csv"
        db_wrapper.extract_table_to_csv(args.extract, csv_file_path, args.delimiter)
        print(f"Extracted {args.extract} to {csv_file_path} with delimiter '{args.delimiter}'")
    
    if args.dump:
        db_wrapper.dump_tables(schema_name=args.dump, delimiter=args.delimiter)  # Call the dump_tables method with the schema name and delimiter

    if args.load:
        db_wrapper.load_csv_files(schema_name=args.load, delimiter=args.delimiter, truncate=True)  # Call the load_csv_files method with the schema name and delimiter

    if args.describe:
        db_wrapper.describe_parquet_file(args.describe)  # Call the describe_parquet_file method with the Parquet file path

    if not args.quiet:
        try:
            db_wrapper.interactive_query()
        finally:
            db_wrapper.close()
    else:
        db_wrapper.close()

if __name__ == "__main__":
    main()
