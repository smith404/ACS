import duckdb
from tabulate import tabulate
import os
import argparse
from duckdb_wrapper import DuckDBWrapper  # Import DuckDBWrapper from the new file

def main():
    """
    Main method to demonstrate the usage of DuckDBWrapper class.
    """
    parser = argparse.ArgumentParser(description="DuckDB Wrapper Script Executor")
    parser.add_argument("--script", type=str, help="The name of the script to be run (optional)")
    parser.add_argument("--table", type=str, help="The name of the table to load data into (required with --data)")
    parser.add_argument("--data", type=str, help="The path to the data file to load (required with --table)")
    parser.add_argument("--convert", type=str, help="The path to the CSV file to convert to Parquet")
    parser.add_argument("--quiet", action="store_true", help="If provided, do not enter interactive query mode")
    parser.add_argument("--extract", type=str, help="The name of the table to extract as CSV")
    args = parser.parse_args()

    if (args.table and not args.data) or (args.data and not args.table):
        parser.error("--table and --data must be provided together")

    database_path = "example.duckdb"
    db_wrapper = DuckDBWrapper(database_path)
    db_wrapper.connect()
    
    if args.script:
        db_wrapper.execute_script_from_file(args.script)  # Use the script name from the argument
    
    if args.table and args.data:
        db_wrapper.load_parquet_to_table(args.data, args.table)  # Load data into the specified table
    
    if args.convert:
        parquet_file_path = args.convert.replace('.csv', '.parquet')
        db_wrapper.convert_csv_to_parquet(args.convert, parquet_file_path)
        print(f"Converted {args.convert} to {parquet_file_path}")
    
    if args.extract:
        csv_file_path = f"{args.extract}.csv"
        db_wrapper.extract_table_to_csv(args.extract, csv_file_path)
        print(f"Extracted {args.extract} to {csv_file_path}")
    
    if not args.quiet:
        try:
            db_wrapper.interactive_query()
        finally:
            db_wrapper.close()
    else:
        db_wrapper.close()

if __name__ == "__main__":
    main()
