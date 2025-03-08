import argparse
import pandas as pd
from enum import Enum
from duckdb_wrapper import DuckDBWrapper  # Import DuckDBWrapper

class TransformationActionType(Enum):
    RENAME = "rename"
    SIMPLE_MAP = "simple_map"
    REFORMAT = "reformat"
    REMOVE = "remove"

class FrameMapper:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def transform(self) -> pd.DataFrame:
        # Apply transformation rules here
        transformed_df = self.df.copy()
        # Example transformation: Add a new column 'new_col' with values doubled from 'existing_col'
        if 'existing_col' in transformed_df.columns:
            transformed_df['new_col'] = transformed_df['existing_col'] * 2
        return transformed_df

def main():
    """
    Main method to demonstrate the usage of FrameMapper class.
    """
    parser = argparse.ArgumentParser(description="Frame Mapper Executor")
    parser.add_argument("--mapname", type=str, help="The name of the mapper to extract")
    args = parser.parse_args()

    if args.mapname:
        query = f"SELECT * FROM data_language.mapper WHERE map_name = '{args.mapname}'"
        database_path = "example.duckdb"
        db_wrapper = DuckDBWrapper(database_path)
        db_wrapper.connect()
        result_json = db_wrapper.execute_select_query_to_json(query)
        print(f"Query Result: {result_json}")

    # Sample DataFrame for demonstration
    data = {'existing_col': [1, 2, 3, 4, 5]}
    df = pd.DataFrame(data)
    
    # Create FrameMapper instance and apply transformation
    frame_mapper = FrameMapper(df)
    transformed_df = frame_mapper.transform()
    
    # Print the transformed DataFrame
    print(f"Mapper: {args.mapname}")
    print(transformed_df)

if __name__ == "__main__":
    main()
