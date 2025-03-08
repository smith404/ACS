import argparse
import pandas as pd
from enum import Enum
from duckdb_wrapper import DuckDBWrapper  # Import DuckDBWrapper
import json  # Import json module

class TransformationActionType(Enum):
    RENAME = "rename"
    SIMPLE_MAP = "simple_map"
    REFORMAT = "reformat"
    REMOVE = "remove"

class Mapper:
    def __init__(self, map_name: str, action: TransformationActionType, source_column: str, target_column: str = None):
        self.map_name = map_name
        self.action = action
        self.source_column = source_column
        self.target_column = target_column

    def __str__(self):
        return f"Mapper: {self.map_name}, Action: {self.action}, Source: {self.source_column}, Target: {self.target_column}"

class DataAsset:
    def __init__(self, data_asset_id: str, domain: str, asset_name: str, asset_description: str = None, medallion_layer: str = "gold", container: str = None, feed_path: str = None):
        self.data_asset_id = data_asset_id
        self.domain = domain
        self.asset_name = asset_name
        self.asset_description = asset_description
        self.medallion_layer = medallion_layer
        self.container = container
        self.feed_path = feed_path

    @classmethod
    def from_json(cls, json_str: str):
        data = json.loads(json_str)
        return cls(
            data_asset_id=data.get('data_asset_id'),
            domain=data.get('domain'),
            asset_name=data.get('asset_name'),
            asset_description=data.get('asset_description', ''),
            medallion_layer=data.get('medallion_layer', 'gold'),
            container=data.get('container', ''),
            feed_path=data.get('feed_path', '')
        )

    def __str__(self):
        return f"DataAsset(ID: {self.data_asset_id}, Domain: {self.domain}, Name: {self.asset_name}, Description: {self.asset_description}, Layer: {self.medallion_layer}, Container: {self.container}, Path: {self.feed_path})"

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
    parser.add_argument("--map", type=str, help="The name of the mapper to extract")
    parser.add_argument("--asset", type=str, help="The JSON string of the data asset")  # Add asset argument
    args = parser.parse_args()

    database_path = "example.duckdb"
    db_wrapper = DuckDBWrapper(database_path)
    db_wrapper.connect()
    
    if args.map:
        query = f"SELECT * FROM data_language.mapper WHERE map_name = '{args.map}'"
        result_json = db_wrapper.execute_select_query_to_json(query)
        for result in json.loads(result_json):
            print(result)

    if args.asset:
        query = f"SELECT * FROM data_language.data_assets WHERE asset_name = '{args.asset}'"
        result_json = db_wrapper.execute_select_query_to_json(query)
        for result in json.loads(result_json):
            print(result)
            data_asset = DataAsset.from_json(json.dumps(result))
            print(f"Data Asset: {data_asset}")

    # Sample DataFrame for demonstration
    data = {'existing_col': [1, 2, 3, 4, 5]}
    df = pd.DataFrame(data)
    
    # Create FrameMapper instance and apply transformation
    frame_mapper = FrameMapper(df)
    transformed_df = frame_mapper.transform()
    
    # Print the transformed DataFrame
    print(transformed_df)

if __name__ == "__main__":
    main()
