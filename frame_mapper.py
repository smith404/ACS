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
    def __init__(self, map_id: str, map_name: str, map_description: str, from_asset_id: str, to_asset_id: str, target_attr_value_id: str):
        self.map_id = map_id
        self.map_name = map_name
        self.map_description = map_description
        self.from_asset_id = from_asset_id
        self.to_asset_id = to_asset_id
        self.target_attr_value_id = target_attr_value_id

    def describe(self) -> str:
        return json.dumps({
            'map_name': self.map_name,
            'map_description': self.map_description,
            'from_asset': self.from_asset_id,
            'to_asset': self.to_asset_id,
            'target_attr_value_id': self.target_attr_value_id
        })

    def to_json(self) -> str:
        return json.dumps({
            'map_id': self.map_id,
            'map_name': self.map_name,
            'map_description': self.map_description,
            'from_asset_id': self.from_asset_id,
            'to_asset_id': self.to_asset_id,
            'target_attr_value_id': self.target_attr_value_id
        })

    @classmethod
    def from_json(cls, json_str: str):
        data = json.loads(json_str)
        return cls(
            map_id=data.get('map_id'),
            map_name=data.get('map_name'),
            map_description=data.get('map_description', ''),
            from_asset_id=data.get('from_asset_id'),
            to_asset_id=data.get('to_asset_id'),
            target_attr_value_id=data.get('target_attr_value_id')
        )

    def __str__(self):
        return f"Mapper(ID: {self.map_id}, Name: {self.map_name}, Description: {self.map_description}, From Asset ID: {self.from_asset_id}, To Asset ID: {self.to_asset_id}, Target Attr Value ID: {self.target_attr_value_id})"

class DataAsset:
    def __init__(self, data_asset_id: str, domain: str, asset_name: str, asset_description: str = None, medallion_layer: str = "gold", container: str = None, feed_path: str = None):
        self.data_asset_id = data_asset_id
        self.domain = domain
        self.asset_name = asset_name
        self.asset_description = asset_description
        self.medallion_layer = medallion_layer
        self.container = container
        self.feed_path = feed_path

    def to_json(self) -> str:
        return json.dumps({
            'data_asset_id': self.data_asset_id,
            'domain': self.domain,
            'asset_name': self.asset_name,
            'asset_description': self.asset_description,
            'medallion_layer': self.medallion_layer,
            'container': self.container,
            'feed_path': self.feed_path
        })

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

class DataAttribute:
    def __init__(self, data_attribute_id: str, attribute_name: str, attribute_description: str, attribute_type: str = "string", attribute_length: int = 255, external_source: str = None):
        self.data_attribute_id = data_attribute_id
        self.attribute_name = attribute_name
        self.attribute_description = attribute_description
        self.attribute_type = attribute_type
        self.attribute_length = attribute_length
        self.external_source = external_source

    def to_json(self) -> str:
        return json.dumps({
            'data_attribute_id': self.data_attribute_id,
            'attribute_name': self.attribute_name,
            'attribute_description': self.attribute_description,
            'attribute_type': self.attribute_type,
            'attribute_length': self.attribute_length,
            'external_source': self.external_source
        })

    @classmethod
    def from_json(cls, json_str: str):
        data = json.loads(json_str)
        return cls(
            data_attribute_id=data.get('data_attribute_id'),
            attribute_name=data.get('attribute_name'),
            attribute_description=data.get('attribute_description', ''),
            attribute_type=data.get('attribute_type', 'string'),
            attribute_length=data.get('attribute_length', 255),
            external_source=data.get('external_source', '')
        )

    def __str__(self):
        return f"DataAttribute(ID: {self.data_attribute_id}, Name: {self.attribute_name}, Description: {self.attribute_description}, Type: {self.attribute_type}, Length: {self.attribute_length}, Source: {self.external_source})"

class MappingRule:
    def __init__(self, rule_id: str, rule_name: str, rule_description: str, rule_type: str, target_attr_value_id: str, target_attr_value_value: str, valid_from: str, valid_to: str):
        self.rule_id = rule_id
        self.rule_name = rule_name
        self.rule_description = rule_description
        self.rule_type = rule_type
        self.target_attr_value_id = target_attr_value_id
        self.target_attr_value_value = target_attr_value_value
        self.valid_from = valid_from
        self.valid_to = valid_to

    def parts(self) -> dict:
        return {
            'rule_id': self.rule_id,
            'rule_name': self.rule_name,
            'rule_description': self.rule_description,
            'rule_type': self.rule_type,
            'target_attr_value_id': self.target_attr_value_id,
            'target_attr_value_value': self.target_attr_value_value,
            'valid_from': self.valid_from,
            'valid_to': self.valid_to
        }
    
    def to_json(self) -> str:
        return json.dumps({
            'rule_id': self.rule_id,
            'rule_name': self.rule_name,
            'rule_description': self.rule_description,
            'rule_type': self.rule_type,
            'target_attr_value_id': self.target_attr_value_id,
            'target_attr_value_value': self.target_attr_value_value,
            'valid_from': self.valid_from,
            'valid_to': self.valid_to
        })

    @classmethod
    def from_json(cls, json_str: str):
        data = json.loads(json_str)
        return cls(
            rule_id=data.get('rule_id'),
            rule_name=data.get('rule_name'),
            rule_description=data.get('rule_description', ''),
            rule_type=data.get('rule_type', ''),
            target_attr_value_id=data.get('target_attr_value_id', ''),
            target_attr_value_value=data.get('target_attr_value_value', ''),
            valid_from=data.get('valid_from', ''),
            valid_to=data.get('valid_to', '')
        )

    def __str__(self):
        return f"MappingRule(ID: {self.rule_id}, Name: {self.rule_name}, Description: {self.rule_description}, Type: {self.rule_type}, Target Attr Value ID: {self.target_attr_value_id}, Target Attr Value: {self.target_attr_value_value}, Valid From: {self.valid_from}, Valid To: {self.valid_to})"

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
        rules_json = db_wrapper.execute_named_query_to_json("get_mapper_rules", parameters={"map_name": args.map})
        for rule in json.loads(rules_json):
            print(f"Mapper: {rule}")
            rule_part_json = db_wrapper.execute_named_query_to_json("get_rule_parts", parameters={"rule_id": rule['rule_id']})
            for rule_part in json.loads(rule_part_json):
                print(f"Rule Part: {rule_part}")

    if args.asset:
        result_json = db_wrapper.execute_named_query_to_json("read_asset", parameters={"asset_name": args.asset})
        for result in json.loads(result_json):
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
