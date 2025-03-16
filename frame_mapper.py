# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

import argparse
from enum import Enum
import json  # Import json module
import yaml  # Import yaml module
import uuid  # Import uuid module
import os  # Import os module
from datetime import datetime  # Import datetime module
from config import database_path
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

class FrameMapper:
    def __init__(self, mapper, spark, uuid_str=None, cob=None, time=None, version=None):
        self.mapper = mapper
        self.spark = spark
        self.uuid = uuid_str if uuid_str else str(uuid.uuid4())
        self.cob = cob if cob else datetime.now().strftime('%Y%m%d')
        self.time = time if time else datetime.now().strftime('%H-%M-%S')
        self.version = version if version else "v1.0.0"
        self.load_config()
        self.load_mapper()

    def load_config(self):
        config_home = os.getenv('FM_CONFIG_HOME', '.')  # Get environment variable or default to current directory
        config_filename = 'config.yaml'
        environment = os.getenv('FM_ENVIRONMENT')
        if environment:
            config_filename = f'config-{environment}.yaml'
        config_path = os.path.join(config_home, config_filename)
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.mapper_directory = self.config.get('mapper_directory', '')

    def load_mapper(self):
        if not self.mapper.endswith('.json'):
            self.mapper += '.json'
        mapper_path = f"{self.mapper_directory}/{self.mapper}"
        with open(mapper_path, 'r') as file:
            self.mapping = json.load(file)

    def get_mapping(self):
        return self.mapping

    def get_mappping_property(self, property_name):
        """
        Get a property value from the config.
        
        :param property_name: The name of the property to retrieve.
        :return: The value of the property or None if the property does not exist.
        """
        value = self.mapping.get(property_name)
        if isinstance(value, str):
            tokens = [token.strip('{}') for token in value.split(sep="/") if token.startswith('{') and token.endswith('}')]
            for token in tokens:
                if token == 'uuid':
                    value = value.replace(f'{{{token}}}', self.uuid)
                elif token == 'cob':
                    value = value.replace(f'{{{token}}}', self.cob)
                elif token == 'time':
                    value = value.replace(f'{{{token}}}', self.time)
                elif token == 'version':
                    value = value.replace(f'{{{token}}}', self.version)
                else:
                    value = value.replace(f'{{{token}}}', self.mapping.get(token, ''))
        return value

    def process_transforms(self):
        from_asset_path = self.get_mappping_property("from_asset_path")
        if from_asset_path:
            df = self.spark.read.format("parquet").option("header", "true").load(from_asset_path)
            transforms = self.mapping.get('transforms', [])
            df = self.apply_transforms(transforms, df)
            to_asset_path = self.get_mappping_property("to_asset_path")
            if to_asset_path:
                compression = self.config.get('compression', 'none')  # Get compression from config
                df.write.format("parquet").mode("overwrite").option("compression", compression).save(to_asset_path)

    def apply_transforms(self, transforms, df):
        if isinstance(transforms, list):
            for transform in transforms:
                df = self.apply_transform(transform, df)
        return df

    def apply_transform(self, transform, df):
        transform_type = transform.get('transform_type')
        if transform_type:
            method_name = f"transfrom_type_{transform_type}"
            method = getattr(self, method_name, None)
            if callable(method):
                 return method(transform, df)
            else:
                print(f"No method found for transform type: {transform_type}")
                return df

    def transfrom_type_rename(self, mapping, df):
        df = df.withColumnRenamed(mapping.get("source_column"), mapping.get("target_column"))
        return df

    def transfrom_type_set_column(self, mapping, df):
        df = df = df.withColumn(mapping.get("source_column"), sf.lit(mapping.get("default_value")))
        return df

    def transfrom_type_drop_column(self, mapping, df):
        df = df.drop(mapping.get("source_column"))
        return df

    def transfrom_type_simplemap(self, mapping, df):
        for key, value in mapping.get("mapping").items():
            df = df.withColumn(mapping.get("source_column"), sf.when(sf.col(mapping.get("source_column")) == key, sf.lit(value)).otherwise(sf.col(mapping.get("source_column"))))
        return df

    def transfrom_type_select(self, mapping, df):
        df = df.select(mapping.get("columns"))
        return df
    
    def transfrom_type_select_expression(self, mapping, df):
        df = df.selectExpr(mapping.get("columns"))
        return df
    
    def transfrom_type_group_by(self, mapping, df):
        method_name = mapping.get('function')
        method = getattr(sf, method_name, None)
        if callable(method):
            df = df.groupBy(mapping.get('columns')).agg(method(mapping.get('source_column')).alias(mapping.get('target_column')))
        else:
            print(f"No aggregation method found for : {method_name}")    
        return df
    
def main():
    """
    Main method to demonstrate the usage of FrameMapper class.
    """
    parser = argparse.ArgumentParser(description="Frame Mapper Executor")
    parser.add_argument("--mapper", type=str, help="The name of the mapper to use")
    args = parser.parse_args()

    if args.mapper:
        # Initialize a SparkSession
        spark_session = SparkSession.builder \
            .appName("PySparkExample") \
            .getOrCreate()

        frame_mapper = FrameMapper(args.mapper, spark=spark_session)
        frame_mapper.process_transforms()
        
if __name__ == "__main__":
    main()
