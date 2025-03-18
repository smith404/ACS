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
        self.apply_spark_config()

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

    def apply_spark_config(self):
        spark_config = self.config.get('spark_config', {})
        for key, value in spark_config.items():
            self.spark.conf.set(key, value)

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
            value = self.replace_tokens(value)
        return value

    def replace_tokens(self, value):
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

    def transfrom_type_rename_columns(self, mapping, df):
        columns = mapping.get("columns", [])
        for column in columns:
            df = df.withColumnRenamed(column.get("source_column"), column.get("target_column"))
        return df

    def transfrom_type_drop_columns(self, mapping, df):
        columns = mapping.get("columns", [])
        for column in columns:
            df = df.drop(column.get("source_column"))
        return df

    def transfrom_type_set_columns(self, mapping, df):
        columns = mapping.get("columns", [])
        for column in columns:
            trim = column.get("trim", False)
            if "pattern_value" in column:
                value = self.replace_tokens(column.get("pattern_value"))
                if "_this_" in value:
                    value = sf.expr(f"regexp_replace('{value}', '_this_', {column.get('source_column')})")
                if trim:
                    value = sf.trim(value)
                df = df.withColumn(column.get("source_column"), sf.lit(value))
            else:
                value = column.get("default_value")
                if trim and isinstance(value, str):
                    value = value.strip()
                df = df.withColumn(column.get("source_column"), sf.lit(value))
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
        aggregations = mapping.get('aggregations', [])
        if isinstance(aggregations, list):
            agg_exprs = []
            for agg in aggregations:
                method_name = agg.get('function')
                method = getattr(sf, method_name, None)
                if callable(method):
                    agg_exprs.append(method(agg.get('source_column')).alias(agg.get('target_column')))
                else:
                    print(f"No aggregation method found for: {method_name}")
            df = df.groupBy(mapping.get('columns')).agg(*agg_exprs)
        return df

    def transfrom_type_update_columns(self, mapping, df):
        columns = mapping.get("columns", [])
        for column in columns:
            condition_expr = self.build_condition_expr(column.get("conditions", []))
            if condition_expr is not None:
                df = df.withColumn(column.get("source_column"), sf.when(condition_expr, sf.lit(column.get("target_value"))).otherwise(sf.col(column.get("source_column"))))
        return df

    def build_condition_expr(self, conditions):
        condition_expr = None
        for condition in conditions:
            col_name = condition.get("column")
            operator = condition.get("operator")
            value = condition.get("value")
            expr = self.get_condition_expr(col_name, operator, value)
            if expr is not None:
                condition_expr = expr if condition_expr is None else condition_expr & expr
        return condition_expr

    def get_condition_expr(self, col_name, operator, value):
        if operator == ">":
            return sf.col(col_name) > value
        elif operator == "<":
            return sf.col(col_name) < value
        elif operator == "==":
            return sf.col(col_name) == value
        elif operator == "!=":
            return sf.col(col_name) != value
        elif operator == ">=":
            return sf.col(col_name) >= value
        elif operator == "<=":
            return sf.col(col_name) <= value
        elif operator == "like":
            return sf.col(col_name).like(value)
        elif operator == "is_null":
            return sf.col(col_name).isNull()
        elif operator == "is_not_null":
            return sf.col(col_name).isNotNull()
        else:
            return None

    def transfrom_type_split_column(self, mapping, df):
        source_column = mapping.get("source_column")
        delimiter = mapping.get("delimiter")
        target_columns = mapping.get("target_columns", [])
        if source_column and delimiter and target_columns:
            split_col = sf.split(sf.col(source_column), delimiter)
            for idx, target_column in enumerate(target_columns):
                df = df.withColumn(target_column, split_col.getItem(idx))
        return df

    def transfrom_type_merge_columns(self, mapping, df):
        target_column = mapping.get("target_column")
        delimiter = mapping.get("delimiter")
        source_columns = mapping.get("source_columns", [])
        if target_column and delimiter and source_columns:
            merged_col = sf.concat_ws(delimiter, *[sf.col(col) for col in source_columns])
            df = df.withColumn(target_column, merged_col)
        return df

    def transfrom_type_set_column_type(self, mapping, df):
        columns = mapping.get("columns", [])
        for column in columns:
            col_name = column.get("column")
            col_type = column.get("type")
            if col_type == "int":
                df = df.withColumn(col_name, sf.col(col_name).cast("int"))
            elif col_type == "float":
                df = df.withColumn(col_name, sf.col(col_name).cast("float"))
            elif col_type == "string":
                df = df.withColumn(col_name, sf.col(col_name).cast("string"))
            elif col_type == "boolean":
                df = df.withColumn(col_name, sf.col(col_name).cast("boolean"))
            elif col_type == "date":
                df = df.withColumn(col_name, sf.col(col_name).cast("date"))
            elif col_type == "timestamp":
                df = df.withColumn(col_name, sf.col(col_name).cast("timestamp"))
            elif col_type == "long":
                df = df.withColumn(col_name, sf.col(col_name).cast("long"))
            elif col_type == "double":
                df = df.withColumn(col_name, sf.col(col_name).cast("double"))
            elif col_type == "short":
                df = df.withColumn(col_name, sf.col(col_name).cast("short"))
            elif col_type == "byte":
                df = df.withColumn(col_name, sf.col(col_name).cast("byte"))
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

        try:
            spark_session.stop()
        except Exception as e:
            print(f"Error stopping Spark session: {e}")
        
if __name__ == "__main__":
    main()
