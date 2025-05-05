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
from io import StringIO
import json  # Import json module
import yaml  # Import yaml module
import uuid  # Import uuid module
import os  # Import os module
from datetime import datetime  # Import datetime module
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql import Window

from frame_mapper import FrameMapper  # Import Window module

class PySparkFrameMapper(FrameMapper):
    def __init__(self, mapper, spark=None, dbutils=None, uuid_str=None, cob=None, time=None, version=None):
        self.spark = spark
        self.dbutils = dbutils
        super().__init__(mapper, uuid_str, cob, time, version)

    def load_config(self):
        config_home = os.getenv('FM_CONFIG_HOME', '.') 
        config_filename = 'config.yaml'
        environment = os.getenv('FM_ENVIRONMENT')
        if environment:
            config_filename = f'config-{environment}.yaml'
        config_path = os.path.join(config_home, config_filename)
        if (self.dbutils):
            config_path = self.dbutils.fs.head(config_path)
            self.config = yaml.safe_load(config_path)
        else:
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
        self.mapper_directory = self.config.get('mapper_directory', '')
        
    def load_mapper(self):
        if not self.mapper.endswith(FrameMapper.JSON_EXTENSION):
            self.mapper += FrameMapper.JSON_EXTENSION
        mapper_path = f"{self.mapper_directory}/{self.mapper}"
        if (self.dbutils):
            config_path = self.dbutils.fs.head(mapper_path)
            self.mapping = json.loads(config_path)
        else:
            with open(mapper_path, 'r') as file:
                self.mapping = json.load(file)

    def apply_config(self):
        spark_config = self.mapping.get('arch_config', {})
        for key, value in spark_config.items():
            self.spark.conf.set(key, value)

    def load_from_data(self, from_asset_path, log_str):
        self.status_signal_path = os.path.dirname(from_asset_path) + "/status.FAILURE"
        return self.spark.read.format("parquet").option("header", "true").load(from_asset_path)

    def write_from_data(self, df, to_asset_path, log_str):
        compression = self.config.get('compression', 'none')
        df.write.format("parquet").mode("overwrite").option("compression", compression).save(to_asset_path)
        self.status_signal_path = os.path.dirname(to_asset_path) + "/status.SUCCESS"

    def write_signal_file(self, status_signal_path, log_str):
        if (self.dbutils):
            self.dbutils.fs.put(status_signal_path, contents=log_str.getvalue(), overwrite=True)
        else:
            with open(status_signal_path, 'w') as file:
                file.write(log_str.getvalue())

    def transfrom_type_include(self, mapping, df, log_str=None):
        transform_rule_path = self.replace_tokens(mapping.get("transform_rule_path"))
        if not transform_rule_path.endswith(FrameMapper.JSON_EXTENSION):
            transform_rule_path += FrameMapper.JSON_EXTENSION
        
        if self.dbutils:
            json_content = self.dbutils.fs.head(transform_rule_path)
            included_transforms = json.loads(json_content)
        else:
            with open(transform_rule_path, 'r') as file:
                included_transforms = json.load(file)

        transforms = included_transforms.get("transforms")
        if isinstance(transforms, list):
            df = self.apply_transforms(transforms, df, log_str)
        else:
            log_str.write(f"Invalid format in included file: {transform_rule_path}\n")
        
        return df

    def transfrom_type_rename_columns(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            df = df.withColumnRenamed(column.get("source_column"), column.get("target_column"))
        return df

    def transfrom_type_drop_columns(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            df = df.drop(column)
        return df

    def transfrom_type_set_columns(self, mapping, df, log_str=None):
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
                value = column.get("target_value")
                if trim and isinstance(value, str):
                    value = value.strip()
                df = df.withColumn(column.get("source_column"), sf.lit(value))
        return df

    def transfrom_type_simplemap(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            for key, value in mapping.get("mapping").items():
                df = df.withColumn(column, sf.when(sf.col(column) == key, sf.lit(value)).otherwise(sf.col(column)))
        return df

    def transfrom_type_select(self, mapping, df, log_str=None):
        df = df.select(mapping.get("columns"))
        filters = mapping.get("filters", [])
        if filters:
            for filter_condition in filters:
                column = filter_condition.get("column")
                operator = filter_condition.get("operator")
                value = filter_condition.get("value")
                condition_expr = self.get_condition_expr(column, operator, value)
                if condition_expr is not None:
                    df = df.filter(condition_expr)
        return df   
    
    def transfrom_type_select_expression(self, mapping, df, log_str=None):
        df = df.selectExpr(mapping.get("columns"))
        filters = mapping.get("filters", [])
        if filters:
            for filter_condition in filters:
                column = filter_condition.get("column")
                operator = filter_condition.get("operator")
                value = filter_condition.get("value")
                condition_expr = self.get_condition_expr(column, operator, value)
                if condition_expr is not None:
                    df = df.filter(condition_expr)
        return df   
    
    def transfrom_type_group_by(self, mapping, df, log_str=None):
        aggregations = mapping.get('aggregations', [])
        if isinstance(aggregations, list):
            agg_exprs = []
            for agg in aggregations:
                method_name = agg.get('function')
                method = getattr(sf, method_name, None)
                if callable(method):
                    agg_exprs.append(method(agg.get('source_column')).alias(agg.get('target_column')))
                else:
                    log_str.write(f"No aggregation method found for: {method_name}\n")
            df = df.groupBy(mapping.get('columns')).agg(*agg_exprs)
        return df

    def transfrom_type_update_columns(self, mapping, df, log_str=None):
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
        elif operator == "not like":
            return ~sf.col(col_name).like(value)
        elif operator == "is_null":
            return sf.col(col_name).isNull()
        elif operator == "is_not_null":
            return sf.col(col_name).isNotNull()
        else:
            return None

    def transfrom_type_split_column(self, mapping, df, log_str=None):
        source_column = mapping.get("source_column")
        delimiter = mapping.get("delimiter")
        target_columns = mapping.get("target_columns", [])
        if source_column and delimiter and target_columns:
            split_col = sf.split(sf.col(source_column), delimiter)
            for idx, target_column in enumerate(target_columns):
                df = df.withColumn(target_column, split_col.getItem(idx))
        return df

    def transfrom_type_merge_columns(self, mapping, df, log_str=None):
        target_column = mapping.get("target_column")
        delimiter = mapping.get("delimiter")
        source_columns = mapping.get("source_columns", [])
        if target_column and delimiter and source_columns:
            merged_col = sf.concat_ws(delimiter, *[sf.col(col) for col in source_columns])
            df = df.withColumn(target_column, merged_col)
        return df

    def transfrom_type_set_column_type(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            col_name = column.get("column")
            col_type = column.get("type")
            col_format = column.get("format", None)
            if col_type == "int":
                df = df.withColumn(col_name, sf.col(col_name).cast("int"))
            elif col_type == "float":
                df = df.withColumn(col_name, sf.col(col_name).cast("float"))
            elif col_type == "string":
                if col_format:
                    current_type = dict(df.dtypes).get(col_name)
                    if current_type and (current_type.startswith("date") or current_type.startswith("timestamp")):
                        df = df.withColumn(col_name, sf.date_format(sf.col(col_name), col_format))
                    else:
                        log_str.write(f"Unsupported format conversion for column '{col_name}' with type '{current_type}'\n")
                df = df.withColumn(col_name, sf.col(col_name).cast("string"))
            elif col_type == "boolean":
                df = df.withColumn(col_name, sf.col(col_name).cast("boolean"))
            elif col_type == "date":
                if col_format:
                    df = df.withColumn(col_name, sf.to_date(sf.col(col_name), col_format))
                else:
                    df = df.withColumn(col_name, sf.col(col_name).cast("date"))
            elif col_type == "timestamp":
                if col_format:
                    df = df.withColumn(col_name, sf.to_timestamp(sf.col(col_name), col_format))
                else:
                    df = df.withColumn(col_name, sf.col(col_name).cast("timestamp"))
            elif col_type == "long":
                df = df.withColumn(col_name, sf.col(col_name).cast("long"))
            elif col_type == "double":
                df = df.withColumn(col_name, sf.col(col_name).cast("double"))
            elif col_type == "short":
                df = df.withColumn(col_name, sf.col(col_name).cast("short"))
            elif col_type == "byte":
                df = df.withColumn(col_name, sf.col(col_name).cast("byte"))
            elif col_type == "counter":
                window_spec = Window.orderBy(sf.monotonically_increasing_id())
                df = df.withColumn(col_name, sf.row_number().over(window_spec))
        return df

    def transfrom_type_copy_columns(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            df = df.withColumn(column.get("target_column"), sf.col(column.get("source_column")))
        return df

    def transfrom_type_trim_columns(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            if dict(df.dtypes).get(column) == "string":
                df = df.withColumn(column, sf.trim(sf.col(column)))
        return df

    def transfrom_type_transpose_columns(self, mapping, df, log_str=None):
        id_column = mapping.get("id_column")
        value_columns = mapping.get("value_columns", [])
        if id_column and value_columns:
            df = df.withColumn(
                "transposed",
                sf.explode(
                    sf.array(
                        *[sf.struct(sf.lit(col).alias(id_column), sf.col(col).alias("value")) for col in value_columns]
                    )
                )
            )
            df = df.select(
                *[col for col in df.columns if col != "transposed"],
                sf.col("transposed." + id_column).alias(id_column),
                sf.col("transposed.value").alias("value")
            )      
        return df

    def transfrom_type_map(self, mapping, df, log_str=None):
        columns = mapping.get("columns", )
        map_dict = mapping.get("mapping", {})
        default_value = mapping.get("default_value", None)
        
        # Create a DataFrame from the mapping dictionary
        map_df = self.spark.createDataFrame(
            [(k, v) for k, v in map_dict.items()],
            ["from", "to"]
        )
   
        for column in columns:
            if default_value:
                df = df.join(map_df, 
                            on=(df[column] == map_df["from"]),
                            how="left"
                        ).withColumn(
                            column,
                            sf.when(sf.col("to").isNotNull(), sf.col("to")).otherwise(sf.lit(default_value))
                        ).drop("from", "to")
            else:
                df = df.join(map_df, 
                            on=(df[column] == map_df["from"]),
                            how="left"
                        ).withColumn(
                            column,
                            sf.when(sf.col("to").isNotNull(), sf.col("to")).otherwise(sf.col(column))
                        ).drop("from", "to")
        
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

        frame_mapper = PySparkFrameMapper(args.mapper, spark=spark_session)
        frame_mapper.process_transforms()

        try:
            spark_session.stop()
        except Exception as e:
            print(f"Error stopping Spark session: {e}")
        
if __name__ == "__main__":
    main()
