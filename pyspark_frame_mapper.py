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
from pyspark.sql import Window  # Import Window module

class FrameMapper:
    def __init__(self, mapper, spark, dbutils=None, uuid_str=None, cob=None, time=None, version=None):
        self.mapper = mapper
        self.spark = spark
        self.dbutils = dbutils
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
        if (self.dbutils):
            config_path = self.dbutils.fs.head(config_path)
            self.config = yaml.safe_load(config_path)
        else:
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
        self.mapper_directory = self.config.get('mapper_directory', '')

    def load_mapper(self):
        if not self.mapper.endswith('.json'):
            self.mapper += '.json'
        mapper_path = f"{self.mapper_directory}/{self.mapper}"
        if (self.dbutils):
            config_path = self.dbutils.fs.head(mapper_path)
            self.mapping = json.loads(config_path)
        else:
            with open(mapper_path, 'r') as file:
                self.mapping = json.load(file)

    def apply_spark_config(self):
        spark_config = self.mapping.get('spark_config', {})
        for key, value in spark_config.items():
            self.spark.conf.set(key, value)

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
        try:
            from_asset_path = self.get_mappping_property("from_asset_path")
            log_str = StringIO()
            log_str.write(json.dumps(self.mapping, indent=4))
            status_signal_path = os.path.dirname(from_asset_path) + "/status.FAILURE"
            if from_asset_path:
                df = self.spark.read.format("parquet").option("header", "true").load(from_asset_path)
                transforms = self.mapping.get('transforms', [])
                df = self.apply_transforms(transforms, df, log_str)
                to_asset_path = self.get_mappping_property("to_asset_path")
                if to_asset_path:
                    compression = self.config.get('compression', 'none')  # Get compression from config
                    df.write.format("parquet").mode("overwrite").option("compression", compression).save(to_asset_path)
                    status_signal_path = os.path.dirname(to_asset_path) + "/status.SUCCESS"
        except Exception as e:
            log_str.write(f"Error processing transforms: {e}")
        finally:
            log_str.close()
            if (self.dbutils):
                self.dbutils.fs.put(status_signal_path, contents=log_str.getvalue(), overwrite=True)
            else:
                with open(status_signal_path, 'w') as file:
                    file.write(log_str.getvalue())

    def apply_transforms(self, transforms, df, log_str=None):
        if isinstance(transforms, list):
            for transform in transforms:
                df = self.apply_transform(transform, df, log_str)
        return df

    def apply_transform(self, transform, df, log_str=None):
        transform_type = transform.get('transform_type')
        if transform_type:
            method_name = f"transfrom_type_{transform_type}"
            method = getattr(self, method_name, None)
            if callable(method):
                 return method(transform, df)
            else:
                log_str.write(f"No method found for transform type: {transform_type}")
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
                    log_str.write(f"No aggregation method found for: {method_name}")
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

        frame_mapper = FrameMapper(args.mapper, spark=spark_session)
        frame_mapper.process_transforms()

        try:
            spark_session.stop()
        except Exception as e:
            print(f"Error stopping Spark session: {e}")
        
if __name__ == "__main__":
    main()
