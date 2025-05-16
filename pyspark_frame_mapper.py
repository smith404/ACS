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

    def apply_arch_config(self):
        spark_config = self.mapping.get("arch_config", {})
        for key, value in spark_config.items():
            self.spark.conf.set(key, value)

    def load_file_to_string(self, file_path):
        if self.dbutils:
            file_content = self.dbutils.fs.head(file_path)
            return StringIO(file_content)
        else:
            with open(file_path, "r") as file:
                return StringIO(file.read())
            
    def write_string_to_file(self, file_path, content, overwrite=True):
        if self.dbutils:
            self.dbutils.fs.put(file_path, contents=content, overwrite=overwrite)
        else:
            mode = "w" if overwrite else "x"  # 'x' mode raises an error if the file exists
            with open(file_path, mode) as file:
                file.write(content)

    def load_from_data(self, from_asset_path, log_str=None):
        self.status_signal_path = os.path.dirname(from_asset_path) + "/" + self.log_name + ".FAILURE"
        return self.spark.read.format("parquet").option("header", "true").load(from_asset_path)

    def write_to_data(self, df, to_asset_path, overwrite=True, log_str=None):
        compression = self.config.get("compression", "none")
        df.write.format("parquet").mode("overwrite").option("compression", compression).save(to_asset_path)
        self.status_signal_path = os.path.dirname(to_asset_path) + "/" + self.log_name + ".SUCCESS"

    def load_data_from_csv(self, file_path, header=True, infer_schema=True):
        return self.spark.read.format("csv").option("header", header).option("inferSchema", infer_schema).load(file_path)

    def load_data_from_parquet(self, file_path):
        return self.spark.read.format("parquet").load(file_path)

    def transfrom_type_include(self, mapping, df, log_str=None):
        transform_rule_path = self.get_file_path(mapping.get("transform_rule_path"))

        file_content = self.load_file_to_string(transform_rule_path)
        included_transforms = json.load(file_content)

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
        aggregations = mapping.get("aggregations", [])
        if isinstance(aggregations, list):
            agg_exprs = []
            for agg in aggregations:
                method_name = agg.get("function")
                method = getattr(sf, method_name, None)
                if callable(method):
                    agg_exprs.append(method(agg.get("source_column")).alias(agg.get("target_column")))
                else:
                    log_str.write(f"No aggregation method found for: {method_name}\n")
            df = df.groupBy(mapping.get("columns")).agg(*agg_exprs)
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
            df = self.apply_column_type(df, col_name, col_type, col_format, log_str)
        return df

    def apply_column_type(self, df, col_name, col_type, col_format, log_str):
        if col_type == "int":
            return df.withColumn(col_name, sf.col(col_name).cast("int"))
        elif col_type == "float":
            return df.withColumn(col_name, sf.col(col_name).cast("float"))
        elif col_type == "string":
            return self.apply_string_type(df, col_name, col_format, log_str)
        elif col_type == "boolean":
            return df.withColumn(col_name, sf.col(col_name).cast("boolean"))
        elif col_type == "date":
            return self.apply_date_type(df, col_name, col_format)
        elif col_type == "timestamp":
            return self.apply_timestamp_type(df, col_name, col_format)
        elif col_type in ["long", "double", "short", "byte"]:
            return df.withColumn(col_name, sf.col(col_name).cast(col_type))
        elif col_type == "counter":
            return self.apply_counter_type(df, col_name)
        return df

    def apply_string_type(self, df, col_name, col_format, log_str):
        if col_format:
            current_type = dict(df.dtypes).get(col_name)
            if current_type and (current_type.startswith("date") or current_type.startswith("timestamp")):
                return df.withColumn(col_name, sf.date_format(sf.col(col_name), col_format))
            else:
                log_str.write(f"Unsupported format conversion for column '{col_name}' with type '{current_type}'\n")
        return df.withColumn(col_name, sf.col(col_name).cast("string"))

    def apply_date_type(self, df, col_name, col_format):
        if col_format:
            return df.withColumn(col_name, sf.to_date(sf.col(col_name), col_format))
        return df.withColumn(col_name, sf.col(col_name).cast("date"))

    def apply_timestamp_type(self, df, col_name, col_format):
        if col_format:
            return df.withColumn(col_name, sf.to_timestamp(sf.col(col_name), col_format))
        return df.withColumn(col_name, sf.col(col_name).cast("timestamp"))

    def apply_counter_type(self, df, col_name):
        window_spec = Window.orderBy(sf.monotonically_increasing_id())
        return df.withColumn(col_name, sf.row_number().over(window_spec))

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
        columns = mapping.get("columns", [])
        mapping_file = mapping.get("mapping_file")
        map_dict = mapping.get("mapping", {})
        default_value = mapping.get("default_value", None)

        # Load mapping from file if mapping_file is provided
        if mapping_file:
            if mapping_file.endswith(".csv"):
                map_df = self.load_data_from_csv(mapping_file)
            elif mapping_file.endswith(".parquet"):
                map_df = self.load_data_from_parquet(mapping_file)
            else:
                log_str.write(f"Unsupported mapping file format: {mapping_file}\n")
                return df
            map_df = map_df.withColumnRenamed("from", "from").withColumnRenamed("to", "to")
        else:
            # Create a DataFrame from the mapping dictionary
            map_df = self.spark.createDataFrame(
                [(k, v) for k, v in map_dict.items()],
                ["from", "to"]
            )

        for column in columns:
            if default_value:
                df = df.join(
                    map_df,
                    on=(df[column] == map_df["from"]),
                    how="left"
                ).withColumn(
                    column,
                    sf.when(sf.col("to").isNotNull(), sf.col("to")).otherwise(sf.lit(default_value))
                ).drop("from", "to")
            else:
                df = df.join(
                    map_df,
                    on=(df[column] == map_df["from"]),
                    how="left"
                ).withColumn(
                    column,
                    sf.when(sf.col("to").isNotNull(), sf.col("to")).otherwise(sf.col(column))
                ).drop("from", "to")

        return df
    
    def transfrom_type_join(self, mapping, df, log_str=None):
        join_file = self.replace_tokens(mapping.get("join_file"))
        how = mapping.get("how", "inner")
        on_rules = mapping.get("on_rules", [])

        # Load the join DataFrame
        if join_file.endswith(".parquet"):
            join_df = self.load_data_from_parquet(join_file)
        elif join_file.endswith(".csv"):
            join_df = self.load_data_from_csv(join_file)
        else:
            if log_str:
                log_str.write(f"Unsupported join file format: {join_file}\n")
            return df

        join_conditions = [
            df[rule["source_column"]] == join_df[rule["target_column"]]
            for rule in on_rules
        ]

        if join_conditions:
            from functools import reduce
            join_condition = reduce(lambda a, b: a & b, join_conditions)
            df = df.join(join_df, join_condition, how=how)
        else:
            if log_str:
                log_str.write("No join conditions specified in 'on_rules'.\n")

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
