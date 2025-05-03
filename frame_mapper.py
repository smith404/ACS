from enum import Enum
from io import StringIO
import json  # Import json module
import yaml  # Import yaml module
import uuid  # Import uuid module
import os  # Import os module
from datetime import datetime  # Import datetime module

class FrameMapper:
    JSON_EXTENSION = '.json'

    def __init__(self, mapper, uuid_str=None, cob=None, time=None, version=None):
        self.mapper = mapper
        self.uuid = uuid_str if uuid_str else str(uuid.uuid4())
        self.cob = cob if cob else datetime.now().strftime('%Y%m%d')
        self.time = time if time else datetime.now().strftime('%H-%M-%S')
        self.version = version if version else "v1.0.0"
        self.load_config()
        self.load_mapper()

    def load_config(self):
        pass

    def load_mapper(self):
        pass

    def get_mapping(self):
        return self.mapping

    def get_mappping_property(self, property_name):
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
            start_time = datetime.now()
            log_str = StringIO()
            log_str.write(f"Start Time: {start_time}\n") 
            from_asset_path = self.get_mappping_property("from_asset_path")
            log_str.write("Running with configuration:\n")
            log_str.write(json.dumps(self.mapping, indent=4))
            log_str.write("\n")
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
            log_str.write(f"Error processing transforms: {e}\n")
        finally:
            end_time = datetime.now()
            log_str.write(f"End Time: {end_time}\n")            
            if (self.dbutils):
                self.dbutils.fs.put(status_signal_path, contents=log_str.getvalue(), overwrite=True)
            else:
                with open(status_signal_path, 'w') as file:
                    file.write(log_str.getvalue())
            log_str.close()

    def apply_transforms(self, transforms, df, log_str=None):
        current_transfrom = "{}"
        try:
            if isinstance(transforms, list):
                for transform in transforms:
                    current_transfrom = json.dumps(transform, indent=4)
                    df = self.apply_transform(transform, df, log_str)
            return df
        except Exception:
            log_str.write(f"Error processing transforms: {current_transfrom}\n")
            raise  # Re-throw the exception

    def apply_transform(self, transform, df, log_str=None):
        current_time = datetime.now()
        transform_type = transform.get('transform_type')
        if transform_type:
            method_name = f"transfrom_type_{transform_type}"
            method = getattr(self, method_name, None)
            if callable(method):
                log_str.write(f"Calling method for transform type: {transform_type} at {current_time}\n")
                return method(transform, df, log_str)
            else:
                log_str.write(f"No method found for transform type: {transform_type} at {current_time}\n")
                return df

    def transfrom_type_include(self, mapping, df, log_str=None):
        transform_rule_path = self.replace_tokens(mapping.get("transform_rule_path"))
        if not transform_rule_path.endswith(self.JSON_EXTENSION):
            transform_rule_path += self.JSON_EXTENSION
        
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
