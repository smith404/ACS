import argparse
import sys
import unittest
from enum import Enum
from io import StringIO
import json  # Import json module
import yaml  # Import yaml module
import uuid  # Import uuid module
import os  # Import os module
from datetime import datetime  # Import datetime module
import pandas as pd

from frame_mapper import FrameMapper  # Import Window module

class PandasFrameMapper(FrameMapper):
    def __init__(self, mapper, uuid_str=None, cob=None, time=None, version=None):
        super().__init__(mapper, uuid_str, cob, time, version)

    def apply_arch_config(self):
        pandar_config = self.mapping.get("arch_config", {})
        for key, value in pandar_config.items():
            pass  # Apply specific configurations if needed

    def load_file_to_string(self, file_path):
        with open(file_path, "r") as file:
            return StringIO(file.read())
            
    def write_string_to_file(self, file_path, content, overwrite=True):
        mode = "w" if overwrite else "x"  # 'x' mode raises an error if the file exists
        with open(file_path, mode) as file:
            file.write(content)

    def load_from_data(self, from_asset_path, log_str=None):
        self.status_signal_path = os.path.dirname(from_asset_path) + "/" + self.log_name + ".FAILURE"
        return pd.read_parquet(from_asset_path)

    def write_to_data(self, df, to_asset_path, overwrite=True, log_str=None):
        compression = self.config.get("compression", "none")
        if not overwrite and os.path.exists(to_asset_path):
            raise FileExistsError(f"File {to_asset_path} already exists and overwrite is set to False.")
        df.to_parquet(to_asset_path, compression=compression if compression != "none" else None, index=False)
        self.status_signal_path = os.path.dirname(to_asset_path) + "/" + self.log_name + ".SUCCESS"

    def load_data_from_csv(self, file_path, header=True, infer_schema=True):
        return pd.read_csv(file_path, header=0 if header else None, dtype=None if infer_schema else str)

    def load_data_from_parquet(self, file_path):
        return pd.read_parquet(file_path)

    def transfrom_type_include(self, mapping, df, log_str=None):
        transform_rule_path = self.get_file_path(self.replace_tokens(mapping.get("transform_rule_path")))

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
        rename_dict = {col.get("source_column"): col.get("target_column") for col in columns}
        df = df.rename(columns=rename_dict)
        return df

    def transfrom_type_drop_columns(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        df = df.drop(columns=columns)
        return df

    def transfrom_type_set_columns(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            trim = column.get("trim", False)
            if "pattern_value" in column:
                value = self.replace_tokens(column.get("pattern_value"))
                # '_this_' replacement is not directly supported in pandas, so skip or implement as needed
                if trim and isinstance(value, str):
                    value = value.strip()
                df[column.get("source_column")] = value
            else:
                value = column.get("target_value")
                if trim and isinstance(value, str):
                    value = value.strip()
                df[column.get("source_column")] = value
        return df

    def transfrom_type_simplemap(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        mapping_dict = mapping.get("mapping", {})
        for column in columns:
            df[column] = df[column].map(mapping_dict).fillna(df[column])
        return df

    def transfrom_type_group_by(self, mapping, df, log_str=None):
        group_columns = mapping.get("columns", [])
        aggregations = mapping.get("aggregations", [])
        agg_dict = {}
        for agg in aggregations:
            func = agg.get("function")
            source = agg.get("source_column")
            target = agg.get("target_column")
            agg_dict[target] = (source, func)
        # Build aggregation dictionary for pandas
        pandas_agg = {target: pd.NamedAgg(column=source, aggfunc=func) for target, (source, func) in agg_dict.items()}
        df = df.groupby(group_columns).agg(**pandas_agg).reset_index()
        return df

    def transfrom_type_split_column(self, mapping, df, log_str=None):
        source_column = mapping.get("source_column")
        delimiter = mapping.get("delimiter")
        target_columns = mapping.get("target_columns", [])
        if source_column and delimiter and target_columns:
            splits = df[source_column].str.split(delimiter, expand=True)
            for idx, target_column in enumerate(target_columns):
                if idx < splits.shape[1]:
                    df[target_column] = splits[idx]
        return df

    def transfrom_type_merge_columns(self, mapping, df, log_str=None):
        target_column = mapping.get("target_column")
        delimiter = mapping.get("delimiter")
        source_columns = mapping.get("source_columns", [])
        if target_column and delimiter and source_columns:
            df[target_column] = df[source_columns].astype(str).agg(delimiter.join, axis=1)
        return df

    def transfrom_type_set_column_type(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            col_name = column.get("column")
            col_type = column.get("type")
            col_format = column.get("format", None)
            if col_type == "int":
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64')
            elif col_type == "float":
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
            elif col_type == "string":
                df[col_name] = df[col_name].astype(str)
            elif col_type == "boolean":
                df[col_name] = df[col_name].astype(bool)
            elif col_type == "date":
                df[col_name] = pd.to_datetime(df[col_name], format=col_format, errors='coerce').dt.date
            elif col_type == "timestamp":
                df[col_name] = pd.to_datetime(df[col_name], format=col_format, errors='coerce')
            # Add more types as needed
        return df

    def transfrom_type_copy_columns(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            df[column.get("target_column")] = df[column.get("source_column")]
        return df

    def transfrom_type_trim_columns(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            if df[column].dtype == object:
                df[column] = df[column].str.strip()
        return df

    def transfrom_type_transpose_columns(self, mapping, df, log_str=None):
        id_column = mapping.get("id_column")
        value_columns = mapping.get("value_columns", [])
        if id_column and value_columns:
            df = df.melt(id_vars=[col for col in df.columns if col not in value_columns],
                         value_vars=value_columns,
                         var_name=id_column,
                         value_name="value")
        return df

    def transfrom_type_map(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        mapping_file = self.replace_tokens(mapping.get("mapping_file", None))
        map_dict = mapping.get("mapping", {})
        default_value = mapping.get("default_value", None)

        if mapping_file:
            if mapping_file.endswith(".csv"):
                map_df = self.load_data_from_csv(mapping_file)
            elif mapping_file.endswith(".parquet"):
                map_df = self.load_data_from_parquet(mapping_file)
            else:
                if log_str:
                    log_str.write(f"Unsupported mapping file format: {mapping_file}\n")
                return df
            map_dict = dict(zip(map_df["from"], map_df["to"]))

        for column in columns:
            if default_value is not None:
                df[column] = df[column].map(map_dict).fillna(default_value)
            else:
                df[column] = df[column].map(map_dict).fillna(df[column])
        return df

    def transfrom_type_join(self, mapping, df, log_str=None):
        join_file = self.replace_tokens(mapping.get("join_file"))
        how = mapping.get("how", "inner")
        on_rules = mapping.get("on_rules", [])

        if join_file.endswith(".parquet"):
            join_df = self.load_data_from_parquet(join_file)
        elif join_file.endswith(".csv"):
            join_df = self.load_data_from_csv(join_file)
        else:
            if log_str:
                log_str.write(f"Unsupported join file format: {join_file}\n")
            return df

        left_on = [rule["source_column"] for rule in on_rules]
        right_on = [rule["target_column"] for rule in on_rules]
        df = df.merge(join_df, left_on=left_on, right_on=right_on, how=how)
        return df

    def transfrom_type_select(self, mapping, df, log_str=None):
        columns = mapping.get("columns")
        df = df[columns]
        if mapping.get("distinct", False):
            df = df.drop_duplicates(subset=columns)
        # Filtering by conditions is not implemented here
        return df

    def transfrom_type_update_columns(self, mapping, df, log_str=None):
        columns = mapping.get("columns", [])
        for column in columns:
            # Only simple unconditional update is supported here
            target_column = column.get("target_column")
            if target_column:
                df[target_column] = df[column.get("source_column")]
            else:
                df[column.get("source_column")] = column.get("target_value")
        return df

    def build_condition_expr(self, conditions, df):
        mask = pd.Series([True] * len(df), index=df.index)
        for condition in conditions:
            col_name = condition.get("column")
            operator = condition.get("operator")
            value = condition.get("value")
            value_column = condition.get("value_column")

            if value_column:
                value_expr = df[value_column]
            else:
                value_expr = value

            expr = self.get_condition_expr(df, col_name, operator, value_expr)

            if expr is not None:
                mask = mask & expr
        return mask

    def get_condition_expr(self, df, col_name, operator, value_expr):
        if operator == ">":
            return df[col_name] > value_expr
        elif operator == "<":
            return df[col_name] < value_expr
        elif operator == "==":
            return df[col_name] == value_expr
        elif operator == "!=":
            return df[col_name] != value_expr
        elif operator == ">=":
            return df[col_name] >= value_expr
        elif operator == "<=":
            return df[col_name] <= value_expr
        elif operator == "like":
            return df[col_name].astype(str).str.contains(str(value_expr), na=False, regex=True)
        elif operator == "not like":
            return ~df[col_name].astype(str).str.contains(str(value_expr), na=False, regex=True)
        elif operator == "is_null":
            return df[col_name].isnull()
        elif operator == "is_not_null":
            return df[col_name].notnull()
        else:
            return None

    def transfrom_type_append(self, mapping, df, log_str=None):
        data_file = self.replace_tokens(mapping.get("data_file"))
        # Load the append DataFrame
        if data_file.endswith(".parquet"):
            append_df = self.load_data_from_parquet(data_file)
        elif data_file.endswith(".csv"):
            append_df = self.load_data_from_csv(data_file)
        else:
            if log_str:
                log_str.write(f"Unsupported append file format: {data_file}\n")
            return df

        # Check schema match (column names and dtypes)
        if list(df.columns) != list(append_df.columns) or not all(df.dtypes == append_df.dtypes):
            if log_str:
                log_str.write(
                    f"Schema mismatch: current columns {list(df.columns)}, dtypes {list(df.dtypes)}; "
                    f"append file columns {list(append_df.columns)}, dtypes {list(append_df.dtypes)}\n"
                )
            return df

        # Append (concat) the data
        return pd.concat([df, append_df], ignore_index=True)

# Unit test support
class TestPySparkFrameMapper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mapper = PandasFrameMapper(mapper=None)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_transfrom_type_set_column_type(self):
        df = pd.DataFrame({
            "a": ["1", "2", "3"],
            "b": ["4.1", "5.2", "6.3"],
            "c": [1, 0, 1],
            "d": ["2020-01-01", "2020-01-02", "2020-01-03"]
        })
        mapping = {
            "columns": [
                {"column": "a", "type": "int"},
                {"column": "b", "type": "float"},
                {"column": "c", "type": "boolean"},
                {"column": "d", "type": "date", "format": "%Y-%m-%d"}
            ]
        }
        result = self.mapper.transfrom_type_set_column_type(mapping, df.copy())
        self.assertTrue(pd.api.types.is_integer_dtype(result["a"]))
        self.assertTrue(pd.api.types.is_float_dtype(result["b"]))
        self.assertTrue(pd.api.types.is_bool_dtype(result["c"]))
        self.assertTrue(pd.api.types.is_object_dtype(result["d"]))  # dates as objects

    def test_transfrom_type_rename_columns(self):
        df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
        mapping = {
            "columns": [
                {"source_column": "x", "target_column": "a"},
                {"source_column": "y", "target_column": "b"}
            ]
        }
        result = self.mapper.transfrom_type_rename_columns(mapping, df.copy())
        self.assertIn("a", result.columns)
        self.assertIn("b", result.columns)
        self.assertNotIn("x", result.columns)
        self.assertNotIn("y", result.columns)

    def test_transfrom_type_drop_columns(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        mapping = {"columns": ["b", "c"]}
        result = self.mapper.transfrom_type_drop_columns(mapping, df.copy())
        self.assertIn("a", result.columns)
        self.assertNotIn("b", result.columns)
        self.assertNotIn("c", result.columns)

    def test_transfrom_type_simplemap(self):
        df = pd.DataFrame({"col1": ["A", "B", "C"]})
        mapping = {
            "columns": ["col1"],
            "mapping": {"A": "X", "B": "Y"}
        }
        result = self.mapper.transfrom_type_simplemap(mapping, df.copy())
        self.assertEqual(list(result["col1"]), ["X", "Y", "C"])

def main():
    if "--unittest" in sys.argv:
        sys.argv = [sys.argv[0]]  # Remove extra args for unittest
        unittest.main(warnings='ignore')
        exit(0)

    parser = argparse.ArgumentParser(description="Frame Mapper Executor")
    parser.add_argument("--mapper", type=str, help="The name of the mapper to use")
    args = parser.parse_args()

    if args.mapper:
        frame_mapper = PandasFrameMapper(args.mapper)
        frame_mapper.process_transforms()
        
if __name__ == "__main__":
    main()
