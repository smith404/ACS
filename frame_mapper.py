from enum import Enum
from io import StringIO
import json  # Import json module
import yaml  # Import yaml module
import uuid  # Import uuid module
import os  # Import os module
from datetime import datetime  # Import datetime module

class FrameMapper:
    JSON_EXTENSION = ".json"

    def __init__(self, mapper, uuid_str=None, cob=None, time=None, version=None):
        self.mapper = mapper
        self.uuid = uuid_str if uuid_str else str(uuid.uuid4())
        self.cob = cob if cob else datetime.now().strftime("%Y%m%d")
        self.time = time if time else datetime.now().strftime("%H-%M-%S")
        self.version = version if version else "v1.0.0"
        self.status_signal_path = "no_file_path.log"
        self.load_config()
        self.load_mapper()
        self.apply_arch_config()

    def load_config(self):
        config_home = os.getenv("FM_CONFIG_HOME", ".") 
        config_filename = "config.yaml"
        environment = os.getenv("FM_ENVIRONMENT")
        if environment:
            config_filename = f"config-{environment}.yaml"
        config_path = os.path.join(config_home, config_filename)
        file_content = self.load_file_to_string(config_path)
        self.config = yaml.safe_load(file_content)
        self.mapper_directory = self.config.get("mapper_directory", "")

    def load_mapper(self):
        if not self.mapper.endswith(FrameMapper.JSON_EXTENSION):
            self.mapper += FrameMapper.JSON_EXTENSION
        mapper_path = f"{self.mapper_directory}/{self.mapper}"
        file_content = self.load_file_to_string(mapper_path)
        self.mapping = json.load(file_content)

    def load_file_to_string(self, file_path):
        # This method is intentionally left empty because the configuration is implementation specific.
        # The logic for loading the file to string will be implemented in a subclass
        pass

    def write_string_to_file(self, file_path, content):
        # This method is intentionally left empty because the configuration is implementation specific.
        # The logic for writing the string to file will be implemented in a subclass
        pass

    def get_mapping(self):
        return self.mapping

    def get_mappping_property(self, property_name):
        value = self.mapping.get(property_name)
        if isinstance(value, str):
            value = self.replace_tokens(value)
        return value

    def get_file_path(self, path, extension=JSON_EXTENSION, absolute_marker="$"):
        transform_rule_path = self.replace_tokens(path)
        if not transform_rule_path.endswith(extension):
            transform_rule_path += extension
        if transform_rule_path.startswith(absolute_marker):
            transform_rule_path = transform_rule_path[len(absolute_marker):]
        else:
            transform_rule_path = f"{self.mapper_directory}/{transform_rule_path}"
        return transform_rule_path

    def replace_tokens(self, value):
        tokens = [token.strip("{}") for token in value.split(sep="/") if token.startswith("{") and token.endswith("}")]
        for token in tokens:
            if token == "uuid":
                value = value.replace(f"{{{token}}}", self.uuid)
            elif token == "cob":
                value = value.replace(f"{{{token}}}", self.cob)
            elif token == "time":
                value = value.replace(f"{{{token}}}", self.time)
            elif token == "version":
                value = value.replace(f"{{{token}}}", self.version)
            else:
                value = value.replace(f"{{{token}}}", self.mapping.get(token, ""))
        return value

    def process_transforms(self, pre_process_method=None, process_method=None, post_process_method=None):
        try:
            start_time = datetime.now()
            log_str = StringIO()
            log_str.write(f"Start Time: {start_time}\n") 
            from_asset_path = self.get_mappping_property("from_asset_path")
            log_str.write("Running with configuration:\n")
            log_str.write(json.dumps(self.mapping, indent=4))
            log_str.write("\n")
            if from_asset_path:
                df = self.load_from_data(from_asset_path, log_str)
                
                # Call pre_process_method if provided
                if pre_process_method:
                    df = pre_process_method(df=df, log_str=log_str)
                
                transforms = self.mapping.get("transforms", [])
                df = self.apply_transforms(transforms, df, log_str=log_str, process_method=process_method)
                
                # Call post_process_method if provided
                if post_process_method:
                    df = post_process_method(df=df, log_str=log_str)
                
                to_asset_path = self.get_mappping_property("to_asset_path")
                if to_asset_path:
                    self.write_to_data(df, to_asset_path, log_str)
        except Exception as e:
            log_str.write(f"Error processing transforms: {e}\n")
        finally:
            end_time = datetime.now()
            log_str.write(f"End Time: {end_time}\n")            
            self.write_log_file(self.status_signal_path, log_str)
            log_str.close()

    def write_log_file(self, status_signal_path, log_str):
        self.write_string_to_file(status_signal_path, log_str.getvalue())

    def load_from_data(self, from_asset_path, log_str):
        # This method is intentionally left empty because the loading is implementation specific.
        # The method must return the DataFrame after loading the data from the specified path.
        # The logic for applying the configuration to the mapper will be implemented in a subclass
        pass

    def write_to_data(self, df, to_asset_path, log_str):
        # This method is intentionally left empty because the saving is implementation specific.
        # The logic for applying the configuration to the mapper will be implemented in a subclass
        pass

    def apply_arch_config(self):
        # This method is intentionally left empty because the configuration is implementation specific.
        # The logic for applying the configuration to the mapper will be implemented in a subclass
        pass

    def apply_transforms(self, transforms, df, log_str=None, process_method=None):
        current_transfrom = "{}"
        try:
            if isinstance(transforms, list):
                for transform in transforms:
                    current_transfrom = json.dumps(transform, indent=4)
                    df = self.apply_transform(transform, df, log_str=log_str, process_method=process_method)
            return df
        except Exception:
            log_str.write(f"Error processing transforms: {current_transfrom}\n")
            raise  # Re-throw the exception

    def apply_transform(self, transform, df, log_str=None, process_method=None):
        current_time = datetime.now()
        transform_type = transform.get("transform_type")
        if transform_type:
            method_name = f"transfrom_type_{transform_type}"
            method = getattr(self, method_name, None)
            if callable(method):
                log_str.write(f"Calling method for transform type: {transform_type} at {current_time}\n")
                # Call process_method if provided
                if process_method:
                    process_method(df=df, log_str=log_str, transfrom=transform)
                return method(transform, df, log_str)
            else:
                log_str.write(f"No method found for transform type: {transform_type} at {current_time}\n")
                return df

    def transfrom_type_include(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "include" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_rename_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "rename_columns" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_drop_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "drop_columns" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_set_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "set_columns" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_simplemap(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "simplemap" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_select(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "select" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass
    
    def transfrom_type_select_expression(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "select_expression" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass
    
    def transfrom_type_group_by(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "group_by" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_update_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "update_columns" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_split_column(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "split_column" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_merge_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "merge_columns" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_set_column_type(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "column_type" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_copy_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "copy_columns" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_trim_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "trim_columns" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_transpose_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "transpose_columns" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_map(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the "map" transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass
