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
        self.status_signal_path = "no_file_path.log"
        self.load_config()
        self.load_mapper()
        self.apply_config()

    def load_config(self):
        # This method is intentionally left empty because the configuration loading logic
        # will be implemented in a subclass or provided later as per specific requirements.
        pass

    def load_mapper(self):
        # This method is intentionally left empty because the mapping loading logic
        # will be implemented in a subclass or provided later as per specific requirements.
        pass

    def apply_config(self):
        # This method is intentionally left empty because the configuration is implementation specific.
        # The logic for applying the configuration to the mapper will be implemented in a subclass
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
            if from_asset_path:
                df = self.load_from_data(from_asset_path, log_str)
                transforms = self.mapping.get('transforms', [])
                df = self.apply_transforms(transforms, df, log_str)
                to_asset_path = self.get_mappping_property("to_asset_path")
                if to_asset_path:
                    self.write_from_data(df, to_asset_path, log_str)
        except Exception as e:
            log_str.write(f"Error processing transforms: {e}\n")
        finally:
            end_time = datetime.now()
            log_str.write(f"End Time: {end_time}\n")            
            self.write_signal_file(self.status_signal_path, log_str)
            log_str.close()

    def load_from_data(self, from_asset_path, log_str):
        # This method is intentionally left empty because the loading is implementation specific.
        # The method must return the DataFrame after loading the data from the specified path.
        # The logic for applying the configuration to the mapper will be implemented in a subclass
        pass

    def write_from_data(self, df, to_asset_path, log_str):
        # This method is intentionally left empty because the saving is implementation specific.
        # The logic for applying the configuration to the mapper will be implemented in a subclass
        pass

    def write_signal_file(self, status_signal_path, log_str):
        # This method is intentionally left empty because the log saving is implementation specific.
        # The logic for applying the configuration to the mapper will be implemented in a subclass
        pass

    def apply_config(self):
        # This method is intentionally left empty because the configuration is implementation specific.
        # The logic for applying the configuration to the mapper will be implemented in a subclass
        pass

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
        # This method is intentionally left empty because the logic for the 'include' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_rename_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'rename_columns' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_drop_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'drop_columns' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_set_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'set_columns' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_simplemap(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'simplemap' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_select(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'select' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass
    
    def transfrom_type_select_expression(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'select_expression' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass
    
    def transfrom_type_group_by(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'group_by' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_update_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'update_columns' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_split_column(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'split_column' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_merge_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'merge_columns' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_set_column_type(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'column_type' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_copy_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'copy_columns' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_trim_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'trim_columns' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_transpose_columns(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'transpose_columns' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass

    def transfrom_type_map(self, mapping, df, log_str=None):
        # This method is intentionally left empty because the logic for the 'map' transform type
        # is not yet defined. It will be implemented in the future based on specific requirements.
        pass
