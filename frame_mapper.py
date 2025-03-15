import argparse
from enum import Enum
import json  # Import json module
import yaml  # Import yaml module
import uuid  # Import uuid module
from datetime import datetime  # Import datetime module
from config import database_path

class FrameMapper:
    def __init__(self, mapper, uuid_str=None, cob=None):
        self.mapper = mapper
        self.uuid = uuid_str if uuid_str else str(uuid.uuid4())
        self.cob = cob if cob else datetime.now().strftime('%Y%m%d')
        self.load_config()
        self.load_mapper()

    def load_config(self):
        with open('config.yaml', 'r') as file:
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
                else:
                    value = value.replace(f'{{{token}}}', self.mapping.get(token, ''))
        return value

def main():
    """
    Main method to demonstrate the usage of FrameMapper class.
    """
    parser = argparse.ArgumentParser(description="Frame Mapper Executor")
    parser.add_argument("--mapper", type=str, help="The name of the mapper to use")
    args = parser.parse_args()

    if args.mapper:
        frame_mapper = FrameMapper(args.mapper)
        print(frame_mapper.get_mappping_property("from_asset_path"))
        print(frame_mapper.get_mappping_property("to_asset_path"))
        
if __name__ == "__main__":
    main()
