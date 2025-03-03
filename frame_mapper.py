import pandas as pd
from enum import Enum

class TransformationActionType(Enum):
    RENAME = "rename"
    SIMPLE_MAP = "simple_map"
    REFORMAT = "reformat"
    REMOVE = "remove"

class FrameMapper:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def transform(self) -> pd.DataFrame:
        # Apply transformation rules here
        transformed_df = self.df.copy()
        # Example transformation: Add a new column 'new_col' with values doubled from 'existing_col'
        if 'existing_col' in transformed_df.columns:
            transformed_df['new_col'] = transformed_df['existing_col'] * 2
        return transformed_df
