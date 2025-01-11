# import pandas as pd
# import numpy as np
# from utils.dlis_utils import summary_dataframe
# from utils.dlis_utils import extract_metadata
# from utils.dlis_utils import extract_units
#
# class DLISParametersProcessor:
#     """
#     Processes the parameters in a DLIS logical file and handles extraction and transformation.
#     """
#     def __init__(self, logical_file_id, parameters, nulls_list=None):
#         """
#         Initialize the DLISParametersProcessor.
#
#         Args:
#             logical_file_id (str): Unique identifier for the logical file.
#             parameters (list): List of parameter objects in the logical file.
#             nulls_list (list, optional): List of null values to remove from the DataFrame. Defaults to None.
#         """
#         self._logical_file_id = logical_file_id
#         self._parameters = parameters
#         self._nulls_list = nulls_list or []
#
#     def extract_parameters(self):
#         """
#         Extracts and processes parameters into a DataFrame.
#
#         Returns:
#             pd.DataFrame: A DataFrame containing parameter data.
#         """
#         param_df = summary_dataframe(
#             self._parameters, name="name", values="value", long_name="description"
#         )
#
#         if param_df.empty:
#             print(f"No parameters found for logical file: {self._logical_file_id}")
#             return param_df
#
#         try:
#             # Extract units for relevant columns and add them as new columns
#             units_relevant_columns = [
#                 "values"
#             ]
#
#             for column in units_relevant_columns:
#                 # Initialize a new column for units (e.g., "height_unit")
#                 unit_column = f"{column}_unit"
#                 param_df[unit_column] = extract_units(metadata=self._parameters, metadata_df=param_df, column_name=column.upper())
#
#             # Add logical file ID
#             param_df["logical-file-id"] = self._logical_file_id
#
#             # Transform parameter values. Convert 'value' to a hashable type
#             param_df["value"] = param_df["value"].apply(
#                 lambda v:
#                 v[0] if isinstance(v, (list, np.ndarray)) and len(v) == 1 else
#                 "; ".join(v) if isinstance(v, (list, np.ndarray)) else
#                 v
#             )
#
#             # Remove rows with null values
#             param_df = param_df[~param_df.isin(self._nulls_list).any(axis=1)]
#
#             # Clean and deduplicate the DataFrame
#             param_df = param_df.drop_duplicates(ignore_index=True)
#             # param_df = param_df.drop_duplicates(subset=["name", "value"], ignore_index=True)
#
#             #converting the data frame to the acceptable json well log format
#             parameters = extract_metadata(param_df)
#             return parameters
#
#         except Exception as e:
#             print(f"Error processing parameters for logical file {self._logical_file_id}: {e}")
#             raise
from DLISProcessorBase import DLISProcessorBase

class DLISParametersProcessor(DLISProcessorBase):
    """
    Processes the parameters in a DLIS logical file and handles extraction and transformation.
    """

    def extract_parameters(self):
        """
        Extracts and processes parameters into a JSON-like format.

        Returns:
            dict: Processed parameter data.
        """
        attributes = {
            "name": "name",
            "values": "values",
            "long_name": "description"
        }
        units_relevant_columns = ["values"]

        return self.process_items(attributes, units_relevant_columns)