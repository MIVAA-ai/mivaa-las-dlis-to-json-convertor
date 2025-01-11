# import pandas as pd
# from utils.dlis_utils import extract_metadata
# from utils.dlis_utils import summary_dataframe
# from utils.dlis_utils import extract_units
#
# class DLISEquipmentsProcessor:
#     """
#     Processes the equipment data in a DLIS logical file and handles extraction and transformation.
#     """
#
#     def __init__(self, logical_file_id, equipments, nulls_list=None):
#         """
#         Initialize the DLISEquipmentsProcessor.
#
#         Args:
#             logical_file_id (str): Unique identifier for the logical file.
#             equipments (list): List of equipment objects in the logical file.
#             nulls_list (list, optional): List of null values to remove from the DataFrame. Defaults to None.
#         """
#         self._logical_file_id = logical_file_id
#         self._equipments = equipments
#         self._nulls_list = nulls_list or []
#
#     def extract_equipments(self):
#         """
#         Extracts and processes equipment data into a DataFrame.
#
#         Returns:
#             pd.DataFrame: A DataFrame containing equipment data.
#         """
#         # Pass the specified parameters dynamically
#         equipments_df = summary_dataframe(
#             self._equipments,
#             name="name",
#             trademark_name="trademark_name",
#             status="status",
#             generic_type="generic_type",
#             serial_number="serial_number",
#             location="location",
#             height="height",
#             length="length",
#             diameter_min="diameter_min",
#             diameter_max="diameter_max",
#             volume="volume",
#             weight="weight",
#             hole_size="hole_size",
#             pressure="pressure",
#             temperature="temperature",
#             vertical_depth="vertical_depth",
#             radial_drift="radial_drift",
#             angular_drift="angular_drift"
#         )
#
#         if equipments_df.empty:
#             print(f"No equipment found for logical file: {self._logical_file_id}")
#             return equipments_df
#
#         try:
#             # Extract units for relevant columns and add them as new columns
#             units_relevant_columns = [
#                 "height", "length", "diameter_min", "diameter_max", "volume",
#                 "weight", "hole_size", "pressure", "temperature", "vertical_depth",
#                 "radial_drift", "angular_drift"
#             ]
#
#             for column in units_relevant_columns:
#                 # Initialize a new column for units (e.g., "height_unit")
#                 unit_column = f"{column}_unit"
#                 equipments_df[unit_column] = extract_units(metadata=self._equipments, metadata_df=equipments_df, column_name=column.upper())
#
#             # Add logical file ID
#             equipments_df["logical-file-id"] = self._logical_file_id
#
#             # Remove rows with null values
#             equipments_df = equipments_df[~equipments_df.isin(self._nulls_list).any(axis=1)]
#
#             # Clean and deduplicate the DataFrame
#             equipments_df = equipments_df.drop_duplicates(ignore_index=True)
#
#             #converting the data frame to the acceptable json well log format
#             equipments = extract_metadata(equipments_df)
#
#             return equipments
#
#         except Exception as e:
#             print(f"Error processing equipment for logical file {self._logical_file_id}: {e}")
#             raise
from DLISProcessorBase import DLISProcessorBase

class DLISEquipmentsProcessor(DLISProcessorBase):
    """
    Processes the equipment data in a DLIS logical file and handles extraction and transformation.
    """

    def extract_equipments(self):
        """
        Extracts and processes equipment data into a JSON-like format.

        Returns:
            dict: Processed equipment data.
        """
        attributes = {
            "name": "name",
            "trademark_name": "trademark_name",
            "status": "status",
            "generic_type": "generic_type",
            "serial_number": "serial_number",
            "location": "location",
            "height": "height",
            "length": "length",
            "diameter_min": "diameter_min",
            "diameter_max": "diameter_max",
            "volume": "volume",
            "weight": "weight",
            "hole_size": "hole_size",
            "pressure": "pressure",
            "temperature": "temperature",
            "vertical_depth": "vertical_depth",
            "radial_drift": "radial_drift",
            "angular_drift": "angular_drift"
        }
        units_relevant_columns = [
            "height", "length", "diameter_min", "diameter_max", "volume",
            "weight", "hole_size", "pressure", "temperature", "vertical_depth",
            "radial_drift", "angular_drift"
        ]

        return self.process_items(attributes, units_relevant_columns)
