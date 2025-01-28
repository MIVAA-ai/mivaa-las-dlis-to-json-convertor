import numpy as np
from utils.dlis_utils import summary_dataframe, extract_metadata, extract_units, extract_relationships, process_dataframe_lists
import traceback
import inspect

class DLISProcessorBase:
    """
    Base class for shared logic between DLIS processors (parameters and equipments).
    """

    def __init__(self, logical_file_id, items, nulls_list=None):
        """
        Initialize the DLISProcessorBase.

        Args:
            logical_file_id (str): Unique identifier for the logical file.
            items (list): List of metadata objects (parameters or equipments).
            nulls_list (list, optional): List of null values to remove from the DataFrame. Defaults to None.
        """
        self._logical_file_id = logical_file_id
        self._items = items
        self._nulls_list = nulls_list or []

    def process_items(self, attributes, units_relevant_columns, related_columns=[]):
        """
        Processes the items into a DataFrame and converts to a JSON-like format.

        Args:
            attributes (dict): Mapping of attributes for the summary DataFrame.
            units_relevant_columns (list): List of columns for which to extract units.

        Returns:
            dict: Processed metadata in a JSON-like format.
        """
        # Get the calling class using the inspect module
        frame = inspect.currentframe()
        try:
            caller_class = inspect.getouterframes(frame)[1].frame.f_locals.get('self', None).__class__.__name__
        finally:
            del frame  # Avoid reference cycles


        # Create a DataFrame using the summary function
        items_df = summary_dataframe(self._items, **attributes)

        if items_df.empty:
            print(f"No items for {caller_class} found for logical file: {self._logical_file_id}")
            return {}

        try:
            # Extract units for relevant columns and add them as new columns
            for column in units_relevant_columns:
                # Transform dataframe values. Convert 'value' to a hashable type
                items_df[column] = items_df[column].apply(
                    lambda v:
                    v[0] if isinstance(v, (list, np.ndarray)) and len(v) == 1 else
                    "; ".join(v) if isinstance(v, (list, np.ndarray)) else
                    v
                )

                unit_column = f"{column}_unit"
                items_df[unit_column] = extract_units(
                    metadata=self._items, metadata_df=items_df, column_name=column.upper()
                )

            for column in related_columns:
                items_df[column] = extract_relationships(metadata_df=items_df, column_name=column)

            # Add logical file ID
            items_df["logical-file-id"] = self._logical_file_id

            # Remove rows with null values
            items_df = items_df[~items_df.isin(self._nulls_list).any(axis=1)]

            # Clean and deduplicate the DataFrame
            items_df = process_dataframe_lists(items_df)
            # Transform the DataFrame into the JSON-like format
            return extract_metadata(items_df)

        except Exception as e:
            print(f"Error processing items for logical file {self._logical_file_id}: {e}")
            print(traceback.format_exc())  # Prints the entire stack trace
            raise