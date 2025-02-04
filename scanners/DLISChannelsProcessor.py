from scanners.DLISProcessorBase import DLISProcessorBase
import traceback
import numpy as np

class DLISChannelsProcessor(DLISProcessorBase):
    """
    Processes the equipment data in a DLIS logical file and handles extraction and transformation.
    """

    def extract_channels(self):
        """
        Extracts and processes equipment data into a JSON-like format.

        Returns:
            dict: Processed equipment data.
        """
        attributes = {
            "name": "name",
            "long_name": "description",
            "reprc": "reprc",
            "units": "units",
            "properties": "properties",
            "dimension": "dimension",
            "axis": "axis",
            "element_limit": "element_limit",
            "source": "source",
            "frame": "frame",
        }
        units_relevant_columns = []

        related_columns = ["frame", "source", "axis"]

        return self.process_items(attributes, units_relevant_columns, related_columns=related_columns)

    # def extract_bulk_data(self, null_value=None):
    #     """
    #     Optimized extraction of bulk data (curve measurements) from DLIS channels.
    #
    #     Args:
    #         null_value (float, optional): Value to replace NaNs. Defaults to None.
    #
    #     Returns:
    #         dict: A dictionary containing "data" as a list of rows with values for each channel.
    #     """
    #     try:
    #         channel_data = []
    #         max_rows = 0
    #         channel_names = []
    #         for channel in self._items:
    #             try:
    #                 # Retrieve data for the channel
    #                 channel_values = channel.curves()
    #                 channel_names.append(channel.name)
    #
    #                 # Determine the number of rows (index length)
    #                 rows = channel_values.shape[0] if hasattr(channel_values, "shape") else len(channel_values)
    #                 max_rows = max(max_rows, rows)
    #
    #                 # Append channel data as-is for now
    #                 channel_data.append(channel_values)
    #
    #             except Exception as e:
    #                 print(f"Error retrieving data for channel '{channel.name}': {e}")
    #                 continue
    #
    #         print(f"Data acquired for channels {channel_names}")
    #         # Prepare the final data structure
    #         formatted_data = []
    #
    #         # Iterate through each row (index) and collect channel values
    #         for row_idx in range(max_rows):
    #             row_data = []
    #             for channel_values in channel_data:
    #                 try:
    #                     if hasattr(channel_values, "shape") and len(channel_values.shape) == 2:
    #                         # Handle 2D data
    #                         if row_idx < channel_values.shape[0]:
    #                             row_data.append(channel_values[row_idx].tolist())
    #                         else:
    #                             row_data.append([null_value] * channel_values.shape[1])  # Pad missing rows
    #                     else:
    #                         # Handle 1D data
    #                         if row_idx < len(channel_values):
    #                             row_data.append(channel_values[row_idx])
    #                         else:
    #                             row_data.append(null_value)  # Pad missing rows
    #                 except Exception as e:
    #                     print(f"Error processing data at row {row_idx}: {e}")
    #                     row_data.append(null_value)
    #
    #             # Add the row to the formatted data
    #             formatted_data.append(row_data)
    #
    #         return formatted_data
    #
    #     except Exception as e:
    #         print(f"Error during bulk data extraction: {e}")
    #         print(traceback.format_exc())  # Prints the entire stack trace
    #         return []

    def extract_bulk_data(self, null_value=None):
        """
        Optimized extraction of bulk data (curve measurements) from DLIS channels.

        Args:
            null_value (float, optional): Value to replace NaNs. Defaults to None.

        Returns:
            list: A list containing "data" as a list of rows with values for each channel.
        """
        try:
            channel_data = []
            channel_names = []
            max_rows = 0

            # Extract and analyze data for each channel
            for channel in self._items:
                try:
                    channel_values = channel.curves()  # Get data
                    rows = channel_values.shape[0] if hasattr(channel_values, "shape") else len(channel_values)

                    channel_names.append(channel.name)
                    channel_data.append(channel_values)
                    max_rows = max(max_rows, rows)

                except Exception as e:
                    print(f"Error retrieving data for channel '{channel.name}': {e}")
                    continue

            print(f"Data acquired for channels: {channel_names}")

            # Pre-allocate storage for formatted data
            formatted_data = np.full((max_rows, len(channel_data)), null_value, dtype=object)

            # Populate the formatted data efficiently
            for col_idx, channel_values in enumerate(channel_data):
                try:
                    if hasattr(channel_values, "shape") and len(channel_values.shape) == 2:
                        # 2D data: Assign directly
                        row_count = channel_values.shape[0]
                        formatted_data[:row_count, col_idx] = [list(row) for row in channel_values]

                    else:
                        # 1D data: Assign directly
                        row_count = len(channel_values)
                        formatted_data[:row_count, col_idx] = channel_values

                except Exception as e:
                    print(f"Error processing column {col_idx}: {e}")

            return formatted_data.tolist()

        except Exception as e:
            print(f"Unexpected error in extract_bulk_data: {e}")
            return []