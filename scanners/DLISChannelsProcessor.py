from DLISProcessorBase import DLISProcessorBase
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
    #     Optimized extraction of bulk data (curve measurements) from DLIS channels using NumPy.
    #
    #     Args:
    #         null_value (float, optional): Value to replace NaNs. Defaults to None.
    #
    #     Returns:
    #         list: A list of data rows, each being an array of values corresponding to the channels.
    #     """
    #     try:
    #         # Extract data from all channels as a NumPy array
    #         channel_data = []
    #
    #         for channel in self._items:
    #             try:
    #                 # Retrieve channel data and append to the list
    #                 print(f"retreving data for {channel.name}")
    #                 channel_values = channel.curves()
    #                 print(f"Channel {channel.name}: Shape: {np.shape(channel_values)}")
    #                 print(f"retreival for {channel.name} completed")
    #                 channel_data.append(channel_values)
    #             except Exception as e:
    #                 print(f"Error retrieving data for channel '{channel.name}': {e}")
    #                 continue
    #
    #
    #         #     # Convert the list of channel data into a NumPy array
    #         # curve_data = np.array(channel_data)
    #         #
    #         # # Transpose the array to align rows with indices and columns with channels
    #         # curve_data = curve_data.T
    #
    #         return curve_data.tolist()
    #         # return channel_data
    #     except Exception as e:
    #         print(f"Error during bulk data extraction: {e}")
    #         raise

    def extract_bulk_data(self, null_value=None):
        """
        Optimized extraction of bulk data (curve measurements) from DLIS channels.

        Args:
            null_value (float, optional): Value to replace NaNs. Defaults to None.

        Returns:
            dict: A dictionary containing "data" as a list of rows with values for each channel.
        """
        try:
            channel_data = []
            max_rows = 0

            for channel in self._items:
                try:
                    # Retrieve data for the channel
                    print(f"Retrieving data for channel: {channel.name}")
                    channel_values = channel.curves()
                    print(f"Retrieval for channel: {channel.name} completed")

                    # Determine the number of rows (index length)
                    rows = channel_values.shape[0] if hasattr(channel_values, "shape") else len(channel_values)
                    max_rows = max(max_rows, rows)

                    # Append channel data as-is for now
                    channel_data.append(channel_values)

                except Exception as e:
                    print(f"Error retrieving data for channel '{channel.name}': {e}")
                    continue

            # Prepare the final data structure
            formatted_data = []

            # Iterate through each row (index) and collect channel values
            for row_idx in range(max_rows):
                row_data = []
                for channel_values in channel_data:
                    try:
                        if hasattr(channel_values, "shape") and len(channel_values.shape) == 2:
                            # Handle 2D data
                            if row_idx < channel_values.shape[0]:
                                row_data.append(channel_values[row_idx].tolist())
                            else:
                                row_data.append([null_value] * channel_values.shape[1])  # Pad missing rows
                        else:
                            # Handle 1D data
                            if row_idx < len(channel_values):
                                row_data.append(channel_values[row_idx])
                            else:
                                row_data.append(null_value)  # Pad missing rows
                    except Exception as e:
                        print(f"Error processing data at row {row_idx}: {e}")
                        row_data.append(null_value)

                # Add the row to the formatted data
                formatted_data.append(row_data)

            return {"data": formatted_data}

        except Exception as e:
            print(f"Error during bulk data extraction: {e}")
            raise