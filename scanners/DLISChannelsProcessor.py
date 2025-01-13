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

    def extract_bulk_data(self, null_value=None):
        """
        Optimized extraction of bulk data (curve measurements) from DLIS channels using NumPy.

        Args:
            null_value (float, optional): Value to replace NaNs. Defaults to None.

        Returns:
            list: A list of data rows, each being an array of values corresponding to the channels.
        """
        try:
            # Extract data from all channels as a NumPy array
            channel_data = []

            for channel in self._items:
                try:
                    # Retrieve channel data and append to the list
                    print(f"retreving data for {channel.name}")
                    channel_values = channel.curves()
                    print(f"retreival for {channel.name} completed")
                    channel_data.append(channel_values)
                except Exception as e:
                    print(f"Error retrieving data for channel '{channel.name}': {e}")
                    continue

                # Convert the list of channel data into a NumPy array
            curve_data = np.array(channel_data)

            # Transpose the array to align rows with indices and columns with channels
            curve_data = curve_data.T

            return curve_data.tolist()
        except Exception as e:
            print(f"Error during bulk data extraction: {e}")
            raise

