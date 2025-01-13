from DLISProcessorBase import DLISProcessorBase

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
