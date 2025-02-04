from scanners.DLISProcessorBase import DLISProcessorBase

class DLISFramesProcessor(DLISProcessorBase):
    """
    Processes the equipment data in a DLIS logical file and handles extraction and transformation.
    """

    def extract_frames(self):
        """
        Extracts and processes equipment data into a JSON-like format.

        Returns:
            dict: Processed equipment data.
        """
        attributes = {
            "name": "name",
            "description": "description",
            "channels": "channels",
            "index_type": "index_type",
            "direction": "direction",
            "spacing": "spacing",
            "index_min": "index_min",
            "index_max": "index_max",
            "encrypted": "encrypted",
        }
        units_relevant_columns = [
            "spacing", "index_min", "index_max"
        ]

        related_columns = ["channels"]

        return self.process_items(attributes, units_relevant_columns, related_columns=related_columns)
