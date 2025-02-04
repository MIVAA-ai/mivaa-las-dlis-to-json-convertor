from scanners.DLISProcessorBase import DLISProcessorBase

class DLISToolsProcessor(DLISProcessorBase):
    """
    Processes the equipment data in a DLIS logical file and handles extraction and transformation.
    """

    def extract_tools(self):
        """
        Extracts and processes equipment data into a JSON-like format.

        Returns:
            dict: Processed equipment data.
        """
        attributes = {
            "name": "name",
            "description": "description",
            "trademark_name": "trademark_name",
            "generic_name": "generic_name",
            "status": "status",
            "parts": "parts",
            "channels": "channels",
            "parameters": "parameters"
        }
        units_relevant_columns = []
        related_columns = ["parts", "channels", "parameters"]

        return self.process_items(attributes, units_relevant_columns, related_columns=related_columns)
