from DLISProcessorBase import DLISProcessorBase

class DLISZoneProcessor(DLISProcessorBase):
    """
    Processes the equipment data in a DLIS logical file and handles extraction and transformation.
    """

    def extract_zones(self):
        """
        Extracts and processes equipment data into a JSON-like format.

        Returns:
            dict: Processed equipment data.
        """
        attributes = {
            "name": "name",
            "domain": "domain",
            "maximum": "maximum",
            "minimum": "minimum"
        }
        units_relevant_columns = [
            "maximum", "minimum"
        ]

        return self.process_items(attributes, units_relevant_columns)
