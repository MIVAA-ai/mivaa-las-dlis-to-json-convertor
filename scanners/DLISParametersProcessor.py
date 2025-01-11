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