from DLISOriginsProcessor import DLISOriginsProcessor
import json

class DLISLogicalFile:
    """
    Represents a logical file in a DLIS physical file.
    Extracts, processes, and transforms origin data using pandas DataFrame.
    """

    def __init__(self, logical_file):
        """
        Initialize the DLISLogicalFile.

        Args:
            logical_file: The DLIS logical file object.
        """
        self._logical_file = logical_file
        self._logical_file_id = logical_file.fileheader.id

    def scan_logical_file(self):
        """
        Scans the logical file, extracts, transforms, and prints origin data as JSON.
        """
        try:
            # Delegate origin processing to DLISOriginsProcessor
            origins_processor = DLISOriginsProcessor(
                logical_file_id=self._logical_file_id,
                origins=self._logical_file.origins
            )

            # Map headers
            header = origins_processor.map_headers()

            # Pretty print the JSON output
            print(json.dumps(header, indent=4))

        except Exception as e:
            print(f"Error scanning logical file {self._logical_file_id}: {e}")
            raise