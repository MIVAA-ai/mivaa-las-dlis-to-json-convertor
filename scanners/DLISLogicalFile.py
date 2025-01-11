from DLISOriginsProcessor import DLISOriginsProcessor
import json
from DLISParametersProcessor import DLISParametersProcessor
from DLISToolsProcessor import DLISToolsProcessor
from scanners.DLISEquipmentsProcessor import DLISEquipmentsProcessor


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

            parameters_processor = DLISParametersProcessor(
                logical_file_id=self._logical_file_id,
                items=self._logical_file.parameters
                # parameters=self._logical_file.parameters
            )

            parameters = parameters_processor.extract_parameters()
            print(json.dumps(parameters, indent=4))

            # refactor dlis parameters first
            # # tools_processor = DLISToolsProcessor(
            # #     logical_file_id=self._logical_file_id,
            # #     tools=self._logical_file.tools
            # # )
            # #
            # # tools_processor.extract_tools()
            #
            equipments_processor = DLISEquipmentsProcessor(
                logical_file_id=self._logical_file_id,
                items=self._logical_file.equipments
                # equipments=self._logical_file.equipments
            )

            equipments = equipments_processor.extract_equipments()
            print(json.dumps(equipments, indent=4))

        except Exception as e:
            print(f"Error scanning logical file {self._logical_file_id}: {e}")
            raise