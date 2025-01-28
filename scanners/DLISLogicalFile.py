from scanners.DLISOriginsProcessor import DLISOriginsProcessor
import json
from scanners.DLISParametersProcessor import DLISParametersProcessor
from scanners.DLISToolsProcessor import DLISToolsProcessor
from scanners.DLISChannelsProcessor import DLISChannelsProcessor
from scanners.DLISEquipmentsProcessor import DLISEquipmentsProcessor
from scanners.DLISFramesProcessor import DLISFramesProcessor
from scanners.DLISZonesProcessor import DLISZoneProcessor
from utils.dlis_utils import transform_curves_to_json_well_log_format


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
        # try:
        # Delegate origin processing to DLISOriginsProcessor
        origins_processor = DLISOriginsProcessor(
            logical_file_id=self._logical_file_id,
            origins=self._logical_file.origins
        )
        header = origins_processor.map_headers()

        parameters_processor = DLISParametersProcessor(
            logical_file_id=self._logical_file_id,
            items=self._logical_file.parameters
        )
        parameters = parameters_processor.extract_parameters()

        equipments_processor = DLISEquipmentsProcessor(
            logical_file_id=self._logical_file_id,
            items=self._logical_file.equipments
        )
        equipments = equipments_processor.extract_equipments()

        zones_processor = DLISZoneProcessor(
            logical_file_id=self._logical_file_id,
            items=self._logical_file.zones
        )
        zones = zones_processor.extract_zones()

        tools_processor = DLISToolsProcessor(
            logical_file_id=self._logical_file_id,
            items=self._logical_file.tools
        )
        tools = tools_processor.extract_tools()

        # Process frames, channels, and curves
        combined_output = []
        for frame in self._logical_file.frames:
            # Extract frame-level metadata
            frames_processor = DLISFramesProcessor(
                logical_file_id=self._logical_file_id,
                items=[frame]
            )
            frame_data = frames_processor.extract_frames()

            # Extract channels and curves for the current frame
            channels_processor = DLISChannelsProcessor(
                logical_file_id=self._logical_file_id,
                items=frame.channels
            )
            channels = channels_processor.extract_channels()
            formatted_channels = transform_curves_to_json_well_log_format(channels)
            curves = channels_processor.extract_bulk_data()

            # Combine all data into the frame-specific dictionary
            frame_output = {
                "header": header,
                "parameters": parameters,
                "equipments": equipments,
                "zones": zones,
                "tools": tools,
                "frame": frame_data,
                "curves": formatted_channels,
                "data": curves
            }

            combined_output.append(frame_output)
        return combined_output