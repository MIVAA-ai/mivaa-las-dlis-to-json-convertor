"""
    to convert dlis into json well log format. we will assume the following

    The hierarchy of DLIS file is as follows
    Physical file
    |_Logical file1
        |_Frame1
            |_Channel1(Curve1)
            |_Channel2(Curve2)
        |_Frame2
    |_Logical file2

    A JSON Well Log file consists of one or more log sets each containing a log header, curve definitions and the corresponding measurement data.
    it means that one JSON Well Log file with represent one Logical File.
"""
from dataclasses import field

from dlisio import dlis
from DLISLogicalFile import DLISLogicalFile
class DLISScanner:
    """
       Scans a DLIS physical file and processes its logical files.
    """

    def __init__(self, file_path):
        self._file_path = file_path
        self._logical_files = []

    def scan(self):
        """
            Load and process all logical files in the DLIS physical file.
        """
        logical_files = dlis.load(self._file_path)
        self._logical_files.extend(logical_files)

        print(f"Loaded {len(self._logical_files)} logical files.")

        # Process the first logical file as an example
        first_logical_file_object = DLISLogicalFile(logical_file=self._logical_files[0])
        first_logical_file_object.scan_logical_file()

        # try:
        #     logical_files = dlis.load(self._file_path)
        #     self._logical_files.extend(logical_files)
        #
        #     print(f"Loaded {len(self._logical_files)} logical files.")
        #
        #     # Process the first logical file as an example
        #     first_logical_file_object = DLISLogicalFile(logical_file=self._logical_files[0])
        #     first_logical_file_object.scan_logical_file()
        #
        # except FileNotFoundError:
        #     print(f"File not found: {self._file_path}")
        # except Exception as e:
        #     print(f"Error scanning DLIS file: {e}")



        # las_file = lasio.read(self._file, engine="normal", encoding="utf-8")
        #
        # # Get different sections of the LAS file in JSON format
        # las_headers = self._extract_header(las_file)
        # null_value = las_headers.get("null", None)  # Use None if NULL is not defined
        # las_curves_headers = self._extract_curve_headers(las_file)
        # las_curves_data = self._extract_bulk_data(las_file, null_value)
        # las_parameters_data = self._extract_parameter_info(las_file)
        #
        # # Combine all sections into a single JSON structure
        # combined_output = [
        #     {
        #         "header": las_headers,
        #         "parameters": las_parameters_data,
        #         "curves": las_curves_headers,
        #         "data": las_curves_data
        #     }
        # ]
        #
        # # Validate against the Pydantic model
        # try:
        #     validated_data = JsonWellLogFormat.model_validate(combined_output)
        #     return validated_data
        # except ValidationError as e:
        #     # Log or handle validation errors
        #     print("Validation Error:", e)
        #     raise
