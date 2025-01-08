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

from dlisio import dlis

class DLISScanner:
    def __init__(self, file):
        self._file = file
        self._logical_files = list()

    def scan(self):
        first_logical_file, *tail_files = dlis.load(rf'{self._file}')

        self._logical_files.append(first_logical_file)

        for logical_file in tail_files:
            self._logical_files.append(logical_file)

        print(self._logical_files)
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
