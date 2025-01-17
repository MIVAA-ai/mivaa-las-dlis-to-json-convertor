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
        return first_logical_file_object.scan_logical_file()