"""
This class will take file path as input and call the appropriate scanner based on file extension
This will create an abstraction layer on scanner classes
"""
import pathlib
from scanners.las_scanner import LasScanner
# from scanners.dlis_scanner import DLISScanner

class Scanner:

    def __init__(self, file):
        self._file = file
        self._scanner = None

    def scan(self):
        file_extension = pathlib.Path(self._file).suffix.upper()

        if file_extension == '.LAS':
            self._scanner = LasScanner(self._file)
            return self._scanner.scan()
        # # adding the logic to support DLIS scanning
        # elif file_extension == '.DLIS':
        #     self.scanner = DLISScanner(self.file)
        #     return self.scanner.scan()