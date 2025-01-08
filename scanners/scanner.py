"""
This class will take file path as input and call the appropriate scanner based on file extension
This will create an abstraction layer on scanner classes
"""
import pathlib
from scanners.las_scanner import LasScanner
from scanners.dlis_scanner import DLISScanner
from mappings.WellLogsFormat import WellLogFormat
from utils.IdentifyWellLogFormat import IdentifyWellLogFormat

class Scanner:

    def __init__(self, file):
        self._file = file
        self._scanner = None

    def scan(self):
        try:
            # Identify the file format based on the file path
            file_extension = IdentifyWellLogFormat.GetFormat(filepath=self._file)

            # Initialize and use the appropriate scanner based on the file format
            if file_extension == WellLogFormat.LAS:
                self._scanner = LasScanner(self._file)
                return self._scanner.scan()
            elif file_extension == WellLogFormat.DLIS:
                self._scanner = DLISScanner(self._file)
                return self._scanner.scan()
            else:
                raise ValueError(f"Unsupported file format for file: {self._file}")
        except ValueError as ve:
            print(f"ValueError: {ve}")
            raise
        except FileNotFoundError as fnfe:
            print(f"FileNotFoundError: The specified file was not found: {fnfe}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred while scanning the file: {e}")
            raise