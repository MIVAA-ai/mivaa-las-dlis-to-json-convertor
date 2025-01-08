from dlisio import dlis
import pandas as pd
import numpy as np
import json
import os.path

class DLISScanner:
    def __init__(self, file):
        self._file = file
        self._logical_files = list()

        first_logical_file, *tail_files = dlis.load(rf'{self._file}')

        self._logical_files.append(first_logical_file)

        for logical_file in tail_files:
            self._logical_files.append(logical_file)
