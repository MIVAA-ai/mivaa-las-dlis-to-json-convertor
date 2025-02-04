from dlisio import dlis

import hashlib
from pathlib import Path

from mappings.WellLogsFormat import WellLogFormat
from scanners.dlis_scanner import DLISScanner
from scanners.las_scanner import LasScanner
from worker.tasks import convert_to_json_task

#pending work to be done before release
# change the checksum logic to calculate on output files
# change the print to logger
# add traceback to exceptions - done

# file_full_path = rf'F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\samples\11_30a-_9Z_dwl_DWL_WIRE_238615014.las'
#
# result = convert_to_json_task(filepath=file_full_path,
#                               output_folder="F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\processed",
#                               scanner_cls=LasScanner,
#                               file_format=WellLogFormat.LAS)
# print(f"Task submitted for las file {file_full_path}, Task ID: {result}")

file_full_path = rf'F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\samples\595304_34281027861_1.dlis'

print(f"Identified as DLIS: {file_full_path}. Extracting logical files for scanning")
logical_files = dlis.load(file_full_path)
print(f"Loaded {len(logical_files)} logical files from DLIS {file_full_path}")

for logical_file in logical_files:
    result = convert_to_json_task(filepath=file_full_path,
                                  output_folder="F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\processed",
                                  scanner_cls=DLISScanner,
                                  file_format=WellLogFormat.DLIS,
                                  logical_file=logical_file)
    print(f"Task submitted for logical file {logical_file.fileheader.id} in DLIS file {file_full_path}, Task ID: {result}")
