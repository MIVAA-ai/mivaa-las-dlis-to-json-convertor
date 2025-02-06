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

file_full_path = rf'F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\uploads\2_4-B-2_CT4__PETROPHYSICS__WLC_PETROPHYSICAL_COMPOSITE_1.DLIS'

print(f"Identified as DLIS: {file_full_path}. Extracting logical files for scanning")
logical_files = dlis.load(file_full_path)
print(f"Loaded {len(logical_files)} logical files from DLIS {file_full_path}")

for logical_file in logical_files:
    # print(str(logical_file.fileheader.id))
    #how extactly does this github copilot work ? can i cha
    # print(None)
    try:
        logical_file_id = str(logical_file.fileheader.id)
    except Exception as e:
        print(f"Error accessing logical file header in {file_full_path}: {e}")
        continue  # Skip this logical file but continue processing others

    print(f'this executed for {logical_file_id}')
    result = convert_to_json_task(filepath=file_full_path,
                                  output_folder="F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\processed",
                                  file_format=WellLogFormat.DLIS.value,
                                  logical_file_id=str(logical_file.fileheader.id))
    print(f"Task submitted for logical file {logical_file.fileheader.id} in DLIS file {file_full_path}, Task ID: {result}")
