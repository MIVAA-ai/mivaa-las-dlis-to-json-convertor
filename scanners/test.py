from scanners.scanner import Scanner
from utils.SerialiseJson import JsonSerializable
import json
from dlisio import dlis

import hashlib
from pathlib import Path
from dlisio import dlis
from worker.tasks import convert_dlis_to_json_task

import hashlib
from pathlib import Path

#pending work to be done before release
# change the checksum logic to calculate on output files
# change the print to logger


file_full_path = rf'F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\uploads\DLIS_HESP.41383.dlis'
# file_full_path = rf'F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\uploads\11_30a-_9Z_dwl_DWL_WIRE_238615014.las'

print(f"Identified as DLIS: {file_full_path}. Extracting logical files for scanning")
logical_files = dlis.load(file_full_path)
print(f"Loaded {len(logical_files)} logical files from DLIS {file_full_path}")

for logical_file in logical_files:
    result = convert_dlis_to_json_task(file_full_path, "F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\processed", logical_file)
    print(f"Task submitted for logical file {logical_file.fileheader.id} in DLIS file {file_full_path}, Task ID: {result}")

# logical_files = dlis.load(file_full_path)
#
# for logical_file in logical_files:
#     print(logical_file.fileheader.id)
#     print(calculate_checksum_and_size(file=logical_file))

# scanner = Scanner(file_full_path)
# normalised_json = scanner.scan()
#
# json_data = JsonSerializable.to_json(normalised_json)
# if isinstance(json_data, str):
#     json_data = json.loads(json_data)
#
# # Replace this with your desired file path
# output_file_path = r'F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\processed\file.json'
#
# # Write the JSON object to the specified file
# with open(output_file_path, 'w') as output_file:
#     json.dump(json_data, output_file, indent=4)
#
# print(f"JSON written to {output_file_path}")