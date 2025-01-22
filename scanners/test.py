from scanners.scanner import Scanner
from utils.SerialiseJson import JsonSerializable
import json

file_full_path = rf'F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\uploads\KAWANG-1_8_5_RCI-GR_2-AUG-2009 (1).DLIS'
# file_full_path = rf'F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\uploads\11_30a-_9Z_dwl_DWL_WIRE_238615014.las'


scanner = Scanner(file_full_path)
normalised_json = scanner.scan()

json_data = JsonSerializable.to_json(normalised_json)
if isinstance(json_data, str):
    json_data = json.loads(json_data)

# Replace this with your desired file path
output_file_path = r'F:\PyCharmProjects\mivaa-las-dlis-to-json-convertor\processed\file.json'

# Write the JSON object to the specified file
with open(output_file_path, 'w') as output_file:
    json.dump(json_data, output_file, indent=4)

print(f"JSON written to {output_file_path}")