from scanners.scanner import Scanner
from utils.SerialiseJson import JsonSerializable
import json

file_full_path = rf'F:\PyCharmProjects\welllogs-rag\uploads\11_30a-_9Z_dwl_DWL_WIRE_238615014.las'


scanner = Scanner(file_full_path)
normalised_json = scanner.scan()
# print(normalised_json)
# print(JsonSerializable.to_json(normalised_json))

# Replace this with your desired file path
output_file_path = r'F:\PyCharmProjects\welllogs-rag\sample-data\file.json'

# Write the JSON object to the specified file
with open(output_file_path, 'w') as output_file:
    json.dump(JsonSerializable.to_json(normalised_json), output_file, indent=4)

print(f"JSON written to {output_file_path}")