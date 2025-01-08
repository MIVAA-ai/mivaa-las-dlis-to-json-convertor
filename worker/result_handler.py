import os
import csv
from .celeryconfig import csv_path
from .celeryconfig import header_file_path
from . import app
import json
import tempfile

def load_headers():
    """
    Load headers from the header file.
    Creates an empty list if the file does not exist.
    """
    if os.path.exists(header_file_path):
        with open(header_file_path, "r") as file:
            return json.load(file)  # Return as a list
    return []  # Return an empty list if the file does not exist

def save_headers(headers):
    """
    Save headers to the header file.
    """
    with open(header_file_path, "w") as file:
        json.dump(headers, file)  # Save headers as a list

def append_row_to_csv(row, global_headers):
    """
    Append a row to the CSV file without rewriting the entire file.
    If new headers are added, the file header is updated.
    """
    # Check if the file exists
    file_exists = os.path.exists(csv_path)

    # If the file exists, ensure headers are updated
    if file_exists:
        with open(csv_path, "r", newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            current_headers = reader.fieldnames or []

            # If new headers are found, rewrite the header only
            if set(global_headers) != set(current_headers):
                rewrite_csv_headers(global_headers)

    # Append the row to the CSV file
    with open(csv_path, mode="a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=global_headers)
        writer.writerow(row)

def rewrite_csv_headers(global_headers):
    """
    Rewrite only the headers of the CSV file without rewriting rows.
    """
    # Read existing rows
    rows = []
    if os.path.exists(csv_path):
        with open(csv_path, "r", newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            rows = list(reader)

    # Write back rows with updated headers
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=global_headers)
        writer.writeheader()
        writer.writerows(rows)

def update_csv(result, json_data=None):
    """
    Update the CSV file dynamically based on the result and json_data.
    :param result: Metadata about the LAS to JSON conversion.
    :param json_data: Full JSON data structure including headers, parameters, curves, and data (optional).
    """
    # Load existing headers
    global_headers = load_headers()

    # Extract dynamic headers from JSON data
    header = {}
    curve_names = []

    if json_data:
        if isinstance(json_data, list) and len(json_data) > 0:
            header = json_data[0].get("header", {})
            curves = json_data[0].get("curves", [])
            curve_names = [curve.get("name", "Unknown") for curve in curves]
        else:
            print(f"Unexpected json_data format: {type(json_data)}")
            header = {}
            curve_names = []

    # Add curve names to the result
    result["Curve Names"] = ", ".join(curve_names) if curve_names else "None"

    # Merge result and dynamic headers
    row = {**result, **header}

    # Update global headers while preserving their order
    for header in row.keys():
        if header not in global_headers:
            global_headers.append(header)

    # Save the updated headers
    save_headers(global_headers)

    # Append the row to the CSV file
    append_row_to_csv(row, global_headers)

# def write_to_csv(row, global_headers):
#     """
#     Write a single row to the CSV file, ensuring consistent column order
#     while adding new headers as columns at the end of the file.
#     """
#     # Align the row with the current headers
#     aligned_row = {header: row.get(header, None) for header in global_headers}
#
#     # Check if the CSV file already exists
#     file_exists = os.path.exists(csv_path)
#
#     # Read existing rows if the file exists
#     existing_rows = []
#     if file_exists:
#         with open(csv_path, mode="r", newline="", encoding="utf-8") as csv_file:
#             reader = csv.DictReader(csv_file)
#             existing_rows = list(reader)
#
#     # Write the data to the CSV file with updated headers
#     with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
#         writer = csv.DictWriter(csv_file, fieldnames=global_headers)
#
#         # Write the header
#         writer.writeheader()
#
#         # Rewrite existing rows with updated headers
#         for existing_row in existing_rows:
#             aligned_existing_row = {header: existing_row.get(header, None) for header in global_headers}
#             writer.writerow(aligned_existing_row)
#
#         # Write the new row
#         writer.writerow(aligned_row)
#
#     # Debugging output for aligned row
#     print("Aligned Row Written to CSV:")
#     print(json.dumps(aligned_row, indent=4))
#
#     # Temporary file for debugging aligned rows
#     temp_file_path = os.path.join(tempfile.gettempdir(), "aligned_rows_debug.txt")
#
#     # Append the aligned row to the temporary file (debugging purpose)
#     with open(temp_file_path, mode="a", encoding="utf-8") as temp_file:
#         temp_file.write(json.dumps(aligned_row, indent=4) + "\n")
#
#     print(f"Aligned row appended to temporary file: {temp_file_path}")

# def write_to_csv(row, global_headers):
#     """
#     Write a single row to the CSV file, ensuring consistent column order
#     while preserving the order in which headers are encountered.
#     """
#     # Align the row with the current headers
#     aligned_row = {header: row.get(header, None) for header in global_headers}
#
#     # Temporary file for debugging aligned rows
#     temp_file_path = os.path.join(tempfile.gettempdir(), "aligned_rows_debug.txt")
#
#     # Append the aligned row to the temporary file (debugging purpose)
#     with open(temp_file_path, mode="a", encoding="utf-8") as temp_file:
#         temp_file.write(json.dumps(aligned_row, indent=4) + "\n")
#
#     print(f"Aligned row appended to temporary file: {temp_file_path}")
#
#     # Debugging output for aligned row
#     print("Aligned Row:")
#     print(json.dumps(aligned_row, indent=4))
#
#     # Check if the CSV file already exists
#     file_exists = os.path.exists(csv_path)
#
#     # Write the data to the CSV file
#     with open(csv_path, mode="a", newline="", encoding="utf-8") as csv_file:
#         writer = csv.DictWriter(csv_file, fieldnames=global_headers)
#
#         # Write the header only if the file is new
#         if not file_exists:
#             writer.writeheader()
#
#         # Write the aligned row
#         writer.writerow(aligned_row)


@app.task(bind=True)
def handle_task_completion(self, result, json_data=None, initial_task_id=None):
    """
    Handle the completion of a task by updating the CSV file.
    This function is chained to run after `convert_las_to_json_task`.
    """
    try:
        # Ensure result is a dictionary
        if not isinstance(result, dict):
            raise ValueError(f"Expected result to be a dict, got {type(result).__name__}")

        # Combine initial task ID with the current task ID
        combined_task_ids = f"{initial_task_id}, {self.request.id}"
        result["task_id"] = combined_task_ids

        # Update the CSV file
        update_csv(result, json_data)
        print(f"CSV updated with task result: {result}")

        # Return a meaningful status
        return f"CSV updated for file: {result['file_name']}"
    except Exception as e:
        print(f"Error updating CSV: {e}")
        return f"Error updating CSV for file: {result.get('file_name', 'Unknown')}"