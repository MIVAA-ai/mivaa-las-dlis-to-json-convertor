# tasks.py
from . import app
from scanners.scanner import Scanner
from utils.SerialiseJson import JsonSerializable
import json
from worker.result_handler import handle_task_completion
from celery import chain
import datetime
import hashlib
import os
from pathlib import Path
from datetime import datetime

def get_file_creation_time(filepath):
    """
    Get the file creation time in a cross-platform manner.
    :param filepath: Path to the file.
    :return: File creation time as a formatted string.
    """
    filepath = Path(filepath)
    try:
        if os.name == 'nt':  # For Windows
            creation_time = filepath.stat().st_ctime
        else:  # For Unix-like systems
            # Use st_birthtime on systems like macOS; fallback to st_ctime
            creation_time = getattr(filepath.stat(), 'st_birthtime', filepath.stat().st_ctime)

        # Convert timestamp to human-readable format
        return datetime.fromtimestamp(creation_time).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error fetching file creation time: {e}")
        return "Unknown"

def calculate_checksum_and_size(filepath, algorithm="sha256"):
    """
    Calculate the checksum of a file using the specified algorithm and gather file metadata.
    :param filepath: Path to the file.
    :param algorithm: Hash algorithm (default: sha256).
    :return: Dictionary containing checksum, size, creation date, and creation user.
    """
    # Initialize the hash function
    hash_func = hashlib.new(algorithm)
    filepath = Path(filepath)

    # Calculate checksum and size
    file_size = 0
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)
            file_size += len(chunk)

    # Return metadata
    return {
        "checksum": hash_func.hexdigest(),
        "file_size": file_size,
    }


@app.task(bind=True)
def convert_las_to_json_task(self, filepath, output_folder):
    """
    Task to convert a LAS file to JSONWellLogFormat.
    """
    filepath = Path(filepath).resolve()
    output_folder = Path(output_folder).resolve()

    # Initialize result with default structure
    result = {
        "status": "ERROR",
        "task_id": self.request.id,  # Capture the Celery Task ID
        "file_name": filepath.name if filepath else "Unknown",
        "input_file_path": str(filepath) if filepath else "Unknown",
        "input_file_size": "Unknown",
        "input_file_creation_date": "Unknown",
        "input_file_creation_user": "Unknown",
        "file_checksum": "Unknown",
        "output_file": "Unknown",
        "output_file_size": "Unknown",
        "message": "An error occurred during processing.",
    }

    try:
        # Fetch file metadata
        print(f"Fetching metadata for {filepath}...")
        creation_time = get_file_creation_time(filepath)
        metadata = calculate_checksum_and_size(filepath)

        # Parse the LAS file
        print(f"Scanning LAS file: {filepath}...")
        scanner = Scanner(filepath)
        normalised_json = scanner.scan()

        # Serialize JSON data
        print(f"Serializing scanned data from {filepath}...")
        json_data = JsonSerializable.to_json(normalised_json)
        if isinstance(json_data, str):
            json_data = json.loads(json_data)

        # Save JSON to file
        filename = filepath.stem + ".json"
        output_path = output_folder / filename
        print(f"Saving JSON data to {output_path}...")
        with open(output_path, "w") as json_file:
            json.dump(json_data, json_file, indent=4)

        # Update result for success
        result.update({
            "status": "SUCCESS",
            "input_file_size": metadata["file_size"],
            "input_file_creation_date": creation_time,
            "file_checksum": metadata["checksum"],
            "output_file": str(output_path),
            "output_file_size": os.path.getsize(output_path) if output_path.exists() else "N/A",
            "message": f"File processed successfully: {filepath}",
        })

        print(f"Task completed successfully: {result}")
        # Chain handle_task_completion
        chain(handle_task_completion.s(result, json_data, self.request.id)).apply_async()
        return result

    except Exception as e:
        # Populate metadata in case of error
        try:
            result.update({
                "input_file_size": os.path.getsize(filepath) if filepath.exists() else "Unknown",
                "input_file_creation_date": get_file_creation_time(filepath),
                "file_checksum": calculate_checksum_and_size(filepath).get("checksum", "Unknown"),
            })
        except Exception as meta_error:
            print(f"Error fetching metadata during exception handling: {meta_error}")

        # Set the error message
        result["message"] = f"Error processing LAS file: {str(e)}"

        print(f"Error processing LAS file: {e}")
        # Chain handle_task_completion for error handling
        chain(handle_task_completion.s(result, None, self.request.id)).apply_async()
        return result