# import hashlib
# from pathlib import Path
#
# def calculate_checksum_and_size(filepath, algorithm="sha256"):
#     """
#     Calculate the checksum of a file using the specified algorithm and gather file metadata.
#     :param filepath: Path to the file.
#     :param algorithm: Hash algorithm (default: sha256).
#     :return: Dictionary containing checksum, size, creation date, and creation user.
#     """
#     # Initialize the hash function
#     hash_func = hashlib.new(algorithm)
#     filepath = Path(filepath)
#
#     # Calculate checksum and size
#     file_size = 0
#     with open(filepath, "rb") as f:
#         for chunk in iter(lambda: f.read(4096), b""):
#             hash_func.update(chunk)
#             file_size += len(chunk)
#
#     # Return metadata
#     return {
#         "checksum": hash_func.hexdigest(),
#         "file_size": file_size,
#     }

import hashlib
import orjson
from pathlib import Path

def calculate_json_checksum(filepath, algorithm="blake2b"):
    """
    Calculates a stable checksum of a JSON file by normalizing it first.

    Args:
        filepath (str or Path): Path to the JSON file.
        algorithm (str, optional): Hash algorithm (default: 'blake2b').

    Returns:
        dict: Dictionary containing checksum and file size.
    """
    filepath = Path(filepath)
    hash_func = hashlib.new(algorithm)

    with open(filepath, "rb") as f:
        try:
            data = orjson.loads(f.read())  # Load JSON efficiently
        except orjson.JSONDecodeError:
            return {"checksum": None, "file_size": None, "error": "Invalid JSON"}

    # **Normalization step**
    normalized_json = orjson.dumps(data, option=orjson.OPT_SORT_KEYS)

    # Compute the checksum
    hash_func.update(normalized_json)

    return hash_func.hexdigest()

# def get_file_size(filepath):
#     """Returns the file size in bytes using pathlib."""
#     return Path(filepath).stat().st_size