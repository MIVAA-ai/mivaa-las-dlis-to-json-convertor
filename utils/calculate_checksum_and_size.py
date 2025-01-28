import hashlib
from pathlib import Path

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