import pandas as pd
import numpy as np

def summary_dataframe(items, **kwargs):
    """
    Converts a list of items into a DataFrame with specified attributes.

    Args:
        items (list): List of items to convert.
        kwargs: Keyword arguments where keys are item attributes and values are column names.

    Returns:
        pd.DataFrame: A DataFrame containing the specified attributes.
    """
    data = {value: [] for value in kwargs.values()}

    for item in items:
        for key, column_name in kwargs.items():
            try:
                data[column_name].append(getattr(item, key, None))
            except Exception as e:
                print(f"Error processing attribute '{key}' for item '{item}': {e}")
                data[column_name].append(None)

    return pd.DataFrame(data)

def extract_metadata(metadata_df):
    """
    Converts a DLIS parameters DataFrame into the desired JSON-like format.

    Args:
        metadata_df (pd.DataFrame): DataFrame containing metadata parameters with columns
                                    like 'name', 'logical-file-id', and additional attributes.

    Returns:
        dict: A dictionary containing parameter information with attributes and objects.
    """

    # Determine the attributes dynamically by excluding 'name' and 'logical-file-id'
    attributes = [col for col in metadata_df.columns if col not in ('name', 'logical-file-id')]

    # Initialize the structure for metadata information
    metadata_info = {
        "attributes": attributes,  # List of attribute names extracted from the DataFrame
        "objects": {}  # Dictionary to hold objects mapped by their 'name'
    }

    # Iterate over each row in the DataFrame
    for _, row in metadata_df.iterrows():
        # Extract and clean the 'name' column (used as the key for objects)
        name = row["name"]
        name = name.strip() if isinstance(name, str) else name

        # Initialize a list to hold attribute values for the current row
        attribute_values = []

        # Process each attribute dynamically
        for attr in attributes:
            value = row.get(attr)  # Get the value for the current attribute

            # If the value is a string, trim spaces
            if isinstance(value, str):
                value = value.strip()
            # If the value is a list, tuple, or array, trim spaces and join as a string
            elif isinstance(value, (list, np.ndarray, tuple)):
                value = ", ".join(map(lambda x: str(x).strip(), value))
            # Convert other types to a trimmed string or set to None if the value is missing
            else:
                value = str(value).strip() if value is not None else None

            # Append the processed value to the attribute values list
            attribute_values.append(value)

        # Add the processed attribute values to the objects dictionary under the 'name' key
        metadata_info["objects"][name] = attribute_values

    # Return the structured metadata information
    return metadata_info

def extract_units(metadata, metadata_df, column_name):
    """
    Extracts units for a specific equipment attribute and aligns them with the DataFrame index.

    Args:
        metadata (list): List of metadata objects containing equipment details.
        metadata_df (pd.DataFrame): DataFrame containing equipment data.
        column_name (str): The attribute name for which units are to be extracted.

    Returns:
        list: A list of units corresponding to the values in the specified column.
    """
    units_column = []  # Initialize a list to store the extracted units

    # Iterate through the metadata objects
    for i, param in enumerate(metadata):
        # Skip entries not present in the DataFrame's index
        if i not in metadata_df.index:
            continue

        try:
            # Extract the unit for the given column from the metadata object
            unit = param.attic[column_name].units
            units_column.append(unit)
        except KeyError:
            # If the unit is not found, append None
            units_column.append(None)
        except AttributeError:
            # Handle cases where the 'attic' or column_name might not exist
            units_column.append(None)

    return units_column  # Return the list of extracted units

# def extract_units(metadata, metadata_df, column_name):
#     """
#     Extracts units for a specific equipment attribute and aligns them with the DataFrame index.
#
#     Args:
#         metadata (list): List of metadata objects containing equipment details.
#         metadata_df (pd.DataFrame): DataFrame containing equipment data.
#         column_name (str): The attribute name for which units are to be extracted.
#
#     Returns:
#         list: A list of units corresponding to the values in the specified column.
#     """
#     units_column = []  # Initialize a list to store the extracted units
#
#     # Iterate through the metadata objects
#     for i, param in enumerate(metadata):
#         # Skip entries not present in the DataFrame's index
#         if i not in metadata_df.index:
#             continue
#
#         try:
#             # Extract the unit for the given column from the metadata object
#             unit = param.attic[column_name].units
#
#             # Attempt to decode the unit if it's in bytes format
#             if isinstance(unit, bytes):
#                 try:
#                     unit = unit.decode('utf-8').strip()
#                 except UnicodeDecodeError:
#                     # Fallback for undecodable bytes
#                     unit = None
#
#             units_column.append(unit)
#         except KeyError:
#             # If the unit is not found, append None
#             units_column.append(None)
#         except AttributeError:
#             # Handle cases where the 'attic' or column_name might not exist
#             units_column.append(None)
#
#     return units_column  # Return the list of extracted units