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
    Converts a DLIS parameters DataFrame into the desired JSON-like format with dynamic type handling.

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
        "attributes": attributes,
        "objects": {}
    }

    # Process each row in the DataFrame
    for _, row in metadata_df.iterrows():
        name = row["name"].strip() if isinstance(row["name"], str) else row["name"]
        attribute_values = [parse_value(row[attr]) for attr in attributes]
        metadata_info["objects"][name] = attribute_values

    return metadata_info


def extract_relationships(metadata_df, column_name):
    """
    Extracts relationships (e.g., channels) from the specified column in the metadata DataFrame
    and transposes the data to match the number of rows in the DataFrame.

    Args:
        metadata_df (pd.DataFrame): DataFrame containing frames and their attributes.
        column_name (str): The name of the column containing the channels information.

    Returns:
        pd.Series: A Series with lists of channels for each row, aligned with metadata_df rows.
    """
    # Ensure the column exists in the DataFrame
    if column_name not in metadata_df.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

    # Initialize a list to store related data per row
    related_data_per_row = []

    # Iterate through the metadata DataFrame
    for _, row in metadata_df.iterrows():
        try:
            # Extract related data for the current row
            related_data = []
            for item in row[column_name]:
                related_data.append(item.name)

            # Append the extracted data for this row
            related_data_per_row.append(related_data)
        except Exception as e:
            print(f"Error processing row to extract {column_name}, {row.name}: {e}")
            related_data_per_row.append([])  # Add an empty list for rows with errors

    # Return a Series aligned with the metadata DataFrame
    return pd.Series(related_data_per_row, index=metadata_df.index)

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

def parse_value(value):
    """
    Parses a value dynamically based on its data type.

    Args:
        value (Any): The value to parse.

    Returns:
        Any: Parsed value in the appropriate data type.
    """
    try:
        # Handle boolean values
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in ["true", "false"]:
            return value.lower() == "true"

        # Handle float values
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
            return float(value)

        # Handle list, tuple, or array
        if isinstance(value, (list, np.ndarray, tuple)):
            return [parse_value(item) for item in value]  # Recursively parse list elements

        # Fallback to string
        return str(value).strip() if value is not None else None
    except Exception:
        return str(value).strip() if value is not None else None