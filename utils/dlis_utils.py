import pandas as pd
import numpy as np
import json

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

def process_relationship_cell(cell_value):
    """
    Processes a single cell value to extract related data (channel names).

    Args:
        cell_value: The value of a cell from the specified column.

    Returns:
        list: A list of extracted channel names or an empty list if no names are found.
    """
    # Return an empty list for None or NaN
    if cell_value is None:
        return []

    # Handle list-like objects (e.g., lists, numpy arrays)
    if isinstance(cell_value, (list, np.ndarray)):
        return [item.name for item in cell_value if hasattr(item, "name")]

    # Handle single objects with a `name` attribute
    if hasattr(cell_value, "name"):
        return [cell_value.name]

    # Fallback for unexpected types
    print(f"Unexpected type: {type(cell_value)}")
    return []

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

    # Apply the `process_cell` function to the specified column
    return metadata_df[column_name].apply(process_relationship_cell)

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

def process_dataframe_lists(df):
    """
    Detects columns with lists, serializes them for hashability, deduplicates the DataFrame,
    and deserializes them back into lists.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The processed DataFrame with lists preserved after deduplication.
    """
    # Detect columns with lists
    list_columns = [
        col for col in df.columns
        if df[col].apply(lambda x: isinstance(x, (list, np.ndarray, tuple))).any()
    ]

    if not list_columns:
        print("No list columns detected. Returning original DataFrame.")
        return df

    # Create a copy of the DataFrame to avoid potential SettingWithCopyWarning
    df = df.copy()

    # Serialize lists into JSON strings
    for col in list_columns:
        df.loc[:, col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, np.ndarray, tuple)) else x)

    # Deduplicate the DataFrame
    df = df.drop_duplicates(subset=["name"], ignore_index=True)

    # Deserialize JSON strings back into lists
    for col in list_columns:
        df.loc[:, col] = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) and x.startswith('[') else x)

    return df

def transform_curves_to_json_well_log_format(curves_data):
    """
    Transforms DLIS curves data into the JSON Well Log format.

    Args:
        curves_data (dict): Dictionary containing curves information in the original format.

    Returns:
        list: A list of dictionaries representing curves in the JSON Well Log format.
    """
    transformed_curves = []

    # Iterate over each curve in the original data
    for curve_name, curve_details in curves_data["objects"].items():
        # Ensure curve name is present and non-null
        if not curve_name:
            print("Skipping curve with missing or null name.")
            continue

        # Extract and transform curve information
        curve_data = {
            "name": curve_name,  # Curve name or mnemonic (mandatory)
            "description": curve_details[0] if len(curve_details) > 0 else None,  # Description (optional)
            "quantity": None,  # Assuming quantity is not present in the input data
            "unit": curve_details[2] if len(curve_details) > 2 else None,  # Unit of measurement (optional)
            "valueType": "float",  # Default to "float" (optional)
            "dimensions": len(curve_details[4]) if len(curve_details) > 4 and isinstance(curve_details[4], list) else 1,  # Dimensions, default to 1 (optional)
            "axis": curve_details[4] if len(curve_details) > 4 and isinstance(curve_details[4], list) else [],  # Axis (optional)
            "maxSize": 20 if curve_details[5] is None else curve_details[5]  # Default to 20 if not provided
        }

        # Adjust maxSize for non-string value types
        if curve_data["valueType"] != "string":
            curve_data.pop("maxSize", None)

        # Append the transformed curve to the list
        transformed_curves.append(curve_data)

    return transformed_curves