import pandas as pd
import numpy as np
from utils.dlis_utils import summary_dataframe

class DLISParametersProcessor:
    """
    Processes the parameters in a DLIS logical file and handles extraction and transformation.
    """
    def __init__(self, logical_file_id, parameters, nulls_list=None):
        """
        Initialize the DLISParametersProcessor.

        Args:
            logical_file_id (str): Unique identifier for the logical file.
            parameters (list): List of parameter objects in the logical file.
            nulls_list (list, optional): List of null values to remove from the DataFrame. Defaults to None.
        """
        self._logical_file_id = logical_file_id
        self._parameters = parameters
        self._nulls_list = nulls_list or []

    def extract_parameters(self):
        """
        Converts a DLIS parameters DataFrame into the desired JSON-like format.

        Returns:
            dict: Parameter information formatted as specified.
        """
        # Read the parameters into a DataFrame
        param_df = self._extract_parameters_df()

        # Exclude 'name' and 'logical-file-id' from attributes
        attributes = [col for col in param_df.columns if col not in ('name', 'logical-file-id')]

        # Initialize the structure for parameter information
        parameter_info = {
            "attributes": attributes,
            "objects": {}
        }

        # Loop through the DataFrame rows
        for _, row in param_df.iterrows():
            # Extract parameter name and trim spaces
            name = row["name"]
            name = name.strip() if isinstance(name, str) else name

            # Initialize a list to hold attribute values
            attribute_values = []

            for attr in attributes:
                value = row.get(attr)

                # Trim spaces if value is a string
                if isinstance(value, str):
                    value = value.strip()
                # If value is a list or array, process accordingly
                elif isinstance(value, (list, np.ndarray, tuple)):
                    # Trim spaces in each element and join
                    value = ", ".join(map(lambda x: str(x).strip(), value))
                else:
                    value = str(value).strip() if value is not None else None

                # Append the processed value to the attribute_values list
                attribute_values.append(value)

            # Add parameter to the objects section
            parameter_info["objects"][name] = attribute_values

        return parameter_info

    def _extract_parameters_df(self):
        """
        Extracts and processes parameters into a DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing parameter data.
        """
        param_df = summary_dataframe(
            self._parameters, name="name", values="value", long_name="description"
        )

        if param_df.empty:
            print(f"No parameters found for logical file: {self._logical_file_id}")
            return param_df

        try:
            # Add units to the DataFrame
            param_df["unit"] = self._extract_units(param_df)

            # Add logical file ID
            param_df["logical-file-id"] = self._logical_file_id

            # Transform parameter values. Convert 'value' to a hashable type
            param_df["value"] = param_df["value"].apply(
                lambda v:
                v[0] if isinstance(v, (list, np.ndarray)) and len(v) == 1 else
                "; ".join(v) if isinstance(v, (list, np.ndarray)) else
                v
            )

            # Remove rows with null values
            param_df = param_df[~param_df.isin(self._nulls_list).any(axis=1)]

            # Clean and deduplicate the DataFrame
            param_df = param_df.drop_duplicates(ignore_index=True)
            # param_df = param_df.drop_duplicates(subset=["name", "value"], ignore_index=True)

            return param_df

        except Exception as e:
            print(f"Error processing parameters for logical file {self._logical_file_id}: {e}")
            raise

    def _extract_units(self, param_df):
        """
        Extracts units from the parameters and aligns them with the DataFrame index.

        Args:
            param_df (pd.DataFrame): DataFrame containing parameter data.

        Returns:
            list: A list of units corresponding to the parameters.
        """
        units_column = []
        for i, param in enumerate(self._parameters):
            if i not in param_df.index:
                continue
            try:
                units_column.append(param.attic["VALUES"].units)
            except KeyError:
                units_column.append(None)
        return units_column